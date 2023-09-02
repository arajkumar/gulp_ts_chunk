import sys
import csv
import json

from dataclasses import dataclass

from collections import namedtuple

Hypertable = namedtuple('Hypertable', ['schema', 'table'])

@dataclass
class State:
    copy_inprogress: bool = False

def read_ndjson(lines):
    for line in lines:
        try:
            data = json.loads(line.strip())
            yield data
        except json.JSONDecodeError as e:
            print(f"json decoder error {e}", file=sys.stderr)

def _get_ht_id(chunk_name):
    parts = chunk_name.split("_")
    assert len(parts) > 2
    return parts[2]


def insert_statement_to_copy(insert):
    tokens = insert.split('(')
    insert_prefix = tokens[0].split(' ')
    table_name = insert_prefix[5]
    column_tokens = tokens[1].split(')')
    columns = column_tokens[0]
    return f"COPY {table_name}({columns}) FROM stdin;"

def execute_to_copy_row(row):
    row = row[16:][:-2]
    data = json.loads(row)
    data = map(lambda n: '\\N' if n is None else n, data)
    return '\t'.join(data)


def transform_insert_to_copy(chunk_map, data, state):
    # PREPARE b6175cf4 AS INSERT INTO "public"."tags" ("id", "name", "fleet", "driver", "model", "device_version", "load_capacity", "fuel_capacity", "nominal_fuel_consumption") overriding system value VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);
    # EXECUTE b6175cf4["592603","truck_0","East","Rodney","F-150","v1.5","2000","200","15"];
    prefix = data[:8].strip()
    dml = data[20:26]
    if prefix == 'PREPARE' and dml == 'INSERT':
        if not state.copy_inprogress:
            state.copy_inprogress = True
            yield insert_statement_to_copy(data)
    elif prefix == 'EXECUTE' and state.copy_inprogress:
        yield execute_to_copy_row(data)
    else:
        if state.copy_inprogress:
            yield '\.'
        state.copy_inprogress = False
        yield data

def write_ndjson(output, lines):
    for l in lines:
        output.write(l + '\n')
        output.flush()


def gulp_timescale_chunks(chunk_map, lines):
    state = State()
    for line in lines:
        yield from transform_insert_to_copy(chunk_map, line, state)

if __name__ == '__main__':
    out_lines = gulp_timescale_chunks({}, sys.stdin)
    write_ndjson(sys.stdout, out_lines)
