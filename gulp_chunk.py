import sys
import csv
import json

from collections import namedtuple

Hypertable = namedtuple('Hypertable', ['schema', 'table'])

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


def gulp_timescale_internal(chunk_map, data):
    # begin
    action = data["action"]
    if action in ["D", "T", "I", "U"]:
        message = data["message"]
        if message["schema"] == "_timescaledb_catalog":
            pass
        elif message["schema"] == "_timescaledb_internal":
            chunk_name = message["table"]
            ht_id = _get_ht_id(chunk_name)
            ht = chunk_map.get(ht_id)
            if ht is not None:
                message["schema"] = ht.schema
                message["table"] = ht.table
                yield data
            else:
                print(f"Missing hypertable mapping for {ht_id}", file=sys.stderr)
        else:
            yield data
    else:
        yield data

def write_ndjson(output, lines):
    for l in lines:
        json_line = json.dumps(l)
        output.write(json_line + '\n')
        output.flush()


def gulp_timescale_chunks(chunk_map, lines):
    for line in lines:
        yield from gulp_timescale_internal(chunk_map, line)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Missing chunk to hypertable mapping file")
        print("You can use the following command to generate it:")
        print("""psql $CONNECTION_STR --csv -c "SELECT id, schema_name, table_name FROM _timescaledb_catalog.hypertable" > htmap.csv """)
        sys.exit(-1)
    chunk_to_ht = {}
    with open(sys.argv[1], "r") as f:
        for l in csv.DictReader(f):
            chunk_to_ht[l["id"]] = Hypertable(schema=l["schema_name"], table=l["table_name"])
    lines = read_ndjson(sys.stdin)
    out_lines = gulp_timescale_chunks(chunk_to_ht, lines)
    write_ndjson(sys.stdout, out_lines)
