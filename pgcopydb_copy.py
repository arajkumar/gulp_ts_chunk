import sys
import json

def insert_statement_to_copy(insert):
    # PREPARE b6175cf4 AS INSERT INTO "public"."tags" ("id", "name", "fleet", "driver", "model", "device_version", "load_capacity", "fuel_capacity", "nominal_fuel_consumption") overriding system value VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);
    tokens = insert.split('(')
    insert_prefix = tokens[0].split(' ')
    table_name = insert_prefix[5]
    column_tokens = tokens[1].split(')')
    columns = column_tokens[0]
    return f"COPY {table_name}({columns}) FROM stdin;"

def execute_to_copy_row(row):
    # EXECUTE b6175cf4["592603","truck_0","East","Rodney","F-150","v1.5","2000","200","15"];
    row = row[16:][:-2]
    data = json.loads(row)
    # replace None with \N, as per COPY format \N represents
    # NULL
    data = map(lambda n: '\\N' if n is None else n, data)
    # copy columns are separated by \t
    return '\t'.join(data)


def transform_insert_to_copy(lines):
    copy_inprogress = False
    for line in lines:
        prefix = line[:8].strip()
        dml = line[20:26]
        if prefix == 'PREPARE' and dml == 'INSERT':
            if not copy_inprogress:
                copy_inprogress = True
                yield insert_statement_to_copy(line)
        elif prefix == 'EXECUTE' and copy_inprogress:
            yield execute_to_copy_row(line)
        else:
            if copy_inprogress:
                yield '\.'
                copy_inprogress = False
            yield line

def write(output, lines):
    for l in lines:
        output.write(l)
        output.write('\n')
        output.flush()


def transform(lines):
    yield from transform_insert_to_copy(lines)

if __name__ == '__main__':
    out_lines = transform(sys.stdin)
    write(sys.stdout, out_lines)
