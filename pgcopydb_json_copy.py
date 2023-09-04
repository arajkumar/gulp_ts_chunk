import sys
import json

class Statement:
    def __init__(self, line):
        self.line = line
        self.table = f'"{line["schema"]}"."{line["table"]}"'

    def _columns(self):
        return map(lambda x: f'"{x["name"]}"', self.line["columns"])

    def copy_begin(self):
        columns = map(lambda x: f'"{x["name"]}"', self.line["columns"])
        columns = ', '.join(columns)
        return f"COPY {self.table}({columns}) FROM stdin;"

    def copy_end():
        return "\\."

    def copy_body(self):
        def _value(x):
            v = x["value"]
            if v is None:
                return '\\N'
            else:
                return str(v)
        row_values = map(_value, self.line["columns"])
        # copy columns are separated by \t
        return '\t'.join(row_values)

def read_ndjson(lines):
    for line in lines:
        try:
            data = json.loads(line.strip())
            yield data
        except json.JSONDecodeError as e:
            print(f"json decoder error {e}", file=sys.stderr)

def transform_insert_to_copy(lines):
    copy_inprogress_table = ''
    for line in lines:
        action = line["action"]
        if action == "B":
            yield "BEGIN;"
        elif action == "C":
            if copy_inprogress_table:
                copy_inprogress_table = ''
                yield Statement.copy_end()

            yield "COMMIT;"
        elif action == "I":
            insert = Statement(line["message"])
            if copy_inprogress_table != insert.table:
                if copy_inprogress_table != '':
                    yield Statement.copy_end()
                yield insert.copy_begin()
                yield insert.copy_body()
                copy_inprogress_table = insert.table
            else:
                yield insert.copy_body()

def write(output, lines):
    for l in lines:
        output.write(l)
        output.write('\n')
        output.flush()


def transform(lines):
    yield from transform_insert_to_copy(lines)

if __name__ == '__main__':
    lines = read_ndjson(sys.stdin)
    out_lines = transform(lines)
    write(sys.stdout, out_lines)
