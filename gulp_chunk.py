import sys
import json

def read_ndjson(lines):
    for line in lines:
        try:
            data = json.loads(line.strip())
            yield data
        except json.JSONDecodeError as e:
            print(f"json decoder error {e}", file=sys.stderr)

def _as_column_map(columns):
    column_map = {}
    for col in columns:
        column_map[col["name"]] = col["value"]
    return column_map

def gulp_timescale_chunks(lines):
    for line in lines:
        action = line["action"]
        if action not in ["I", "D", "T"] and message["schema"] != "_timescaledb_catalog" and message["table"] != "chunk":
            continue
        if action == "I":
            chunk = _as_column_map(message["columns"])
            create_chunk_in_target(chunk)
        elif action == "D":
            pass
        elif action == "T":
            pass
        else :
            raise Exception("Unsupported operation")

if __name__ == '__main__':
    lines = read_ndjson(sys.stdin)
    out_lines = gulp_timescale_chunks(lines)
