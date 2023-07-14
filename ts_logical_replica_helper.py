import sys
import json
import argparse
import psycopg2
import time
from psycopg2.pool import SimpleConnectionPool
from collections import namedtuple

Chunk = namedtuple('Chunk', ['schema', 'table'])
Hyperspace = namedtuple('Hyperspace', ['name', 'chunk_schema', 'chunk_table', 'slices'])

class getcursor:
    def __init__(self, pool):
        self.pool = pool
    def __enter__(self):
        self.conn = self.pool.getconn()
        return self.conn
    def __exit__(self, *args):
        self.pool.putconn(self.conn)

class LogicalReplicaHelper:
    def __init__(self, source_conn, target_conn, snapshot, subscription_name):
        self.snapshot = snapshot
        self.subscription_name = subscription_name
        self.source_pool = SimpleConnectionPool(1, 5, dsn = source_conn)
        self.target_pool = SimpleConnectionPool(1, 5, dsn = target_conn)


    def list_chunks_in_source(self):
        with getcursor(self.source_pool) as conn:
            with conn.cursor() as c:
                if self.snapshot:
                    c.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                    c.execute(f"SET TRANSACTION SNAPSHOT '{self.snapshot}'")
                c.execute("SELECT schema_name, table_name FROM _timescaledb_catalog.chunk")
                result = c.fetchall()
                for r in result:
                    yield Chunk(r[0], r[1])

    def show_chunk_in_source(self, chunk, use_snapshot=False):
        with getcursor(self.source_pool) as conn:
            with conn.cursor() as c:
                if use_snapshot and self.snapshot:
                    c.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                    c.execute(f"SET TRANSACTION SNAPSHOT '{self.snapshot}'")
                query = f"SELECT CONCAT(h.schema_name, '.', h.table_name) as hypertable_name, c.schema_name, c.table_name, c.slices FROM _timescaledb_internal.show_chunk('{chunk.schema}.{chunk.table}') c JOIN _timescaledb_catalog.hypertable h ON(c.hypertable_id=h.id) "
                # print(query)
                c.execute(query)
                result = c.fetchone()
                return Hyperspace(result[0], result[1], result[2], json.dumps(result[3]))

    def create_chunk_in_target(self, hyperspace):
        with getcursor(self.target_pool) as conn:
            with conn.cursor() as c:
                query = f"SELECT _timescaledb_internal.create_chunk('{hyperspace.name}', '{hyperspace.slices}', '{hyperspace.chunk_schema}', '{hyperspace.chunk_table}')"
                # print(query)
                c.execute(query)
                conn.commit()
        with getcursor(self.target_pool) as conn:
            conn.set_session(autocommit=True)
            with conn.cursor() as c:
                try:
                    c.execute(f"ALTER SUBSCRIPTION {self.subscription_name} REFRESH PUBLICATION")
                except Exception as e:
                    print(e)

    def create_chunks_from_source(self, chunks):
        for chunk in chunks:
            print(chunk)
            chunk_info = self.show_chunk_in_source(chunk)
            self.create_chunk_in_target(chunk_info)

    def drop_chunk_in_target(self, chunk):
        # drop chunk
        pass

def read_ndjson(lines):
    for line in lines:
        try:
            data = json.loads(line.strip())
            yield data
        except json.JSONDecodeError as e:
            print(f"json decoder error {e} {line}", file=sys.stderr)

def _as_column_map(columns):
    column_map = {}
    for col in columns:
        column_map[col["name"]] = col["value"]
    return column_map

def _main():
# Define the command-line arguments
    parser = argparse.ArgumentParser(description='Python program to connect to source and target PostgreSQL databases')
    parser.add_argument('--snapshot', type=str, help='Flag to indicate snapshot mode')
    parser.add_argument('--subscription', type=str, help='Flag to indicate subscription name')
    parser.add_argument('source_conn_dsn', type=str, help='Connection string for the source database')
    parser.add_argument('target_conn_dsn', type=str, help='Connection string for the target database')
    args = parser.parse_args()

    logical = LogicalReplicaHelper(args.source_conn_dsn, args.target_conn_dsn, args.snapshot, args.subscription)
    logical.create_chunks_from_source(logical.list_chunks_in_source())
    lines = read_ndjson(sys.stdin)
    for line in lines:
        action = line["action"]
        if action not in ["I", "D", "T"]:
            continue
        message = line["message"]
        if not (message["schema"] == "_timescaledb_catalog" and message["table"] == "chunk"):
            continue
        if action == "I":
            chunk = _as_column_map(message["columns"])
            chunk = Chunk(chunk["schema_name"], chunk["table_name"])
            logical.create_chunks_from_source([chunk])
        elif action == "D":
            pass
        elif action == "T":
            pass
        else :
            raise Exception("Unsupported operation")

if __name__ == '__main__':
    _main()
