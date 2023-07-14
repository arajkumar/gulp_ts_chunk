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
    def __exit__(self):
        self.pool.putconn(self.conn)

class LogicalReplicaHelper:
    def __init__(self, source_conn, target_conn, snapshot, subscription_name):
        self.snapshot = snapshot
        self.subscription_name = subscription_name
        self.source_pool = SimpleConnectionPool(1, 5, dsn = source_conn)
        self.target_pool = SimpleConnectionPool(1, 5, dsn = target_conn)


    def list_chunks_in_source(self):
        with getcursor(self.source_pool) as cur:
            with cur as c:
                c.execute(f"SET TRANSACTION SNAPSHOT {self.snapshot_cursor}")
                c.execute('SELECT schema_name, table_name FROM _timescaledb_catalog.chunk')
                result = c.fetchall()
                for r in result:
                    yield Chunk(r[0], r[1])

    def show_chunk_in_source(self, chunk, use_snapshot=False):
        with getcursor(self.source_pool) as cur:
            with cur as c:
                if use_snapshot:
                    c.execute(f"SET TRANSACTION SNAPSHOT {self.snapshot_cursor}")
                c.execute(f"SELECT CONCAT(h.schema_name, '.', h.table_name) as hypertable_name, c.schema_name, c.table_name, c.slices FROM _timescale_internal.show_chunk('{chunk.schema}.{chunk.name}') c JOIN _timescaledb_catalog.hypertable h ON(c.hypertable_id=h.id) ")
                result = c.fetchone()
                return Hyperspace(result[0], result[1], result[2], result[3])

    def create_chunk_in_target(self, hyperspace):
        with getcursor(self.target_pool) as cur:
            with cur as c:
                c.execute(f"SELECT _timescaledb_internal.create_chunk('{hyperspace.name}', '{hyperspace.slices}', '{hyperspace.schema_name}', '{hyperspace.table_name}')")
                c.commit()
        with getcursor(self.target_pool) as cur:
            with cur as c:
                c.execute(f"ALTER SUBSCRIPTION {self.subscription_name} REFRESH PUBLICATION")

    def create_preexisting_chunks(self):
        for chunk in self.list_chunks_in_source():
            chunk_info = self.show_chunk_in_source(chunk)
            self.create_chunk_in_target(chunk_info)

    def drop_chunk_in_target(self, chunk):
        # drop chunk
        pass

def _main():
# Define the command-line arguments
    parser = argparse.ArgumentParser(description='Python program to connect to source and target PostgreSQL databases')
    parser.add_argument('--snapshot', action='store_true', help='Flag to indicate snapshot mode')
    parser.add_argument('source_conn_dsn', type=str, help='Connection string for the source database')
    parser.add_argument('target_conn_dsn', type=str, help='Connection string for the target database')
    args = parser.parse_args()

    logical = LogicalReplicaHelper(args.source_conn_dsn, args.target_conn_dsn, args.snapshot, "sub1")
    logical.create_preexisting_chunks()

if __name__ == '__main__':
    _main()

