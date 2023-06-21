import pytest

from textwrap import dedent
from io import StringIO

import gulp_chunk

def test_gulping_timescale_chunk_insert():
    lines = StringIO("""{"action":"B","xid":"1172365","lsn":"19/E6000D28","timestamp":"2023-06-20 10:34:59.345874+0530","message":{"action":"B","xid":1172365}}
        {"action":"I","xid":"1172365","lsn":"19/E6000D90","timestamp":"2023-06-20 10:34:59.345940+0530","message":{"action":"I","xid":1172365,"schema":"_timescaledb_catalog","table":"dimension_slice","columns":[{"name":"id","type":"integer","value":186},{"name":"dimension_id","type":"integer","value":8},{"name":"range_start","type":"bigint","value":1686787200000000},{"name":"range_end","type":"bigint","value":1687392000000000}]}}
        {"action":"I","xid":"1172365","lsn":"19/E600A958","timestamp":"2023-06-20 10:34:59.346033+0530","message":{"action":"I","xid":1172365,"schema":"_timescaledb_catalog","table":"chunk","columns":[{"name":"id","type":"integer","value":186},{"name":"hypertable_id","type":"integer","value":8},{"name":"schema_name","type":"name","value":"_timescaledb_internal"},{"name":"table_name","type":"name","value":"_hyper_8_186_chunk"},{"name":"compressed_chunk_id","type":"integer","value":null},{"name":"dropped","type":"boolean","value":false},{"name":"status","type":"integer","value":0},{"name":"osm_chunk","type":"boolean","value":false}]}}
        {"action":"I","xid":"1172365","lsn":"19/E600DC68","timestamp":"2023-06-20 10:34:59.346049+0530","message":{"action":"I","xid":1172365,"schema":"_timescaledb_catalog","table":"chunk_constraint","columns":[{"name":"chunk_id","type":"integer","value":186},{"name":"dimension_slice_id","type":"integer","value":186},{"name":"constraint_name","type":"name","value":"constraint_186"},{"name":"hypertable_constraint_name","type":"name","value":null}]}}
        {"action":"I","xid":"1172365","lsn":"19/E60107D8","timestamp":"2023-06-20 10:34:59.346056+0530","message":{"action":"I","xid":1172365,"schema":"_timescaledb_catalog","table":"chunk_constraint","columns":[{"name":"chunk_id","type":"integer","value":186},{"name":"dimension_slice_id","type":"integer","value":null},{"name":"constraint_name","type":"name","value":"186_7_metric_pkey"},{"name":"hypertable_constraint_name","type":"name","value":"metric_pkey"}]}}
        {"action":"I","xid":"1172365","lsn":"19/E601C230","timestamp":"2023-06-20 10:34:59.346072+0530","message":{"action":"I","xid":1172365,"schema":"_timescaledb_catalog","table":"chunk_index","columns":[{"name":"chunk_id","type":"integer","value":186},{"name":"index_name","type":"name","value":"186_7_metric_pkey"},{"name":"hypertable_id","type":"integer","value":8},{"name":"hypertable_index_name","type":"name","value":"metric_pkey"}]}}
        {"action":"I","xid":"1172365","lsn":"19/E601F668","timestamp":"2023-06-20 10:34:59.346081+0530","message":{"action":"I","xid":1172365,"schema":"_timescaledb_catalog","table":"chunk_index","columns":[{"name":"chunk_id","type":"integer","value":186},{"name":"index_name","type":"name","value":"_hyper_8_186_chunk_metric_time_idx"},{"name":"hypertable_id","type":"integer","value":8},{"name":"hypertable_index_name","type":"name","value":"metric_time_idx"}]}}
        {"action":"I","xid":"1172365","lsn":"19/E601F7D8","timestamp":"2023-06-20 10:34:59.346134+0530","message":{"action":"I","xid":1172365,"schema":"_timescaledb_internal","table":"_hyper_8_186_chunk","columns":[{"name":"device_id","type":"integer","value":1},{"name":"time","type":"timestamp with time zone","value":"2023-06-20 10:34:59.116855+00"},{"name":"val","type":"double precision","value":"123"}]}}
        {"action":"C","xid":"1172365","lsn":"19/E601FD98","timestamp":"2023-06-20 10:34:59.346143+0530","message":{"action":"C","xid":1172365}}"""
    )
    expected = StringIO("""{"action":"B","xid":"1172365","lsn":"19/E6000D28","timestamp":"2023-06-20 10:34:59.345874+0530","message":{"action":"B","xid":1172365}}
        {"action":"I","xid":"1172365","lsn":"19/E601F7D8","timestamp":"2023-06-20 10:34:59.346134+0530","message":{"action":"I","xid":1172365,"schema":"public","table":"hello","columns":[{"name":"device_id","type":"integer","value":1},{"name":"time","type":"timestamp with time zone","value":"2023-06-20 10:34:59.116855+00"},{"name":"val","type":"double precision","value":"123"}]}}
        {"action":"C","xid":"1172365","lsn":"19/E601FD98","timestamp":"2023-06-20 10:34:59.346143+0530","message":{"action":"C","xid":1172365}}"""
    )
    chunk_to_ht = {
            "8": gulp_chunk.Hypertable(schema='public', table='hello')
    }
    lines = gulp_chunk.read_ndjson(lines)
    out_lines = gulp_chunk.gulp_timescale_chunks(chunk_to_ht, lines)

    expected_lines = gulp_chunk.read_ndjson(expected)

    assert list(expected_lines) == list(out_lines)
