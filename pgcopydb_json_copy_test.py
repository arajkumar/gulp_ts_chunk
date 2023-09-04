import pytest

from io import StringIO

import pgcopydb_json_copy

def test_pgcopydb_json_copy():
    lines = StringIO("""{"action":"B","xid":"183058","lsn":"25/31C973B8","timestamp":"2023-09-04 05:35:20.77393+0000","message":{"action":"B","xid":183058}}
{"action":"I","xid":"183058","lsn":"25/31C97420","timestamp":"2023-09-04 05:35:20.78003+0000","message":{"action":"I","xid":183058,"schema":"public","table":"tags","columns":[{"name":"id","type":"integer","value":821688},{"name":"name","type":"text","value":"truck_0"},{"name":"fleet","type":"text","value":"East"},{"name":"driver","type":"text","value":"Rodney"},{"name":"model","type":"text","value":"F-150"},{"name":"device_version","type":"text","value":"v1.5"},{"name":"load_capacity","type":"double precision","value":2000},{"name":"fuel_capacity","type":"double precision","value":200},{"name":"nominal_fuel_consumption","type":"double precision","value":15}]}}
{"action":"I","xid":"183058","lsn":"25/31C97420","timestamp":"2023-09-04 05:35:20.78003+0000","message":{"action":"I","xid":183058,"schema":"public","table":"tags","columns":[{"name":"id","type":"integer","value":821688},{"name":"name","type":"text","value":"truck_0"},{"name":"fleet","type":"text","value":"East"},{"name":"driver","type":"text","value":"Rodney"},{"name":"model","type":"text","value":"F-150"},{"name":"device_version","type":"text","value":"v1.5"},{"name":"load_capacity","type":"double precision","value":2000},{"name":"fuel_capacity","type":"double precision","value":200},{"name":"nominal_fuel_consumption","type":"double precision","value":15}]}}
{"action":"I","xid":"183058","lsn":"25/31C97420","timestamp":"2023-09-04 05:35:20.78003+0000","message":{"action":"I","xid":183058,"schema":"public","table":"hello","columns":[{"name":"id","type":"integer","value":821688},{"name":"name","type":"text","value":"truck_0"},{"name":"fleet","type":"text","value":"East"},{"name":"driver","type":"text","value":"Rodney"},{"name":"model","type":"text","value":"F-150"},{"name":"device_version","type":"text","value":"v1.5"},{"name":"load_capacity","type":"double precision","value":2000},{"name":"fuel_capacity","type":"double precision","value":200},{"name":"nominal_fuel_consumption","type":"double precision","value":15}]}}
{"action":"C","xid":"183058","lsn":"25/31CA7AE8","timestamp":"2023-09-04 05:35:20.79299+0000","message":{"action":"C","xid":183058}}
""")
    expected = StringIO("""BEGIN;
COPY "public"."tags"("id", "name", "fleet", "driver", "model", "device_version", "load_capacity", "fuel_capacity", "nominal_fuel_consumption") FROM stdin;
821688\ttruck_0\tEast\tRodney\tF-150\tv1.5\t2000\t200\t15
821688\ttruck_0\tEast\tRodney\tF-150\tv1.5\t2000\t200\t15
\\.
COPY "public"."hello"("id", "name", "fleet", "driver", "model", "device_version", "load_capacity", "fuel_capacity", "nominal_fuel_consumption") FROM stdin;
821688\ttruck_0\tEast\tRodney\tF-150\tv1.5\t2000\t200\t15
\\.
COMMIT;
""")
    lines = pgcopydb_json_copy.read_ndjson(lines)
    out_lines = pgcopydb_json_copy.transform(lines)
    out = StringIO()
    pgcopydb_json_copy.write(out, out_lines)
    assert expected.getvalue() == out.getvalue()
