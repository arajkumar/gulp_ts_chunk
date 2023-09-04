import pytest

from textwrap import dedent
from io import StringIO

import pgcopydb_copy

def test_pgcopydb_copy():
    lines = StringIO("""BEGIN;
PREPARE b6175cf4 AS INSERT INTO "public"."tags" ("id", "name", "fleet", "driver", "model", "device_version", "load_capacity", "fuel_capacity", "nominal_fuel_consumption") overriding system value VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);
EXECUTE b6175cf4["592603","truck_0","East","Rodney","F-150","v1.5","2000","200","15"];
PREPARE b6175cf4 AS INSERT INTO "public"."tags" ("id", "name", "fleet", "driver", "model", "device_version", "load_capacity", "fuel_capacity", "nominal_fuel_consumption") overriding system value VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);
EXECUTE b6175cf4["592603","truck_0","East","Rodney","F-150","v1.5","2000","200","15"];
PREPARE b6175cf5 AS INSERT INTO "public"."hello" ("id", "name", "fleet", "driver", "model", "device_version", "load_capacity", "fuel_capacity", "nominal_fuel_consumption") overriding system value VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);
EXECUTE b6175cf5["592603","truck_0","East","Rodney","F-150","v1.5","2000","200","15"];
END;
""")
    expected = StringIO("""BEGIN;
COPY "public"."tags"("id", "name", "fleet", "driver", "model", "device_version", "load_capacity", "fuel_capacity", "nominal_fuel_consumption") FROM stdin;
592603\ttruck_0\tEast\tRodney\tF-150\tv1.5\t2000\t200\t15
592603\ttruck_0\tEast\tRodney\tF-150\tv1.5\t2000\t200\t15
\\.
COPY "public"."hello"("id", "name", "fleet", "driver", "model", "device_version", "load_capacity", "fuel_capacity", "nominal_fuel_consumption") FROM stdin;
592603\ttruck_0\tEast\tRodney\tF-150\tv1.5\t2000\t200\t15
\\.
END;
""")
    out_lines = pgcopydb_copy.transform(lines)
    out = StringIO()
    pgcopydb_copy.write(out, out_lines)
    assert expected.getvalue() == out.getvalue()
