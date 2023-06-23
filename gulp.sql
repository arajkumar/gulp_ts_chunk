-- psql -f gulp.sql $PGCOPYDB_SOURCE_PGURI -t > /tmp/replace.sed
-- sed -u -E -f /tmp/replace.sed
SELECT concat('s/', '_timescaledb_internal','._hyper_', id,'_[^[:space:]]*/"', schema_name, '"."', table_name,'"', '/g') FROM _timescaledb_catalog.hypertable;
SELECT concat('s/', '"_timescaledb_internal"','."_hyper_', id,'_[^[:space:]]*/"', schema_name, '"."', table_name,'"', '/g') FROM _timescaledb_catalog.hypertable;
SELECT '/"_timescaledb_catalog"/d';
SELECT 's/TRUNCATE ONLY/TRUNCATE/';
