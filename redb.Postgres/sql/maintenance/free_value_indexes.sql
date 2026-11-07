-- =====================================================================
-- REDB (PostgreSQL): value indexes for the FREE provider
-- =====================================================================
-- The PVT module 0.7.14-0.7.16 retired the object-leading value indexes: the stock covering index
-- IX__values__structure_object_lookup and the family (_id_structure, _id_object, <value>). Pro reads the
-- covering index of its streams form instead (redb.Postgres.Pro/Sql/pvt_streams_indexes.sql), the free
-- provider has no replacement for it. Measured on a copy of a production database (38 million values):
-- one free query over a whole scheme went from 8.8 s to 18-20 s once they were gone.
--
-- This file brings them back under names of their own (IX__values__free_*). The module drops the old
-- names again on every change of its version; it neither drops nor checks these, so they survive
-- upgrades. Pro with the planner on (the default) does not need them, do not run the file there. Pro with
-- EnablePvtPrefilter = false reads the same forms as the free provider and may gain from them as well.
--
-- Left out on purpose: the seven indexes the module 0.7.13 dropped as duplicates. The leading columns of
-- each are the leading columns of an index or UNIQUE constraint of the same table that stays, so they
-- served no plan of their own.
--
-- Price: every index here is updated on each save of a value. On 38 million values the covering one
-- took about 1.7 GB, the partial ones less. Run the file only if free queries got slower.
--
-- Nothing runs this file automatically. Run it yourself, once per database:
--   psql -d <database> -f free_value_indexes.sql
-- Do not wrap it in a transaction: CONCURRENTLY cannot run inside one. Reads and writes go on while the
-- indexes are built. Running it again is safe: an existing index is kept, an index left INVALID by an
-- interrupted build is dropped first and built again.
--
-- To remove them:
--   DROP INDEX CONCURRENTLY IF EXISTS "IX__values__free_structure_object_lookup"; (and so on for each name below)
-- =====================================================================

-- An interrupted CREATE INDEX CONCURRENTLY leaves an INVALID index, and IF NOT EXISTS would keep it.
DO $$
DECLARE r record;
BEGIN
    FOR r IN SELECT c.relname
             FROM pg_class c
             JOIN pg_index i ON i.indexrelid = c.oid AND i.indrelid = '_values'::regclass
             WHERE c.relname LIKE 'IX\_\_values\_\_free\_%' AND NOT i.indisvalid
    LOOP
        EXECUTE format('DROP INDEX %I', r.relname);
    END LOOP;
END $$;

-- 1. Values of a field by object, every scalar column but the string (was IX__values__structure_object_lookup)
CREATE INDEX CONCURRENTLY IF NOT EXISTS "IX__values__free_structure_object_lookup"
    ON _values (_id_structure, _id_object, _Long, _DateTimeOffset, _Boolean, _Double, _Guid, _Numeric, _ListItem, _Object);

-- 2. String value of a field by object (was IX__values__String_not_null). A btree entry is limited to about
-- 2.7 kB, so longer strings stay out of the index; the length guard keeps an insert of such a string working.
CREATE INDEX CONCURRENTLY IF NOT EXISTS "IX__values__free_string_by_object"
    ON _values (_id_structure, _id_object, _String)
    WHERE _String IS NOT NULL AND length(_String) < 2000;

-- 3. Number, date and decimal value of a field by object (were IX__values__{Long,DateTimeOffset,Numeric}_not_null)
CREATE INDEX CONCURRENTLY IF NOT EXISTS "IX__values__free_long_by_object"
    ON _values (_id_structure, _id_object, _Long)
    WHERE _Long IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS "IX__values__free_datetimeoffset_by_object"
    ON _values (_id_structure, _id_object, _DateTimeOffset)
    WHERE _DateTimeOffset IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS "IX__values__free_numeric_by_object"
    ON _values (_id_structure, _id_object, _Numeric)
    WHERE _Numeric IS NOT NULL;

-- 4. Scalar (non-array) values of a field by object (was ix__values__array_null_structure)
CREATE INDEX CONCURRENTLY IF NOT EXISTS "IX__values__free_array_null_structure"
    ON _values (_id_structure, _id_object, _Long)
    WHERE _array_index IS NULL;

ANALYZE _values;
