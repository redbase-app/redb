-- =====================================================================
-- REDB (SQLite): value indexes for the FREE provider
-- =====================================================================
-- Schema step 11 (2026-10-17) replaced the object-leading value indexes (_id_structure, _id_object,
-- <value>) with value-leading ones (_id_structure, <value>, _id_object, _array_index). The new ones
-- serve a filter on a field; the old ones served reading the value of a known object.
--
-- This file brings them back under names of their own (IX__values__free_*). Schema steps drop the old
-- names again whenever the schema version of the file goes up; they neither drop nor check these, so
-- they survive upgrades. Pro with the planner on (the default) does not need them, do not run the file
-- there. Pro with EnablePvtPrefilter = false reads the same forms as the free provider and may gain from
-- them as well.
--
-- Left out on purpose:
--   IX__values__scalar_* - built by Pro only, never part of the free schema;
--   the six indexes schema step 10 dropped as duplicates: the leading columns of each are the leading
--       columns of an index or UNIQUE constraint of the same table that stays.
-- IX__values__structure_object_lookup was never dropped on SQLite and stays in the schema.
--
-- Price: every index here is updated on each save of a value. SQLite has no length limit on an index
-- entry, so the string index holds every string in full: on a file with long texts it is large. Run the
-- file only if free queries got slower.
--
-- Nothing runs this file automatically. Run it yourself, once per database file, with the application
-- stopped or idle:
--   sqlite3 <file> < free_value_indexes.sql
-- The file is locked for writing while the indexes are built. Running it again is safe: an existing
-- index is kept.
--
-- To remove them:
--   DROP INDEX IF EXISTS "IX__values__free_string_by_object"; (and so on for each name below)
-- =====================================================================

BEGIN IMMEDIATE;

-- 1. String, number, date and decimal value of a field by object (were IX__values__*_not_null)
CREATE INDEX IF NOT EXISTS "IX__values__free_string_by_object"
    ON _values (_id_structure, _id_object, _String) WHERE _String IS NOT NULL;
CREATE INDEX IF NOT EXISTS "IX__values__free_long_by_object"
    ON _values (_id_structure, _id_object, _Long) WHERE _Long IS NOT NULL;
CREATE INDEX IF NOT EXISTS "IX__values__free_datetimeoffset_by_object"
    ON _values (_id_structure, _id_object, _DateTimeOffset) WHERE _DateTimeOffset IS NOT NULL;
CREATE INDEX IF NOT EXISTS "IX__values__free_numeric_by_object"
    ON _values (_id_structure, _id_object, _Numeric) WHERE _Numeric IS NOT NULL;

-- 2. Scalar (non-array) values of a field by object (was ix__values__array_null_structure)
CREATE INDEX IF NOT EXISTS "IX__values__free_array_null_structure"
    ON _values (_id_structure, _id_object, _Long) WHERE _array_index IS NULL;

COMMIT;

ANALYZE _values;
