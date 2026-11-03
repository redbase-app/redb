-- =====================================================================
-- REDB (SQLite): bring three _objects indexes of an EXISTING file to the current schema
-- =====================================================================
-- redbSqlite.sql no longer puts _date_modify into these indexes. That file creates indexes with
-- CREATE INDEX IF NOT EXISTS, which does not change an index that already exists, so an existing
-- file keeps the old definitions until this file is run.
--
-- Why it matters: _date_modify changes on every save, so an index that carries it is rewritten for
-- every saved object. On PostgreSQL, with frequent saves, these three had grown to 700-1400 bytes per
-- entry and shrank 10-20 times when rebuilt without the column.
--
-- Nothing runs this file automatically. Run it yourself, once per database file, with the application
-- stopped or idle:
--   sqlite3 <file> < rebuild_objects_indexes_without_date_modify.sql
--
-- SQLite cannot rename an index, so each one is dropped and created again under its name, all in one
-- transaction: the file is locked for writing meanwhile, and a failure leaves the old indexes in place.
-- Running the file again only pays for one more rebuild.
-- =====================================================================

BEGIN IMMEDIATE;

-- 1. Descendants of an object
DROP INDEX IF EXISTS "IX__objects__parent_id_descendant_lookup";
CREATE INDEX "IX__objects__parent_id_descendant_lookup"
    ON _objects (_id_parent, _id_scheme, _id, _id_owner, _date_create) WHERE _id_parent IS NOT NULL;

-- 2. Tree queries sorted by creation date
DROP INDEX IF EXISTS "IX__objects__scheme_parent_date_create";
CREATE INDEX "IX__objects__scheme_parent_date_create"
    ON _objects (_id_scheme, _id_parent, _date_create DESC, _id, _id_owner);

-- 3. Tree queries filtered by owner or name
DROP INDEX IF EXISTS "IX__objects__scheme_parent_owner";
CREATE INDEX "IX__objects__scheme_parent_owner"
    ON _objects (_id_scheme, _id_parent, _id, _id_owner, _date_create, _name) WHERE _id_parent IS NOT NULL;

COMMIT;

ANALYZE _objects;
