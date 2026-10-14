-- =====================================================================
-- REDB (PostgreSQL): bring three _objects indexes of an EXISTING database to the current schema
-- =====================================================================
-- redbPostgre.sql no longer puts _date_modify into INCLUDE of these indexes. That file runs only
-- on a new database, and CREATE INDEX IF NOT EXISTS does not change an index that already exists,
-- so an existing database keeps the old definitions until this file is run.
--
-- Why it matters: _date_modify changes on every save, so an index that carries it gets a new entry
-- for every saved object. On a database with frequent saves these three had grown to 700-1400
-- bytes per entry and shrank 10-20 times when rebuilt without the column.
--
-- Nothing runs this file automatically. Run it yourself, once per database:
--   psql -d <database> -f rebuild_objects_indexes_without_date_modify.sql
-- Do not wrap it in a transaction: CONCURRENTLY cannot run inside one.
--
-- Each index is built next to the old one, the old one is dropped, the new one takes its name.
-- Reads and writes continue throughout. Each step needs free disk space for the new index.
-- An interrupted build leaves an INVALID index named *_new; drop it and run the file again.
-- A database that already has the new definitions only pays for one more rebuild.
-- =====================================================================

-- 1. Descendants of an object
CREATE INDEX CONCURRENTLY IF NOT EXISTS "IX__objects__parent_id_descendant_lookup_new"
    ON _objects (_id_parent, _id_scheme)
    INCLUDE (_id, _id_owner, _date_create)
    WHERE _id_parent IS NOT NULL;
DROP INDEX CONCURRENTLY IF EXISTS "IX__objects__parent_id_descendant_lookup";
ALTER INDEX "IX__objects__parent_id_descendant_lookup_new" RENAME TO "IX__objects__parent_id_descendant_lookup";

-- 2. Tree queries sorted by creation date
CREATE INDEX CONCURRENTLY IF NOT EXISTS "IX__objects__scheme_parent_date_create_new"
    ON _objects (_id_scheme, _id_parent, _date_create DESC)
    INCLUDE (_id, _id_owner);
DROP INDEX CONCURRENTLY IF EXISTS "IX__objects__scheme_parent_date_create";
ALTER INDEX "IX__objects__scheme_parent_date_create_new" RENAME TO "IX__objects__scheme_parent_date_create";

-- 3. Tree queries filtered by owner or name
CREATE INDEX CONCURRENTLY IF NOT EXISTS "IX__objects__scheme_parent_owner_new"
    ON _objects (_id_scheme, _id_parent)
    INCLUDE (_id, _id_owner, _date_create, _name)
    WHERE _id_parent IS NOT NULL;
DROP INDEX CONCURRENTLY IF EXISTS "IX__objects__scheme_parent_owner";
ALTER INDEX "IX__objects__scheme_parent_owner_new" RENAME TO "IX__objects__scheme_parent_owner";

ANALYZE _objects;
