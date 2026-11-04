-- =====================================================================
-- REDB (PostgreSQL): trigram index for StringCollation
-- =====================================================================
-- With StringCollation set (for example "und-x-icu"), Pro with the planner on (EnablePvtPrefilter, the default)
-- runs an ignore-case string search (Contains, StartsWith, EndsWith with StringComparison.OrdinalIgnoreCase) as
--   (_String COLLATE "<collation>") ILIKE '%x%'
-- on _values. A collated operand cannot use the stock index IX__values__String_pattern: it is built over bare
-- _String with the collation of the database. Without an index over the same expression such a search reads
-- every row of _values. Nothing errors; the index just shows no reads. Pro start-up logs a Warning while the
-- index is missing. The free provider, and Pro with the planner off, read no trigram index at all: they do not
-- need this file.
--
-- Check the ctype of the database first:
--   SELECT datctype FROM pg_database WHERE datname = current_database();
-- UTF-8 ctype: plain ILIKE already folds every script; leaving StringCollation unset keeps the stock index
-- serving the search, and this file is not needed. Ctype C or POSIX: pg_trgm builds no trigram from non-ASCII
-- text, so this index serves patterns with ASCII letters only; a non-ASCII substring search stays a scan.
--
-- This file builds that index for one collation. The collation is part of the index expression, so the index
-- must match the configured StringCollation exactly: a different collation needs an index of its own.
--
-- CreateStringCollationIndex = true has Pro build the same index (same name) at start-up. This file is for the
-- installations that keep schema changes with the DBA (AutoApplyDatabaseUpgrades off, a role without the right).
--
-- Run it yourself, once per database, with the value of StringCollation:
--   psql -d <database> -v collation=und-x-icu -f string_collation_index.sql
-- Do not wrap it in a transaction: CONCURRENTLY cannot run inside one. Reads and writes go on while the index
-- is built. Running it again is safe: an existing index is kept, an index left INVALID by an interrupted build
-- is dropped first and built again.
--
-- Price: a second GIN index on the largest table, updated on each save of a string value. Its size is close to
-- that of IX__values__String_pattern.
--
-- IX__values__String_pattern is still read by case-sensitive Contains / StartsWith / EndsWith (plain LIKE, no
-- COLLATE). Drop it only if the project does no case-sensitive substring search; the statement is at the end of
-- this file, commented out.
-- =====================================================================

\set ON_ERROR_STOP on

\if :{?collation}
\else
    \echo 'Set the collation: psql -v collation=<StringCollation> -f string_collation_index.sql'
    \quit
\endif

CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- The name start-up uses as well: IX__values__String_pattern_<collation lowercased, other characters as _>.
SELECT 'IX__values__String_pattern_' || regexp_replace(lower(:'collation'), '[^a-z0-9]', '_', 'g') AS ix_name \gset

-- An interrupted CREATE INDEX CONCURRENTLY leaves an INVALID index, and IF NOT EXISTS would keep it.
SELECT EXISTS (
    SELECT 1
    FROM pg_index i
    JOIN pg_class c ON c.oid = i.indexrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = :'ix_name' AND n.nspname = current_schema() AND NOT i.indisvalid
) AS ix_invalid \gset

\if :ix_invalid
DROP INDEX CONCURRENTLY IF EXISTS :"ix_name";
\endif

CREATE INDEX CONCURRENTLY IF NOT EXISTS :"ix_name"
    ON _values USING gin ((_String COLLATE :"collation") gin_trgm_ops)
    WHERE _String IS NOT NULL;

ANALYZE _values;

-- Only if no case-sensitive substring search is left in the project (see the header):
-- DROP INDEX CONCURRENTLY IF EXISTS "IX__values__String_pattern";
