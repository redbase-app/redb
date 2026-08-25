-- =====================================================
-- MIGRATION: DateOnly gets db_type 'DateTimeOffset' (SQLite)
-- =====================================================
-- DateOnly was seeded with _db_type = 'DateTime', a value no other type uses and
-- no JSON projection knows about: get_object_json switches on _db_type and has
-- branches for 'DateTimeOffset', 'String', 'Long', ... but none for 'DateTime'.
-- The column was written correctly and then dropped on the way out, so every
-- DateOnly property materialised as 0001-01-01 on every provider.
--
-- Retyping it to 'DateTimeOffset' routes it through the branch that already
-- exists everywhere — JSON projections, PVT builders and the SQLite native
-- extension — with no change to any SQL function and no PVT version bump.
-- Storage is unchanged: the value already lives in _values._DateTimeOffset as
-- midnight of that date.
--
-- Idempotent. Run on existing databases; new ones get the right value from
-- redbSqlite.sql.
-- =====================================================

UPDATE _types
   SET _db_type = 'DateTimeOffset'
 WHERE _id = -9223372036854775686        -- DateOnly
   AND _db_type <> 'DateTimeOffset';

-- SQLite has no stored procedure for the cache; drop the affected rows and let
-- the next scheme sync rebuild them.
DELETE FROM _scheme_metadata_cache
 WHERE _scheme_id IN (SELECT DISTINCT _id_scheme FROM _structures
                       WHERE _id_type = -9223372036854775686);
