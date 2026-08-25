-- =====================================================
-- MIGRATION: DateOnly gets db_type 'DateTimeOffset' (PostgreSQL)
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
-- redbPostgre.sql.
-- =====================================================

UPDATE _types
   SET _db_type = 'DateTimeOffset'
 WHERE _id = -9223372036854775686        -- DateOnly
   AND _db_type <> 'DateTimeOffset';

-- The scheme metadata cache copies _db_type; refresh it for every scheme that
-- has a DateOnly field, otherwise readers keep answering from the stale copy.
SELECT sync_metadata_cache_for_scheme(s._id)
  FROM _schemes s
 WHERE EXISTS (SELECT 1 FROM _structures st
                WHERE st._id_scheme = s._id
                  AND st._id_type = -9223372036854775686);

-- =====================================================
-- VERIFICATION
-- =====================================================
-- SELECT _name, _db_type, _type FROM _types WHERE _id = -9223372036854775686;
--   expected: DateOnly | DateTimeOffset | DateOnly
