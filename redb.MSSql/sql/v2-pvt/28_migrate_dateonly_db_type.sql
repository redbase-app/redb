-- =====================================================
-- MIGRATION: DateOnly gets db_type 'DateTimeOffset' (SQL Server)
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
-- redbMSSQL.sql.
-- =====================================================

UPDATE [dbo].[_types]
   SET [_db_type] = 'DateTimeOffset'
 WHERE [_id] = -9223372036854775686        -- DateOnly
   AND [_db_type] <> 'DateTimeOffset'
GO

-- The scheme metadata cache copies _db_type, so every scheme with a DateOnly field must be
-- resynced after the retype. That resync lives at the TAIL of 29_metadata_cache_sync.sql, not
-- here (BR-5 parity with PostgreSQL): the cursor here only worked on a fresh database because it
-- iterated zero rows; a dump-restored database WITH DateOnly schemes and WITHOUT the module would
-- have hit the missing procedure the same way.
