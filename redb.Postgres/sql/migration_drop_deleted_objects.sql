-- =====================================================
-- MIGRATION: Drop legacy _deleted_objects archive system
-- Part of Background Deletion System migration
-- =====================================================
-- This script removes the old _deleted_objects table and its trigger.
-- The new soft-delete system uses @@__deleted scheme and trash containers.
-- Run this script on existing databases to migrate to new system.
-- =====================================================

-- 1. Drop the trigger first (depends on function)
DROP TRIGGER IF EXISTS TR__objects__deleted_objects ON _objects;

-- 2. Drop the archive function
DROP FUNCTION IF EXISTS ftr__objects__deleted_objects();

-- 3. Drop the archive table
DROP TABLE IF EXISTS _deleted_objects;

-- 4. Add the @@__deleted scheme if not exists
INSERT INTO _schemes (_id, _name, _alias, _type) 
VALUES (-10, '@@__deleted', 'Deleted Objects', -9223372036854775703)
ON CONFLICT (_id) DO NOTHING;

-- 5. Update validate_scheme_name function to allow @@ prefix
-- (This is done by re-creating the function - see redbPostgre.sql)

-- =====================================================
-- VERIFICATION
-- =====================================================
-- After running this script, verify:
-- SELECT * FROM _schemes WHERE _id = -10;
-- Expected: @@__deleted scheme exists
-- 
-- SELECT * FROM pg_trigger WHERE tgname = 'tr__objects__deleted_objects';
-- Expected: No rows (trigger removed)
-- =====================================================

