-- =====================================================
-- MIGRATION: Drop legacy _deleted_objects archive system
-- Part of Background Deletion System migration
-- =====================================================
-- This script removes the old _deleted_objects table and its trigger.
-- The new soft-delete system uses @@__deleted scheme and trash containers.
-- Run this script on existing databases to migrate to new system.
-- =====================================================

-- 1. Drop the trigger first
-- IF EXISTS (SELECT * FROM sys.triggers WHERE name = 'TR__objects__deleted_objects')
-- DROP TRIGGER [dbo].[TR__objects__deleted_objects]
-- GO

-- 2. Drop the archive table
IF OBJECT_ID('[dbo].[_deleted_objects]', 'U') IS NOT NULL 
DROP TABLE [dbo].[_deleted_objects]
GO

-- 3. Add the @@__deleted scheme if not exists
IF NOT EXISTS (SELECT 1 FROM [dbo].[_schemes] WHERE [_id] = -10)
INSERT INTO [dbo].[_schemes] ([_id], [_name], [_alias], [_type]) 
VALUES (-10, '@@__deleted', 'Deleted Objects', -9223372036854775703)
GO

-- =====================================================
-- VERIFICATION
-- =====================================================
-- After running this script, verify:
-- SELECT * FROM [dbo].[_schemes] WHERE [_id] = -10
-- Expected: @@__deleted scheme exists
-- 
-- SELECT * FROM sys.triggers WHERE name = 'TR__objects__deleted_objects'
-- Expected: No rows (trigger removed)
-- =====================================================

