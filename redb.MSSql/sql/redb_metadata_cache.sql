-- ============================================================
-- METADATA CACHE: Solution for repeated JOINs
-- MS SQL Server version
-- ============================================================
-- Goal: Avoid repeated JOIN _structures <- _types in every query
-- Approach: Regular table + automatic sync via triggers on _structure_hash
-- Benefits:
--   No recursion issues (indexes created once)
--   Works with connection pooling (global table)
--   Automatic invalidation (triggers on _schemes._structure_hash)
--   No C# code changes required
--   Minimal cache rebuilds (only on real schema changes)
-- ============================================================


-- =====================================================
-- DROP EXISTING OBJECTS
-- =====================================================

IF EXISTS (SELECT * FROM sys.triggers WHERE name = 'TR__schemes__sync_metadata_cache')
    DROP TRIGGER [dbo].[TR__schemes__sync_metadata_cache]
GO

IF EXISTS (SELECT * FROM sys.triggers WHERE name = 'TR__schemes__cleanup_metadata_cache')
    DROP TRIGGER [dbo].[TR__schemes__cleanup_metadata_cache]
GO

IF EXISTS (SELECT * FROM sys.triggers WHERE name = 'TR__types__invalidate_metadata_cache')
    DROP TRIGGER [dbo].[TR__types__invalidate_metadata_cache]
GO

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'sync_metadata_cache_for_scheme')
    DROP PROCEDURE [dbo].[sync_metadata_cache_for_scheme]
GO

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'warmup_all_metadata_caches')
    DROP PROCEDURE [dbo].[warmup_all_metadata_caches]
GO

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'check_metadata_cache_consistency')
    DROP PROCEDURE [dbo].[check_metadata_cache_consistency]
GO

IF OBJECT_ID('[dbo].[_scheme_metadata_cache]', 'U') IS NOT NULL 
    DROP TABLE [dbo].[_scheme_metadata_cache]
GO

-- =====================================================
-- 1. CREATE METADATA CACHE TABLE
-- =====================================================

CREATE TABLE [dbo].[_scheme_metadata_cache] (
    -- Identifiers
    [_scheme_id] BIGINT NOT NULL,
    [_structure_id] BIGINT NOT NULL,
    [_parent_structure_id] BIGINT NULL,
    [_id_override] BIGINT NULL,
    
    -- Names and aliases
    [_name] NVARCHAR(450) NOT NULL,
    [_alias] NVARCHAR(450) NULL,
    
    -- Structure type info
    [_type_id] BIGINT NOT NULL,
    [_list_id] BIGINT NULL,
    [type_name] NVARCHAR(450) NOT NULL,
    [db_type] NVARCHAR(450) NOT NULL,
    [type_semantic] NVARCHAR(450) NOT NULL,
    
    -- Scheme type info (Class/Array/Dictionary/JsonDocument/XDocument)
    [_scheme_type] BIGINT NULL,
    [scheme_type_name] NVARCHAR(450) NULL,
    
    -- Structure attributes
    [_order] BIGINT NULL,
    [_collection_type] BIGINT NULL,           -- NULL = not collection, Array/Dictionary type ID
    [collection_type_name] NVARCHAR(450) NULL, -- Collection type name
    [_key_type] BIGINT NULL,                  -- Key type for Dictionary
    [key_type_name] NVARCHAR(450) NULL,       -- Key type name
    [_readonly] BIT NULL,
    [_allow_not_null] BIT NULL,
    [_is_compress] BIT NULL,
    [_store_null] BIT NULL,
    
    -- Default values
    [_default_value] VARBINARY(MAX) NULL,
    [_default_editor] NVARCHAR(MAX) NULL
)
GO

-- =====================================================
-- 2. CREATE INDEXES
-- =====================================================

CREATE INDEX [idx_metadata_cache_lookup] 
    ON [dbo].[_scheme_metadata_cache]([_scheme_id], [_parent_structure_id], [_order])
GO

CREATE INDEX [idx_metadata_cache_structure] 
    ON [dbo].[_scheme_metadata_cache]([_structure_id])
GO

CREATE INDEX [idx_metadata_cache_scheme]
    ON [dbo].[_scheme_metadata_cache]([_scheme_id])
GO

CREATE INDEX [idx_metadata_cache_name]
    ON [dbo].[_scheme_metadata_cache]([_scheme_id], [_name])
GO

CREATE INDEX [idx_metadata_cache_collection]
    ON [dbo].[_scheme_metadata_cache]([_scheme_id], [_collection_type])
    WHERE [_collection_type] IS NOT NULL
GO

CREATE INDEX [idx_metadata_cache_scheme_type]
    ON [dbo].[_scheme_metadata_cache]([_scheme_id], [_scheme_type])
GO

CREATE INDEX [idx_metadata_cache_key_type]
    ON [dbo].[_scheme_metadata_cache]([_scheme_id], [_key_type])
    WHERE [_key_type] IS NOT NULL
GO

-- =====================================================
-- 3. SYNC PROCEDURE FOR SINGLE SCHEME
-- =====================================================

CREATE PROCEDURE [dbo].[sync_metadata_cache_for_scheme]
    @target_scheme_id BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Delete old cache data for scheme
    DELETE FROM [dbo].[_scheme_metadata_cache] 
    WHERE [_scheme_id] = @target_scheme_id;
    
    -- Insert current data (with collection types and scheme type support)
    INSERT INTO [dbo].[_scheme_metadata_cache] (
        [_scheme_id], [_structure_id], [_parent_structure_id], [_id_override],
        [_name], [_alias],
        [_type_id], [_list_id], [type_name], [db_type], [type_semantic],
        [_scheme_type], [scheme_type_name],
        [_order], [_collection_type], [collection_type_name], [_key_type], [key_type_name],
        [_readonly], [_allow_not_null], [_is_compress], [_store_null],
        [_default_value], [_default_editor]
    )
    SELECT 
        s.[_id_scheme],
        s.[_id],
        s.[_id_parent],
        s.[_id_override],
        s.[_name],
        s.[_alias],
        t.[_id],
        s.[_id_list],
        t.[_name],
        t.[_db_type],
        t.[_type],
        sch.[_type],                    -- Scheme type
        scht.[_name],                   -- Scheme type name
        s.[_order],
        s.[_collection_type],           -- Collection type (Array/Dictionary/NULL)
        ct.[_name],                     -- Collection type name
        s.[_key_type],                  -- Key type for Dictionary
        kt.[_name],                     -- Key type name
        s.[_readonly],
        s.[_allow_not_null],
        s.[_is_compress],
        s.[_store_null],
        s.[_default_value],
        s.[_default_editor]
    FROM [dbo].[_structures] s
    INNER JOIN [dbo].[_types] t ON t.[_id] = s.[_id_type]
    INNER JOIN [dbo].[_schemes] sch ON sch.[_id] = s.[_id_scheme]
    LEFT JOIN [dbo].[_types] scht ON scht.[_id] = sch.[_type]         -- Scheme type
    LEFT JOIN [dbo].[_types] ct ON ct.[_id] = s.[_collection_type]    -- Collection type
    LEFT JOIN [dbo].[_types] kt ON kt.[_id] = s.[_key_type]           -- Key type
    WHERE s.[_id_scheme] = @target_scheme_id;
END
GO

-- =====================================================
-- 4. TRIGGER: Sync cache on _structure_hash change
-- =====================================================

CREATE TRIGGER [dbo].[TR__schemes__sync_metadata_cache]
ON [dbo].[_schemes]
AFTER INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Handle UPDATE: rebuild cache if _structure_hash changed
    IF EXISTS (SELECT 1 FROM deleted)
    BEGIN
        DECLARE @scheme_id BIGINT;
        
        DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
            SELECT i.[_id]
            FROM inserted i
            INNER JOIN deleted d ON i.[_id] = d.[_id]
            WHERE i.[_structure_hash] IS NOT NULL 
              AND (d.[_structure_hash] IS NULL OR i.[_structure_hash] <> d.[_structure_hash]);
        
        OPEN cur;
        FETCH NEXT FROM cur INTO @scheme_id;
        
        WHILE @@FETCH_STATUS = 0
        BEGIN
            EXEC [dbo].[sync_metadata_cache_for_scheme] @scheme_id;
            PRINT 'Metadata cache rebuilt for scheme_id=' + CAST(@scheme_id AS NVARCHAR(20));
            FETCH NEXT FROM cur INTO @scheme_id;
        END
        
        CLOSE cur;
        DEALLOCATE cur;
    END
    ELSE
    BEGIN
        -- Handle INSERT: create cache for new schemes with hash
        DECLARE cur_insert CURSOR LOCAL FAST_FORWARD FOR
            SELECT [_id] FROM inserted WHERE [_structure_hash] IS NOT NULL;
        
        OPEN cur_insert;
        FETCH NEXT FROM cur_insert INTO @scheme_id;
        
        WHILE @@FETCH_STATUS = 0
        BEGIN
            EXEC [dbo].[sync_metadata_cache_for_scheme] @scheme_id;
            PRINT 'Metadata cache created for new scheme_id=' + CAST(@scheme_id AS NVARCHAR(20));
            FETCH NEXT FROM cur_insert INTO @scheme_id;
        END
        
        CLOSE cur_insert;
        DEALLOCATE cur_insert;
    END
END
GO

-- =====================================================
-- 5. TRIGGER: Cleanup cache on scheme delete
-- =====================================================

CREATE TRIGGER [dbo].[TR__schemes__cleanup_metadata_cache]
ON [dbo].[_schemes]
AFTER DELETE
AS
BEGIN
    SET NOCOUNT ON;
    
    DELETE c FROM [dbo].[_scheme_metadata_cache] c
    INNER JOIN deleted d ON c.[_scheme_id] = d.[_id];
    
    IF @@ROWCOUNT > 0
        PRINT 'Metadata cache cleared for deleted schemes';
END
GO

-- =====================================================
-- 6. TRIGGER: Invalidate all caches on _types change
-- =====================================================

CREATE TRIGGER [dbo].[TR__types__invalidate_metadata_cache]
ON [dbo].[_types]
AFTER UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    
    TRUNCATE TABLE [dbo].[_scheme_metadata_cache];
    PRINT 'All metadata caches invalidated due to _types change';
END
GO

-- =====================================================
-- 7. WARMUP PROCEDURE (for app start or after crash)
-- =====================================================

CREATE PROCEDURE [dbo].[warmup_all_metadata_caches]
AS
BEGIN
    SET NOCOUNT ON;
    
    TRUNCATE TABLE [dbo].[_scheme_metadata_cache];
    
    -- Rebuild cache for ALL schemes
    DECLARE @scheme_id BIGINT;
    
    DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
        SELECT [_id] FROM [dbo].[_schemes];
    
    OPEN cur;
    FETCH NEXT FROM cur INTO @scheme_id;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        EXEC [dbo].[sync_metadata_cache_for_scheme] @scheme_id;
        FETCH NEXT FROM cur INTO @scheme_id;
    END
    
    CLOSE cur;
    DEALLOCATE cur;
    
    -- Return statistics
    SELECT 
        s.[_id] AS scheme_id,
        COUNT(c.[_structure_id]) AS structures_count,
        s.[_name] AS scheme_name,
        s.[_structure_hash] AS structure_hash
    FROM [dbo].[_schemes] s
    LEFT JOIN [dbo].[_scheme_metadata_cache] c ON c.[_scheme_id] = s.[_id]
    GROUP BY s.[_id], s.[_name], s.[_structure_hash]
    ORDER BY s.[_id];
END
GO

-- =====================================================
-- 8. CONSISTENCY CHECK PROCEDURE
-- =====================================================

CREATE PROCEDURE [dbo].[check_metadata_cache_consistency]
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT 
        s.[_id] AS scheme_id,
        s.[_name] AS scheme_name,
        ISNULL(cache_stats.cached_count, 0) AS cached_count,
        ISNULL(actual_stats.actual_count, 0) AS actual_count,
        CASE 
            WHEN ISNULL(cache_stats.cached_count, 0) = ISNULL(actual_stats.actual_count, 0) 
            THEN CAST(1 AS BIT) 
            ELSE CAST(0 AS BIT) 
        END AS is_consistent
    FROM [dbo].[_schemes] s
    LEFT JOIN (
        SELECT [_scheme_id], COUNT(*) AS cached_count
        FROM [dbo].[_scheme_metadata_cache]
        GROUP BY [_scheme_id]
    ) cache_stats ON cache_stats.[_scheme_id] = s.[_id]
    LEFT JOIN (
        SELECT [_id_scheme], COUNT(*) AS actual_count
        FROM [dbo].[_structures]
        GROUP BY [_id_scheme]
    ) actual_stats ON actual_stats.[_id_scheme] = s.[_id]
    ORDER BY s.[_id];
END
GO

-- =====================================================
-- DONE! Usage:
-- 
-- INSTEAD OF:
--   FROM _structures s 
--   JOIN _types t ON t._id = s._id_type
--   WHERE s._id_scheme = @object_scheme_id
--
-- USE:
--   FROM _scheme_metadata_cache c
--   WHERE c._scheme_id = @object_scheme_id
-- 
-- AVAILABLE FIELDS:
--   _scheme_id, _structure_id, _parent_structure_id, _id_override
--   _name, _alias
--   _type_id, _list_id, type_name, db_type, type_semantic
--   _scheme_type, scheme_type_name (scheme type: Class/Array/Dictionary/JsonDocument/XDocument)
--   _order, _collection_type, collection_type_name (collection type: Array/Dictionary/NULL)
--   _key_type, key_type_name (key type for Dictionary)
--   _readonly, _allow_not_null, _is_compress, _store_null
--   _default_value, _default_editor
--
-- COLLECTION CHECK (instead of _is_array):
--   _collection_type IS NOT NULL = this is a collection (array or dictionary)
--   _collection_type IS NULL = not a collection
--
-- WARMUP:
--   EXEC warmup_all_metadata_caches
--
-- CONSISTENCY CHECK:
--   EXEC check_metadata_cache_consistency
-- =====================================================


PRINT '========================================='
PRINT 'Metadata cache for MS SQL Server created!'
PRINT 'Run: EXEC warmup_all_metadata_caches'
PRINT '========================================='
GO

