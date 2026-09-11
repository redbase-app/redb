-- ============================================================
-- METADATA CACHE SYNC — module-owned since V4 (MS SQL Server)
-- ============================================================
-- sync_metadata_cache_for_scheme and warmup_all_metadata_caches carry the
-- column list of _scheme_metadata_cache. A new _structures column (V4:
-- _unique, _unique_version) must appear in that list on EXISTING databases,
-- and sql/redb_metadata_cache.sql lands only in redb_init.sql — fresh
-- databases only. So the two procedures ride the versioned bundle, exactly
-- like dbo.get_object_json (09_core_object_json.sql).
--
-- The cache TABLE, its indexes and the triggers stay in
-- sql/redb_metadata_cache.sql: the triggers EXEC these procedures by name at
-- run time. Column additions to the table itself go through the
-- "0. Schema upgrades" block of 00_module_init.sql.
-- ============================================================

IF OBJECT_ID('[dbo].[sync_metadata_cache_for_scheme]', 'P') IS NOT NULL
    DROP PROCEDURE [dbo].[sync_metadata_cache_for_scheme];
GO
IF OBJECT_ID('[dbo].[warmup_all_metadata_caches]', 'P') IS NOT NULL
    DROP PROCEDURE [dbo].[warmup_all_metadata_caches];
GO

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
        [_unique], [_unique_version], [_unique_scope], [_lazy], [_tags],
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
        s.[_unique],                    -- V4: unique key flag ([RedbUnique])
        s.[_unique_version],            -- V4: encoder version of the stored keys
        s.[_unique_scope],              -- S3: element-key scope of a collection key
        s.[_lazy],                      -- V4 (LAZY Л2): lazy reference marker
        s.[_tags],                      -- V4: free-form marker
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

-- Warmup procedure (for app start or after crash)
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
-- Repair pass moved here from 28_migrate_dateonly_db_type.sql (BR-5 parity): the cache copies
-- _db_type, so every scheme with a DateOnly field is resynced after the retype in 28 - and only
-- here are the procedures above guaranteed to exist, whatever the database's history.
-- Idempotent and cheap: only schemes that actually have a DateOnly structure.
-- =====================================================
DECLARE @schemeId BIGINT
DECLARE scheme_cursor CURSOR LOCAL FAST_FORWARD FOR
    SELECT DISTINCT st.[_id_scheme] FROM [dbo].[_structures] st
     WHERE st.[_id_type] = -9223372036854775686
OPEN scheme_cursor
FETCH NEXT FROM scheme_cursor INTO @schemeId
WHILE @@FETCH_STATUS = 0
BEGIN
    EXEC [dbo].[sync_metadata_cache_for_scheme] @schemeId
    FETCH NEXT FROM scheme_cursor INTO @schemeId
END
CLOSE scheme_cursor
DEALLOCATE scheme_cursor
GO
