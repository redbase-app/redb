-- ============================================================
-- MIGRATE STRUCTURE TYPE
-- MS SQL Server version
-- Atomic data migration when changing structure type
-- ============================================================


-- =====================================================
-- DROP EXISTING OBJECTS
-- =====================================================

IF OBJECT_ID('dbo.get_value_column', 'FN') IS NOT NULL
    DROP FUNCTION dbo.get_value_column;
GO

IF OBJECT_ID('dbo.migrate_structure_type', 'P') IS NOT NULL
    DROP PROCEDURE dbo.migrate_structure_type;
GO

-- =====================================================
-- HELPER FUNCTION: get_value_column
-- Returns column name for type
-- =====================================================
CREATE FUNCTION dbo.get_value_column(@type_name NVARCHAR(100))
RETURNS NVARCHAR(50)
WITH SCHEMABINDING
AS
BEGIN
    RETURN CASE LOWER(@type_name)
        -- String types (base + derived)
        WHEN 'string' THEN '_String'
        WHEN 'text' THEN '_String'
        WHEN 'mimetype' THEN '_String'
        WHEN 'filepath' THEN '_String'
        WHEN 'filename' THEN '_String'
        -- Long types
        WHEN 'long' THEN '_Long'
        WHEN 'int' THEN '_Long'
        WHEN 'int32' THEN '_Long'
        WHEN 'int64' THEN '_Long'
        WHEN 'short' THEN '_Long'
        WHEN 'byte' THEN '_Long'
        WHEN 'timespan' THEN '_Long'
        -- Double types
        WHEN 'double' THEN '_Double'
        WHEN 'float' THEN '_Double'
        -- Numeric types
        WHEN 'numeric' THEN '_Numeric'
        WHEN 'decimal' THEN '_Numeric'
        -- Boolean types
        WHEN 'boolean' THEN '_Boolean'
        WHEN 'bool' THEN '_Boolean'
        -- DateTime types
        WHEN 'datetimeoffset' THEN '_DateTimeOffset'
        WHEN 'datetime' THEN '_DateTimeOffset'
        WHEN 'dateonly' THEN '_DateTimeOffset'
        WHEN 'timeonly' THEN '_DateTimeOffset'
        -- Guid types
        WHEN 'guid' THEN '_Guid'
        WHEN 'uuid' THEN '_Guid'
        -- Binary types
        WHEN 'bytearray' THEN '_ByteArray'
        WHEN 'bytes' THEN '_ByteArray'
        -- Object/List types
        WHEN 'object' THEN '_Object'
        WHEN 'listitem' THEN '_ListItem'
        ELSE NULL
    END;
END;
GO

-- =====================================================
-- MAIN PROCEDURE: migrate_structure_type
-- Migrates data when changing structure type
-- =====================================================
CREATE PROCEDURE dbo.migrate_structure_type
    @p0 BIGINT,              -- structure_id
    @p1 NVARCHAR(100),       -- old_type_name
    @p2 NVARCHAR(100),       -- new_type_name
    @p3 BIT = 0              -- dry_run
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @v_source_col NVARCHAR(50);
    DECLARE @v_target_col NVARCHAR(50);
    DECLARE @v_affected_rows INT = 0;
    DECLARE @v_success_count INT = 0;
    DECLARE @v_has_collision BIT = 0;
    DECLARE @v_conversion_sql NVARCHAR(MAX);
    DECLARE @v_count_sql NVARCHAR(MAX);
    DECLARE @v_collision_sql NVARCHAR(MAX);
    
    -- Get column names
    SET @v_source_col = dbo.get_value_column(@p1);
    SET @v_target_col = dbo.get_value_column(@p2);
    
    -- Validate source type
    IF @v_source_col IS NULL
    BEGIN
        SELECT 0 AS affected_rows, 0 AS success_count, 0 AS error_count, 
               N'Unknown source type: ' + @p1 AS errors;
        RETURN;
    END;
    
    -- Validate target type
    IF @v_target_col IS NULL
    BEGIN
        SELECT 0 AS affected_rows, 0 AS success_count, 0 AS error_count, 
               N'Unknown target type: ' + @p2 AS errors;
        RETURN;
    END;
    
    -- Same columns - no migration needed (e.g. Int->Long both in _Long)
    IF @v_source_col = @v_target_col
    BEGIN
        SELECT 0 AS affected_rows, 0 AS success_count, 0 AS error_count, 
               CAST(NULL AS NVARCHAR(MAX)) AS errors;
        RETURN;
    END;
    
    -- Check structure exists
    IF NOT EXISTS (SELECT 1 FROM _structures WHERE _id = @p0)
    BEGIN
        SELECT 0 AS affected_rows, 0 AS success_count, 0 AS error_count, 
               N'Structure ' + CAST(@p0 AS NVARCHAR(20)) + N' not found' AS errors;
        RETURN;
    END;
    
    -- Count affected rows
    SET @v_count_sql = N'SELECT @cnt = COUNT(*) FROM _values WHERE _id_structure = @sid AND ' + 
                       QUOTENAME(@v_source_col) + N' IS NOT NULL';
    EXEC sp_executesql @v_count_sql, 
        N'@sid BIGINT, @cnt INT OUTPUT', 
        @sid = @p0, @cnt = @v_affected_rows OUTPUT;
    
    -- Dry run - only count
    IF @p3 = 1
    BEGIN
        SELECT @v_affected_rows AS affected_rows, 0 AS success_count, 0 AS error_count, 
               CAST(NULL AS NVARCHAR(MAX)) AS errors;
        RETURN;
    END;
    
    -- ========================================
    -- COLLISION CHECK (key point!)
    -- If target is filled but source is empty - data already migrated manually
    -- ========================================
    SET @v_collision_sql = N'SELECT @has_collision = CASE WHEN EXISTS(
        SELECT 1 FROM _values 
        WHERE _id_structure = @sid 
          AND ' + QUOTENAME(@v_target_col) + N' IS NOT NULL
          AND ' + QUOTENAME(@v_source_col) + N' IS NULL
    ) THEN 1 ELSE 0 END';
    
    EXEC sp_executesql @v_collision_sql,
        N'@sid BIGINT, @has_collision BIT OUTPUT',
        @sid = @p0, @has_collision = @v_has_collision OUTPUT;
    
    IF @v_has_collision = 1
    BEGIN
        SELECT @v_affected_rows AS affected_rows, 0 AS success_count, @v_affected_rows AS error_count,
               N'TYPE_MIGRATION_COLLISION: Data already in ' + @v_target_col + 
               N' but _id_type = ' + @p1 + 
               N'. Fix manually: UPDATE _structures SET _id_type = (SELECT _id FROM _types WHERE _name = ''' + 
               @p2 + N''') WHERE _id = ' + CAST(@p0 AS NVARCHAR(20)) AS errors;
        RETURN;
    END;
    
    -- No data to migrate
    IF @v_affected_rows = 0
    BEGIN
        SELECT 0 AS affected_rows, 0 AS success_count, 0 AS error_count, 
               CAST(NULL AS NVARCHAR(MAX)) AS errors;
        RETURN;
    END;
    
    -- ========================================
    -- CONVERSION MATRIX
    -- ========================================
    SET @v_conversion_sql = NULL;
    
    -- STRING -> *
    IF @v_source_col = '_String'
    BEGIN
        IF @v_target_col = '_Long'
            SET @v_conversion_sql = N'UPDATE _values SET _Long = TRY_CAST(_String AS BIGINT), _String = NULL 
                WHERE _id_structure = @sid AND _String IS NOT NULL AND TRY_CAST(_String AS BIGINT) IS NOT NULL';
        ELSE IF @v_target_col = '_Double'
            SET @v_conversion_sql = N'UPDATE _values SET [_Double] = TRY_CAST(_String AS FLOAT), _String = NULL 
                WHERE _id_structure = @sid AND _String IS NOT NULL AND TRY_CAST(_String AS FLOAT) IS NOT NULL';
        ELSE IF @v_target_col = '_Numeric'
            SET @v_conversion_sql = N'UPDATE _values SET _Numeric = TRY_CAST(_String AS DECIMAL(38,18)), _String = NULL 
                WHERE _id_structure = @sid AND _String IS NOT NULL AND TRY_CAST(_String AS DECIMAL(38,18)) IS NOT NULL';
        ELSE IF @v_target_col = '_Boolean'
            SET @v_conversion_sql = N'UPDATE _values SET _Boolean = CASE 
                WHEN LOWER(_String) IN (''true'', ''1'', ''yes'', ''t'', ''y'') THEN 1 
                WHEN LOWER(_String) IN (''false'', ''0'', ''no'', ''f'', ''n'') THEN 0 
                ELSE NULL END, _String = NULL 
                WHERE _id_structure = @sid AND _String IS NOT NULL';
        ELSE IF @v_target_col = '_DateTimeOffset'
            SET @v_conversion_sql = N'UPDATE _values SET _DateTimeOffset = TRY_CAST(_String AS DATETIMEOFFSET), _String = NULL 
                WHERE _id_structure = @sid AND _String IS NOT NULL AND TRY_CAST(_String AS DATETIMEOFFSET) IS NOT NULL';
        ELSE IF @v_target_col = '_Guid'
            SET @v_conversion_sql = N'UPDATE _values SET _Guid = TRY_CAST(_String AS UNIQUEIDENTIFIER), _String = NULL 
                WHERE _id_structure = @sid AND _String IS NOT NULL AND TRY_CAST(_String AS UNIQUEIDENTIFIER) IS NOT NULL';
    END
    
    -- LONG -> *
    ELSE IF @v_source_col = '_Long'
    BEGIN
        IF @v_target_col = '_String'
            SET @v_conversion_sql = N'UPDATE _values SET _String = CAST(_Long AS NVARCHAR(MAX)), _Long = NULL 
                WHERE _id_structure = @sid AND _Long IS NOT NULL';
        ELSE IF @v_target_col = '_Double'
            SET @v_conversion_sql = N'UPDATE _values SET [_Double] = CAST(_Long AS FLOAT), _Long = NULL 
                WHERE _id_structure = @sid AND _Long IS NOT NULL';
        ELSE IF @v_target_col = '_Numeric'
            SET @v_conversion_sql = N'UPDATE _values SET _Numeric = CAST(_Long AS DECIMAL(38,18)), _Long = NULL 
                WHERE _id_structure = @sid AND _Long IS NOT NULL';
        ELSE IF @v_target_col = '_Boolean'
            SET @v_conversion_sql = N'UPDATE _values SET _Boolean = CASE WHEN _Long != 0 THEN 1 ELSE 0 END, _Long = NULL 
                WHERE _id_structure = @sid AND _Long IS NOT NULL';
        ELSE IF @v_target_col = '_DateTimeOffset'
            SET @v_conversion_sql = N'UPDATE _values SET _DateTimeOffset = DATEADD(SECOND, _Long, ''1970-01-01''), _Long = NULL 
                WHERE _id_structure = @sid AND _Long IS NOT NULL';
    END
    
    -- DOUBLE -> *
    ELSE IF @v_source_col = '_Double'
    BEGIN
        IF @v_target_col = '_String'
            SET @v_conversion_sql = N'UPDATE _values SET _String = FORMAT([_Double], ''G'', ''en-US''), [_Double] = NULL 
                WHERE _id_structure = @sid AND [_Double] IS NOT NULL';
        ELSE IF @v_target_col = '_Long'
            SET @v_conversion_sql = N'UPDATE _values SET _Long = CAST(ROUND([_Double], 0) AS BIGINT), [_Double] = NULL 
                WHERE _id_structure = @sid AND [_Double] IS NOT NULL';
        ELSE IF @v_target_col = '_Numeric'
            SET @v_conversion_sql = N'UPDATE _values SET _Numeric = CAST([_Double] AS DECIMAL(38,18)), [_Double] = NULL 
                WHERE _id_structure = @sid AND [_Double] IS NOT NULL';
    END
    
    -- NUMERIC -> *
    ELSE IF @v_source_col = '_Numeric'
    BEGIN
        IF @v_target_col = '_String'
            SET @v_conversion_sql = N'UPDATE _values SET _String = CAST(_Numeric AS NVARCHAR(MAX)), _Numeric = NULL 
                WHERE _id_structure = @sid AND _Numeric IS NOT NULL';
        ELSE IF @v_target_col = '_Long'
            SET @v_conversion_sql = N'UPDATE _values SET _Long = CAST(ROUND(_Numeric, 0) AS BIGINT), _Numeric = NULL 
                WHERE _id_structure = @sid AND _Numeric IS NOT NULL';
        ELSE IF @v_target_col = '_Double'
            SET @v_conversion_sql = N'UPDATE _values SET [_Double] = CAST(_Numeric AS FLOAT), _Numeric = NULL 
                WHERE _id_structure = @sid AND _Numeric IS NOT NULL';
    END
    
    -- BOOLEAN -> *
    ELSE IF @v_source_col = '_Boolean'
    BEGIN
        IF @v_target_col = '_String'
            SET @v_conversion_sql = N'UPDATE _values SET _String = CASE WHEN _Boolean = 1 THEN ''true'' ELSE ''false'' END, _Boolean = NULL 
                WHERE _id_structure = @sid AND _Boolean IS NOT NULL';
        ELSE IF @v_target_col = '_Long'
            SET @v_conversion_sql = N'UPDATE _values SET _Long = CASE WHEN _Boolean = 1 THEN 1 ELSE 0 END, _Boolean = NULL 
                WHERE _id_structure = @sid AND _Boolean IS NOT NULL';
    END
    
    -- DATETIMEOFFSET -> *
    ELSE IF @v_source_col = '_DateTimeOffset'
    BEGIN
        IF @v_target_col = '_String'
            SET @v_conversion_sql = N'UPDATE _values SET _String = CONVERT(NVARCHAR(50), _DateTimeOffset, 127), _DateTimeOffset = NULL 
                WHERE _id_structure = @sid AND _DateTimeOffset IS NOT NULL';
        ELSE IF @v_target_col = '_Long'
            SET @v_conversion_sql = N'UPDATE _values SET _Long = DATEDIFF_BIG(SECOND, ''1970-01-01'', _DateTimeOffset), _DateTimeOffset = NULL 
                WHERE _id_structure = @sid AND _DateTimeOffset IS NOT NULL';
    END
    
    -- GUID -> *
    ELSE IF @v_source_col = '_Guid'
    BEGIN
        IF @v_target_col = '_String'
            SET @v_conversion_sql = N'UPDATE _values SET _String = CAST(_Guid AS NVARCHAR(50)), _Guid = NULL 
                WHERE _id_structure = @sid AND _Guid IS NOT NULL';
    END;
    
    -- Conversion not supported
    IF @v_conversion_sql IS NULL
    BEGIN
        SELECT @v_affected_rows AS affected_rows, 0 AS success_count, @v_affected_rows AS error_count,
               N'Conversion ' + @p1 + N' -> ' + @p2 + N' not supported' AS errors;
        RETURN;
    END;
    
    -- Execute migration
    EXEC sp_executesql @v_conversion_sql, N'@sid BIGINT', @sid = @p0;
    SET @v_success_count = @@ROWCOUNT;
    
    -- Return result
    SELECT @v_affected_rows AS affected_rows, 
           @v_success_count AS success_count, 
           @v_affected_rows - @v_success_count AS error_count, 
           CAST(NULL AS NVARCHAR(MAX)) AS errors;
END;
GO

PRINT 'migrate_structure_type procedure created!'
PRINT ''
PRINT 'Usage:'
PRINT '  EXEC migrate_structure_type @p0, @p_old_type, @p_new_type, @p3'
PRINT ''
PRINT 'Examples:'
PRINT '  EXEC migrate_structure_type 12345, ''String'', ''Long'', 1  -- dry run'
PRINT '  EXEC migrate_structure_type 12345, ''String'', ''Long'', 0  -- execute'
GO

