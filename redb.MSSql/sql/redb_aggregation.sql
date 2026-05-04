-- =====================================================
-- REDB EAV AGGREGATION FUNCTIONS (MSSQL)
-- Aggregations for EAV fields (_values)
-- Supports: simple fields, nested Class, arrays, dictionaries
-- Ported from PostgreSQL version
-- =====================================================

SET NOCOUNT ON;
GO

-- ===== DROP EXISTING OBJECTS =====
IF OBJECT_ID('dbo.aggregate_field', 'P') IS NOT NULL 
    DROP PROCEDURE dbo.aggregate_field;
GO

IF OBJECT_ID('dbo.aggregate_batch', 'P') IS NOT NULL 
    DROP PROCEDURE dbo.aggregate_batch;
GO

IF OBJECT_ID('dbo.aggregate_batch_preview', 'P') IS NOT NULL 
    DROP PROCEDURE dbo.aggregate_batch_preview;
GO

-- =====================================================
-- aggregate_field: Single field aggregation
-- =====================================================
-- Parameters:
--   @scheme_id    - Scheme ID
--   @field_path   - Path to field:
--                   "Price"             - simple field
--                   "Customer.Name"     - nested Class
--                   "Items[].Price"     - array (ALL elements)
--                   "Items[2].Price"    - array (SPECIFIC element)
--                   "PhoneBook[home]"   - Dictionary (SPECIFIC key)
--   @function     - SUM, AVG, MIN, MAX, COUNT
--   @filter_json  - JSON filter or NULL
--
-- Returns: numeric result
-- =====================================================
CREATE PROCEDURE dbo.aggregate_field
    @scheme_id BIGINT,
    @field_path NVARCHAR(255),
    @function NVARCHAR(20),
    @filter_json NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @structure_id BIGINT;
    DECLARE @db_type NVARCHAR(50);
    DECLARE @dict_key NVARCHAR(255);
    DECLARE @array_index NVARCHAR(50);
    DECLARE @column_name NVARCHAR(50);
    DECLARE @array_condition NVARCHAR(500) = N'';
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @result DECIMAL(38, 10);
    
    -- 1. Resolve field path
    EXEC resolve_field_path @scheme_id, @field_path,
        @structure_id OUTPUT, @db_type OUTPUT, @dict_key OUTPUT, @array_index OUTPUT;
    
    IF @structure_id IS NULL
    BEGIN
        RAISERROR('Field "%s" not found in scheme %d', 16, 1, @field_path, @scheme_id);
        RETURN;
    END
    
    -- 2. Determine column by data type
    SET @column_name = CASE @db_type
        WHEN N'Long' THEN N'_Long'
        WHEN N'Double' THEN N'_Double'
        WHEN N'Numeric' THEN N'_Numeric'
        WHEN N'Int' THEN N'_Long'
        WHEN N'Decimal' THEN N'_Numeric'
        WHEN N'Money' THEN N'_Numeric'
        ELSE N'_Long'
    END;
    
    -- 3. Build array/dict condition (with SQL injection protection)
    IF @dict_key IS NOT NULL
        SET @array_condition = N' AND v._array_index = N''' + REPLACE(@dict_key, '''', '''''') + N'''';
    ELSE IF @array_index IS NOT NULL
        SET @array_condition = N' AND v._array_index = N''' + REPLACE(@array_index, '''', '''''') + N'''';
    ELSE IF @dict_key IS NULL AND @array_index IS NULL AND @field_path NOT LIKE N'%[]%'
        SET @array_condition = N' AND v._array_index IS NULL';
    
    -- 4. Build and execute SQL
    IF @filter_json IS NOT NULL AND @filter_json <> N'' AND @filter_json <> N'null'
    BEGIN
        -- With filter - use temp table for object IDs
        CREATE TABLE #filtered_ids (_id BIGINT PRIMARY KEY);
        
        INSERT INTO #filtered_ids
        EXEC get_filtered_object_ids @scheme_id, @filter_json, 10;
        
        IF NOT EXISTS (SELECT 1 FROM #filtered_ids)
        BEGIN
            IF @function = N'COUNT'
                SELECT 0 AS result;
            ELSE
                SELECT NULL AS result;
            DROP TABLE #filtered_ids;
            RETURN;
        END
        
        SET @sql = N'
            SELECT @res = ' + UPPER(@function) + N'(CAST(v.' + @column_name + N' AS DECIMAL(38,10)))
            FROM _values v
            WHERE v._id_structure = @struct_id
              AND v._id_object IN (SELECT _id FROM #filtered_ids)' + @array_condition;
        
        EXEC sp_executesql @sql, 
            N'@struct_id BIGINT, @res DECIMAL(38,10) OUTPUT',
            @struct_id = @structure_id,
            @res = @result OUTPUT;
        
        DROP TABLE #filtered_ids;
    END
    ELSE
    BEGIN
        -- Without filter - all objects in scheme
        SET @sql = N'
            SELECT @res = ' + UPPER(@function) + N'(CAST(v.' + @column_name + N' AS DECIMAL(38,10)))
            FROM _values v
            JOIN _objects o ON o._id = v._id_object
            WHERE v._id_structure = @struct_id
              AND o._id_scheme = @scheme' + @array_condition;
        
        EXEC sp_executesql @sql,
            N'@struct_id BIGINT, @scheme BIGINT, @res DECIMAL(38,10) OUTPUT',
            @struct_id = @structure_id,
            @scheme = @scheme_id,
            @res = @result OUTPUT;
    END
    
    SELECT @result AS result;
END;
GO

-- =====================================================
-- aggregate_batch: Multiple aggregations in ONE query
-- =====================================================
CREATE PROCEDURE dbo.aggregate_batch
    @scheme_id BIGINT,
    @aggregations NVARCHAR(MAX),
    @filter_json NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @select_parts NVARCHAR(MAX) = N'';
    DECLARE @has_eav_fields BIT = 0;
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @field NVARCHAR(255);
    DECLARE @func NVARCHAR(20);
    DECLARE @alias NVARCHAR(255);
    DECLARE @structure_id BIGINT;
    DECLARE @db_type NVARCHAR(50);
    DECLARE @dict_key NVARCHAR(255);
    DECLARE @array_index NVARCHAR(50);
    DECLARE @column_name NVARCHAR(50);
    DECLARE @array_condition NVARCHAR(500);
    
    -- Parse aggregations JSON
    DECLARE @agg_cursor CURSOR;
    SET @agg_cursor = CURSOR FOR
        SELECT 
            JSON_VALUE(value, '$.field') AS field,
            UPPER(JSON_VALUE(value, '$.func')) AS func,
            JSON_VALUE(value, '$.alias') AS alias
        FROM OPENJSON(@aggregations);
    
    OPEN @agg_cursor;
    FETCH NEXT FROM @agg_cursor INTO @field, @func, @alias;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- COUNT(*) special case
        IF @func = N'COUNT' AND (@field IS NULL OR @field = N'*' OR @field = N'')
        BEGIN
            IF @select_parts <> N'' SET @select_parts = @select_parts + N', ';
            SET @select_parts = @select_parts + N'COUNT(DISTINCT v._id_object) AS [' + REPLACE(@alias, '''', '''''') + N']';
            SET @has_eav_fields = 1;
            FETCH NEXT FROM @agg_cursor INTO @field, @func, @alias;
            CONTINUE;
        END
        
        -- Base field with 0$: prefix
        IF @field LIKE N'0$:%'
        BEGIN
            DECLARE @raw_field NVARCHAR(100) = SUBSTRING(@field, 4, 100);
            DECLARE @sql_column NVARCHAR(100) = dbo.normalize_base_field_name(@raw_field);
            
            IF @sql_column IS NOT NULL
            BEGIN
                IF @select_parts <> N'' SET @select_parts = @select_parts + N', ';
                
                IF @func IN (N'SUM', N'AVG')
                    SET @select_parts = @select_parts + @func + N'(CAST(o.' + @sql_column + N' AS DECIMAL(38,10))) AS [' + REPLACE(@alias, '''', '''''') + N']';
                ELSE
                    SET @select_parts = @select_parts + @func + N'(o.' + @sql_column + N') AS [' + REPLACE(@alias, '''', '''''') + N']';
            END
        END
        ELSE
        BEGIN
            -- EAV field
            EXEC resolve_field_path @scheme_id, @field,
                @structure_id OUTPUT, @db_type OUTPUT, @dict_key OUTPUT, @array_index OUTPUT;
            
            IF @structure_id IS NOT NULL
            BEGIN
                SET @has_eav_fields = 1;
                
                SET @column_name = CASE @db_type
                    WHEN N'Long' THEN N'_Long'
                    WHEN N'Double' THEN N'_Double'
                    WHEN N'Numeric' THEN N'_Numeric'
                    WHEN N'Int' THEN N'_Long'
                    WHEN N'Decimal' THEN N'_Numeric'
                    WHEN N'Money' THEN N'_Numeric'
                    ELSE N'_Long'
                END;
                
                -- Build array condition (with SQL injection protection)
                IF @dict_key IS NOT NULL
                    SET @array_condition = N' AND v._array_index = N''' + REPLACE(@dict_key, '''', '''''') + N'''';
                ELSE IF @array_index IS NOT NULL
                    SET @array_condition = N' AND v._array_index = N''' + REPLACE(@array_index, '''', '''''') + N'''';
                ELSE IF @field NOT LIKE N'%[]%'
                    SET @array_condition = N' AND v._array_index IS NULL';
                ELSE
                    SET @array_condition = N'';
                
                IF @select_parts <> N'' SET @select_parts = @select_parts + N', ';
                SET @select_parts = @select_parts + @func + 
                    N'(CASE WHEN v._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + 
                    @array_condition + N' THEN CAST(v.' + @column_name + N' AS DECIMAL(38,10)) END) AS [' + REPLACE(@alias, '''', '''''') + N']';
            END
        END
        
        FETCH NEXT FROM @agg_cursor INTO @field, @func, @alias;
    END
    
    CLOSE @agg_cursor;
    DEALLOCATE @agg_cursor;
    
    IF @select_parts = N''
    BEGIN
        SELECT N'{}' AS result;
        RETURN;
    END
    
    -- Execute with or without filter
    IF @filter_json IS NOT NULL AND @filter_json <> N'' AND @filter_json <> N'null'
    BEGIN
        -- With filter
        CREATE TABLE #filtered_ids (_id BIGINT PRIMARY KEY);
        
        INSERT INTO #filtered_ids
        EXEC get_filtered_object_ids @scheme_id, @filter_json, 10;
        
        IF NOT EXISTS (SELECT 1 FROM #filtered_ids)
        BEGIN
            SELECT N'{}' AS result;
            DROP TABLE #filtered_ids;
            RETURN;
        END
        
        IF @has_eav_fields = 1
        BEGIN
            SET @sql = N'
                SELECT ' + @select_parts + N'
                FROM _values v
                JOIN _objects o ON o._id = v._id_object
                WHERE v._id_object IN (SELECT _id FROM #filtered_ids)
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER';
        END
        ELSE
        BEGIN
            SET @sql = N'
                SELECT ' + @select_parts + N'
                FROM _objects o
                WHERE o._id IN (SELECT _id FROM #filtered_ids)
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER';
        END
        
        EXEC sp_executesql @sql;
        DROP TABLE #filtered_ids;
    END
    ELSE
    BEGIN
        -- Without filter
        IF @has_eav_fields = 1
        BEGIN
            SET @sql = N'
                SELECT ' + @select_parts + N'
                FROM _values v
                JOIN _objects o ON o._id = v._id_object
                WHERE o._id_scheme = ' + CAST(@scheme_id AS NVARCHAR(20)) + N'
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER';
        END
        ELSE
        BEGIN
            SET @sql = N'
                SELECT ' + @select_parts + N'
                FROM _objects o
                WHERE o._id_scheme = ' + CAST(@scheme_id AS NVARCHAR(20)) + N'
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER';
        END
        
        EXEC sp_executesql @sql;
    END
END;
GO

-- =====================================================
-- aggregate_batch_preview: SQL Preview for debugging
-- =====================================================
CREATE PROCEDURE dbo.aggregate_batch_preview
    @scheme_id BIGINT,
    @aggregations NVARCHAR(MAX),
    @filter_json NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @select_parts NVARCHAR(MAX) = N'';
    DECLARE @structure_ids NVARCHAR(MAX) = N'';
    DECLARE @field NVARCHAR(255);
    DECLARE @func NVARCHAR(20);
    DECLARE @alias NVARCHAR(255);
    DECLARE @structure_id BIGINT;
    DECLARE @db_type NVARCHAR(50);
    DECLARE @dict_key NVARCHAR(255);
    DECLARE @array_index NVARCHAR(50);
    DECLARE @column_name NVARCHAR(50);
    DECLARE @array_condition NVARCHAR(500);
    DECLARE @sql NVARCHAR(MAX);
    
    -- Parse aggregations
    DECLARE @agg_cursor CURSOR;
    SET @agg_cursor = CURSOR FOR
        SELECT 
            JSON_VALUE(value, '$.field') AS field,
            UPPER(JSON_VALUE(value, '$.func')) AS func,
            JSON_VALUE(value, '$.alias') AS alias
        FROM OPENJSON(@aggregations);
    
    OPEN @agg_cursor;
    FETCH NEXT FROM @agg_cursor INTO @field, @func, @alias;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @func = N'COUNT' AND (@field IS NULL OR @field = N'*' OR @field = N'')
        BEGIN
            IF @select_parts <> N'' SET @select_parts = @select_parts + N',
  ';
            SET @select_parts = @select_parts + N'COUNT(DISTINCT v._id_object) AS [' + REPLACE(@alias, '''', '''''') + N']';
            FETCH NEXT FROM @agg_cursor INTO @field, @func, @alias;
            CONTINUE;
        END
        
        EXEC resolve_field_path @scheme_id, @field,
            @structure_id OUTPUT, @db_type OUTPUT, @dict_key OUTPUT, @array_index OUTPUT;
        
        IF @structure_id IS NOT NULL
        BEGIN
            IF @structure_ids <> N'' SET @structure_ids = @structure_ids + N', ';
            SET @structure_ids = @structure_ids + CAST(@structure_id AS NVARCHAR(20));
            
            SET @column_name = CASE @db_type
                WHEN N'Long' THEN N'_Long'
                WHEN N'Double' THEN N'_Double'
                WHEN N'Numeric' THEN N'_Numeric'
                ELSE N'_Long'
            END;
            
            IF @dict_key IS NOT NULL
                SET @array_condition = N' AND v._array_index = N''' + REPLACE(@dict_key, '''', '''''') + N'''';
            ELSE IF @array_index IS NOT NULL
                SET @array_condition = N' AND v._array_index = N''' + REPLACE(@array_index, '''', '''''') + N'''';
            ELSE IF @field NOT LIKE N'%[]%'
                SET @array_condition = N' AND v._array_index IS NULL';
            ELSE
                SET @array_condition = N'';
            
            IF @select_parts <> N'' SET @select_parts = @select_parts + N',
  ';
            SET @select_parts = @select_parts + @func + 
                N'(CASE WHEN v._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + 
                @array_condition + N' THEN CAST(v.' + @column_name + N' AS DECIMAL(38,10)) END) AS [' + 
                REPLACE(@alias, '''', '''''') + N'] /* ' + REPLACE(@field, '''', '''''') + N' */';
        END
        
        FETCH NEXT FROM @agg_cursor INTO @field, @func, @alias;
    END
    
    CLOSE @agg_cursor;
    DEALLOCATE @agg_cursor;
    
    IF @select_parts = N''
    BEGIN
        SELECT N'-- No aggregations to execute' AS sql_preview;
        RETURN;
    END
    
    -- Build preview SQL
    IF @filter_json IS NOT NULL
    BEGIN
        SET @sql = N'-- AGGREGATE BATCH SQL PREVIEW
-- Scheme: ' + CAST(@scheme_id AS NVARCHAR(20)) + N'
-- Filter: ' + @filter_json + N'

-- Step 1: Get object_ids via get_filtered_object_ids
-- Step 2: Aggregation by object_ids

SELECT
  ' + @select_parts + N'
FROM _values v
WHERE v._id_structure IN (' + @structure_ids + N')
  AND v._id_object IN (SELECT _id FROM #filtered_ids);';
    END
    ELSE
    BEGIN
        SET @sql = N'-- AGGREGATE BATCH SQL PREVIEW
-- Scheme: ' + CAST(@scheme_id AS NVARCHAR(20)) + N'
-- Filter: NULL

SELECT
  ' + @select_parts + N'
FROM _values v
JOIN _objects o ON o._id = v._id_object
WHERE v._id_structure IN (' + @structure_ids + N')
  AND o._id_scheme = ' + CAST(@scheme_id AS NVARCHAR(20)) + N';';
    END
    
    SELECT @sql AS sql_preview;
END;
GO

PRINT N'=========================================';
PRINT N'Aggregation procedures created!';
PRINT N'';
PRINT N'PROCEDURES:';
PRINT N'  aggregate_field - Single field aggregation';
PRINT N'  aggregate_batch - Multiple aggregations in one call';
PRINT N'  aggregate_batch_preview - SQL preview for debugging';
PRINT N'';
PRINT N'SUPPORTED FUNCTIONS:';
PRINT N'  SUM, AVG, MIN, MAX, COUNT';
PRINT N'';
PRINT N'FEATURES:';
PRINT N'  - Base fields with 0$: prefix';
PRINT N'  - EAV fields from _values';
PRINT N'  - Array support: Items[].Price (all), Items[2].Price (specific)';
PRINT N'  - Dictionary support: PhoneBook[home]';
PRINT N'  - Filters via get_filtered_object_ids';
PRINT N'=========================================';
GO

