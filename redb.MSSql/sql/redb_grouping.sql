-- =====================================================
-- REDB EAV GROUPING FUNCTIONS (MSSQL)
-- GroupBy aggregations for EAV model
-- Ported from PostgreSQL version
-- =====================================================

SET NOCOUNT ON;
GO

-- ===== DROP EXISTING OBJECTS =====
IF OBJECT_ID('dbo.aggregate_grouped', 'P') IS NOT NULL 
    DROP PROCEDURE dbo.aggregate_grouped;
GO

IF OBJECT_ID('dbo.aggregate_grouped_preview', 'P') IS NOT NULL 
    DROP PROCEDURE dbo.aggregate_grouped_preview;
GO

IF OBJECT_ID('dbo.aggregate_array_grouped', 'P') IS NOT NULL 
    DROP PROCEDURE dbo.aggregate_array_grouped;
GO

-- =====================================================
-- aggregate_grouped: GroupBy with aggregations
-- =====================================================
-- Parameters:
--   @scheme_id     - Scheme ID
--   @group_fields  - JSON array of grouping fields:
--                    [{"field":"Category","alias":"Category"}]
--   @aggregations  - JSON array of aggregations:
--                    [{"field":"Stock","func":"SUM","alias":"TotalStock"}]
--   @filter_json   - JSON filter (optional)
--
-- Returns: JSON array of groups
-- =====================================================
CREATE PROCEDURE dbo.aggregate_grouped
    @scheme_id BIGINT,
    @group_fields NVARCHAR(MAX),
    @aggregations NVARCHAR(MAX),
    @filter_json NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @select_parts NVARCHAR(MAX) = N'';
    DECLARE @join_parts NVARCHAR(MAX) = N'';
    DECLARE @group_parts NVARCHAR(MAX) = N'';
    DECLARE @where_clause NVARCHAR(MAX) = N'';
    
    DECLARE @join_idx INT = 0;
    DECLARE @join_alias NVARCHAR(10);
    DECLARE @column_name NVARCHAR(50);
    
    DECLARE @field NVARCHAR(255);
    DECLARE @alias NVARCHAR(255);
    DECLARE @func NVARCHAR(20);
    
    DECLARE @structure_id BIGINT;
    DECLARE @db_type NVARCHAR(50);
    DECLARE @dict_key NVARCHAR(255);
    DECLARE @array_index NVARCHAR(50);
    DECLARE @array_condition NVARCHAR(500);
    
    DECLARE @sql NVARCHAR(MAX);
    
    -- =========================================
    -- 1. Process grouping fields
    -- =========================================
    DECLARE @group_cursor CURSOR;
    SET @group_cursor = CURSOR FOR
        SELECT 
            JSON_VALUE(value, '$.field') AS field,
            ISNULL(JSON_VALUE(value, '$.alias'), JSON_VALUE(value, '$.field')) AS alias
        FROM OPENJSON(@group_fields);
    
    OPEN @group_cursor;
    FETCH NEXT FROM @group_cursor INTO @field, @alias;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @field IS NOT NULL
        BEGIN
            -- Check for base field (0$: prefix)
            IF @field LIKE N'0$:%'
            BEGIN
                DECLARE @raw_field NVARCHAR(100) = SUBSTRING(@field, 4, 100);
                DECLARE @sql_column NVARCHAR(100) = dbo.normalize_base_field_name(@raw_field);
                
                IF @sql_column IS NOT NULL
                BEGIN
                    IF @select_parts <> N'' SET @select_parts = @select_parts + N', ';
                    SET @select_parts = @select_parts + N'o.' + @sql_column + N' AS [' + REPLACE(@alias, '''', '''''') + N']';
                    
                    IF @group_parts <> N'' SET @group_parts = @group_parts + N', ';
                    SET @group_parts = @group_parts + N'o.' + @sql_column;
                END
            END
            ELSE
            BEGIN
                -- EAV field
                EXEC resolve_field_path @scheme_id, @field,
                    @structure_id OUTPUT, @db_type OUTPUT, @dict_key OUTPUT, @array_index OUTPUT;
                
                IF @structure_id IS NOT NULL
                BEGIN
                    SET @join_idx = @join_idx + 1;
                    SET @join_alias = N'g' + CAST(@join_idx AS NVARCHAR(10));
                    
                    SET @column_name = CASE @db_type
                        WHEN N'Long' THEN N'_Long'
                        WHEN N'Double' THEN N'_Double'
                        WHEN N'Numeric' THEN N'_Numeric'
                        WHEN N'String' THEN N'_String'
                        WHEN N'Bool' THEN N'_Boolean'
                        WHEN N'ListItem' THEN N'_ListItem'
                        ELSE N'_String'
                    END;
                    
                    IF @select_parts <> N'' SET @select_parts = @select_parts + N', ';
                    SET @select_parts = @select_parts + @join_alias + N'.' + @column_name + N' AS [' + REPLACE(@alias, '''', '''''') + N']';
                    
                    -- Build JOIN (with SQL injection protection)
                    IF @dict_key IS NOT NULL
                        SET @join_parts = @join_parts + N' JOIN _values ' + @join_alias + 
                            N' ON ' + @join_alias + N'._id_object = o._id AND ' + 
                            @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) +
                            N' AND ' + @join_alias + N'._array_index = N''' + REPLACE(@dict_key, '''', '''''') + N'''';
                    ELSE IF @array_index IS NOT NULL
                        SET @join_parts = @join_parts + N' JOIN _values ' + @join_alias + 
                            N' ON ' + @join_alias + N'._id_object = o._id AND ' + 
                            @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) +
                            N' AND ' + @join_alias + N'._array_index = N''' + REPLACE(@array_index, '''', '''''') + N'''';
                    ELSE
                        SET @join_parts = @join_parts + N' JOIN _values ' + @join_alias + 
                            N' ON ' + @join_alias + N'._id_object = o._id AND ' + 
                            @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) +
                            N' AND ' + @join_alias + N'._array_index IS NULL';
                    
                    IF @group_parts <> N'' SET @group_parts = @group_parts + N', ';
                    SET @group_parts = @group_parts + @join_alias + N'.' + @column_name;
                END
            END
        END
        
        FETCH NEXT FROM @group_cursor INTO @field, @alias;
    END
    
    CLOSE @group_cursor;
    DEALLOCATE @group_cursor;
    
    -- =========================================
    -- 2. Process aggregations
    -- =========================================
    DECLARE @agg_cursor CURSOR;
    SET @agg_cursor = CURSOR FOR
        SELECT 
            JSON_VALUE(value, '$.field') AS field,
            UPPER(JSON_VALUE(value, '$.func')) AS func,
            ISNULL(JSON_VALUE(value, '$.alias'), UPPER(JSON_VALUE(value, '$.func'))) AS alias
        FROM OPENJSON(@aggregations);
    
    OPEN @agg_cursor;
    FETCH NEXT FROM @agg_cursor INTO @field, @func, @alias;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- COUNT(*) special case
        IF @func = N'COUNT' AND (@field IS NULL OR @field = N'*' OR @field = N'')
        BEGIN
            IF @select_parts <> N'' SET @select_parts = @select_parts + N', ';
            SET @select_parts = @select_parts + N'COUNT(DISTINCT o._id) AS [' + REPLACE(@alias, '''', '''''') + N']';
            FETCH NEXT FROM @agg_cursor INTO @field, @func, @alias;
            CONTINUE;
        END
        
        IF @field IS NOT NULL
        BEGIN
            -- Check for base field (0$: prefix)
            IF @field LIKE N'0$:%'
            BEGIN
                SET @raw_field = SUBSTRING(@field, 4, 100);
                SET @sql_column = dbo.normalize_base_field_name(@raw_field);
                
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
                    SET @join_idx = @join_idx + 1;
                    SET @join_alias = N'a' + CAST(@join_idx AS NVARCHAR(10));
                    
                    SET @column_name = CASE @db_type
                        WHEN N'Long' THEN N'_Long'
                        WHEN N'Double' THEN N'_Double'
                        WHEN N'Numeric' THEN N'_Numeric'
                        ELSE N'_Long'
                    END;
                    
                    -- Build array condition
                    IF @dict_key IS NOT NULL
                        SET @array_condition = N' AND ' + @join_alias + N'._array_index = N''' + REPLACE(@dict_key, '''', '''''') + N'''';
                    ELSE IF @array_index IS NOT NULL
                        SET @array_condition = N' AND ' + @join_alias + N'._array_index = N''' + REPLACE(@array_index, '''', '''''') + N'''';
                    ELSE IF @field NOT LIKE N'%[]%'
                        SET @array_condition = N' AND ' + @join_alias + N'._array_index IS NULL';
                    ELSE
                        SET @array_condition = N'';
                    
                    SET @join_parts = @join_parts + N' LEFT JOIN _values ' + @join_alias + 
                        N' ON ' + @join_alias + N'._id_object = o._id AND ' + 
                        @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + @array_condition;
                    
                    IF @select_parts <> N'' SET @select_parts = @select_parts + N', ';
                    SET @select_parts = @select_parts + @func + N'(CAST(' + @join_alias + N'.' + @column_name + N' AS DECIMAL(38,10))) AS [' + REPLACE(@alias, '''', '''''') + N']';
                END
            END
        END
        
        FETCH NEXT FROM @agg_cursor INTO @field, @func, @alias;
    END
    
    CLOSE @agg_cursor;
    DEALLOCATE @agg_cursor;
    
    -- =========================================
    -- 3. Process filter
    -- =========================================
    IF @filter_json IS NOT NULL AND @filter_json <> N'' AND @filter_json <> N'null'
    BEGIN
        CREATE TABLE #filtered_ids (_id BIGINT PRIMARY KEY);
        
        INSERT INTO #filtered_ids
        EXEC get_filtered_object_ids @scheme_id, @filter_json, 10;
        
        IF NOT EXISTS (SELECT 1 FROM #filtered_ids)
        BEGIN
            SELECT N'[]' AS result;
            DROP TABLE #filtered_ids;
            RETURN;
        END
        
        SET @where_clause = N' AND o._id IN (SELECT _id FROM #filtered_ids)';
    END
    
    -- =========================================
    -- 4. Build and execute SQL
    -- =========================================
    DECLARE @result NVARCHAR(MAX);
    
    IF @group_parts = N'' OR @group_parts IS NULL
    BEGIN
        -- No grouping (simple aggregation) - still return array for consistency
        SET @sql = N'
            SELECT @result = (
                SELECT ' + @select_parts + N'
                FROM _objects o' + @join_parts + N'
                WHERE o._id_scheme = ' + CAST(@scheme_id AS NVARCHAR(20)) + @where_clause + N'
                FOR JSON PATH
            )';
    END
    ELSE
    BEGIN
        -- With grouping
        SET @sql = N'
            SELECT @result = (
                SELECT ' + @select_parts + N'
                FROM _objects o' + @join_parts + N'
                WHERE o._id_scheme = ' + CAST(@scheme_id AS NVARCHAR(20)) + @where_clause + N'
                GROUP BY ' + @group_parts + N'
                ORDER BY ' + @group_parts + N'
                FOR JSON PATH
            )';
    END
    
    EXEC sp_executesql @sql, N'@result NVARCHAR(MAX) OUTPUT', @result = @result OUTPUT;
    
    -- Return with 'result' column name for C# mapping
    SELECT @result AS result;
    
    IF OBJECT_ID('tempdb..#filtered_ids') IS NOT NULL
        DROP TABLE #filtered_ids;
END;
GO

-- =====================================================
-- aggregate_grouped_preview: SQL preview for debugging
-- =====================================================
CREATE PROCEDURE dbo.aggregate_grouped_preview
    @scheme_id BIGINT,
    @group_fields NVARCHAR(MAX),
    @aggregations NVARCHAR(MAX),
    @filter_json NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @select_parts NVARCHAR(MAX) = N'';
    DECLARE @join_parts NVARCHAR(MAX) = N'';
    DECLARE @group_parts NVARCHAR(MAX) = N'';
    
    DECLARE @join_idx INT = 0;
    DECLARE @join_alias NVARCHAR(10);
    DECLARE @column_name NVARCHAR(50);
    
    DECLARE @field NVARCHAR(255);
    DECLARE @alias NVARCHAR(255);
    DECLARE @func NVARCHAR(20);
    
    DECLARE @structure_id BIGINT;
    DECLARE @db_type NVARCHAR(50);
    DECLARE @dict_key NVARCHAR(255);
    DECLARE @array_index NVARCHAR(50);
    
    DECLARE @sql NVARCHAR(MAX);
    
    -- Process grouping fields
    DECLARE @group_cursor CURSOR;
    SET @group_cursor = CURSOR FOR
        SELECT 
            JSON_VALUE(value, '$.field') AS field,
            ISNULL(JSON_VALUE(value, '$.alias'), JSON_VALUE(value, '$.field')) AS alias
        FROM OPENJSON(@group_fields);
    
    OPEN @group_cursor;
    FETCH NEXT FROM @group_cursor INTO @field, @alias;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        EXEC resolve_field_path @scheme_id, @field,
            @structure_id OUTPUT, @db_type OUTPUT, @dict_key OUTPUT, @array_index OUTPUT;
        
        IF @structure_id IS NOT NULL
        BEGIN
            SET @join_idx = @join_idx + 1;
            SET @join_alias = N'g' + CAST(@join_idx AS NVARCHAR(10));
            SET @column_name = CASE @db_type WHEN N'Long' THEN N'_Long' WHEN N'String' THEN N'_String' WHEN N'ListItem' THEN N'_ListItem' ELSE N'_String' END;
            
            IF @select_parts <> N'' SET @select_parts = @select_parts + N', ';
            SET @select_parts = @select_parts + @join_alias + N'.' + @column_name + N' AS [' + REPLACE(@alias, '''', '''''') + N']';
            
            IF @dict_key IS NOT NULL
                SET @join_parts = @join_parts + N'
JOIN _values ' + @join_alias + N' ON ' + @join_alias + N'._id_object = o._id AND ' + @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' AND ' + @join_alias + N'._array_index = N''' + REPLACE(@dict_key, '''', '''''') + N'''';
            ELSE IF @array_index IS NOT NULL
                SET @join_parts = @join_parts + N'
JOIN _values ' + @join_alias + N' ON ' + @join_alias + N'._id_object = o._id AND ' + @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' AND ' + @join_alias + N'._array_index = N''' + REPLACE(@array_index, '''', '''''') + N'''';
            ELSE
                SET @join_parts = @join_parts + N'
JOIN _values ' + @join_alias + N' ON ' + @join_alias + N'._id_object = o._id AND ' + @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' AND ' + @join_alias + N'._array_index IS NULL';
            
            IF @group_parts <> N'' SET @group_parts = @group_parts + N', ';
            SET @group_parts = @group_parts + @join_alias + N'.' + @column_name;
        END
        
        FETCH NEXT FROM @group_cursor INTO @field, @alias;
    END
    
    CLOSE @group_cursor;
    DEALLOCATE @group_cursor;
    
    -- Process aggregations
    DECLARE @agg_cursor CURSOR;
    SET @agg_cursor = CURSOR FOR
        SELECT 
            JSON_VALUE(value, '$.field') AS field,
            UPPER(JSON_VALUE(value, '$.func')) AS func,
            ISNULL(JSON_VALUE(value, '$.alias'), UPPER(JSON_VALUE(value, '$.func'))) AS alias
        FROM OPENJSON(@aggregations);
    
    OPEN @agg_cursor;
    FETCH NEXT FROM @agg_cursor INTO @field, @func, @alias;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @func = N'COUNT' AND (@field IS NULL OR @field = N'*')
        BEGIN
            IF @select_parts <> N'' SET @select_parts = @select_parts + N', ';
            SET @select_parts = @select_parts + N'COUNT(DISTINCT o._id) AS [' + REPLACE(@alias, '''', '''''') + N']';
        END
        ELSE IF @field IS NOT NULL
        BEGIN
            EXEC resolve_field_path @scheme_id, @field,
                @structure_id OUTPUT, @db_type OUTPUT, @dict_key OUTPUT, @array_index OUTPUT;
            
            IF @structure_id IS NOT NULL
            BEGIN
                SET @join_idx = @join_idx + 1;
                SET @join_alias = N'a' + CAST(@join_idx AS NVARCHAR(10));
                SET @column_name = CASE @db_type WHEN N'Long' THEN N'_Long' ELSE N'_Numeric' END;
                
                SET @join_parts = @join_parts + N'
LEFT JOIN _values ' + @join_alias + N' ON ' + @join_alias + N'._id_object = o._id AND ' + @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' AND ' + @join_alias + N'._array_index IS NULL';
                
                IF @select_parts <> N'' SET @select_parts = @select_parts + N', ';
                SET @select_parts = @select_parts + @func + N'(' + @join_alias + N'.' + @column_name + N') AS [' + REPLACE(@alias, '''', '''''') + N']';
            END
        END
        
        FETCH NEXT FROM @agg_cursor INTO @field, @func, @alias;
    END
    
    CLOSE @agg_cursor;
    DEALLOCATE @agg_cursor;
    
    SET @sql = N'SELECT ' + @select_parts + N'
FROM _objects o' + @join_parts + N'
WHERE o._id_scheme = ' + CAST(@scheme_id AS NVARCHAR(20)) + N'
GROUP BY ' + @group_parts;
    
    SELECT @sql AS sql_preview;
END;
GO

-- =====================================================
-- aggregate_array_grouped: GroupBy by array elements
-- =====================================================
CREATE PROCEDURE dbo.aggregate_array_grouped
    @scheme_id BIGINT,
    @array_path NVARCHAR(255),
    @group_fields NVARCHAR(MAX),
    @aggregations NVARCHAR(MAX),
    @filter_json NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @array_struct_id BIGINT;
    DECLARE @structure_id BIGINT;
    DECLARE @db_type NVARCHAR(50);
    DECLARE @dict_key NVARCHAR(255);
    DECLARE @array_index NVARCHAR(50);
    
    DECLARE @select_parts NVARCHAR(MAX) = N'';
    DECLARE @group_parts NVARCHAR(MAX) = N'';
    DECLARE @join_parts NVARCHAR(MAX) = N'';
    
    DECLARE @join_idx INT = 0;
    DECLARE @join_alias NVARCHAR(10);
    DECLARE @column_name NVARCHAR(50);
    
    DECLARE @field NVARCHAR(255);
    DECLARE @alias NVARCHAR(255);
    DECLARE @func NVARCHAR(20);
    DECLARE @full_path NVARCHAR(500);
    
    DECLARE @sql NVARCHAR(MAX);
    
    -- 1. Get array structure_id
    EXEC resolve_field_path @scheme_id, @array_path,
        @array_struct_id OUTPUT, @db_type OUTPUT, @dict_key OUTPUT, @array_index OUTPUT;
    
    IF @array_struct_id IS NULL
    BEGIN
        RAISERROR('Array "%s" not found in scheme %d', 16, 1, @array_path, @scheme_id);
        RETURN;
    END
    
    -- 2. Process grouping fields
    DECLARE @group_cursor CURSOR;
    SET @group_cursor = CURSOR FOR
        SELECT 
            JSON_VALUE(value, '$.field') AS field,
            ISNULL(JSON_VALUE(value, '$.alias'), JSON_VALUE(value, '$.field')) AS alias
        FROM OPENJSON(@group_fields);
    
    OPEN @group_cursor;
    FETCH NEXT FROM @group_cursor INTO @field, @alias;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @full_path = @array_path + N'[].' + @field;
        
        EXEC resolve_field_path @scheme_id, @full_path,
            @structure_id OUTPUT, @db_type OUTPUT, @dict_key OUTPUT, @array_index OUTPUT;
        
        IF @structure_id IS NOT NULL
        BEGIN
            SET @join_idx = @join_idx + 1;
            SET @join_alias = N'g' + CAST(@join_idx AS NVARCHAR(10));
            SET @column_name = CASE @db_type WHEN N'Long' THEN N'_Long' WHEN N'String' THEN N'_String' WHEN N'ListItem' THEN N'_ListItem' ELSE N'_String' END;
            
            SET @join_parts = @join_parts + N' LEFT JOIN _values ' + @join_alias + 
                N' ON ' + @join_alias + N'._id_object = arr._id_object AND ' + 
                @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) +
                N' AND ' + @join_alias + N'._array_parent_id = arr._id';
            
            IF @select_parts <> N'' SET @select_parts = @select_parts + N', ';
            SET @select_parts = @select_parts + @join_alias + N'.' + @column_name + N' AS [' + REPLACE(@alias, '''', '''''') + N']';
            
            IF @group_parts <> N'' SET @group_parts = @group_parts + N', ';
            SET @group_parts = @group_parts + @join_alias + N'.' + @column_name;
        END
        
        FETCH NEXT FROM @group_cursor INTO @field, @alias;
    END
    
    CLOSE @group_cursor;
    DEALLOCATE @group_cursor;
    
    -- 3. Process aggregations
    DECLARE @agg_cursor CURSOR;
    SET @agg_cursor = CURSOR FOR
        SELECT 
            JSON_VALUE(value, '$.field') AS field,
            UPPER(JSON_VALUE(value, '$.func')) AS func,
            ISNULL(JSON_VALUE(value, '$.alias'), UPPER(JSON_VALUE(value, '$.func'))) AS alias
        FROM OPENJSON(@aggregations);
    
    OPEN @agg_cursor;
    FETCH NEXT FROM @agg_cursor INTO @field, @func, @alias;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @func = N'COUNT' AND (@field IS NULL OR @field = N'*')
        BEGIN
            IF @select_parts <> N'' SET @select_parts = @select_parts + N', ';
            SET @select_parts = @select_parts + N'COUNT(*) AS [' + REPLACE(@alias, '''', '''''') + N']';
        END
        ELSE IF @field IS NOT NULL
        BEGIN
            SET @full_path = @array_path + N'[].' + @field;
            
            EXEC resolve_field_path @scheme_id, @full_path,
                @structure_id OUTPUT, @db_type OUTPUT, @dict_key OUTPUT, @array_index OUTPUT;
            
            IF @structure_id IS NOT NULL
            BEGIN
                SET @join_idx = @join_idx + 1;
                SET @join_alias = N'a' + CAST(@join_idx AS NVARCHAR(10));
                SET @column_name = CASE @db_type WHEN N'Long' THEN N'_Long' WHEN N'Double' THEN N'_Double' ELSE N'_Long' END;
                
                SET @join_parts = @join_parts + N' LEFT JOIN _values ' + @join_alias + 
                    N' ON ' + @join_alias + N'._id_object = arr._id_object AND ' + 
                    @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) +
                    N' AND ' + @join_alias + N'._array_parent_id = arr._id';
                
                IF @select_parts <> N'' SET @select_parts = @select_parts + N', ';
                SET @select_parts = @select_parts + @func + N'(' + @join_alias + N'.' + @column_name + N') AS [' + REPLACE(@alias, '''', '''''') + N']';
            END
        END
        
        FETCH NEXT FROM @agg_cursor INTO @field, @func, @alias;
    END
    
    CLOSE @agg_cursor;
    DEALLOCATE @agg_cursor;
    
    -- 4. Build and execute SQL
    DECLARE @result NVARCHAR(MAX);
    
    SET @sql = N'
        SELECT @result = (
            SELECT ' + @select_parts + N'
            FROM _values arr
            JOIN _objects o ON o._id = arr._id_object' + @join_parts + N'
            WHERE o._id_scheme = ' + CAST(@scheme_id AS NVARCHAR(20)) + N'
              AND arr._id_structure = ' + CAST(@array_struct_id AS NVARCHAR(20)) + N'
              AND arr._array_index IS NOT NULL
            GROUP BY ' + @group_parts + N'
            FOR JSON PATH
        )';
    
    EXEC sp_executesql @sql, N'@result NVARCHAR(MAX) OUTPUT', @result = @result OUTPUT;
    
    -- Return with 'result' column name for C# mapping
    SELECT @result AS result;
END;
GO

PRINT N'=========================================';
PRINT N'Grouping procedures created!';
PRINT N'';
PRINT N'PROCEDURES:';
PRINT N'  aggregate_grouped - GroupBy with aggregations';
PRINT N'  aggregate_grouped_preview - SQL preview for debugging';
PRINT N'  aggregate_array_grouped - GroupBy by array elements';
PRINT N'';
PRINT N'FEATURES:';
PRINT N'  - Base fields with 0$: prefix';
PRINT N'  - EAV fields from _values';
PRINT N'  - Nested paths: Address.City';
PRINT N'  - Array support: Items[].Price, Items[2].Price';
PRINT N'  - Dictionary support: PhoneBook[home]';
PRINT N'  - Multiple grouping keys';
PRINT N'  - Filters via get_filtered_object_ids';
PRINT N'=========================================';
GO

