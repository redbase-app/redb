-- =====================================================
-- REDB EAV WINDOW FUNCTIONS (MSSQL)
-- Window functions for EAV model
-- ROW_NUMBER, RANK, SUM OVER, etc.
-- Ported from PostgreSQL version
-- =====================================================

SET NOCOUNT ON;
GO

-- ===== DROP EXISTING OBJECTS =====
IF OBJECT_ID('dbo.query_with_window', 'P') IS NOT NULL 
    DROP PROCEDURE dbo.query_with_window;
GO

IF OBJECT_ID('dbo.resolve_field_path', 'P') IS NOT NULL 
    DROP PROCEDURE dbo.resolve_field_path;
GO

-- =====================================================
-- Helper: resolve_field_path
-- Resolves field path to structure info
-- Supports: simple fields, nested paths (Address1.City),
-- dictionaries (PhoneBook[home]), nested dict (AddressBook[home].City)
-- =====================================================
CREATE PROCEDURE dbo.resolve_field_path
    @scheme_id BIGINT,
    @field_path NVARCHAR(255),
    @structure_id BIGINT OUTPUT,
    @db_type NVARCHAR(50) OUTPUT,
    @dict_key NVARCHAR(255) OUTPUT,
    @array_index NVARCHAR(50) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    SET @structure_id = NULL;
    SET @db_type = NULL;
    SET @dict_key = NULL;
    SET @array_index = NULL;
    
    DECLARE @remaining_path NVARCHAR(255) = @field_path;
    DECLARE @current_segment NVARCHAR(255);
    DECLARE @parent_structure_id BIGINT = NULL;
    DECLARE @dot_pos INT;
    DECLARE @bracket_pos INT;
    DECLARE @key_end INT;
    DECLARE @segment_name NVARCHAR(255);
    DECLARE @key_value NVARCHAR(255);
    
    -- Process path segments (split by dots, respecting brackets)
    WHILE @remaining_path IS NOT NULL AND LEN(@remaining_path) > 0
    BEGIN
        -- Find next segment: either up to dot or end of string
        -- But dots inside [] should be ignored
        SET @bracket_pos = CHARINDEX(N'[', @remaining_path);
        SET @dot_pos = CHARINDEX(N'.', @remaining_path);
        
        -- If bracket comes before dot, find dot after bracket end
        IF @bracket_pos > 0 AND (@dot_pos = 0 OR @bracket_pos < @dot_pos)
        BEGIN
            SET @key_end = CHARINDEX(N']', @remaining_path);
            -- Find dot after bracket end
            IF @key_end > 0
                SET @dot_pos = CHARINDEX(N'.', @remaining_path, @key_end);
        END
        
        IF @dot_pos > 0
        BEGIN
            SET @current_segment = LEFT(@remaining_path, @dot_pos - 1);
            SET @remaining_path = SUBSTRING(@remaining_path, @dot_pos + 1, LEN(@remaining_path));
        END
        ELSE
        BEGIN
            SET @current_segment = @remaining_path;
            SET @remaining_path = NULL;
        END
        
        -- Parse current segment for bracket notation
        SET @bracket_pos = CHARINDEX(N'[', @current_segment);
        IF @bracket_pos > 0
        BEGIN
            SET @key_end = CHARINDEX(N']', @current_segment);
            SET @segment_name = LEFT(@current_segment, @bracket_pos - 1);
            SET @key_value = SUBSTRING(@current_segment, @bracket_pos + 1, @key_end - @bracket_pos - 1);
            
            -- Save key when we find bracket notation (for dict/array access)
            -- This applies even for intermediate segments like AddressBook[home].City
            IF @key_value <> N'' AND @key_value NOT LIKE N'%[^0-9]%'
                SET @array_index = @key_value;
            ELSE IF @key_value <> N''
                SET @dict_key = @key_value;
        END
        ELSE
        BEGIN
            SET @segment_name = @current_segment;
        END
        
        -- Find structure for this segment
        IF @parent_structure_id IS NULL
        BEGIN
            -- Top-level field
            SELECT TOP 1
                @structure_id = c._structure_id,
                @db_type = c.db_type
            FROM _scheme_metadata_cache c
            WHERE c._scheme_id = @scheme_id
              AND c._name = @segment_name
              AND c._parent_structure_id IS NULL;
        END
        ELSE
        BEGIN
            -- Nested field
            SELECT TOP 1
                @structure_id = c._structure_id,
                @db_type = c.db_type
            FROM _scheme_metadata_cache c
            WHERE c._scheme_id = @scheme_id
              AND c._name = @segment_name
              AND c._parent_structure_id = @parent_structure_id;
        END
        
        -- If not found, try to find by full path (for backward compatibility)
        IF @structure_id IS NULL AND @parent_structure_id IS NULL
        BEGIN
            SELECT TOP 1
                @structure_id = c._structure_id,
                @db_type = c.db_type
            FROM _scheme_metadata_cache c
            WHERE c._scheme_id = @scheme_id
              AND c._name = @field_path
              AND c._parent_structure_id IS NULL;
            
            IF @structure_id IS NOT NULL
                RETURN; -- Found by full path, exit
        END
        
        IF @structure_id IS NULL
            RETURN; -- Segment not found
        
        -- Move to next level
        SET @parent_structure_id = @structure_id;
    END
END;
GO

-- =====================================================
-- query_with_window: Query with window functions
-- =====================================================
-- Parameters:
--   @scheme_id      - Scheme ID
--   @select_fields  - JSON array of SELECT fields:
--                     [{"field":"Name","alias":"Name"}]
--   @window_funcs   - JSON array of window functions:
--                     [{"func":"ROW_NUMBER","alias":"Rank"}]
--   @partition_by   - JSON array for PARTITION BY:
--                     [{"field":"Category"}]
--   @order_by       - JSON array for ORDER BY inside window:
--                     [{"field":"Stock","dir":"DESC"}]
--   @filter_json    - JSON filter (optional)
--   @limit          - Record limit
--   @frame_json     - Frame specification (optional)
--
-- Returns: JSON array of objects with window functions
-- =====================================================
CREATE PROCEDURE dbo.query_with_window
    @scheme_id BIGINT,
    @select_fields NVARCHAR(MAX),
    @window_funcs NVARCHAR(MAX),
    @partition_by NVARCHAR(MAX) = N'[]',
    @order_by NVARCHAR(MAX) = N'[]',
    @filter_json NVARCHAR(MAX) = NULL,
    @limit INT = 1000,
    @frame_json NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @select_parts NVARCHAR(MAX) = N'';
    DECLARE @join_parts NVARCHAR(MAX) = N'';
    DECLARE @partition_parts NVARCHAR(MAX) = N'';
    DECLARE @order_parts NVARCHAR(MAX) = N'';
    DECLARE @over_clause NVARCHAR(MAX);
    DECLARE @frame_clause NVARCHAR(500) = N'';
    DECLARE @where_clause NVARCHAR(MAX) = N'';
    
    DECLARE @join_idx INT = 0;
    DECLARE @join_alias NVARCHAR(10);
    DECLARE @column_name NVARCHAR(50);
    
    DECLARE @field_path NVARCHAR(255);
    DECLARE @alias NVARCHAR(255);
    DECLARE @func_name NVARCHAR(50);
    DECLARE @dir NVARCHAR(10);
    DECLARE @buckets INT;
    
    DECLARE @structure_id BIGINT;
    DECLARE @db_type NVARCHAR(50);
    DECLARE @dict_key NVARCHAR(255);
    DECLARE @array_index NVARCHAR(50);
    
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @result NVARCHAR(MAX);
    
    -- =========================================
    -- 1. Base SELECT fields
    -- =========================================
    SET @select_parts = N'o._id AS [id], o._name AS [name]';
    
    -- Process select fields from JSON
    DECLARE @field_cursor CURSOR;
    SET @field_cursor = CURSOR FOR
        SELECT 
            JSON_VALUE(value, '$.field') AS field_path,
            ISNULL(JSON_VALUE(value, '$.alias'), JSON_VALUE(value, '$.field')) AS alias
        FROM OPENJSON(@select_fields);
    
    OPEN @field_cursor;
    FETCH NEXT FROM @field_cursor INTO @field_path, @alias;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @field_path IS NOT NULL
        BEGIN
            -- Check for base field (0$: prefix)
            IF @field_path LIKE N'0$:%'
            BEGIN
                DECLARE @raw_field NVARCHAR(100) = SUBSTRING(@field_path, 4, 100);
                DECLARE @sql_column NVARCHAR(100) = dbo.normalize_base_field_name(@raw_field);
                
                IF @sql_column IS NOT NULL
                    SET @select_parts = @select_parts + N', o.' + @sql_column + N' AS [' + @alias + N']';
            END
            ELSE
            BEGIN
                -- EAV field from _values
                EXEC resolve_field_path @scheme_id, @field_path, 
                    @structure_id OUTPUT, @db_type OUTPUT, @dict_key OUTPUT, @array_index OUTPUT;
                
                IF @structure_id IS NOT NULL
                BEGIN
                    SET @join_idx = @join_idx + 1;
                    SET @join_alias = N's' + CAST(@join_idx AS NVARCHAR(10));
                    
                    SET @column_name = CASE @db_type
                        WHEN N'Long' THEN N'_Long'
                        WHEN N'String' THEN N'_String'
                        WHEN N'Double' THEN N'_Double'
                        WHEN N'Numeric' THEN N'_Numeric'
                        WHEN N'DateTimeOffset' THEN N'_DateTimeOffset'
                        WHEN N'Boolean' THEN N'_Boolean'
                        ELSE N'_String'
                    END;
                    
                    SET @select_parts = @select_parts + N', ' + @join_alias + N'.' + @column_name + N' AS [' + @alias + N']';
                    
                    -- Build JOIN
                    IF @dict_key IS NOT NULL
                        SET @join_parts = @join_parts + N' LEFT JOIN _values ' + @join_alias + 
                            N' ON ' + @join_alias + N'._id_object = o._id AND ' + 
                            @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) +
                            N' AND ' + @join_alias + N'._array_index = N''' + @dict_key + N'''';
                    ELSE IF @array_index IS NOT NULL
                        SET @join_parts = @join_parts + N' LEFT JOIN _values ' + @join_alias + 
                            N' ON ' + @join_alias + N'._id_object = o._id AND ' + 
                            @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) +
                            N' AND ' + @join_alias + N'._array_index = N''' + @array_index + N'''';
                    ELSE
                        SET @join_parts = @join_parts + N' LEFT JOIN _values ' + @join_alias + 
                            N' ON ' + @join_alias + N'._id_object = o._id AND ' + 
                            @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) +
                            N' AND ' + @join_alias + N'._array_index IS NULL';
                END
            END
        END
        
        FETCH NEXT FROM @field_cursor INTO @field_path, @alias;
    END
    
    CLOSE @field_cursor;
    DEALLOCATE @field_cursor;
    
    -- =========================================
    -- 2. PARTITION BY
    -- =========================================
    SET @field_cursor = CURSOR FOR
        SELECT JSON_VALUE(value, '$.field') AS field_path
        FROM OPENJSON(@partition_by);
    
    OPEN @field_cursor;
    FETCH NEXT FROM @field_cursor INTO @field_path;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @field_path IS NOT NULL
        BEGIN
            IF @field_path LIKE N'0$:%'
            BEGIN
                SET @raw_field = SUBSTRING(@field_path, 4, 100);
                SET @sql_column = dbo.normalize_base_field_name(@raw_field);
                
                IF @sql_column IS NOT NULL
                BEGIN
                    IF @partition_parts <> N'' SET @partition_parts = @partition_parts + N', ';
                    SET @partition_parts = @partition_parts + N'o.' + @sql_column;
                END
            END
            ELSE
            BEGIN
                EXEC resolve_field_path @scheme_id, @field_path, 
                    @structure_id OUTPUT, @db_type OUTPUT, @dict_key OUTPUT, @array_index OUTPUT;
                
                IF @structure_id IS NOT NULL
                BEGIN
                    SET @join_idx = @join_idx + 1;
                    SET @join_alias = N'p' + CAST(@join_idx AS NVARCHAR(10));
                    
                    SET @column_name = CASE @db_type
                        WHEN N'Long' THEN N'_Long'
                        WHEN N'String' THEN N'_String'
                        WHEN N'Double' THEN N'_Double'
                        WHEN N'Numeric' THEN N'_Numeric'
                        WHEN N'DateTimeOffset' THEN N'_DateTimeOffset'
                        WHEN N'Boolean' THEN N'_Boolean'
                        ELSE N'_String'
                    END;
                    
                    IF @partition_parts <> N'' SET @partition_parts = @partition_parts + N', ';
                    SET @partition_parts = @partition_parts + @join_alias + N'.' + @column_name;
                    
                    -- Build JOIN
                    IF @dict_key IS NOT NULL
                        SET @join_parts = @join_parts + N' LEFT JOIN _values ' + @join_alias + 
                            N' ON ' + @join_alias + N'._id_object = o._id AND ' + 
                            @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) +
                            N' AND ' + @join_alias + N'._array_index = N''' + @dict_key + N'''';
                    ELSE IF @array_index IS NOT NULL
                        SET @join_parts = @join_parts + N' LEFT JOIN _values ' + @join_alias + 
                            N' ON ' + @join_alias + N'._id_object = o._id AND ' + 
                            @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) +
                            N' AND ' + @join_alias + N'._array_index = N''' + @array_index + N'''';
                    ELSE
                        SET @join_parts = @join_parts + N' LEFT JOIN _values ' + @join_alias + 
                            N' ON ' + @join_alias + N'._id_object = o._id AND ' + 
                            @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) +
                            N' AND ' + @join_alias + N'._array_index IS NULL';
                END
            END
        END
        
        FETCH NEXT FROM @field_cursor INTO @field_path;
    END
    
    CLOSE @field_cursor;
    DEALLOCATE @field_cursor;
    
    -- =========================================
    -- 3. ORDER BY inside window
    -- =========================================
    SET @field_cursor = CURSOR FOR
        SELECT 
            JSON_VALUE(value, '$.field') AS field_path,
            ISNULL(UPPER(JSON_VALUE(value, '$.dir')), N'ASC') AS dir
        FROM OPENJSON(@order_by);
    
    OPEN @field_cursor;
    FETCH NEXT FROM @field_cursor INTO @field_path, @dir;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @field_path IS NOT NULL
        BEGIN
            IF @field_path LIKE N'0$:%'
            BEGIN
                SET @raw_field = SUBSTRING(@field_path, 4, 100);
                SET @sql_column = dbo.normalize_base_field_name(@raw_field);
                
                IF @sql_column IS NOT NULL
                BEGIN
                    IF @order_parts <> N'' SET @order_parts = @order_parts + N', ';
                    SET @order_parts = @order_parts + N'o.' + @sql_column + N' ' + @dir;
                END
            END
            ELSE
            BEGIN
                EXEC resolve_field_path @scheme_id, @field_path, 
                    @structure_id OUTPUT, @db_type OUTPUT, @dict_key OUTPUT, @array_index OUTPUT;
                
                IF @structure_id IS NOT NULL
                BEGIN
                    SET @join_idx = @join_idx + 1;
                    SET @join_alias = N'w' + CAST(@join_idx AS NVARCHAR(10));
                    
                    SET @column_name = CASE @db_type
                        WHEN N'Long' THEN N'_Long'
                        WHEN N'Double' THEN N'_Double'
                        WHEN N'Numeric' THEN N'_Numeric'
                        WHEN N'DateTimeOffset' THEN N'_DateTimeOffset'
                        WHEN N'Boolean' THEN N'_Boolean'
                        ELSE N'_String'
                    END;
                    
                    IF @order_parts <> N'' SET @order_parts = @order_parts + N', ';
                    SET @order_parts = @order_parts + @join_alias + N'.' + @column_name + N' ' + @dir;
                    
                    -- Build JOIN
                    IF @dict_key IS NOT NULL
                        SET @join_parts = @join_parts + N' LEFT JOIN _values ' + @join_alias + 
                            N' ON ' + @join_alias + N'._id_object = o._id AND ' + 
                            @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) +
                            N' AND ' + @join_alias + N'._array_index = N''' + @dict_key + N'''';
                    ELSE IF @array_index IS NOT NULL
                        SET @join_parts = @join_parts + N' LEFT JOIN _values ' + @join_alias + 
                            N' ON ' + @join_alias + N'._id_object = o._id AND ' + 
                            @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) +
                            N' AND ' + @join_alias + N'._array_index = N''' + @array_index + N'''';
                    ELSE
                        SET @join_parts = @join_parts + N' LEFT JOIN _values ' + @join_alias + 
                            N' ON ' + @join_alias + N'._id_object = o._id AND ' + 
                            @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) +
                            N' AND ' + @join_alias + N'._array_index IS NULL';
                END
            END
        END
        
        FETCH NEXT FROM @field_cursor INTO @field_path, @dir;
    END
    
    CLOSE @field_cursor;
    DEALLOCATE @field_cursor;
    
    -- =========================================
    -- 4. Build OVER clause (with ROWS BETWEEN support)
    -- =========================================
    IF @frame_json IS NOT NULL AND @frame_json <> N'' AND @frame_json <> N'null'
    BEGIN
        DECLARE @frame_type NVARCHAR(10) = ISNULL(UPPER(JSON_VALUE(@frame_json, '$.type')), N'ROWS');
        DECLARE @start_kind NVARCHAR(50) = UPPER(JSON_VALUE(@frame_json, '$.start.kind'));
        DECLARE @start_offset INT = ISNULL(CAST(JSON_VALUE(@frame_json, '$.start.offset') AS INT), 0);
        DECLARE @end_kind NVARCHAR(50) = UPPER(JSON_VALUE(@frame_json, '$.end.kind'));
        DECLARE @end_offset INT = ISNULL(CAST(JSON_VALUE(@frame_json, '$.end.offset') AS INT), 0);
        
        SET @frame_clause = @frame_type + N' BETWEEN ';
        
        -- Start bound
        SET @frame_clause = @frame_clause + CASE @start_kind
            WHEN N'UNBOUNDEDPRECEDING' THEN N'UNBOUNDED PRECEDING'
            WHEN N'CURRENTROW' THEN N'CURRENT ROW'
            WHEN N'PRECEDING' THEN CAST(@start_offset AS NVARCHAR(10)) + N' PRECEDING'
            WHEN N'FOLLOWING' THEN CAST(@start_offset AS NVARCHAR(10)) + N' FOLLOWING'
            ELSE N'UNBOUNDED PRECEDING'
        END;
        
        SET @frame_clause = @frame_clause + N' AND ';
        
        -- End bound
        SET @frame_clause = @frame_clause + CASE @end_kind
            WHEN N'UNBOUNDEDFOLLOWING' THEN N'UNBOUNDED FOLLOWING'
            WHEN N'CURRENTROW' THEN N'CURRENT ROW'
            WHEN N'PRECEDING' THEN CAST(@end_offset AS NVARCHAR(10)) + N' PRECEDING'
            WHEN N'FOLLOWING' THEN CAST(@end_offset AS NVARCHAR(10)) + N' FOLLOWING'
            ELSE N'CURRENT ROW'
        END;
    END
    
    -- Build OVER clause
    SET @over_clause = N'OVER (';
    IF @partition_parts <> N''
        SET @over_clause = @over_clause + N'PARTITION BY ' + @partition_parts;
    IF @order_parts <> N''
    BEGIN
        IF @partition_parts <> N'' SET @over_clause = @over_clause + N' ';
        SET @over_clause = @over_clause + N'ORDER BY ' + @order_parts;
    END
    IF @frame_clause <> N''
    BEGIN
        IF @partition_parts <> N'' OR @order_parts <> N'' SET @over_clause = @over_clause + N' ';
        SET @over_clause = @over_clause + @frame_clause;
    END
    SET @over_clause = @over_clause + N')';
    
    -- =========================================
    -- 5. Window functions
    -- =========================================
    SET @field_cursor = CURSOR FOR
        SELECT 
            UPPER(JSON_VALUE(value, '$.func')) AS func_name,
            ISNULL(JSON_VALUE(value, '$.alias'), UPPER(JSON_VALUE(value, '$.func'))) AS alias,
            JSON_VALUE(value, '$.field') AS field_path,
            ISNULL(CAST(JSON_VALUE(value, '$.buckets') AS INT), 4) AS buckets
        FROM OPENJSON(@window_funcs);
    
    OPEN @field_cursor;
    FETCH NEXT FROM @field_cursor INTO @func_name, @alias, @field_path, @buckets;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @func_name IN (N'ROW_NUMBER', N'RANK', N'DENSE_RANK', N'COUNT')
        BEGIN
            -- Ranking functions (no field)
            IF @func_name = N'COUNT'
                SET @select_parts = @select_parts + N', COUNT(*) ' + @over_clause + N' AS [' + @alias + N']';
            ELSE
                SET @select_parts = @select_parts + N', ' + @func_name + N'() ' + @over_clause + N' AS [' + @alias + N']';
        END
        ELSE IF @func_name = N'NTILE'
        BEGIN
            SET @select_parts = @select_parts + N', NTILE(' + CAST(@buckets AS NVARCHAR(10)) + N') ' + @over_clause + N' AS [' + @alias + N']';
        END
        ELSE IF @func_name IN (N'SUM', N'AVG', N'MIN', N'MAX', N'LAG', N'LEAD', N'FIRST_VALUE', N'LAST_VALUE')
        BEGIN
            IF @field_path IS NOT NULL AND @field_path <> N''
            BEGIN
                EXEC resolve_field_path @scheme_id, @field_path, 
                    @structure_id OUTPUT, @db_type OUTPUT, @dict_key OUTPUT, @array_index OUTPUT;
                
                IF @structure_id IS NOT NULL
                BEGIN
                    SET @join_idx = @join_idx + 1;
                    SET @join_alias = N'f' + CAST(@join_idx AS NVARCHAR(10));
                    
                    SET @column_name = CASE @db_type
                        WHEN N'Long' THEN N'_Long'
                        WHEN N'Double' THEN N'_Double'
                        WHEN N'Numeric' THEN N'_Numeric'
                        WHEN N'String' THEN N'_String'
                        WHEN N'DateTimeOffset' THEN N'_DateTimeOffset'
                        WHEN N'Boolean' THEN N'_Boolean'
                        ELSE N'_Long'
                    END;
                    
                    -- Build JOIN
                    IF @dict_key IS NOT NULL
                        SET @join_parts = @join_parts + N' LEFT JOIN _values ' + @join_alias + 
                            N' ON ' + @join_alias + N'._id_object = o._id AND ' + 
                            @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) +
                            N' AND ' + @join_alias + N'._array_index = N''' + @dict_key + N'''';
                    ELSE IF @array_index IS NOT NULL
                        SET @join_parts = @join_parts + N' LEFT JOIN _values ' + @join_alias + 
                            N' ON ' + @join_alias + N'._id_object = o._id AND ' + 
                            @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) +
                            N' AND ' + @join_alias + N'._array_index = N''' + @array_index + N'''';
                    ELSE
                        SET @join_parts = @join_parts + N' LEFT JOIN _values ' + @join_alias + 
                            N' ON ' + @join_alias + N'._id_object = o._id AND ' + 
                            @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) +
                            N' AND ' + @join_alias + N'._array_index IS NULL';
                    
                    SET @select_parts = @select_parts + N', ' + @func_name + N'(' + @join_alias + N'.' + @column_name + N') ' + @over_clause + N' AS [' + @alias + N']';
                END
            END
        END
        
        FETCH NEXT FROM @field_cursor INTO @func_name, @alias, @field_path, @buckets;
    END
    
    CLOSE @field_cursor;
    DEALLOCATE @field_cursor;
    
    -- =========================================
    -- 6. Filter
    -- =========================================
    IF @filter_json IS NOT NULL AND @filter_json <> N'' AND @filter_json <> N'null'
    BEGIN
        -- Use internal_parse_filters to get WHERE clause
        DECLARE @filter_result NVARCHAR(MAX);
        EXEC internal_parse_filters @scheme_id, @filter_json, N'o', N'AND', 0, @filter_result OUTPUT;
        
        IF @filter_result IS NOT NULL AND @filter_result <> N''
            SET @where_clause = N' AND ' + @filter_result;
    END
    
    -- =========================================
    -- 7. Build and execute SQL
    -- =========================================
    SET @sql = N'
        SELECT @result = (
            SELECT ' + @select_parts + N'
            FROM _objects o' + @join_parts + N'
            WHERE o._id_scheme = ' + CAST(@scheme_id AS NVARCHAR(20));
    
    IF @where_clause IS NOT NULL AND @where_clause <> N''
        SET @sql = @sql + @where_clause;
    
    SET @sql = @sql + N'
            ORDER BY o._id
            OFFSET 0 ROWS FETCH NEXT ' + CAST(@limit AS NVARCHAR(20)) + N' ROWS ONLY
            FOR JSON PATH
        )';
    
    EXEC sp_executesql @sql, N'@result NVARCHAR(MAX) OUTPUT', @result = @result OUTPUT;
    
    SELECT ISNULL(@result, N'[]') AS result;
END;
GO

PRINT N'=========================================';
PRINT N'Window Functions procedures created!';
PRINT N'';
PRINT N'PROCEDURES:';
PRINT N'  query_with_window - Query with window functions';
PRINT N'  resolve_field_path - Helper to resolve field paths';
PRINT N'';
PRINT N'SUPPORTED FUNCTIONS:';
PRINT N'  Ranking: ROW_NUMBER, RANK, DENSE_RANK, NTILE';
PRINT N'  Aggregate: SUM, AVG, MIN, MAX, COUNT';
PRINT N'  Offset: LAG, LEAD, FIRST_VALUE, LAST_VALUE';
PRINT N'';
PRINT N'FEATURES:';
PRINT N'  - Base fields with 0$: prefix';
PRINT N'  - EAV fields from _values';
PRINT N'  - Dictionary support: PhoneBook[home]';
PRINT N'  - Array index support: Items[2]';
PRINT N'  - ROWS BETWEEN frame support';
PRINT N'=========================================';
GO

