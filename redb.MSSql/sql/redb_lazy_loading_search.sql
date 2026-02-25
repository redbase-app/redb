-- ===== LAZY LOADING SUPPORT FOR FACET SEARCH (MSSQL) =====
-- Functions for returning base objects WITHOUT Props
-- Ported from PostgreSQL version
-- Original functions (search_objects_with_facets, search_tree_objects_with_facets) remain unchanged

SET NOCOUNT ON;
GO

-- ===== DROP EXISTING OBJECTS =====
IF OBJECT_ID('dbo.get_object_base_fields', 'FN') IS NOT NULL 
    DROP FUNCTION dbo.get_object_base_fields;
GO

IF OBJECT_ID('dbo.execute_objects_query_base', 'P') IS NOT NULL 
    DROP PROCEDURE dbo.execute_objects_query_base;
GO

IF OBJECT_ID('dbo.search_objects_with_facets_base', 'P') IS NOT NULL 
    DROP PROCEDURE dbo.search_objects_with_facets_base;
GO

IF OBJECT_ID('dbo.get_filtered_object_ids', 'P') IS NOT NULL 
    DROP PROCEDURE dbo.get_filtered_object_ids;
GO

IF OBJECT_ID('dbo.search_tree_objects_with_facets_base', 'P') IS NOT NULL 
    DROP PROCEDURE dbo.search_tree_objects_with_facets_base;
GO

IF OBJECT_ID('dbo.get_search_sql_preview_base', 'P') IS NOT NULL 
    DROP PROCEDURE dbo.get_search_sql_preview_base;
GO

IF OBJECT_ID('dbo.get_search_tree_sql_preview_base', 'P') IS NOT NULL 
    DROP PROCEDURE dbo.get_search_tree_sql_preview_base;
GO

-- ========== FUNCTION 1: Return base fields WITHOUT Props ==========
-- Returns JSON object with base fields only (no properties from _values)
CREATE FUNCTION dbo.get_object_base_fields(@object_id BIGINT)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    DECLARE @result NVARCHAR(MAX);
    
    SELECT @result = (
        SELECT 
            o._id AS [id],
            o._name AS [name],
            o._id_scheme AS [scheme_id],
            o._id_parent AS [parent_id],
            o._id_owner AS [owner_id],
            o._id_who_change AS [who_change_id],
            o._date_create AS [date_create],
            o._date_modify AS [date_modify],
            o._date_begin AS [date_begin],
            o._date_complete AS [date_complete],
            o._key AS [key],
            o._value_long AS [value_long],
            o._value_string AS [value_string],
            o._value_guid AS [value_guid],
            o._note AS [note],
            o._value_bool AS [value_bool],
            o._value_double AS [value_double],
            o._value_numeric AS [value_numeric],
            o._value_datetime AS [value_datetime],
            o._value_bytes AS [value_bytes],
            o._hash AS [hash]
        FROM _objects o
        WHERE o._id = @object_id
        FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    );
    
    RETURN @result;
END;
GO

-- ========== PROCEDURE 2: Execute query with base fields ==========
-- Returns search results with base objects (no Props)
CREATE PROCEDURE dbo.execute_objects_query_base
    @scheme_id BIGINT,
    @base_conditions NVARCHAR(MAX),
    @hierarchical_conditions NVARCHAR(MAX),
    @order_conditions NVARCHAR(MAX),
    @limit_count INT = NULL,
    @offset_count INT = 0,
    @distinct_hash BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @count_sql NVARCHAR(MAX);
    DECLARE @final_where NVARCHAR(MAX);
    DECLARE @objects_json NVARCHAR(MAX);
    DECLARE @total_count INT;
    DECLARE @order_clause NVARCHAR(MAX);
    
    -- Build WHERE clause
    SET @final_where = N'WHERE obj._id_scheme = ' + CAST(@scheme_id AS NVARCHAR(20));
    IF @base_conditions IS NOT NULL AND @base_conditions <> N''
        SET @final_where = @final_where + N' AND ' + @base_conditions;
    IF @hierarchical_conditions IS NOT NULL AND @hierarchical_conditions <> N''
        SET @final_where = @final_where + N' AND ' + @hierarchical_conditions;
    
    -- Build ORDER clause
    IF @order_conditions IS NOT NULL AND @order_conditions <> N''
        SET @order_clause = @order_conditions;
    ELSE
        SET @order_clause = N'ORDER BY obj._id';
    
    -- Handle DISTINCT by hash using ROW_NUMBER
    IF @distinct_hash = 1
    BEGIN
        -- DISTINCT ON equivalent: use ROW_NUMBER to get first row per hash
        SET @sql = N'
            SELECT @result = (
                SELECT 
                    o._id AS [id],
                    o._name AS [name],
                    o._id_scheme AS [scheme_id],
                    o._id_parent AS [parent_id],
                    o._id_owner AS [owner_id],
                    o._id_who_change AS [who_change_id],
                    o._date_create AS [date_create],
                    o._date_modify AS [date_modify],
                    o._date_begin AS [date_begin],
                    o._date_complete AS [date_complete],
                    o._key AS [key],
                    o._value_long AS [value_long],
                    o._value_string AS [value_string],
                    o._value_guid AS [value_guid],
                    o._note AS [note],
                    o._value_bool AS [value_bool],
                    o._value_double AS [value_double],
                    o._value_numeric AS [value_numeric],
                    o._value_datetime AS [value_datetime],
                    o._value_bytes AS [value_bytes],
                    o._hash AS [hash]
                FROM (
                    SELECT obj._id, obj._hash,
                           ROW_NUMBER() OVER (PARTITION BY obj._hash ORDER BY obj._id) AS rn
                    FROM _objects obj
                    ' + @final_where + N'
                ) ranked
                JOIN _objects o ON o._id = ranked._id
                WHERE ranked.rn = 1';
        
        -- Add ORDER BY for outer query (must use 'o' alias, not 'obj')
        IF @order_conditions IS NOT NULL AND @order_conditions <> N''
            SET @sql = @sql + N' ' + REPLACE(REPLACE(@order_conditions, N'obj.', N'o.'), N'obj._', N'o._');
        ELSE
            SET @sql = @sql + N' ORDER BY o._id';
        
        -- Add pagination
        IF @limit_count IS NOT NULL AND @limit_count > 0
            SET @sql = @sql + N' OFFSET ' + CAST(@offset_count AS NVARCHAR(20)) + 
                              N' ROWS FETCH NEXT ' + CAST(@limit_count AS NVARCHAR(20)) + N' ROWS ONLY';
        ELSE
            SET @sql = @sql + N' OFFSET 0 ROWS';  -- Allow ORDER BY without FETCH for count queries
        
        SET @sql = @sql + N' FOR JSON PATH)';
        
        -- Count query for distinct
        SET @count_sql = N'SELECT @cnt = COUNT(DISTINCT obj._hash) FROM _objects obj ' + @final_where;
    END
    ELSE
    BEGIN
        -- Standard query without distinct
        SET @sql = N'
            SELECT @result = (
                SELECT 
                    o._id AS [id],
                    o._name AS [name],
                    o._id_scheme AS [scheme_id],
                    o._id_parent AS [parent_id],
                    o._id_owner AS [owner_id],
                    o._id_who_change AS [who_change_id],
                    o._date_create AS [date_create],
                    o._date_modify AS [date_modify],
                    o._date_begin AS [date_begin],
                    o._date_complete AS [date_complete],
                    o._key AS [key],
                    o._value_long AS [value_long],
                    o._value_string AS [value_string],
                    o._value_guid AS [value_guid],
                    o._note AS [note],
                    o._value_bool AS [value_bool],
                    o._value_double AS [value_double],
                    o._value_numeric AS [value_numeric],
                    o._value_datetime AS [value_datetime],
                    o._value_bytes AS [value_bytes],
                    o._hash AS [hash]
                FROM _objects o
                JOIN (
                    SELECT obj._id
                    FROM _objects obj
                    ' + @final_where + N'
                    ' + @order_clause;
        
        -- Add pagination
        IF @limit_count IS NOT NULL AND @limit_count > 0
            SET @sql = @sql + N' OFFSET ' + CAST(@offset_count AS NVARCHAR(20)) + 
                              N' ROWS FETCH NEXT ' + CAST(@limit_count AS NVARCHAR(20)) + N' ROWS ONLY';
        ELSE
            SET @sql = @sql + N' OFFSET 0 ROWS';  -- Allow ORDER BY without FETCH for count queries
        
        SET @sql = @sql + N'
                ) sub ON sub._id = o._id
                FOR JSON PATH)';
        
        -- Count query
        SET @count_sql = N'SELECT @cnt = COUNT(*) FROM _objects obj ' + @final_where;
    END
    
    -- Execute count query first
    EXEC sp_executesql @count_sql, N'@cnt INT OUTPUT', @cnt = @total_count OUTPUT;
    
    -- OPTIMIZATION: For CountAsync (limit = 0), skip objects loading
    IF @limit_count IS NULL OR @limit_count > 0
    BEGIN
        EXEC sp_executesql @sql, N'@result NVARCHAR(MAX) OUTPUT', @result = @objects_json OUTPUT;
    END;
    
    -- Return result as JSON with 'result' column for C# mapping
    -- Use JSON_QUERY to embed JSON arrays properly (not as escaped strings)
    SELECT ISNULL((
        SELECT 
            JSON_QUERY(ISNULL(@objects_json, N'[]')) AS objects,
            ISNULL(@total_count, 0) AS total_count,
            ISNULL(@limit_count, 2147483647) AS [limit],
            ISNULL(@offset_count, 0) AS [offset],
            JSON_QUERY(N'[]') AS facets
        FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    ), N'{"objects":[],"total_count":0,"limit":2147483647,"offset":0,"facets":[]}') AS result;
END;
GO

-- ========== PROCEDURE 3: Search with facets (base objects) ==========
-- Faceted search returning base objects WITHOUT Props (7 parameters - for lazy loading)
CREATE PROCEDURE dbo.search_objects_with_facets_base
    @scheme_id BIGINT,
    @facet_filters NVARCHAR(MAX) = NULL,
    @limit_count INT = NULL,
    @offset_count INT = 0,
    @order_by NVARCHAR(MAX) = NULL,
    @max_recursion_depth INT = 10,
    @distinct_hash BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @base_conditions NVARCHAR(MAX);
    DECLARE @hierarchical_conditions NVARCHAR(MAX);
    DECLARE @order_conditions NVARCHAR(MAX);
    
    -- Build conditions using existing procedures from redb_facets_search.sql
    EXEC dbo.internal_parse_filters @scheme_id, @facet_filters, N'obj', N'AND', 0, @base_conditions OUTPUT;
    
    -- Build order conditions
    SET @order_conditions = dbo.build_order_by_clause(@order_by, N'obj');
    
    -- Execute base query
    EXEC execute_objects_query_base 
        @scheme_id,
        @base_conditions,
        NULL, -- hierarchical_conditions not implemented yet
        @order_conditions,
        @limit_count,
        @offset_count,
        @distinct_hash;
END;
GO

-- NOTE: search_objects_with_facets (8 params) already exists in redb_facets_search.sql
-- We only define search_objects_with_facets_base (7 params) here for lazy loading

-- ========== PROCEDURE 4: Get filtered object IDs only ==========
-- Optimized for aggregations - returns only IDs without JSON overhead
CREATE PROCEDURE dbo.get_filtered_object_ids
    @scheme_id BIGINT,
    @filter_json NVARCHAR(MAX) = NULL,
    @max_recursion_depth INT = 10
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @base_conditions NVARCHAR(MAX);
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @final_where NVARCHAR(MAX);
    
    -- Build conditions
    EXEC dbo.internal_parse_filters @scheme_id, @filter_json, N'obj', N'AND', 0, @base_conditions OUTPUT;
    
    -- Build WHERE clause
    SET @final_where = N'WHERE obj._id_scheme = ' + CAST(@scheme_id AS NVARCHAR(20));
    IF @base_conditions IS NOT NULL AND @base_conditions <> N''
        SET @final_where = @final_where + N' AND ' + @base_conditions;
    
    -- Return only IDs
    SET @sql = N'SELECT obj._id FROM _objects obj ' + @final_where;
    
    EXEC sp_executesql @sql;
END;
GO

-- ========== PROCEDURE 5: Tree search (base objects) ==========
-- Tree search returning base objects WITHOUT Props
CREATE PROCEDURE dbo.search_tree_objects_with_facets_base
    @scheme_id BIGINT,
    @parent_ids NVARCHAR(MAX), -- Comma-separated list of parent IDs
    @facet_filters NVARCHAR(MAX) = NULL,
    @limit_count INT = NULL,
    @offset_count INT = 0,
    @order_by NVARCHAR(MAX) = NULL,
    @max_depth INT = 10,
    @max_recursion_depth INT = 10
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @count_sql NVARCHAR(MAX);
    DECLARE @base_conditions NVARCHAR(MAX);
    DECLARE @order_conditions NVARCHAR(MAX);
    DECLARE @objects_json NVARCHAR(MAX);
    DECLARE @total_count INT;
    
    -- Ensure metadata cache is populated
    IF NOT EXISTS (SELECT 1 FROM _scheme_metadata_cache WHERE _scheme_id = @scheme_id)
    BEGIN
        EXEC sync_metadata_cache_for_scheme @scheme_id;
    END
    
    -- Build conditions (use 'o' alias - all conditions on outer _objects table)
    EXEC dbo.internal_parse_filters @scheme_id, @facet_filters, N'o', N'AND', 0, @base_conditions OUTPUT;
    
    -- Build order conditions (use 'o' alias - ORDER BY is on outer level)
    SET @order_conditions = dbo.build_order_by_clause(@order_by, N'o');
    IF @order_conditions IS NULL OR @order_conditions = N''
        SET @order_conditions = N'ORDER BY o._id';
    
    -- Direct children only (max_depth = 1)
    IF @max_depth = 1
    BEGIN
        SET @sql = N'
            SELECT @result = (
                SELECT 
                    o._id AS [id],
                    o._name AS [name],
                    o._id_scheme AS [scheme_id],
                    o._id_parent AS [parent_id],
                    o._id_owner AS [owner_id],
                    o._id_who_change AS [who_change_id],
                    o._date_create AS [date_create],
                    o._date_modify AS [date_modify],
                    o._date_begin AS [date_begin],
                    o._date_complete AS [date_complete],
                    o._key AS [key],
                    o._value_long AS [value_long],
                    o._value_string AS [value_string],
                    o._value_guid AS [value_guid],
                    o._note AS [note],
                    o._value_bool AS [value_bool],
                    o._value_double AS [value_double],
                    o._value_numeric AS [value_numeric],
                    o._value_datetime AS [value_datetime],
                    o._value_bytes AS [value_bytes],
                    o._hash AS [hash]
                FROM _objects o
                WHERE o._id_scheme = ' + CAST(@scheme_id AS NVARCHAR(20)) + N'
                  AND o._id_parent IN (SELECT CAST(value AS BIGINT) FROM STRING_SPLIT(@parents, '',''))';
        
        IF @base_conditions IS NOT NULL AND @base_conditions <> N''
            SET @sql = @sql + N' AND ' + @base_conditions;
        
        -- ORDER BY on clean SELECT
        SET @sql = @sql + N'
                ' + @order_conditions;
        
        IF @limit_count IS NOT NULL AND @limit_count > 0
            SET @sql = @sql + N' OFFSET ' + CAST(@offset_count AS NVARCHAR(20)) + 
                              N' ROWS FETCH NEXT ' + CAST(@limit_count AS NVARCHAR(20)) + N' ROWS ONLY';
        ELSE
            SET @sql = @sql + N' OFFSET 0 ROWS';  -- Allow ORDER BY without FETCH for count queries
        
        SET @sql = @sql + N'
                FOR JSON PATH)';
        
        -- Count query (use alias 'o' to match base_conditions)
        SET @count_sql = N'
            SELECT @cnt = COUNT(*)
            FROM _objects o
            WHERE o._id_scheme = ' + CAST(@scheme_id AS NVARCHAR(20)) + N'
              AND o._id_parent IN (SELECT CAST(value AS BIGINT) FROM STRING_SPLIT(@parents, '',''))';
        
        IF @base_conditions IS NOT NULL AND @base_conditions <> N''
            SET @count_sql = @count_sql + N' AND ' + @base_conditions;
    END
    ELSE
    BEGIN
        -- Recursive search for descendants
        -- Use WHERE ... IN instead of JOIN with DISTINCT to avoid MSSQL ORDER BY issue
        SET @sql = N'
            ;WITH descendants AS (
                SELECT CAST(value AS BIGINT) AS _id, 0 AS depth
                FROM STRING_SPLIT(@parents, '','')
                UNION ALL
                SELECT obj._id, d.depth + 1
                FROM _objects obj
                JOIN descendants d ON obj._id_parent = d._id
                WHERE d.depth < ' + CAST(@max_depth AS NVARCHAR(10)) + N'
            )
            SELECT @result = (
                SELECT 
                    o._id AS [id],
                    o._name AS [name],
                    o._id_scheme AS [scheme_id],
                    o._id_parent AS [parent_id],
                    o._id_owner AS [owner_id],
                    o._id_who_change AS [who_change_id],
                    o._date_create AS [date_create],
                    o._date_modify AS [date_modify],
                    o._date_begin AS [date_begin],
                    o._date_complete AS [date_complete],
                    o._key AS [key],
                    o._value_long AS [value_long],
                    o._value_string AS [value_string],
                    o._value_guid AS [value_guid],
                    o._note AS [note],
                    o._value_bool AS [value_bool],
                    o._value_double AS [value_double],
                    o._value_numeric AS [value_numeric],
                    o._value_datetime AS [value_datetime],
                    o._value_bytes AS [value_bytes],
                    o._hash AS [hash]
                FROM _objects o
                WHERE o._id_scheme = ' + CAST(@scheme_id AS NVARCHAR(20)) + N'
                  AND EXISTS (
                      SELECT 1 FROM descendants dt
                      WHERE dt._id = o._id AND dt.depth > 0
                  )';
        
        IF @base_conditions IS NOT NULL AND @base_conditions <> N''
            SET @sql = @sql + N' AND ' + @base_conditions;
        
        -- ORDER BY on clean SELECT (no JOIN with DISTINCT)
        SET @sql = @sql + N'
                ' + @order_conditions;
        
        IF @limit_count IS NOT NULL AND @limit_count > 0
            SET @sql = @sql + N' OFFSET ' + CAST(@offset_count AS NVARCHAR(20)) + 
                              N' ROWS FETCH NEXT ' + CAST(@limit_count AS NVARCHAR(20)) + N' ROWS ONLY';
        ELSE
            SET @sql = @sql + N' OFFSET 0 ROWS';  -- Allow ORDER BY without FETCH for count queries
        
        SET @sql = @sql + N'
                FOR JSON PATH)';
        
        -- Count query (use alias 'o' and WHERE IN to match main query)
        SET @count_sql = N'
            ;WITH descendants AS (
                SELECT CAST(value AS BIGINT) AS _id, 0 AS depth
                FROM STRING_SPLIT(@parents, '','')
                UNION ALL
                SELECT obj._id, d.depth + 1
                FROM _objects obj
                JOIN descendants d ON obj._id_parent = d._id
                WHERE d.depth < ' + CAST(@max_depth AS NVARCHAR(10)) + N'
            )
            SELECT @cnt = COUNT(*)
            FROM _objects o
            WHERE o._id_scheme = ' + CAST(@scheme_id AS NVARCHAR(20)) + N'
              AND EXISTS (
                  SELECT 1 FROM descendants dt
                  WHERE dt._id = o._id AND dt.depth > 0
              )';
        
        IF @base_conditions IS NOT NULL AND @base_conditions <> N''
            SET @count_sql = @count_sql + N' AND ' + @base_conditions;
    END
    
    -- Execute queries
    EXEC sp_executesql @sql, 
        N'@parents NVARCHAR(MAX), @result NVARCHAR(MAX) OUTPUT', 
        @parents = @parent_ids, 
        @result = @objects_json OUTPUT;
    
    EXEC sp_executesql @count_sql, 
        N'@parents NVARCHAR(MAX), @cnt INT OUTPUT', 
        @parents = @parent_ids, 
        @cnt = @total_count OUTPUT;
    
    -- Return result as JSON with 'result' column for C# mapping
    -- Use JSON_QUERY to embed JSON arrays properly (not as escaped strings)
    SELECT ISNULL((
        SELECT 
            JSON_QUERY(ISNULL(@objects_json, N'[]')) AS objects,
            ISNULL(@total_count, 0) AS total_count,
            ISNULL(@limit_count, 2147483647) AS [limit],
            ISNULL(@offset_count, 0) AS [offset],
            N'[' + ISNULL(@parent_ids, N'') + N']' AS parent_ids,
            ISNULL(@max_depth, 10) AS max_depth,
            JSON_QUERY(N'[]') AS facets
        FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    ), N'{"objects":[],"total_count":0,"limit":2147483647,"offset":0,"parent_ids":[],"max_depth":10,"facets":[]}') AS result;
END;
GO

-- ========== PROCEDURE 6: SQL Preview for base search ==========
-- Returns generated SQL for debugging
CREATE PROCEDURE dbo.get_search_sql_preview_base
    @scheme_id BIGINT,
    @facet_filters NVARCHAR(MAX) = NULL,
    @limit_count INT = NULL,
    @offset_count INT = 0,
    @order_by NVARCHAR(MAX) = NULL,
    @max_recursion_depth INT = 10,
    @distinct_hash BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @base_conditions NVARCHAR(MAX);
    DECLARE @order_conditions NVARCHAR(MAX);
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @final_where NVARCHAR(MAX);
    
    -- Build conditions
    EXEC dbo.internal_parse_filters @scheme_id, @facet_filters, N'obj', N'AND', 0, @base_conditions OUTPUT;
    
    -- Build order conditions
    SET @order_conditions = dbo.build_order_by_clause(@order_by, N'obj');
    IF @order_conditions IS NULL OR @order_conditions = N''
        SET @order_conditions = N'ORDER BY obj._id';
    
    -- Build WHERE clause
    SET @final_where = N'WHERE obj._id_scheme = ' + CAST(@scheme_id AS NVARCHAR(20));
    IF @base_conditions IS NOT NULL AND @base_conditions <> N''
        SET @final_where = @final_where + N' AND ' + @base_conditions;
    
    -- Build preview SQL
    IF @distinct_hash = 1
    BEGIN
        SET @sql = N'
SELECT 
    o._id AS [id], o._name AS [name], o._id_scheme AS [scheme_id],
    o._id_parent AS [parent_id], o._hash AS [hash], ...
FROM (
    SELECT obj._id, obj._hash,
           ROW_NUMBER() OVER (PARTITION BY obj._hash ORDER BY obj._id) AS rn
    FROM _objects obj
    ' + @final_where + N'
) ranked
JOIN _objects o ON o._id = ranked._id
WHERE ranked.rn = 1
' + CASE WHEN @order_conditions IS NOT NULL AND @order_conditions <> N'' 
         THEN REPLACE(REPLACE(@order_conditions, N'obj.', N'o.'), N'obj._', N'o._')
         ELSE N'ORDER BY o._id' END;
    END
    ELSE
    BEGIN
        SET @sql = N'
SELECT 
    o._id AS [id], o._name AS [name], o._id_scheme AS [scheme_id],
    o._id_parent AS [parent_id], o._hash AS [hash], ...
FROM _objects o
JOIN (
    SELECT obj._id
    FROM _objects obj
    ' + @final_where + N'
    ' + @order_conditions;
    END
    
    IF @limit_count IS NOT NULL AND @limit_count > 0
        SET @sql = @sql + N'
    OFFSET ' + CAST(@offset_count AS NVARCHAR(20)) + N' ROWS 
    FETCH NEXT ' + CAST(@limit_count AS NVARCHAR(20)) + N' ROWS ONLY';
    ELSE
        SET @sql = @sql + N' OFFSET 0 ROWS';  -- Allow ORDER BY without FETCH for count queries
    
    IF @distinct_hash = 0
        SET @sql = @sql + N'
) sub ON sub._id = o._id';
    
    SET @sql = @sql + N'
FOR JSON PATH';
    
    SELECT @sql AS sql_preview;
END;
GO

-- ========== PROCEDURE 7: SQL Preview for tree search ==========
-- Returns generated SQL for tree search debugging
CREATE PROCEDURE dbo.get_search_tree_sql_preview_base
    @scheme_id BIGINT,
    @parent_ids NVARCHAR(MAX),
    @facet_filters NVARCHAR(MAX) = NULL,
    @limit_count INT = NULL,
    @offset_count INT = 0,
    @order_by NVARCHAR(MAX) = NULL,
    @max_depth INT = 10,
    @max_recursion_depth INT = 10
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @base_conditions NVARCHAR(MAX);
    DECLARE @order_conditions NVARCHAR(MAX);
    DECLARE @sql NVARCHAR(MAX);
    
    -- Build conditions (use 'o' alias - all conditions on outer _objects table)
    EXEC dbo.internal_parse_filters @scheme_id, @facet_filters, N'o', N'AND', 0, @base_conditions OUTPUT;
    
    -- Build order conditions (use 'o' alias)
    SET @order_conditions = dbo.build_order_by_clause(@order_by, N'o');
    IF @order_conditions IS NULL OR @order_conditions = N''
        SET @order_conditions = N'ORDER BY o._id';
    
    IF @max_depth = 1
    BEGIN
        SET @sql = N'
-- Direct children only (WHERE ... IN structure)
SELECT o._id, o._name, o._hash, ...
FROM _objects o
WHERE o._id_scheme = ' + CAST(@scheme_id AS NVARCHAR(20)) + N'
  AND o._id_parent IN (SELECT CAST(value AS BIGINT) FROM STRING_SPLIT(''' + @parent_ids + N''', '',''))';
        
        IF @base_conditions IS NOT NULL AND @base_conditions <> N''
            SET @sql = @sql + N'
  AND ' + @base_conditions;
        
        -- ORDER BY on clean SELECT
        SET @sql = @sql + N'
' + @order_conditions;
    END
    ELSE
    BEGIN
        SET @sql = N'
-- Recursive descendants search (WHERE ... IN structure)
;WITH descendants AS (
    SELECT CAST(value AS BIGINT) AS _id, 0 AS depth
    FROM STRING_SPLIT(''' + @parent_ids + N''', '','')
    UNION ALL
    SELECT obj._id, d.depth + 1
    FROM _objects obj
    JOIN descendants d ON obj._id_parent = d._id
    WHERE d.depth < ' + CAST(@max_depth AS NVARCHAR(10)) + N'
)
SELECT o._id, o._name, o._hash, ...
FROM _objects o
WHERE o._id_scheme = ' + CAST(@scheme_id AS NVARCHAR(20)) + N'
  AND EXISTS (
      SELECT 1 FROM descendants dt
      WHERE dt._id = o._id AND dt.depth > 0
  )';
        
        IF @base_conditions IS NOT NULL AND @base_conditions <> N''
            SET @sql = @sql + N'
  AND ' + @base_conditions;
        
        -- ORDER BY on clean SELECT
        SET @sql = @sql + N'
' + @order_conditions;
    END
    
    IF @limit_count IS NOT NULL AND @limit_count > 0
        SET @sql = @sql + N'
    OFFSET ' + CAST(@offset_count AS NVARCHAR(20)) + N' ROWS 
    FETCH NEXT ' + CAST(@limit_count AS NVARCHAR(20)) + N' ROWS ONLY';
    ELSE
        SET @sql = @sql + N' OFFSET 0 ROWS';  -- Allow ORDER BY without FETCH for count queries
    
    SET @sql = @sql + N'
FOR JSON PATH';
    
    SELECT @sql AS sql_preview;
END;
GO

-- ========== HELPER: Build ORDER BY clause ==========
-- Creates ORDER BY clause from JSON specification
IF OBJECT_ID('dbo.build_order_by_clause', 'FN') IS NOT NULL 
    DROP FUNCTION dbo.build_order_by_clause;
GO

CREATE FUNCTION dbo.build_order_by_clause(
    @order_by NVARCHAR(MAX),
    @table_alias NVARCHAR(10)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    DECLARE @result NVARCHAR(MAX) = N'';
    
    -- If no order specified, return empty (including string 'null' from C#)
    IF @order_by IS NULL OR @order_by = N'' OR @order_by = N'[]' OR @order_by = N'null'
        RETURN @result;
    
    -- Parse JSON array of order items
    -- Expected format: [{"field": "Name", "direction": "ASC"}]
    SELECT @result = STRING_AGG(order_item, N', ')
    FROM (
        SELECT 
            CASE 
                -- Base field with 0$: prefix
                WHEN JSON_VALUE(arr.value, '$.field') LIKE N'0$:%' THEN
                    @table_alias + N'.' + dbo.normalize_base_field_name(
                        SUBSTRING(JSON_VALUE(arr.value, '$.field'), 4, 100)
                    ) + N' ' + 
                    UPPER(ISNULL(JSON_VALUE(arr.value, '$.direction'), N'ASC'))
                -- Regular field - sort via subquery to _values
                ELSE
                    N'(SELECT TOP 1 COALESCE(v._String, CAST(v._Long AS NVARCHAR(50)), FORMAT(v._Double, ''G'', ''en-US''))
                       FROM _values v 
                       JOIN _scheme_metadata_cache c ON c._structure_id = v._id_structure
                       WHERE v._id_object = ' + @table_alias + N'._id 
                         AND c._name = N''' + JSON_VALUE(arr.value, '$.field') + N'''
                         AND v._array_index IS NULL) ' +
                    UPPER(ISNULL(JSON_VALUE(arr.value, '$.direction'), N'ASC'))
            END AS order_item
        FROM OPENJSON(@order_by) arr
        WHERE JSON_VALUE(arr.value, '$.field') IS NOT NULL
    ) items;
    
    IF @result IS NOT NULL AND @result <> N''
        SET @result = N'ORDER BY ' + @result;
    
    RETURN @result;
END;
GO

PRINT N'=========================================';
PRINT N'Lazy Loading Search procedures created!';
PRINT N'';
PRINT N'PROCEDURES:';
PRINT N'  search_objects_with_facets_base - Search without Props';
PRINT N'  search_tree_objects_with_facets_base - Tree search without Props';
PRINT N'  get_filtered_object_ids - Get only IDs for aggregations';
PRINT N'  execute_objects_query_base - Execute query with base fields';
PRINT N'  get_search_sql_preview_base - SQL preview for debugging';
PRINT N'  get_search_tree_sql_preview_base - Tree SQL preview';
PRINT N'';
PRINT N'FUNCTIONS:';
PRINT N'  get_object_base_fields - Get single object base fields';
PRINT N'  build_order_by_clause - Build ORDER BY from JSON';
PRINT N'=========================================';
GO

