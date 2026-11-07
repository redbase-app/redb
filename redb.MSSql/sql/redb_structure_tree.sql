-- ====================================================================================================
-- FUNCTIONS FOR SCHEME STRUCTURE TREE
-- MS SQL Server version
-- ====================================================================================================
-- Supports hierarchical navigation: parent -> children -> descendants
-- Solves flat structure search issues in SaveAsync
-- ====================================================================================================



-- =====================================================
-- DROP EXISTING OBJECTS
-- =====================================================

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'get_scheme_structure_tree')
    DROP PROCEDURE [dbo].[get_scheme_structure_tree]
GO

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'get_structure_children')
    DROP PROCEDURE [dbo].[get_structure_children]
GO

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'validate_structure_tree')
    DROP PROCEDURE [dbo].[validate_structure_tree]
GO

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'get_structure_descendants')
    DROP PROCEDURE [dbo].[get_structure_descendants]
GO

-- =====================================================
-- 1. MAIN PROCEDURE: Build scheme structure tree
-- =====================================================

CREATE PROCEDURE [dbo].[get_scheme_structure_tree]
    @scheme_id BIGINT,
    @parent_id BIGINT = NULL,
    @max_depth INT = 10,
    @result NVARCHAR(MAX) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @structure_id BIGINT;
    DECLARE @name NVARCHAR(450);
    DECLARE @order BIGINT;
    DECLARE @is_array BIT;
    DECLARE @collection_type BIGINT;
    DECLARE @store_null BIT;
    DECLARE @allow_not_null BIT;
    DECLARE @type_name NVARCHAR(450);
    DECLARE @db_type NVARCHAR(450);
    DECLARE @type_semantic NVARCHAR(450);
    DECLARE @children_json NVARCHAR(MAX);
    DECLARE @item_json NVARCHAR(MAX);
    DECLARE @next_depth INT;
    
    SET @result = N'[]';
    
    -- Protection from infinite recursion
    IF @max_depth <= 0
    BEGIN
        SET @result = N'[{"error":"Max recursion depth reached"}]';
        RETURN;
    END
    
    -- Check if scheme exists
    IF NOT EXISTS(SELECT 1 FROM [dbo].[_schemes] WHERE [_id] = @scheme_id)
    BEGIN
        SET @result = N'[{"error":"Scheme not found"}]';
        RETURN;
    END
    
    -- Auto-fill cache if empty
    IF NOT EXISTS(SELECT 1 FROM [dbo].[_scheme_metadata_cache] WHERE [_scheme_id] = @scheme_id)
    BEGIN
        EXEC [dbo].[sync_metadata_cache_for_scheme] @scheme_id;
    END
    
    -- Create temp table for results
    DECLARE @items TABLE (
        [structure_id] BIGINT,
        [name] NVARCHAR(450),
        [order] BIGINT,
        [is_array] BIT,
        [collection_type] BIGINT,
        [store_null] BIT,
        [allow_not_null] BIT,
        [type_name] NVARCHAR(450),
        [db_type] NVARCHAR(450),
        [type_semantic] NVARCHAR(450),
        [children] NVARCHAR(MAX)
    );
    
    -- Get structures for current level from cache
    DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
        SELECT 
            c.[_structure_id],
            c.[_name],
            c.[_order],
            CASE WHEN c.[_collection_type] IS NOT NULL THEN 1 ELSE 0 END,
            c.[_collection_type],
            c.[_store_null],
            c.[_allow_not_null],
            c.[type_name],
            c.[db_type],
            c.[type_semantic]
        FROM [dbo].[_scheme_metadata_cache] c
        WHERE c.[_scheme_id] = @scheme_id
          AND ((@parent_id IS NULL AND c.[_parent_structure_id] IS NULL) 
               OR (@parent_id IS NOT NULL AND c.[_parent_structure_id] = @parent_id))
        ORDER BY c.[_order], c.[_structure_id];
    
    OPEN cur;
    FETCH NEXT FROM cur INTO @structure_id, @name, @order, @is_array, @collection_type,
                              @store_null, @allow_not_null, @type_name, @db_type, @type_semantic;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Check if has children
        IF EXISTS(SELECT 1 FROM [dbo].[_structures] 
                  WHERE [_id_scheme] = @scheme_id AND [_id_parent] = @structure_id)
        BEGIN
            -- Recursively get children
            SET @next_depth = @max_depth - 1;
            EXEC [dbo].[get_scheme_structure_tree] 
                @scheme_id, 
                @structure_id, 
                @next_depth, 
                @children_json OUTPUT;
        END
        ELSE
        BEGIN
            SET @children_json = N'[]';
        END
        
        -- Insert into temp table
        INSERT INTO @items VALUES (
            @structure_id, @name, @order, @is_array, @collection_type,
            @store_null, @allow_not_null, @type_name, @db_type, @type_semantic,
            @children_json
        );
        
        FETCH NEXT FROM cur INTO @structure_id, @name, @order, @is_array, @collection_type,
                                  @store_null, @allow_not_null, @type_name, @db_type, @type_semantic;
    END
    
    CLOSE cur;
    DEALLOCATE cur;
    
    -- Build JSON result
    SELECT @result = N'[' + ISNULL(STRING_AGG(
        N'{"structure_id":' + CAST([structure_id] AS NVARCHAR(20)) +
        N',"name":' + QUOTENAME([name], '"') +
        N',"order":' + ISNULL(CAST([order] AS NVARCHAR(20)), 'null') +
        N',"is_array":' + CASE WHEN [is_array] = 1 THEN 'true' ELSE 'false' END +
        N',"collection_type":' + ISNULL(CAST([collection_type] AS NVARCHAR(20)), 'null') +
        N',"store_null":' + CASE WHEN [store_null] = 1 THEN 'true' WHEN [store_null] = 0 THEN 'false' ELSE 'null' END +
        N',"allow_not_null":' + CASE WHEN [allow_not_null] = 1 THEN 'true' WHEN [allow_not_null] = 0 THEN 'false' ELSE 'null' END +
        N',"type_name":' + QUOTENAME([type_name], '"') +
        N',"db_type":' + QUOTENAME([db_type], '"') +
        N',"type_semantic":' + QUOTENAME([type_semantic], '"') +
        N',"children":' + [children] + N'}'
    , ','), '') + N']'
    FROM @items;
END
GO

-- =====================================================
-- 2. HELPER: Get direct children only
-- =====================================================

CREATE PROCEDURE [dbo].[get_structure_children]
    @scheme_id BIGINT,
    @parent_id BIGINT,
    @result NVARCHAR(MAX) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Auto-fill cache if empty
    IF NOT EXISTS(SELECT 1 FROM [dbo].[_scheme_metadata_cache] WHERE [_scheme_id] = @scheme_id)
    BEGIN
        EXEC [dbo].[sync_metadata_cache_for_scheme] @scheme_id;
    END
    
    SELECT @result = N'[' + ISNULL(STRING_AGG(json_item, ',') WITHIN GROUP (ORDER BY [_order], [_structure_id]), '') + N']'
    FROM (
        SELECT 
            c.[_structure_id],
            c.[_order],
            N'{"structure_id":' + CAST(c.[_structure_id] AS NVARCHAR(20)) +
            N',"name":' + QUOTENAME(c.[_name], '"') +
            N',"order":' + ISNULL(CAST(c.[_order] AS NVARCHAR(20)), 'null') +
            N',"is_array":' + CASE WHEN c.[_collection_type] IS NOT NULL THEN 'true' ELSE 'false' END +
            N',"collection_type":' + ISNULL(CAST(c.[_collection_type] AS NVARCHAR(20)), 'null') +
            N',"type_name":' + QUOTENAME(c.[type_name], '"') +
            N',"db_type":' + QUOTENAME(c.[db_type], '"') +
            N',"type_semantic":' + QUOTENAME(c.[type_semantic], '"') + N'}' AS json_item
        FROM [dbo].[_scheme_metadata_cache] c
        WHERE c.[_scheme_id] = @scheme_id
          AND c.[_parent_structure_id] = @parent_id
    ) sub;
END
GO

-- =====================================================
-- 3. DIAGNOSTIC: Validate structure tree
-- =====================================================

CREATE PROCEDURE [dbo].[validate_structure_tree]
    @scheme_id BIGINT,
    @result NVARCHAR(MAX) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @excessive_json NVARCHAR(MAX);
    DECLARE @orphaned_json NVARCHAR(MAX);
    DECLARE @circular_json NVARCHAR(MAX);
    DECLARE @total_count INT;
    DECLARE @is_valid BIT;
    
    -- 1. Find excessive structures (no values linked)
    SELECT @excessive_json = N'[' + ISNULL(STRING_AGG(
        N'{"structure_id":' + CAST(s.[_id] AS NVARCHAR(20)) +
        N',"name":' + QUOTENAME(s.[_name], '"') +
        N',"parent_name":' + ISNULL(QUOTENAME(parent_s.[_name], '"'), 'null') +
        N',"issue":"No values found - possibly excessive structure"}'
    , ','), '') + N']'
    FROM [dbo].[_structures] s
    LEFT JOIN [dbo].[_structures] parent_s ON parent_s.[_id] = s.[_id_parent]
    LEFT JOIN [dbo].[_values] v ON v.[_id_structure] = s.[_id]
    WHERE s.[_id_scheme] = @scheme_id
      AND v.[_id] IS NULL
      AND s.[_id_parent] IS NOT NULL;
    
    -- 2. Find orphaned structures (parent does not exist)
    SELECT @orphaned_json = N'[' + ISNULL(STRING_AGG(
        N'{"structure_id":' + CAST(s.[_id] AS NVARCHAR(20)) +
        N',"name":' + QUOTENAME(s.[_name], '"') +
        N',"parent_id":' + CAST(s.[_id_parent] AS NVARCHAR(20)) +
        N',"issue":"Parent structure does not exist"}'
    , ','), '') + N']'
    FROM [dbo].[_structures] s
    WHERE s.[_id_scheme] = @scheme_id
      AND s.[_id_parent] IS NOT NULL
      AND NOT EXISTS(SELECT 1 FROM [dbo].[_structures] parent_s WHERE parent_s.[_id] = s.[_id_parent]);
    
    -- 3. Check for circular references (using path string)
    ;WITH cycle_check AS (
        SELECT 
            [_id], 
            [_id_parent], 
            CAST([_id] AS NVARCHAR(MAX)) AS [path],
            0 AS has_cycle
        FROM [dbo].[_structures] 
        WHERE [_id_scheme] = @scheme_id AND [_id_parent] IS NOT NULL
        
        UNION ALL
        
        SELECT 
            s.[_id], 
            s.[_id_parent], 
            cc.[path] + ',' + CAST(s.[_id] AS NVARCHAR(20)),
            CASE WHEN CHARINDEX(',' + CAST(s.[_id] AS NVARCHAR(20)) + ',', ',' + cc.[path] + ',') > 0 THEN 1 ELSE 0 END
        FROM [dbo].[_structures] s
        INNER JOIN cycle_check cc ON cc.[_id_parent] = s.[_id]
        WHERE cc.has_cycle = 0 AND LEN(cc.[path]) < 1000
    )
    SELECT @circular_json = N'[' + ISNULL(STRING_AGG(
        N'{"structure_id":' + CAST([_id] AS NVARCHAR(20)) +
        N',"path":"' + [path] +
        N'","issue":"Circular reference detected"}'
    , ','), '') + N']'
    FROM cycle_check 
    WHERE has_cycle = 1;
    
    -- Get total count
    SELECT @total_count = COUNT(*) FROM [dbo].[_structures] WHERE [_id_scheme] = @scheme_id;
    
    -- Determine if valid
    SET @is_valid = CASE 
        WHEN @excessive_json = N'[]' AND @orphaned_json = N'[]' AND @circular_json = N'[]' 
        THEN 1 ELSE 0 
    END;
    
    -- Build result
    SET @result = N'{' +
        N'"scheme_id":' + CAST(@scheme_id AS NVARCHAR(20)) +
        N',"validation_date":"' + CONVERT(NVARCHAR(30), GETDATE(), 127) + '"' +
        N',"excessive_structures":' + ISNULL(@excessive_json, N'[]') +
        N',"orphaned_structures":' + ISNULL(@orphaned_json, N'[]') +
        N',"circular_references":' + ISNULL(@circular_json, N'[]') +
        N',"total_structures":' + CAST(@total_count AS NVARCHAR(10)) +
        N',"is_valid":' + CASE WHEN @is_valid = 1 THEN 'true' ELSE 'false' END +
    N'}';
END
GO

-- =====================================================
-- 4. HELPER: Get all descendants (flat list)
-- =====================================================

CREATE PROCEDURE [dbo].[get_structure_descendants]
    @scheme_id BIGINT,
    @parent_id BIGINT,
    @result NVARCHAR(MAX) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    ;WITH descendants AS (
        -- Direct children
        SELECT [_id], [_name], [_id_parent], 0 AS [level]
        FROM [dbo].[_structures] 
        WHERE [_id_scheme] = @scheme_id AND [_id_parent] = @parent_id
        
        UNION ALL
        
        -- Recursive descendants
        SELECT s.[_id], s.[_name], s.[_id_parent], d.[level] + 1
        FROM [dbo].[_structures] s
        INNER JOIN descendants d ON d.[_id] = s.[_id_parent]
        WHERE s.[_id_scheme] = @scheme_id AND d.[level] < 10
    )
    SELECT @result = N'[' + ISNULL(STRING_AGG(json_item, ',') WITHIN GROUP (ORDER BY [level], [_id]), '') + N']'
    FROM (
        SELECT 
            [_id],
            [level],
            N'{"structure_id":' + CAST([_id] AS NVARCHAR(20)) +
            N',"name":' + QUOTENAME([_name], '"') +
            N',"parent_id":' + ISNULL(CAST([_id_parent] AS NVARCHAR(20)), 'null') +
            N',"level":' + CAST([level] AS NVARCHAR(10)) + N'}' AS json_item
        FROM descendants
    ) sub;
END
GO

-- =====================================================
-- USAGE EXAMPLES
-- =====================================================
-- 
-- Get full structure tree:
--   DECLARE @json NVARCHAR(MAX);
--   EXEC get_scheme_structure_tree @scheme_id = 123, @result = @json OUTPUT;
--   SELECT @json;
--
-- Get direct children:
--   DECLARE @json NVARCHAR(MAX);
--   EXEC get_structure_children @scheme_id = 123, @parent_id = 456, @result = @json OUTPUT;
--   SELECT @json;
--
-- Validate tree:
--   DECLARE @json NVARCHAR(MAX);
--   EXEC validate_structure_tree @scheme_id = 123, @result = @json OUTPUT;
--   SELECT @json;
--
-- Get all descendants:
--   DECLARE @json NVARCHAR(MAX);
--   EXEC get_structure_descendants @scheme_id = 123, @parent_id = 456, @result = @json OUTPUT;
--   SELECT @json;
-- =====================================================

PRINT '========================================='
PRINT 'Structure tree procedures created!'
PRINT '========================================='
GO

