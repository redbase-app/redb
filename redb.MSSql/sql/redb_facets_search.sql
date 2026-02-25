-- ===== REDB FACETS & SEARCH MODULE FOR MSSQL =====
-- Full featured faceted search and object filtering module
-- Architecture: Stored procedures with dynamic SQL (SQL injection safe)
-- Supports: LINQ operators, logical operators, Class fields, Arrays, Dictionary, hierarchical search
--
-- DEPENDENCIES: 
--   1. redbMSSQL.sql (core tables)
--   2. redb_metadata_cache.sql (metadata cache)
-- Run those scripts BEFORE this one!

-- =====================================================
-- CHECK DEPENDENCIES
-- =====================================================
IF OBJECT_ID('dbo._scheme_metadata_cache', 'U') IS NULL
BEGIN
    RAISERROR('ERROR: Table _scheme_metadata_cache not found! Run redb_metadata_cache.sql first.', 16, 1);
    RETURN;
END;

IF OBJECT_ID('dbo.sync_metadata_cache_for_scheme', 'P') IS NULL
BEGIN
    RAISERROR('ERROR: Procedure sync_metadata_cache_for_scheme not found! Run redb_metadata_cache.sql first.', 16, 1);
    RETURN;
END;
GO

PRINT 'Dependencies OK. Creating facets search module...'
GO

-- =====================================================
-- DROP EXISTING OBJECTS
-- =====================================================

IF OBJECT_ID('dbo.preview_facet_query', 'P') IS NOT NULL DROP PROCEDURE dbo.preview_facet_query;
IF OBJECT_ID('dbo.search_objects_with_facets', 'P') IS NOT NULL DROP PROCEDURE dbo.search_objects_with_facets;
IF OBJECT_ID('dbo.search_tree_objects_with_facets', 'P') IS NOT NULL DROP PROCEDURE dbo.search_tree_objects_with_facets;
IF OBJECT_ID('dbo.get_facets', 'P') IS NOT NULL DROP PROCEDURE dbo.get_facets;
IF OBJECT_ID('dbo.internal_parse_filters', 'P') IS NOT NULL DROP PROCEDURE dbo.internal_parse_filters;
IF OBJECT_ID('dbo.internal_build_exists', 'P') IS NOT NULL DROP PROCEDURE dbo.internal_build_exists;
IF OBJECT_ID('dbo.normalize_base_field_name', 'FN') IS NOT NULL DROP FUNCTION dbo.normalize_base_field_name;
IF OBJECT_ID('dbo.get_value_column_by_type', 'FN') IS NOT NULL DROP FUNCTION dbo.get_value_column_by_type;
IF OBJECT_ID('dbo.get_object_level', 'FN') IS NOT NULL DROP FUNCTION dbo.get_object_level;
IF OBJECT_ID('dbo.is_ancestor_of', 'FN') IS NOT NULL DROP FUNCTION dbo.is_ancestor_of;
IF OBJECT_ID('dbo.is_descendant_of', 'FN') IS NOT NULL DROP FUNCTION dbo.is_descendant_of;
IF OBJECT_ID('dbo.build_facet_field_path', 'FN') IS NOT NULL DROP FUNCTION dbo.build_facet_field_path;
IF OBJECT_ID('dbo.get_facets_extended', 'P') IS NOT NULL DROP PROCEDURE dbo.get_facets_extended;
IF OBJECT_ID('dbo.preview_tree_facet_query', 'P') IS NOT NULL DROP PROCEDURE dbo.preview_tree_facet_query;
GO

-- =====================================================
-- HELPER FUNCTION: normalize_base_field_name
-- Maps C# names to SQL column names on _objects table
-- =====================================================
CREATE FUNCTION dbo.normalize_base_field_name(@field_name NVARCHAR(100))
RETURNS NVARCHAR(100)
WITH SCHEMABINDING
AS
BEGIN
    RETURN CASE @field_name
        WHEN 'id' THEN '_id' WHEN 'Id' THEN '_id' WHEN '_id' THEN '_id'
        WHEN 'parent_id' THEN '_id_parent' WHEN 'ParentId' THEN '_id_parent' WHEN '_id_parent' THEN '_id_parent'
        WHEN 'scheme_id' THEN '_id_scheme' WHEN 'SchemeId' THEN '_id_scheme' WHEN '_id_scheme' THEN '_id_scheme'
        WHEN 'owner_id' THEN '_id_owner' WHEN 'OwnerId' THEN '_id_owner' WHEN '_id_owner' THEN '_id_owner'
        WHEN 'who_change_id' THEN '_id_who_change' WHEN 'WhoChangeId' THEN '_id_who_change' WHEN '_id_who_change' THEN '_id_who_change'
        WHEN 'value_long' THEN '_value_long' WHEN 'ValueLong' THEN '_value_long' WHEN '_value_long' THEN '_value_long'
        WHEN 'value_string' THEN '_value_string' WHEN 'ValueString' THEN '_value_string' WHEN '_value_string' THEN '_value_string'
        WHEN 'value_guid' THEN '_value_guid' WHEN 'ValueGuid' THEN '_value_guid' WHEN '_value_guid' THEN '_value_guid'
        WHEN 'key' THEN '_key' WHEN 'Key' THEN '_key' WHEN '_key' THEN '_key'
        WHEN 'name' THEN '_name' WHEN 'Name' THEN '_name' WHEN '_name' THEN '_name'
        WHEN 'note' THEN '_note' WHEN 'Note' THEN '_note' WHEN '_note' THEN '_note'
        WHEN 'value_bool' THEN '_value_bool' WHEN 'ValueBool' THEN '_value_bool' WHEN '_value_bool' THEN '_value_bool'
        WHEN 'value_double' THEN '_value_double' WHEN 'ValueDouble' THEN '_value_double' WHEN '_value_double' THEN '_value_double'
        WHEN 'value_numeric' THEN '_value_numeric' WHEN 'ValueNumeric' THEN '_value_numeric' WHEN '_value_numeric' THEN '_value_numeric'
        WHEN 'value_datetime' THEN '_value_datetime' WHEN 'ValueDatetime' THEN '_value_datetime' WHEN '_value_datetime' THEN '_value_datetime'
        WHEN 'value_bytes' THEN '_value_bytes' WHEN 'ValueBytes' THEN '_value_bytes' WHEN '_value_bytes' THEN '_value_bytes'
        WHEN 'hash' THEN '_hash' WHEN 'Hash' THEN '_hash' WHEN '_hash' THEN '_hash'
        WHEN 'date_create' THEN '_date_create' WHEN 'DateCreate' THEN '_date_create' WHEN '_date_create' THEN '_date_create'
        WHEN 'date_modify' THEN '_date_modify' WHEN 'DateModify' THEN '_date_modify' WHEN '_date_modify' THEN '_date_modify'
        WHEN 'date_begin' THEN '_date_begin' WHEN 'DateBegin' THEN '_date_begin' WHEN '_date_begin' THEN '_date_begin'
        WHEN 'date_complete' THEN '_date_complete' WHEN 'DateComplete' THEN '_date_complete' WHEN '_date_complete' THEN '_date_complete'
        ELSE NULL
    END;
END;
GO

-- =====================================================
-- HELPER FUNCTION: get_value_column_by_type
-- Returns _values column name based on db_type
-- =====================================================
CREATE FUNCTION dbo.get_value_column_by_type(@db_type NVARCHAR(50), @type_semantic NVARCHAR(100))
RETURNS NVARCHAR(50)
WITH SCHEMABINDING
AS
BEGIN
    RETURN CASE 
        WHEN @db_type = 'String' THEN '_String'
        WHEN @db_type = 'Long' AND @type_semantic = '_RObject' THEN '_Long'
        WHEN @db_type = 'Long' THEN '_Long'
        WHEN @db_type = 'Double' THEN '_Double'
        WHEN @db_type = 'Numeric' THEN '_Numeric'
        WHEN @db_type = 'Boolean' THEN '_Boolean'
        WHEN @db_type = 'DateTimeOffset' THEN '_DateTimeOffset'
        WHEN @db_type = 'Guid' THEN '_Guid'
        WHEN @db_type = 'ListItem' THEN '_listitem'
        WHEN @db_type = 'ByteArray' THEN '_ByteArray'
        ELSE '_String'
    END;
END;
GO

-- =====================================================
-- HELPER FUNCTION: get_object_level
-- Returns the hierarchical level of an object (0 = root)
-- =====================================================
CREATE FUNCTION dbo.get_object_level(@object_id BIGINT)
RETURNS INT
AS
BEGIN
    DECLARE @level INT = 0;
    DECLARE @current_parent BIGINT;
    
    SELECT @current_parent = _id_parent FROM _objects WHERE _id = @object_id;
    
    WHILE @current_parent IS NOT NULL AND @level < 100
    BEGIN
        SET @level = @level + 1;
        SELECT @current_parent = _id_parent FROM _objects WHERE _id = @current_parent;
    END;
    
    RETURN @level;
END;
GO

-- =====================================================
-- HELPER FUNCTION: is_ancestor_of
-- Returns 1 if @ancestor_id is an ancestor of @object_id
-- =====================================================
CREATE FUNCTION dbo.is_ancestor_of(@object_id BIGINT, @ancestor_id BIGINT)
RETURNS BIT
AS
BEGIN
    IF @object_id IS NULL OR @ancestor_id IS NULL RETURN 0;
    IF @object_id = @ancestor_id RETURN 0; -- not ancestor of itself
    
    DECLARE @current_parent BIGINT;
    DECLARE @depth INT = 0;
    
    SELECT @current_parent = _id_parent FROM _objects WHERE _id = @object_id;
    
    WHILE @current_parent IS NOT NULL AND @depth < 100
    BEGIN
        IF @current_parent = @ancestor_id RETURN 1;
        SELECT @current_parent = _id_parent FROM _objects WHERE _id = @current_parent;
        SET @depth = @depth + 1;
    END;
    
    RETURN 0;
END;
GO

-- =====================================================
-- HELPER FUNCTION: is_descendant_of
-- Returns 1 if @descendant_id is a descendant of @object_id
-- =====================================================
CREATE FUNCTION dbo.is_descendant_of(@object_id BIGINT, @descendant_id BIGINT)
RETURNS BIT
AS
BEGIN
    -- descendant_id is descendant of object_id means object_id is ancestor of descendant_id
    RETURN dbo.is_ancestor_of(@descendant_id, @object_id);
END;
GO

-- =====================================================
-- HELPER FUNCTION: build_order_by_clause
-- Creates ORDER BY clause from JSON specification
-- Format: [{"field": "Name", "direction": "ASC"}]
-- =====================================================
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
    
    -- If no order specified, return empty
    IF @order_by IS NULL OR @order_by = N'' OR @order_by = N'[]' OR @order_by = N'null'
        RETURN @result;
    
    -- Parse JSON array: [{"field": "Name", "direction": "ASC"}]
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
                -- Regular Props field - sort via subquery to _values
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

-- =====================================================
-- INTERNAL: Build single EXISTS condition
-- Called by internal_parse_filters for each field
-- =====================================================
CREATE PROCEDURE dbo.internal_build_exists
    @scheme_id BIGINT,
    @field_path NVARCHAR(500),
    @operator NVARCHAR(50),
    @value NVARCHAR(MAX),
    @value_type NVARCHAR(20),
    @table_alias NVARCHAR(10),
    @result NVARCHAR(MAX) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @structure_id BIGINT;
    DECLARE @parent_structure_id BIGINT;
    DECLARE @nested_structure_id BIGINT;
    DECLARE @db_type NVARCHAR(50);
    DECLARE @type_semantic NVARCHAR(100);
    DECLARE @collection_type BIGINT;
    DECLARE @value_column NVARCHAR(50);
    
    DECLARE @root_field NVARCHAR(450);
    DECLARE @nested_field NVARCHAR(450);
    DECLARE @is_array BIT = 0;
    DECLARE @is_nested BIT = 0;
    DECLARE @dict_key NVARCHAR(500) = NULL;
    
    DECLARE @safe_value NVARCHAR(MAX);
    SET @safe_value = REPLACE(@value, N'''', N'''''');
    
    -- Property function: $length, $count
    DECLARE @property_function NVARCHAR(20) = NULL;
    
    -- Check for property function suffix: Name.$length, Tags[].$count
    IF @field_path LIKE '%.$length'
    BEGIN
        SET @property_function = '$length';
        SET @field_path = LEFT(@field_path, LEN(@field_path) - 8); -- Remove '.$length'
    END
    ELSE IF @field_path LIKE '%.$count'
    BEGIN
        SET @property_function = '$count';
        SET @field_path = LEFT(@field_path, LEN(@field_path) - 7); -- Remove '.$count'
    END;
    
    -- Parse field path
    -- Check for Dictionary: Field[key]
    IF @field_path LIKE '%[[]%]%' AND @field_path NOT LIKE '%[[]]%'
    BEGIN
        SET @root_field = LEFT(@field_path, CHARINDEX('[', @field_path) - 1);
        SET @dict_key = SUBSTRING(@field_path, CHARINDEX('[', @field_path) + 1, 
                        CHARINDEX(']', @field_path) - CHARINDEX('[', @field_path) - 1);
        -- Check for nested after dictionary: Field[key].Prop
        IF CHARINDEX('].', @field_path) > 0
        BEGIN
            SET @nested_field = SUBSTRING(@field_path, CHARINDEX('].', @field_path) + 2, LEN(@field_path));
            SET @is_nested = 1;
        END;
    END
    -- Check for Array: Field[]
    ELSE IF @field_path LIKE '%[[]]%'
    BEGIN
        SET @is_array = 1;
        IF CHARINDEX('.', REPLACE(@field_path, '[]', '')) > 0
        BEGIN
            SET @root_field = LEFT(REPLACE(@field_path, '[]', ''), CHARINDEX('.', REPLACE(@field_path, '[]', '')) - 1);
            SET @nested_field = SUBSTRING(REPLACE(@field_path, '[]', ''), CHARINDEX('.', REPLACE(@field_path, '[]', '')) + 1, LEN(@field_path));
            SET @is_nested = 1;
        END
        ELSE
        BEGIN
            SET @root_field = REPLACE(@field_path, '[]', '');
        END;
    END
    -- Check for nested: Field.Prop
    ELSE IF CHARINDEX('.', @field_path) > 0
    BEGIN
        SET @root_field = LEFT(@field_path, CHARINDEX('.', @field_path) - 1);
        SET @nested_field = SUBSTRING(@field_path, CHARINDEX('.', @field_path) + 1, LEN(@field_path));
        SET @is_nested = 1;
    END
    ELSE
    BEGIN
        SET @root_field = @field_path;
    END;
    
    -- Find root structure
    SELECT @structure_id = c._structure_id, 
           @db_type = c.db_type, 
           @type_semantic = c.type_semantic,
           @collection_type = c._collection_type
    FROM _scheme_metadata_cache c
    WHERE c._scheme_id = @scheme_id 
      AND c._name = @root_field
      AND c._parent_structure_id IS NULL;
    
    IF @structure_id IS NULL
    BEGIN
        -- Debug: Field not found in metadata cache
        -- Check if cache is empty or field doesn't exist
        DECLARE @total_cache INT;
        SELECT @total_cache = COUNT(*) FROM _scheme_metadata_cache WHERE _scheme_id = @scheme_id;
        
        IF @total_cache = 0
        BEGIN
            -- Cache is empty - likely not synced
            PRINT 'WARNING: Metadata cache is EMPTY for scheme ' + CAST(@scheme_id AS NVARCHAR(20)) + '. Run sync_metadata_cache_for_scheme first!';
        END
        ELSE
        BEGIN
            PRINT 'WARNING: Field "' + @root_field + '" not found in scheme ' + CAST(@scheme_id AS NVARCHAR(20)) + ' (cache has ' + CAST(@total_cache AS NVARCHAR(10)) + ' entries)';
        END;
        
        SET @result = N'1=1'; -- Field not found, skip
        RETURN;
    END;
    
    -- Find nested structure if needed
    IF @is_nested = 1 AND @nested_field IS NOT NULL
    BEGIN
        -- Special handling for ListItem: Value, Alias, Id, IdList are NOT structures in cache
        -- They are columns in _list_items table (like PostgreSQL _get_listitem_field_type_info)
        IF @type_semantic = '_RListItem' AND @nested_field IN ('Value', 'Alias', 'Id', 'IdList')
        BEGIN
            -- Keep @type_semantic = '_RListItem', don't search for nested structure
            SET @nested_structure_id = NULL;
            -- Set correct db_type for ListItem field so @condition is built correctly
            SET @db_type = CASE @nested_field
                WHEN 'Value' THEN 'String'
                WHEN 'Alias' THEN 'String'
                WHEN 'Id' THEN 'Long'
                WHEN 'IdList' THEN 'Long'
            END;
        END
        ELSE
        BEGIN
            SELECT @nested_structure_id = c._structure_id,
                   @db_type = c.db_type,
                   @type_semantic = c.type_semantic
            FROM _scheme_metadata_cache c
            WHERE c._scheme_id = @scheme_id
              AND c._name = @nested_field
              AND c._parent_structure_id = @structure_id;
        END;
    END;
    
    SET @value_column = dbo.get_value_column_by_type(@db_type, @type_semantic);
    
    -- Build EXISTS based on field type and operator
    DECLARE @condition NVARCHAR(MAX);
    
    -- Smart value condition based on operator and actual db_type from cache
    -- This handles type-aware comparisons like PostgreSQL version
    DECLARE @is_numeric BIT = CASE WHEN @value LIKE '[0-9]%' OR @value LIKE '-[0-9]%' THEN 1 ELSE 0 END;
    DECLARE @is_decimal BIT = CASE WHEN @is_numeric = 1 AND CHARINDEX('.', @value) > 0 THEN 1 ELSE 0 END;
    DECLARE @is_bool BIT = CASE WHEN @value IN ('true', 'false') THEN 1 ELSE 0 END;
    DECLARE @is_date BIT = CASE WHEN @value LIKE '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]%' THEN 1 ELSE 0 END;
    DECLARE @is_guid BIT = CASE WHEN @value LIKE '[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]-[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]-[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]-[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]-[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]' THEN 1 ELSE 0 END;
    
    -- Build value condition based on operator with TYPE-AWARE DETECTION (using @db_type from schema)
    SET @condition = CASE @operator
        WHEN '$eq' THEN 
            CASE 
                WHEN @value IS NULL OR @value = 'null' THEN N'v.' + @value_column + N' IS NULL'
                WHEN @db_type = 'Boolean' THEN N'v._Boolean = ' + CASE WHEN @value = 'true' THEN '1' ELSE '0' END
                WHEN @db_type = 'DateTimeOffset' THEN N'v._DateTimeOffset = CONVERT(datetimeoffset, ''' + @safe_value + ''', 127)'
                WHEN @db_type = 'Guid' THEN N'v._Guid = ''' + @safe_value + ''''
                WHEN @db_type = 'Long' THEN N'v._Long = ' + @value
                WHEN @db_type = 'Double' THEN N'v.[_Double] = ' + @value
                WHEN @db_type = 'Numeric' THEN N'v._Numeric = ' + @value
                WHEN @is_bool = 1 THEN N'v._Boolean = ' + CASE WHEN @value = 'true' THEN '1' ELSE '0' END
                WHEN @is_date = 1 THEN N'v._DateTimeOffset = CONVERT(datetimeoffset, ''' + @safe_value + ''', 127)'
                WHEN @is_guid = 1 THEN N'v._Guid = ''' + @safe_value + ''''
                WHEN @is_numeric = 1 THEN N'(v._Long = ' + @value + N' OR v.[_Double] = ' + @value + N' OR v._Numeric = ' + @value + N')'
                ELSE N'v._String = N''' + @safe_value + N'''' 
            END
        WHEN '$ne' THEN 
            CASE 
                WHEN @value IS NULL OR @value = 'null' THEN N'v.' + @value_column + N' IS NOT NULL'
                WHEN @db_type = 'Boolean' THEN N'v._Boolean <> ' + CASE WHEN @value = 'true' THEN '1' ELSE '0' END
                WHEN @db_type = 'DateTimeOffset' THEN N'v._DateTimeOffset <> CONVERT(datetimeoffset, ''' + @safe_value + ''', 127)'
                WHEN @db_type = 'Guid' THEN N'v._Guid <> ''' + @safe_value + ''''
                WHEN @db_type = 'Long' THEN N'v._Long <> ' + @value
                WHEN @db_type = 'Double' THEN N'v.[_Double] <> ' + @value
                WHEN @db_type = 'Numeric' THEN N'v._Numeric <> ' + @value
                WHEN @is_bool = 1 THEN N'v._Boolean <> ' + CASE WHEN @value = 'true' THEN '1' ELSE '0' END
                WHEN @is_date = 1 THEN N'v._DateTimeOffset <> CONVERT(datetimeoffset, ''' + @safe_value + ''', 127)'
                WHEN @is_guid = 1 THEN N'v._Guid <> ''' + @safe_value + ''''
                WHEN @is_numeric = 1 THEN N'(v._Long <> ' + @value + N' AND v.[_Double] <> ' + @value + N' AND v._Numeric <> ' + @value + N')'
                ELSE N'v._String <> N''' + @safe_value + N''''
            END
        WHEN '$gt' THEN 
            CASE 
                WHEN @db_type = 'DateTimeOffset' THEN N'v._DateTimeOffset > CONVERT(datetimeoffset, ''' + @safe_value + ''', 127)'
                WHEN @db_type = 'Long' THEN N'v._Long > ' + @value
                WHEN @db_type = 'Double' THEN N'v.[_Double] > ' + @value
                WHEN @db_type = 'Numeric' THEN N'v._Numeric > ' + @value
                WHEN @is_date = 1 THEN N'v._DateTimeOffset > CONVERT(datetimeoffset, ''' + @safe_value + ''', 127)'
                WHEN @is_numeric = 1 THEN N'(v._Long > ' + @value + N' OR v.[_Double] > ' + @value + N' OR v._Numeric > ' + @value + N')'
                ELSE N'v._String > N''' + @safe_value + ''''
            END
        WHEN '$gte' THEN 
            CASE 
                WHEN @db_type = 'DateTimeOffset' THEN N'v._DateTimeOffset >= CONVERT(datetimeoffset, ''' + @safe_value + ''', 127)'
                WHEN @db_type = 'Long' THEN N'v._Long >= ' + @value
                WHEN @db_type = 'Double' THEN N'v.[_Double] >= ' + @value
                WHEN @db_type = 'Numeric' THEN N'v._Numeric >= ' + @value
                WHEN @is_date = 1 THEN N'v._DateTimeOffset >= CONVERT(datetimeoffset, ''' + @safe_value + ''', 127)'
                WHEN @is_numeric = 1 THEN N'(v._Long >= ' + @value + N' OR v.[_Double] >= ' + @value + N' OR v._Numeric >= ' + @value + N')'
                ELSE N'v._String >= N''' + @safe_value + ''''
            END
        WHEN '$lt' THEN 
            CASE 
                WHEN @db_type = 'DateTimeOffset' THEN N'v._DateTimeOffset < CONVERT(datetimeoffset, ''' + @safe_value + ''', 127)'
                WHEN @db_type = 'Long' THEN N'v._Long < ' + @value
                WHEN @db_type = 'Double' THEN N'v.[_Double] < ' + @value
                WHEN @db_type = 'Numeric' THEN N'v._Numeric < ' + @value
                WHEN @is_date = 1 THEN N'v._DateTimeOffset < CONVERT(datetimeoffset, ''' + @safe_value + ''', 127)'
                WHEN @is_numeric = 1 THEN N'(v._Long < ' + @value + N' OR v.[_Double] < ' + @value + N' OR v._Numeric < ' + @value + N')'
                ELSE N'v._String < N''' + @safe_value + ''''
            END
        WHEN '$lte' THEN 
            CASE 
                WHEN @db_type = 'DateTimeOffset' THEN N'v._DateTimeOffset <= CONVERT(datetimeoffset, ''' + @safe_value + ''', 127)'
                WHEN @db_type = 'Long' THEN N'v._Long <= ' + @value
                WHEN @db_type = 'Double' THEN N'v.[_Double] <= ' + @value
                WHEN @db_type = 'Numeric' THEN N'v._Numeric <= ' + @value
                WHEN @is_date = 1 THEN N'v._DateTimeOffset <= CONVERT(datetimeoffset, ''' + @safe_value + ''', 127)'
                WHEN @is_numeric = 1 THEN N'(v._Long <= ' + @value + N' OR v.[_Double] <= ' + @value + N' OR v._Numeric <= ' + @value + N')'
                ELSE N'v._String <= N''' + @safe_value + ''''
            END
        WHEN '$contains' THEN N'v._String LIKE N''%' + @safe_value + N'%'''
        WHEN '$containsIgnoreCase' THEN N'v._String LIKE N''%' + @safe_value + N'%'' COLLATE Latin1_General_CI_AS'
        WHEN '$startsWith' THEN N'v._String LIKE N''' + @safe_value + N'%'''
        WHEN '$startsWithIgnoreCase' THEN N'v._String LIKE N''' + @safe_value + N'%'' COLLATE Latin1_General_CI_AS'
        WHEN '$endsWith' THEN N'v._String LIKE N''%' + @safe_value + N''''
        WHEN '$endsWithIgnoreCase' THEN N'v._String LIKE N''%' + @safe_value + N''' COLLATE Latin1_General_CI_AS'
        WHEN '$in' THEN 
            CASE 
                WHEN @db_type = 'Long' THEN N'v._Long IN (SELECT CAST([value] AS BIGINT) FROM OPENJSON(N''' + @safe_value + N'''))'
                WHEN @db_type = 'Double' THEN N'v.[_Double] IN (SELECT CAST([value] AS FLOAT) FROM OPENJSON(N''' + @safe_value + N'''))'
                WHEN @db_type = 'Numeric' THEN N'v._Numeric IN (SELECT CAST([value] AS DECIMAL(38,18)) FROM OPENJSON(N''' + @safe_value + N'''))'
                WHEN @is_numeric = 1 THEN N'(v._Long IN (SELECT CAST([value] AS BIGINT) FROM OPENJSON(N''' + @safe_value + N''')) OR v.[_Double] IN (SELECT CAST([value] AS FLOAT) FROM OPENJSON(N''' + @safe_value + N''')))'
                ELSE N'v._String IN (SELECT [value] FROM OPENJSON(N''' + @safe_value + N'''))'
            END
        WHEN '$notIn' THEN 
            CASE 
                WHEN @db_type = 'Long' THEN N'v._Long NOT IN (SELECT CAST([value] AS BIGINT) FROM OPENJSON(N''' + @safe_value + N'''))'
                WHEN @db_type = 'Double' THEN N'v.[_Double] NOT IN (SELECT CAST([value] AS FLOAT) FROM OPENJSON(N''' + @safe_value + N'''))'
                WHEN @db_type = 'Numeric' THEN N'v._Numeric NOT IN (SELECT CAST([value] AS DECIMAL(38,18)) FROM OPENJSON(N''' + @safe_value + N'''))'
                WHEN @is_numeric = 1 THEN N'v._Long NOT IN (SELECT CAST([value] AS BIGINT) FROM OPENJSON(N''' + @safe_value + N''')) AND v.[_Double] NOT IN (SELECT CAST([value] AS FLOAT) FROM OPENJSON(N''' + @safe_value + N'''))'
                ELSE N'v._String NOT IN (SELECT [value] FROM OPENJSON(N''' + @safe_value + N'''))'
            END
        WHEN '$exists' THEN CASE WHEN @value = 'true' THEN N'1=1' ELSE N'1=0' END  -- EXISTS/NOT EXISTS handles this
        WHEN '$between' THEN NULL  -- Special handling below
        -- $match: Full-Text Search (requires FTS catalog)
        WHEN '$match' THEN N'CONTAINS(v._String, N''' + @safe_value + N''')'
        WHEN '$matchPrefix' THEN N'CONTAINS(v._String, N''"' + @safe_value + N'*"'')'
        -- $regex alternative for MSSQL: pattern matching with LIKE
        WHEN '$like' THEN N'v._String LIKE N''' + @safe_value + N''''
        WHEN '$likeIgnoreCase' THEN N'v._String LIKE N''' + @safe_value + N''' COLLATE Latin1_General_CI_AS'
        -- Array operators (use @db_type for type-aware queries)
        WHEN '$arrayContains' THEN 
            CASE 
                WHEN @db_type = 'Boolean' THEN N'v._Boolean = ' + CASE WHEN @value = 'true' THEN '1' ELSE '0' END
                WHEN @db_type = 'Long' THEN N'v._Long = ' + @value
                WHEN @db_type = 'Double' THEN N'v.[_Double] = ' + @value
                WHEN @db_type = 'Numeric' THEN N'v._Numeric = ' + @value
                WHEN @is_bool = 1 THEN N'v._Boolean = ' + CASE WHEN @value = 'true' THEN '1' ELSE '0' END
                WHEN @is_numeric = 1 THEN N'(v._Long = ' + @value + N' OR v.[_Double] = ' + @value + N')'
                ELSE N'v._String = N''' + @safe_value + N''''
            END
        WHEN '$arrayAny' THEN N'1=1'
        WHEN '$arrayEmpty' THEN N'1=0'  -- Will be wrapped in NOT EXISTS
        WHEN '$arrayCount' THEN NULL  -- Special handling below
        WHEN '$arrayCountGt' THEN NULL
        WHEN '$arrayCountGte' THEN NULL
        WHEN '$arrayCountLt' THEN NULL
        WHEN '$arrayCountLte' THEN NULL
        -- Extended array operators
        WHEN '$arrayFirst' THEN 
            CASE 
                WHEN @db_type = 'Long' THEN N'v._array_index = ''0'' AND v._Long = ' + @value
                WHEN @db_type = 'Double' THEN N'v._array_index = ''0'' AND v.[_Double] = ' + @value
                WHEN @is_numeric = 1 THEN N'v._array_index = ''0'' AND (v._Long = ' + @value + N' OR v.[_Double] = ' + @value + N')'
                ELSE N'v._array_index = ''0'' AND v._String = N''' + @safe_value + N''''
            END
        WHEN '$arrayLast' THEN NULL  -- Special handling below
        WHEN '$arrayAt' THEN N'v._array_index = N''' + @safe_value + N''''
        WHEN '$arrayStartsWith' THEN N'v._String LIKE N''' + @safe_value + N'%'''
        WHEN '$arrayEndsWith' THEN N'v._String LIKE N''%' + @safe_value + N''''
        ELSE 
            CASE 
                WHEN @db_type = 'Boolean' THEN N'v._Boolean = ' + CASE WHEN @value = 'true' THEN '1' ELSE '0' END
                WHEN @db_type = 'DateTimeOffset' THEN N'v._DateTimeOffset = CONVERT(datetimeoffset, ''' + @safe_value + ''', 127)'
                WHEN @db_type = 'Guid' THEN N'v._Guid = ''' + @safe_value + ''''
                WHEN @db_type = 'Long' THEN N'v._Long = ' + @value
                WHEN @db_type = 'Double' THEN N'v.[_Double] = ' + @value
                WHEN @db_type = 'Numeric' THEN N'v._Numeric = ' + @value
                WHEN @is_bool = 1 THEN N'v._Boolean = ' + CASE WHEN @value = 'true' THEN '1' ELSE '0' END
                WHEN @is_date = 1 THEN N'v._DateTimeOffset = CONVERT(datetimeoffset, ''' + @safe_value + ''', 127)'
                WHEN @is_guid = 1 THEN N'v._Guid = ''' + @safe_value + ''''
                WHEN @is_numeric = 1 THEN N'(v._Long = ' + @value + N' OR v.[_Double] = ' + @value + N' OR v._Numeric = ' + @value + N')'
                ELSE N'v._String = N''' + @safe_value + N''''
            END
    END;
    
    -- Handle array count operators specially
    IF @operator LIKE '$arrayCount%'
    BEGIN
        DECLARE @count_op NVARCHAR(10) = CASE @operator
            WHEN '$arrayCount' THEN '='
            WHEN '$arrayCountGt' THEN '>'
            WHEN '$arrayCountGte' THEN '>='
            WHEN '$arrayCountLt' THEN '<'
            WHEN '$arrayCountLte' THEN '<='
        END;
        SET @result = N'(SELECT COUNT(*) FROM _values v WHERE v._id_object = ' + @table_alias + N'._id ' +
                      N'AND v._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' ' +
                      N'AND v._array_index IS NOT NULL) ' + @count_op + N' ' + @value;
        RETURN;
    END;
    
    -- Handle $arrayLast - needs subquery to find max index
    IF @operator = '$arrayLast'
    BEGIN
        SET @result = N'EXISTS (SELECT 1 FROM _values v WHERE v._id_object = ' + @table_alias + N'._id ' +
            N'AND v._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' ' +
            N'AND v._array_index = (SELECT MAX(v2._array_index) FROM _values v2 WHERE v2._id_object = ' + @table_alias + N'._id ' +
            N'AND v2._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' AND v2._array_index IS NOT NULL) ' +
            N'AND ' + CASE 
                WHEN @is_numeric = 1 THEN N'(v._Long = ' + @value + N' OR v._Double = ' + @value + N')'
                ELSE N'v._String = N''' + @safe_value + N''''
            END + N')';
        RETURN;
    END;
    
    -- Handle $between - value is JSON array [min, max]
    IF @operator = '$between'
    BEGIN
        DECLARE @min_val NVARCHAR(500);
        DECLARE @max_val NVARCHAR(500);
        SELECT @min_val = [value] FROM OPENJSON(@value) WHERE [key] = '0';
        SELECT @max_val = [value] FROM OPENJSON(@value) WHERE [key] = '1';
        
        IF @min_val IS NOT NULL AND @max_val IS NOT NULL
        BEGIN
            SET @result = N'EXISTS (SELECT 1 FROM _values v WHERE v._id_object = ' + @table_alias + N'._id ' +
                N'AND v._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' ' +
                N'AND v._array_index IS NULL AND (' +
                CASE 
                    WHEN @min_val LIKE '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]%' 
                    THEN N'v._DateTimeOffset BETWEEN CONVERT(datetimeoffset, ''' + REPLACE(@min_val, '''', '''''') + N''', 127) AND CONVERT(datetimeoffset, ''' + REPLACE(@max_val, '''', '''''') + N''', 127)'
                    WHEN @min_val LIKE '[0-9]%' OR @min_val LIKE '-[0-9]%'
                    THEN N'(v._Long BETWEEN ' + @min_val + N' AND ' + @max_val + N') OR (v._Double BETWEEN ' + @min_val + N' AND ' + @max_val + N')'
                    ELSE N'v._String BETWEEN N''' + REPLACE(@min_val, '''', '''''') + N''' AND N''' + REPLACE(@max_val, '''', '''''') + N''''
                END + N'))';
        END
        ELSE
            SET @result = N'1=1';
        RETURN;
    END;
    
    -- Handle array aggregation operators
    IF @operator IN ('$arraySum', '$arrayAvg', '$arrayMin', '$arrayMax')
    BEGIN
        DECLARE @agg_func NVARCHAR(10) = CASE @operator
            WHEN '$arraySum' THEN 'SUM'
            WHEN '$arrayAvg' THEN 'AVG'
            WHEN '$arrayMin' THEN 'MIN'
            WHEN '$arrayMax' THEN 'MAX'
        END;
        SET @result = N'(SELECT ' + @agg_func + N'(COALESCE(v._Long, v._Double, v._Numeric)) ' +
                      N'FROM _values v WHERE v._id_object = ' + @table_alias + N'._id ' +
                      N'AND v._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' ' +
                      N'AND v._array_index IS NOT NULL) = ' + @value;
        RETURN;
    END;
    
    -- Handle array aggregation comparison operators ($arraySumGt, $arraySumLt, etc.)
    IF @operator LIKE '$arraySum%' OR @operator LIKE '$arrayAvg%' OR @operator LIKE '$arrayMin%' OR @operator LIKE '$arrayMax%'
    BEGIN
        DECLARE @agg_base NVARCHAR(20);
        DECLARE @agg_cmp NVARCHAR(10);
        
        IF @operator LIKE '%Gt' SET @agg_cmp = '>';
        ELSE IF @operator LIKE '%Gte' SET @agg_cmp = '>=';
        ELSE IF @operator LIKE '%Lt' SET @agg_cmp = '<';
        ELSE IF @operator LIKE '%Lte' SET @agg_cmp = '<=';
        ELSE SET @agg_cmp = '=';
        
        IF @operator LIKE '$arraySum%' SET @agg_base = 'SUM';
        ELSE IF @operator LIKE '$arrayAvg%' SET @agg_base = 'AVG';
        ELSE IF @operator LIKE '$arrayMin%' SET @agg_base = 'MIN';
        ELSE IF @operator LIKE '$arrayMax%' SET @agg_base = 'MAX';
        
        SET @result = N'(SELECT ' + @agg_base + N'(COALESCE(v._Long, v._Double, v._Numeric)) ' +
                      N'FROM _values v WHERE v._id_object = ' + @table_alias + N'._id ' +
                      N'AND v._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' ' +
                      N'AND v._array_index IS NOT NULL) ' + @agg_cmp + N' ' + @value;
        RETURN;
    END;
    
    -- Build final EXISTS clause
    IF @dict_key IS NOT NULL
    BEGIN
        -- Dictionary access: Field[key]
        IF @is_nested = 1 AND @nested_structure_id IS NOT NULL
        BEGIN
            -- Dictionary with nested: Field[key].Prop
            SET @result = N'EXISTS (SELECT 1 FROM _values rv ' +
                N'JOIN _values v ON v._array_parent_id = rv._id AND v._id_structure = ' + CAST(@nested_structure_id AS NVARCHAR(20)) + N' ' +
                N'WHERE rv._id_object = ' + @table_alias + N'._id ' +
                N'AND rv._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' ' +
                N'AND rv._array_index = N''' + REPLACE(@dict_key, '''', '''''') + N''' ' +
                N'AND ' + @condition + N')';
        END
        ELSE
        BEGIN
            -- Simple dictionary: Field[key]
            SET @result = N'EXISTS (SELECT 1 FROM _values v ' +
                N'WHERE v._id_object = ' + @table_alias + N'._id ' +
                N'AND v._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' ' +
                N'AND v._array_index = N''' + REPLACE(@dict_key, '''', '''''') + N''' ' +
                N'AND ' + @condition + N')';
        END;
    END
    ELSE IF @is_array = 1 AND @is_nested = 1 AND @nested_structure_id IS NOT NULL
    BEGIN
        -- Array of Class: Field[].Prop
        SET @result = N'EXISTS (SELECT 1 FROM _values rv ' +
            N'JOIN _values v ON v._array_parent_id = rv._id AND v._id_structure = ' + CAST(@nested_structure_id AS NVARCHAR(20)) + N' ' +
            N'WHERE rv._id_object = ' + @table_alias + N'._id ' +
            N'AND rv._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' ' +
            N'AND rv._array_index IS NOT NULL ' +
            N'AND ' + @condition + N')';
    END
    ELSE IF @is_array = 1 AND (@type_semantic IS NULL OR @type_semantic <> '_RListItem' OR @is_nested = 0)
    BEGIN
        -- Simple array: Field[] (not ListItem array with nested field)
        IF @operator = '$arrayEmpty'
        BEGIN
            SET @result = N'NOT EXISTS (SELECT 1 FROM _values v ' +
                N'WHERE v._id_object = ' + @table_alias + N'._id ' +
                N'AND v._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' ' +
                N'AND v._array_index IS NOT NULL)';
        END
        ELSE
        BEGIN
            SET @result = N'EXISTS (SELECT 1 FROM _values v ' +
                N'WHERE v._id_object = ' + @table_alias + N'._id ' +
                N'AND v._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' ' +
                N'AND v._array_index IS NOT NULL ' +
                N'AND ' + @condition + N')';
        END;
    END
    ELSE IF @is_nested = 1 AND @nested_structure_id IS NOT NULL
    BEGIN
        -- Class field: Field.Prop
        SET @result = N'EXISTS (SELECT 1 FROM _values rv ' +
            N'JOIN _values v ON v._id_object = rv._id_object AND v._id_structure = ' + CAST(@nested_structure_id AS NVARCHAR(20)) + N' ' +
            N'WHERE rv._id_object = ' + @table_alias + N'._id ' +
            N'AND rv._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' ' +
            N'AND rv._array_index IS NULL ' +
            N'AND v._array_index IS NULL ' +
            N'AND ' + @condition + N')';
    END
    -- ListItem field: Status.Value, Status.Alias, Roles[].Value
    ELSE IF @is_nested = 1 AND @type_semantic = '_RListItem' AND @nested_field IN ('Value', 'Alias', 'Id', 'IdList')
    BEGIN
        DECLARE @li_column NVARCHAR(20) = CASE @nested_field
            WHEN 'Value' THEN '_value'
            WHEN 'Alias' THEN '_alias'
            WHEN 'Id' THEN '_id'
            WHEN 'IdList' THEN '_id_list'
        END;
        DECLARE @li_condition NVARCHAR(MAX) = REPLACE(@condition, N'v._String', N'li.' + @li_column);
        SET @li_condition = REPLACE(@li_condition, N'v._Long', N'li.' + @li_column);
        
        IF @is_array = 1
        BEGIN
            -- ListItem array: Roles[].Value
            SET @result = N'EXISTS (SELECT 1 FROM _values v ' +
                N'JOIN _list_items li ON li._id = v._listitem ' +
                N'WHERE v._id_object = ' + @table_alias + N'._id ' +
                N'AND v._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' ' +
                N'AND v._array_index IS NOT NULL ' +
                N'AND ' + @li_condition + N')';
        END
        ELSE
        BEGIN
            -- Simple ListItem: Status.Value
            SET @result = N'EXISTS (SELECT 1 FROM _values v ' +
                N'JOIN _list_items li ON li._id = v._listitem ' +
                N'WHERE v._id_object = ' + @table_alias + N'._id ' +
                N'AND v._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' ' +
                N'AND v._array_index IS NULL ' +
                N'AND ' + @li_condition + N')';
        END;
    END
    -- ListItem direct: search by ID (Status = 123)
    ELSE IF @type_semantic = '_RListItem' AND @is_nested = 0
    BEGIN
        SET @result = N'EXISTS (SELECT 1 FROM _values v ' +
            N'WHERE v._id_object = ' + @table_alias + N'._id ' +
            N'AND v._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' ' +
            N'AND v._array_index IS NULL ' +
            N'AND v._listitem = ' + @value + N')';
    END
    -- Dictionary existence check: PhoneBook != null or PhoneBook.$exists
    -- For Dictionary, values are stored with _array_index = key (not NULL)
    ELSE IF @collection_type IS NOT NULL AND @dict_key IS NULL AND (@operator = '$exists' OR (@operator = '$ne' AND (@value IS NULL OR @value = 'null')))
    BEGIN
        -- Check if any dictionary entry exists (any _array_index value)
        IF @operator = '$exists' AND @value = 'true'
        BEGIN
            SET @result = N'EXISTS (SELECT 1 FROM _values v ' +
                N'WHERE v._id_object = ' + @table_alias + N'._id ' +
                N'AND v._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' ' +
                N'AND v._array_index IS NOT NULL)';
        END
        ELSE IF @operator = '$exists' AND @value = 'false'
        BEGIN
            SET @result = N'NOT EXISTS (SELECT 1 FROM _values v ' +
                N'WHERE v._id_object = ' + @table_alias + N'._id ' +
                N'AND v._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' ' +
                N'AND v._array_index IS NOT NULL)';
        END
        ELSE IF @operator = '$ne' AND (@value IS NULL OR @value = 'null')
        BEGIN
            -- PhoneBook != null means dictionary has at least one entry
            SET @result = N'EXISTS (SELECT 1 FROM _values v ' +
                N'WHERE v._id_object = ' + @table_alias + N'._id ' +
                N'AND v._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' ' +
                N'AND v._array_index IS NOT NULL)';
        END
        ELSE
        BEGIN
            SET @result = N'1=1';
        END;
    END
    -- Property function: $length (string length) or $count (array count)
    ELSE IF @property_function IS NOT NULL
    BEGIN
        DECLARE @func_compare_op NVARCHAR(10) = CASE @operator
            WHEN '$eq' THEN '='
            WHEN '$ne' THEN '<>'
            WHEN '$gt' THEN '>'
            WHEN '$gte' THEN '>='
            WHEN '$lt' THEN '<'
            WHEN '$lte' THEN '<='
            ELSE '='
        END;
        
        IF @property_function = '$length'
        BEGIN
            -- String length: LEN(v._String)
            SET @result = N'EXISTS (SELECT 1 FROM _values v ' +
                N'WHERE v._id_object = ' + @table_alias + N'._id ' +
                N'AND v._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' ' +
                N'AND v._array_index IS NULL ' +
                N'AND LEN(v._String) ' + @func_compare_op + N' ' + @value + N')';
        END
        ELSE IF @property_function = '$count'
        BEGIN
            -- Array count: COUNT of array elements
            SET @result = N'(SELECT COUNT(*) FROM _values v ' +
                N'WHERE v._id_object = ' + @table_alias + N'._id ' +
                N'AND v._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' ' +
                N'AND v._array_index IS NOT NULL) ' + @func_compare_op + N' ' + @value;
        END
        ELSE
        BEGIN
            SET @result = N'1=1'; -- Unknown function, skip
        END;
    END
    ELSE
    BEGIN
        -- Simple field
        IF @operator = '$ne' AND @value IS NOT NULL AND @value <> 'null'
        BEGIN
            -- $ne needs NOT EXISTS for proper null handling in EAV
            -- Use numeric format for numeric values, string format for strings
            DECLARE @ne_value_expr NVARCHAR(100);
            IF @is_numeric = 1
                SET @ne_value_expr = @value;  -- Numeric: just the value
            ELSE
                SET @ne_value_expr = N'N''' + @safe_value + N'''';  -- String: N'value'
            
            SET @result = N'NOT EXISTS (SELECT 1 FROM _values v ' +
                N'WHERE v._id_object = ' + @table_alias + N'._id ' +
                N'AND v._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' ' +
                N'AND v._array_index IS NULL ' +
                N'AND v.' + @value_column + N' = ' + @ne_value_expr + N')';
        END
        ELSE
        BEGIN
            SET @result = N'EXISTS (SELECT 1 FROM _values v ' +
                N'WHERE v._id_object = ' + @table_alias + N'._id ' +
                N'AND v._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' ' +
                N'AND v._array_index IS NULL ' +
                N'AND ' + @condition + N')';
        END;
    END;
END;
GO

-- =====================================================
-- INTERNAL: Recursive filter parser
-- Parses JSON filters and builds WHERE clause
-- =====================================================
CREATE PROCEDURE dbo.internal_parse_filters
    @scheme_id BIGINT,
    @json NVARCHAR(MAX),
    @table_alias NVARCHAR(10),
    @logical_op NVARCHAR(10),  -- 'AND' or 'OR'
    @negate BIT,               -- For $not
    @result NVARCHAR(MAX) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Handle NULL, empty, '{}' and string 'null' from C#
    IF @json IS NULL OR @json = N'' OR @json = N'{}' OR @json = N'null'
    BEGIN
        SET @result = N'';
        RETURN;
    END;
    
    -- All variable declarations at the top (MSSQL requirement)
    DECLARE @conditions TABLE (idx INT IDENTITY(1,1), condition NVARCHAR(MAX));
    DECLARE @key NVARCHAR(500);
    DECLARE @value NVARCHAR(MAX);
    DECLARE @type INT;
    DECLARE @exists_sql NVARCHAR(MAX);
    DECLARE @sub_result NVARCHAR(MAX);
    
    -- Variables for $and
    DECLARE @and_conditions TABLE (idx INT IDENTITY(1,1), cond NVARCHAR(MAX));
    DECLARE @and_item NVARCHAR(MAX);
    DECLARE @and_result NVARCHAR(MAX);
    
    -- Variables for $or
    DECLARE @or_conditions TABLE (idx INT IDENTITY(1,1), cond NVARCHAR(MAX));
    DECLARE @or_item NVARCHAR(MAX);
    DECLARE @or_result NVARCHAR(MAX);
    
    -- Variables for $hasAncestor
    DECLARE @anc_condition NVARCHAR(MAX);
    DECLARE @anc_scheme_id BIGINT;
    DECLARE @anc_max_depth INT;
    DECLARE @anc_temp_json NVARCHAR(MAX);
    DECLARE @anc_built_condition NVARCHAR(MAX);
    
    -- Variables for $hasDescendant
    DECLARE @desc_condition NVARCHAR(MAX);
    DECLARE @desc_scheme_id BIGINT;
    DECLARE @desc_max_depth INT;
    DECLARE @desc_temp_json NVARCHAR(MAX);
    DECLARE @desc_built_condition NVARCHAR(MAX);
    
    -- Variables for $level
    DECLARE @level_op NVARCHAR(50);
    DECLARE @level_val NVARCHAR(50);
    DECLARE @level_cmp NVARCHAR(10);
    
    -- Variables for base fields (0$:)
    DECLARE @base_field NVARCHAR(100);
    DECLARE @base_col NVARCHAR(100);
    DECLARE @base_op NVARCHAR(50);
    DECLARE @base_val NVARCHAR(MAX);
    DECLARE @base_type INT;
    DECLARE @base_cond NVARCHAR(MAX);
    DECLARE @is_datetime_col BIT;
    DECLARE @safe_base_val NVARCHAR(MAX);
    DECLARE @is_datetime_simple BIT;
    DECLARE @safe_value_simple NVARCHAR(MAX);
    
    -- Variables for field operators
    DECLARE @op_name NVARCHAR(50);
    DECLARE @op_value NVARCHAR(MAX);
    DECLARE @op_type INT;
    DECLARE @op_value_type NVARCHAR(20);
    
    -- Variable for STRING_AGG separator
    DECLARE @separator NVARCHAR(10);
    
    -- Parse JSON object
    DECLARE json_cursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT [key], [value], [type] FROM OPENJSON(@json);
    
    OPEN json_cursor;
    FETCH NEXT FROM json_cursor INTO @key, @value, @type;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Handle logical operators
        IF @key = '$and'
        BEGIN
            -- $and: array of conditions, all must match
            DELETE FROM @and_conditions;
            
            DECLARE and_cursor CURSOR LOCAL FAST_FORWARD FOR
                SELECT [value] FROM OPENJSON(@value);
            
            OPEN and_cursor;
            FETCH NEXT FROM and_cursor INTO @and_item;
            
            WHILE @@FETCH_STATUS = 0
            BEGIN
                EXEC dbo.internal_parse_filters @scheme_id, @and_item, @table_alias, N'AND', 0, @and_result OUTPUT;
                IF @and_result IS NOT NULL AND @and_result <> N''
                    INSERT INTO @and_conditions (cond) VALUES (@and_result);
                FETCH NEXT FROM and_cursor INTO @and_item;
            END;
            
            CLOSE and_cursor;
            DEALLOCATE and_cursor;
            
            SELECT @sub_result = N'(' + STRING_AGG(cond, N' AND ') + N')'
            FROM @and_conditions;
            
            IF @sub_result IS NOT NULL
                INSERT INTO @conditions (condition) VALUES (@sub_result);
            
            DELETE FROM @and_conditions;
        END
        ELSE IF @key = '$or'
        BEGIN
            -- $or: array of conditions, any must match
            DELETE FROM @or_conditions;
            
            DECLARE or_cursor CURSOR LOCAL FAST_FORWARD FOR
                SELECT [value] FROM OPENJSON(@value);
            
            OPEN or_cursor;
            FETCH NEXT FROM or_cursor INTO @or_item;
            
            WHILE @@FETCH_STATUS = 0
            BEGIN
                EXEC dbo.internal_parse_filters @scheme_id, @or_item, @table_alias, N'AND', 0, @or_result OUTPUT;
                IF @or_result IS NOT NULL AND @or_result <> N''
                    INSERT INTO @or_conditions (cond) VALUES (@or_result);
                FETCH NEXT FROM or_cursor INTO @or_item;
            END;
            
            CLOSE or_cursor;
            DEALLOCATE or_cursor;
            
            SELECT @sub_result = N'(' + STRING_AGG(cond, N' OR ') + N')'
            FROM @or_conditions;
            
            IF @sub_result IS NOT NULL
                INSERT INTO @conditions (condition) VALUES (@sub_result);
            
            DELETE FROM @or_conditions;
        END
        ELSE IF @key = '$not'
        BEGIN
            -- $not: negate the inner condition
            EXEC dbo.internal_parse_filters @scheme_id, @value, @table_alias, N'AND', 1, @sub_result OUTPUT;
            IF @sub_result IS NOT NULL AND @sub_result <> N''
                INSERT INTO @conditions (condition) VALUES (N'NOT (' + @sub_result + N')');
        END
        -- Hierarchical operators
        ELSE IF @key = '$isRoot'
        BEGIN
            IF @value = 'true'
                INSERT INTO @conditions (condition) VALUES (@table_alias + N'._id_parent IS NULL');
            ELSE
                INSERT INTO @conditions (condition) VALUES (@table_alias + N'._id_parent IS NOT NULL');
        END
        ELSE IF @key = '$isLeaf'
        BEGIN
            IF @value = 'true'
                INSERT INTO @conditions (condition) VALUES (N'NOT EXISTS (SELECT 1 FROM _objects c WHERE c._id_parent = ' + @table_alias + N'._id)');
            ELSE
                INSERT INTO @conditions (condition) VALUES (N'EXISTS (SELECT 1 FROM _objects c WHERE c._id_parent = ' + @table_alias + N'._id)');
        END
        ELSE IF @key = '$childrenOf'
        BEGIN
            -- $childrenOf: direct children of specified parent
            INSERT INTO @conditions (condition) VALUES (@table_alias + N'._id_parent = ' + @value);
        END
        ELSE IF @key = '$hasAncestor'
        BEGIN
            -- $hasAncestor: check if object has ancestor matching condition
            SET @anc_condition = N'';
            SET @anc_scheme_id = NULL;
            SET @anc_max_depth = 50;
            
            IF @type = 5  -- object
            BEGIN
                SET @anc_scheme_id = JSON_VALUE(@value, '$.scheme_id');
                SET @anc_max_depth = ISNULL(CAST(JSON_VALUE(@value, '$.max_depth') AS INT), 50);
                
                SET @anc_temp_json = JSON_QUERY(@value, '$.condition');
                IF @anc_temp_json IS NOT NULL
                BEGIN
                    EXEC dbo.internal_parse_filters @anc_scheme_id, @anc_temp_json, N'anc', N'AND', 0, @anc_condition OUTPUT;
                END;
            END;
            
            -- Build condition string separately (CASE not allowed in VALUES)
            SET @anc_built_condition = N'(SELECT 1 WHERE EXISTS (SELECT 1 FROM _objects anc WHERE anc._id IN (' +
                N'SELECT p1._id FROM _objects p1 WHERE p1._id = ' + @table_alias + N'._id_parent ' +
                N'UNION SELECT p2._id FROM _objects p2 JOIN _objects p1 ON p2._id = p1._id_parent WHERE p1._id = ' + @table_alias + N'._id_parent ' +
                N'UNION SELECT p3._id FROM _objects p3 JOIN _objects p2 ON p3._id = p2._id_parent JOIN _objects p1 ON p2._id = p1._id_parent WHERE p1._id = ' + @table_alias + N'._id_parent ' +
                N'UNION SELECT p4._id FROM _objects p4 JOIN _objects p3 ON p4._id = p3._id_parent JOIN _objects p2 ON p3._id = p2._id_parent JOIN _objects p1 ON p2._id = p1._id_parent WHERE p1._id = ' + @table_alias + N'._id_parent ' +
                N'UNION SELECT p5._id FROM _objects p5 JOIN _objects p4 ON p5._id = p4._id_parent JOIN _objects p3 ON p4._id = p3._id_parent JOIN _objects p2 ON p3._id = p2._id_parent JOIN _objects p1 ON p2._id = p1._id_parent WHERE p1._id = ' + @table_alias + N'._id_parent ' +
                N')';
            
            IF @anc_scheme_id IS NOT NULL
                SET @anc_built_condition = @anc_built_condition + N' AND anc._id_scheme = ' + CAST(@anc_scheme_id AS NVARCHAR(20));
            IF @anc_condition IS NOT NULL AND @anc_condition <> N''
                SET @anc_built_condition = @anc_built_condition + N' AND ' + @anc_condition;
            
            SET @anc_built_condition = @anc_built_condition + N')) = 1';
            
            INSERT INTO @conditions (condition) VALUES (@anc_built_condition);
        END
        ELSE IF @key = '$hasDescendant'
        BEGIN
            -- $hasDescendant: check if object has descendant matching condition
            SET @desc_condition = N'';
            SET @desc_scheme_id = NULL;
            SET @desc_max_depth = 50;
            
            IF @type = 5  -- object
            BEGIN
                SET @desc_scheme_id = JSON_VALUE(@value, '$.scheme_id');
                SET @desc_max_depth = ISNULL(CAST(JSON_VALUE(@value, '$.max_depth') AS INT), 50);
                
                SET @desc_temp_json = JSON_QUERY(@value, '$.condition');
                IF @desc_temp_json IS NOT NULL
                BEGIN
                    EXEC dbo.internal_parse_filters @desc_scheme_id, @desc_temp_json, N'desc_obj', N'AND', 0, @desc_condition OUTPUT;
                END;
            END;
            
            -- Build condition string separately (CASE not allowed in VALUES)
            SET @desc_built_condition = N'EXISTS (SELECT 1 FROM _objects desc_obj WHERE desc_obj._id IN (' +
                N'SELECT c1._id FROM _objects c1 WHERE c1._id_parent = ' + @table_alias + N'._id ' +
                N'UNION SELECT c2._id FROM _objects c2 JOIN _objects c1 ON c2._id_parent = c1._id WHERE c1._id_parent = ' + @table_alias + N'._id ' +
                N'UNION SELECT c3._id FROM _objects c3 JOIN _objects c2 ON c3._id_parent = c2._id JOIN _objects c1 ON c2._id_parent = c1._id WHERE c1._id_parent = ' + @table_alias + N'._id ' +
                N'UNION SELECT c4._id FROM _objects c4 JOIN _objects c3 ON c4._id_parent = c3._id JOIN _objects c2 ON c3._id_parent = c2._id JOIN _objects c1 ON c2._id_parent = c1._id WHERE c1._id_parent = ' + @table_alias + N'._id ' +
                N'UNION SELECT c5._id FROM _objects c5 JOIN _objects c4 ON c5._id_parent = c4._id JOIN _objects c3 ON c4._id_parent = c3._id JOIN _objects c2 ON c3._id_parent = c2._id JOIN _objects c1 ON c2._id_parent = c1._id WHERE c1._id_parent = ' + @table_alias + N'._id' +
                N')';
            
            IF @desc_scheme_id IS NOT NULL
                SET @desc_built_condition = @desc_built_condition + N' AND desc_obj._id_scheme = ' + CAST(@desc_scheme_id AS NVARCHAR(20));
            IF @desc_condition IS NOT NULL AND @desc_condition <> N''
                SET @desc_built_condition = @desc_built_condition + N' AND ' + @desc_condition;
            
            SET @desc_built_condition = @desc_built_condition + N')';
            
            INSERT INTO @conditions (condition) VALUES (@desc_built_condition);
        END
        ELSE IF @key = '$level'
        BEGIN
            -- $level: use parent chain counting
            IF @type = 2  -- number (direct value like "$level": 2)
            BEGIN
                -- Simple level check via counting parent chain
                IF @value = '0'
                BEGIN
                    INSERT INTO @conditions (condition) VALUES (@table_alias + N'._id_parent IS NULL');
                END
                ELSE
                BEGIN
                    INSERT INTO @conditions (condition) VALUES (
                        N'(SELECT COUNT(*) FROM (' +
                        N'SELECT p1._id FROM _objects p1 WHERE p1._id = ' + @table_alias + N'._id_parent ' +
                        N'UNION ALL SELECT p2._id FROM _objects p2 JOIN _objects p1 ON p2._id = p1._id_parent WHERE p1._id = ' + @table_alias + N'._id_parent ' +
                        N'UNION ALL SELECT p3._id FROM _objects p3 JOIN _objects p2 ON p3._id = p2._id_parent JOIN _objects p1 ON p2._id = p1._id_parent WHERE p1._id = ' + @table_alias + N'._id_parent ' +
                        N'UNION ALL SELECT p4._id FROM _objects p4 JOIN _objects p3 ON p4._id = p3._id_parent JOIN _objects p2 ON p3._id = p2._id_parent JOIN _objects p1 ON p2._id = p1._id_parent WHERE p1._id = ' + @table_alias + N'._id_parent ' +
                        N'UNION ALL SELECT p5._id FROM _objects p5 JOIN _objects p4 ON p5._id = p4._id_parent JOIN _objects p3 ON p4._id = p3._id_parent JOIN _objects p2 ON p3._id = p2._id_parent JOIN _objects p1 ON p2._id = p1._id_parent WHERE p1._id = ' + @table_alias + N'._id_parent ' +
                        N') AS parents) = ' + @value
                    );
                END;
            END
            ELSE IF @type = 5  -- object with operators like "$level": {"$gt": 2}
            BEGIN
                DECLARE level_cursor CURSOR LOCAL FAST_FORWARD FOR
                    SELECT [key], [value] FROM OPENJSON(@value);
                
                OPEN level_cursor;
                FETCH NEXT FROM level_cursor INTO @level_op, @level_val;
                
                WHILE @@FETCH_STATUS = 0
                BEGIN
                    SET @level_cmp = CASE @level_op
                        WHEN '$gt' THEN '>'
                        WHEN '$gte' THEN '>='
                        WHEN '$lt' THEN '<'
                        WHEN '$lte' THEN '<='
                        WHEN '$eq' THEN '='
                        WHEN '$ne' THEN '<>'
                        ELSE '='
                    END;
                    
                    -- Optimized checks for common cases
                    IF @level_val = '0' AND @level_cmp = '='
                    BEGIN
                        INSERT INTO @conditions (condition) VALUES (@table_alias + N'._id_parent IS NULL');
                    END
                    ELSE IF @level_val = '0' AND @level_cmp = '>'
                    BEGIN
                        INSERT INTO @conditions (condition) VALUES (@table_alias + N'._id_parent IS NOT NULL');
                    END
                    ELSE IF @level_val = '0' AND @level_cmp = '>='
                    BEGIN
                        INSERT INTO @conditions (condition) VALUES (N'1=1'); -- all objects have level >= 0
                    END
                    ELSE IF @level_val = '1' AND @level_cmp = '='
                    BEGIN
                        INSERT INTO @conditions (condition) VALUES (
                            @table_alias + N'._id_parent IS NOT NULL AND EXISTS (SELECT 1 FROM _objects p WHERE p._id = ' + @table_alias + N'._id_parent AND p._id_parent IS NULL)'
                        );
                    END
                    ELSE
                    BEGIN
                        -- General case: count parent chain (supports up to 10 levels)
                        INSERT INTO @conditions (condition) VALUES (
                            N'(SELECT COUNT(*) FROM (' +
                            N'SELECT p1._id FROM _objects p1 WHERE p1._id = ' + @table_alias + N'._id_parent ' +
                            N'UNION ALL SELECT p2._id FROM _objects p2 JOIN _objects p1 ON p2._id = p1._id_parent WHERE p1._id = ' + @table_alias + N'._id_parent ' +
                            N'UNION ALL SELECT p3._id FROM _objects p3 JOIN _objects p2 ON p3._id = p2._id_parent JOIN _objects p1 ON p2._id = p1._id_parent WHERE p1._id = ' + @table_alias + N'._id_parent ' +
                            N'UNION ALL SELECT p4._id FROM _objects p4 JOIN _objects p3 ON p4._id = p3._id_parent JOIN _objects p2 ON p3._id = p2._id_parent JOIN _objects p1 ON p2._id = p1._id_parent WHERE p1._id = ' + @table_alias + N'._id_parent ' +
                            N'UNION ALL SELECT p5._id FROM _objects p5 JOIN _objects p4 ON p5._id = p4._id_parent JOIN _objects p3 ON p4._id = p3._id_parent JOIN _objects p2 ON p3._id = p2._id_parent JOIN _objects p1 ON p2._id = p1._id_parent WHERE p1._id = ' + @table_alias + N'._id_parent ' +
                            N'UNION ALL SELECT p6._id FROM _objects p6 JOIN _objects p5 ON p6._id = p5._id_parent JOIN _objects p4 ON p5._id = p4._id_parent JOIN _objects p3 ON p4._id = p3._id_parent JOIN _objects p2 ON p3._id = p2._id_parent JOIN _objects p1 ON p2._id = p1._id_parent WHERE p1._id = ' + @table_alias + N'._id_parent ' +
                            N'UNION ALL SELECT p7._id FROM _objects p7 JOIN _objects p6 ON p7._id = p6._id_parent JOIN _objects p5 ON p6._id = p5._id_parent JOIN _objects p4 ON p5._id = p4._id_parent JOIN _objects p3 ON p4._id = p3._id_parent JOIN _objects p2 ON p3._id = p2._id_parent JOIN _objects p1 ON p2._id = p1._id_parent WHERE p1._id = ' + @table_alias + N'._id_parent ' +
                            N'UNION ALL SELECT p8._id FROM _objects p8 JOIN _objects p7 ON p8._id = p7._id_parent JOIN _objects p6 ON p7._id = p6._id_parent JOIN _objects p5 ON p6._id = p5._id_parent JOIN _objects p4 ON p5._id = p4._id_parent JOIN _objects p3 ON p4._id = p3._id_parent JOIN _objects p2 ON p3._id = p2._id_parent JOIN _objects p1 ON p2._id = p1._id_parent WHERE p1._id = ' + @table_alias + N'._id_parent ' +
                            N'UNION ALL SELECT p9._id FROM _objects p9 JOIN _objects p8 ON p9._id = p8._id_parent JOIN _objects p7 ON p8._id = p7._id_parent JOIN _objects p6 ON p7._id = p6._id_parent JOIN _objects p5 ON p6._id = p5._id_parent JOIN _objects p4 ON p5._id = p4._id_parent JOIN _objects p3 ON p4._id = p3._id_parent JOIN _objects p2 ON p3._id = p2._id_parent JOIN _objects p1 ON p2._id = p1._id_parent WHERE p1._id = ' + @table_alias + N'._id_parent ' +
                            N'UNION ALL SELECT p10._id FROM _objects p10 JOIN _objects p9 ON p10._id = p9._id_parent JOIN _objects p8 ON p9._id = p8._id_parent JOIN _objects p7 ON p8._id = p7._id_parent JOIN _objects p6 ON p7._id = p6._id_parent JOIN _objects p5 ON p6._id = p5._id_parent JOIN _objects p4 ON p5._id = p4._id_parent JOIN _objects p3 ON p4._id = p3._id_parent JOIN _objects p2 ON p3._id = p2._id_parent JOIN _objects p1 ON p2._id = p1._id_parent WHERE p1._id = ' + @table_alias + N'._id_parent ' +
                            N') AS parents) ' + @level_cmp + N' ' + @level_val
                        );
                    END;
                    
                    FETCH NEXT FROM level_cursor INTO @level_op, @level_val;
                END;
                
                CLOSE level_cursor;
                DEALLOCATE level_cursor;
            END;
        END
        -- Base field (0$: prefix)
        ELSE IF @key LIKE '0$:%'
        BEGIN
            SET @base_field = SUBSTRING(@key, 4, LEN(@key));
            SET @base_col = dbo.normalize_base_field_name(@base_field);
            
            IF @base_col IS NOT NULL
            BEGIN
                IF @type = 5  -- object with operators
                BEGIN
                    DECLARE base_cursor CURSOR LOCAL FAST_FORWARD FOR
                        SELECT [key], [value], [type] FROM OPENJSON(@value);
                    
                    OPEN base_cursor;
                    FETCH NEXT FROM base_cursor INTO @base_op, @base_val, @base_type;
                    
                    WHILE @@FETCH_STATUS = 0
                    BEGIN
                        -- Detect datetime base columns for proper CONVERT handling
                        SET @is_datetime_col = CASE WHEN @base_col IN ('_date_create', '_date_modify', '_date_begin', '_date_complete') THEN 1 ELSE 0 END;
                        SET @safe_base_val = REPLACE(@base_val, '''', '''''');
                        
                        SET @base_cond = CASE @base_op
                            WHEN '$eq' THEN 
                                CASE WHEN @is_datetime_col = 1 
                                    THEN @table_alias + N'.' + @base_col + N' = CONVERT(datetimeoffset, ''' + @safe_base_val + N''', 127)'
                                    ELSE @table_alias + N'.' + @base_col + N' = N''' + @safe_base_val + N''''
                                END
                            WHEN '$ne' THEN 
                                CASE WHEN @is_datetime_col = 1 
                                    THEN @table_alias + N'.' + @base_col + N' <> CONVERT(datetimeoffset, ''' + @safe_base_val + N''', 127)'
                                    ELSE @table_alias + N'.' + @base_col + N' <> N''' + @safe_base_val + N''''
                                END
                            WHEN '$gt' THEN 
                                CASE WHEN @is_datetime_col = 1 
                                    THEN @table_alias + N'.' + @base_col + N' > CONVERT(datetimeoffset, ''' + @safe_base_val + N''', 127)'
                                    ELSE @table_alias + N'.' + @base_col + N' > ' + @base_val
                                END
                            WHEN '$gte' THEN 
                                CASE WHEN @is_datetime_col = 1 
                                    THEN @table_alias + N'.' + @base_col + N' >= CONVERT(datetimeoffset, ''' + @safe_base_val + N''', 127)'
                                    ELSE @table_alias + N'.' + @base_col + N' >= ' + @base_val
                                END
                            WHEN '$lt' THEN 
                                CASE WHEN @is_datetime_col = 1 
                                    THEN @table_alias + N'.' + @base_col + N' < CONVERT(datetimeoffset, ''' + @safe_base_val + N''', 127)'
                                    ELSE @table_alias + N'.' + @base_col + N' < ' + @base_val
                                END
                            WHEN '$lte' THEN 
                                CASE WHEN @is_datetime_col = 1 
                                    THEN @table_alias + N'.' + @base_col + N' <= CONVERT(datetimeoffset, ''' + @safe_base_val + N''', 127)'
                                    ELSE @table_alias + N'.' + @base_col + N' <= ' + @base_val
                                END
                            WHEN '$contains' THEN @table_alias + N'.' + @base_col + N' LIKE N''%' + @safe_base_val + N'%'''
                            WHEN '$startsWith' THEN @table_alias + N'.' + @base_col + N' LIKE N''' + @safe_base_val + N'%'''
                            WHEN '$endsWith' THEN @table_alias + N'.' + @base_col + N' LIKE N''%' + @safe_base_val + N''''
                            WHEN '$in' THEN @table_alias + N'.' + @base_col + N' IN (SELECT [value] FROM OPENJSON(N''' + @safe_base_val + N'''))'
                            WHEN '$exists' THEN CASE WHEN @base_val = 'true' THEN @table_alias + N'.' + @base_col + N' IS NOT NULL' ELSE @table_alias + N'.' + @base_col + N' IS NULL' END
                            ELSE 
                                CASE WHEN @is_datetime_col = 1 
                                    THEN @table_alias + N'.' + @base_col + N' = CONVERT(datetimeoffset, ''' + @safe_base_val + N''', 127)'
                                    ELSE @table_alias + N'.' + @base_col + N' = N''' + @safe_base_val + N''''
                                END
                        END;
                        
                        IF @base_cond IS NOT NULL
                            INSERT INTO @conditions (condition) VALUES (@base_cond);
                        
                        FETCH NEXT FROM base_cursor INTO @base_op, @base_val, @base_type;
                    END;
                    
                    CLOSE base_cursor;
                    DEALLOCATE base_cursor;
                END
                ELSE
                BEGIN
                    -- Simple value - detect datetime columns for proper CONVERT
                    SET @is_datetime_simple = CASE WHEN @base_col IN ('_date_create', '_date_modify', '_date_begin', '_date_complete') THEN 1 ELSE 0 END;
                    SET @safe_value_simple = REPLACE(@value, '''', '''''');
                    
                    IF @is_datetime_simple = 1
                        INSERT INTO @conditions (condition) VALUES (
                            @table_alias + N'.' + @base_col + N' = CONVERT(datetimeoffset, ''' + @safe_value_simple + N''', 127)'
                        );
                    ELSE
                        INSERT INTO @conditions (condition) VALUES (
                            @table_alias + N'.' + @base_col + N' = N''' + @safe_value_simple + N''''
                        );
                END;
            END;
        END
        -- Dictionary indexer: FieldName[key] (e.g.: "PhoneBook[home]": {"$eq": "+7-999..."})
        -- Uses db_type from metadata cache for correct column selection
        ELSE IF @key LIKE '%[%]%' AND @key NOT LIKE '$%'
        BEGIN
            DECLARE @dict_idx_field_name NVARCHAR(450);
            DECLARE @dict_idx_key NVARCHAR(450);
            DECLARE @dict_idx_structure_id BIGINT;
            DECLARE @dict_idx_db_type NVARCHAR(50);
            DECLARE @dict_idx_op NVARCHAR(50);
            DECLARE @dict_idx_val NVARCHAR(MAX);
            DECLARE @dict_idx_val_type INT;
            DECLARE @dict_idx_condition NVARCHAR(MAX);
            DECLARE @dict_idx_value_cond NVARCHAR(MAX);
            DECLARE @dict_idx_safe_val NVARCHAR(MAX);
            
            -- Parse: "PhoneBook[home]" -> field="PhoneBook", key="home"
            SET @dict_idx_field_name = LEFT(@key, CHARINDEX('[', @key) - 1);
            SET @dict_idx_key = SUBSTRING(@key, CHARINDEX('[', @key) + 1, CHARINDEX(']', @key) - CHARINDEX('[', @key) - 1);
            
            -- Find structure and db_type for dictionary field from metadata cache
            SELECT @dict_idx_structure_id = c._structure_id, @dict_idx_db_type = c.db_type
            FROM _scheme_metadata_cache c
            WHERE c._scheme_id = @scheme_id 
              AND c._name = @dict_idx_field_name
              AND c._parent_structure_id IS NULL;
            
            IF @dict_idx_structure_id IS NOT NULL
            BEGIN
                IF @type = 5  -- object with operators like {"$eq": "value"}
                BEGIN
                    DECLARE dict_idx_cursor CURSOR LOCAL FAST_FORWARD FOR
                        SELECT [key], [value], [type] FROM OPENJSON(@value);
                    
                    OPEN dict_idx_cursor;
                    FETCH NEXT FROM dict_idx_cursor INTO @dict_idx_op, @dict_idx_val, @dict_idx_val_type;
                    
                    WHILE @@FETCH_STATUS = 0
                    BEGIN
                        SET @dict_idx_safe_val = REPLACE(@dict_idx_val, '''', '''''');
                        
                        -- Build value condition based on operator AND db_type
                        IF @dict_idx_op IN ('$gt', '$gte', '$lt', '$lte')
                        BEGIN
                            -- Numeric comparisons - use db_type for correct column
                            SET @dict_idx_value_cond = CASE @dict_idx_db_type
                                WHEN 'Numeric' THEN N'dv._Numeric ' + 
                                    CASE @dict_idx_op WHEN '$gt' THEN '>' WHEN '$gte' THEN '>=' WHEN '$lt' THEN '<' WHEN '$lte' THEN '<=' END +
                                    N' ' + @dict_idx_val
                                WHEN 'Double' THEN N'dv.[_Double] ' + 
                                    CASE @dict_idx_op WHEN '$gt' THEN '>' WHEN '$gte' THEN '>=' WHEN '$lt' THEN '<' WHEN '$lte' THEN '<=' END +
                                    N' ' + @dict_idx_val
                                ELSE N'dv._Long ' + 
                                    CASE @dict_idx_op WHEN '$gt' THEN '>' WHEN '$gte' THEN '>=' WHEN '$lt' THEN '<' WHEN '$lte' THEN '<=' END +
                                    N' ' + @dict_idx_val
                            END;
                        END
                        ELSE IF @dict_idx_op = '$in'
                        BEGIN
                            -- $in operator - use db_type for correct column
                            SET @dict_idx_value_cond = CASE @dict_idx_db_type
                                WHEN 'Long' THEN N'dv._Long IN (SELECT CAST([value] AS BIGINT) FROM OPENJSON(N''' + @dict_idx_safe_val + N'''))'
                                WHEN 'Numeric' THEN N'dv._Numeric IN (SELECT CAST([value] AS DECIMAL(38,18)) FROM OPENJSON(N''' + @dict_idx_safe_val + N'''))'
                                WHEN 'Double' THEN N'dv.[_Double] IN (SELECT CAST([value] AS FLOAT) FROM OPENJSON(N''' + @dict_idx_safe_val + N'''))'
                                ELSE N'dv._String IN (SELECT [value] FROM OPENJSON(N''' + @dict_idx_safe_val + N'''))'
                            END;
                        END
                        ELSE IF @dict_idx_op = '$nin'
                        BEGIN
                            -- $nin operator - use db_type for correct column
                            SET @dict_idx_value_cond = CASE @dict_idx_db_type
                                WHEN 'Long' THEN N'dv._Long NOT IN (SELECT CAST([value] AS BIGINT) FROM OPENJSON(N''' + @dict_idx_safe_val + N'''))'
                                WHEN 'Numeric' THEN N'dv._Numeric NOT IN (SELECT CAST([value] AS DECIMAL(38,18)) FROM OPENJSON(N''' + @dict_idx_safe_val + N'''))'
                                WHEN 'Double' THEN N'dv.[_Double] NOT IN (SELECT CAST([value] AS FLOAT) FROM OPENJSON(N''' + @dict_idx_safe_val + N'''))'
                                ELSE N'dv._String NOT IN (SELECT [value] FROM OPENJSON(N''' + @dict_idx_safe_val + N'''))'
                            END;
                        END
                        ELSE
                        BEGIN
                            -- Other operators
                            SET @dict_idx_value_cond = CASE @dict_idx_op
                                -- Equality/Inequality - use db_type for correct column
                                WHEN '$eq' THEN CASE @dict_idx_db_type
                                    WHEN 'Long' THEN N'dv._Long = ' + @dict_idx_val
                                    WHEN 'Numeric' THEN N'dv._Numeric = ' + @dict_idx_val
                                    WHEN 'Double' THEN N'dv.[_Double] = ' + @dict_idx_val
                                    ELSE N'dv._String = N''' + @dict_idx_safe_val + N''''
                                END
                                WHEN '$ne' THEN CASE @dict_idx_db_type
                                    WHEN 'Long' THEN N'dv._Long <> ' + @dict_idx_val
                                    WHEN 'Numeric' THEN N'dv._Numeric <> ' + @dict_idx_val
                                    WHEN 'Double' THEN N'dv.[_Double] <> ' + @dict_idx_val
                                    ELSE N'dv._String <> N''' + @dict_idx_safe_val + N''''
                                END
                                -- String operations (always use _String)
                                WHEN '$contains' THEN N'dv._String LIKE N''%' + @dict_idx_safe_val + N'%'''
                                WHEN '$startsWith' THEN N'dv._String LIKE N''' + @dict_idx_safe_val + N'%'''
                                WHEN '$endsWith' THEN N'dv._String LIKE N''%' + @dict_idx_safe_val + N''''
                                WHEN '$containsIgnoreCase' THEN N'dv._String LIKE N''%' + @dict_idx_safe_val + N'%'' COLLATE Latin1_General_CI_AS'
                                WHEN '$startsWithIgnoreCase' THEN N'dv._String LIKE N''' + @dict_idx_safe_val + N'%'' COLLATE Latin1_General_CI_AS'
                                WHEN '$endsWithIgnoreCase' THEN N'dv._String LIKE N''%' + @dict_idx_safe_val + N''' COLLATE Latin1_General_CI_AS'
                                ELSE CASE @dict_idx_db_type
                                    WHEN 'Long' THEN N'dv._Long = ' + @dict_idx_val
                                    WHEN 'Numeric' THEN N'dv._Numeric = ' + @dict_idx_val
                                    WHEN 'Double' THEN N'dv.[_Double] = ' + @dict_idx_val
                                    ELSE N'dv._String = N''' + @dict_idx_safe_val + N''''
                                END
                            END;
                        END;
                        
                        SET @dict_idx_condition = 
                            N'EXISTS (SELECT 1 FROM _values dv WHERE dv._id_object = ' + @table_alias + N'._id ' +
                            N'AND dv._id_structure = ' + CAST(@dict_idx_structure_id AS NVARCHAR(20)) + N' ' +
                            N'AND dv._array_index = N''' + REPLACE(@dict_idx_key, '''', '''''') + N''' ' +
                            N'AND ' + @dict_idx_value_cond + N')';
                        
                        INSERT INTO @conditions (condition) VALUES (@dict_idx_condition);
                        
                        FETCH NEXT FROM dict_idx_cursor INTO @dict_idx_op, @dict_idx_val, @dict_idx_val_type;
                    END;
                    
                    CLOSE dict_idx_cursor;
                    DEALLOCATE dict_idx_cursor;
                END
                ELSE
                BEGIN
                    -- Simple value: direct equality (use db_type for correct column)
                    SET @dict_idx_safe_val = REPLACE(@value, '''', '''''');
                    SET @dict_idx_condition = 
                        N'EXISTS (SELECT 1 FROM _values dv WHERE dv._id_object = ' + @table_alias + N'._id ' +
                        N'AND dv._id_structure = ' + CAST(@dict_idx_structure_id AS NVARCHAR(20)) + N' ' +
                        N'AND dv._array_index = N''' + REPLACE(@dict_idx_key, '''', '''''') + N''' ' +
                        N'AND ' + CASE @dict_idx_db_type
                            WHEN 'Long' THEN N'dv._Long = ' + @value
                            WHEN 'Numeric' THEN N'dv._Numeric = ' + @value
                            WHEN 'Double' THEN N'dv.[_Double] = ' + @value
                            ELSE N'dv._String = N''' + @dict_idx_safe_val + N''''
                        END + N')';
                    
                    INSERT INTO @conditions (condition) VALUES (@dict_idx_condition);
                END;
            END;
        END
        -- Dictionary ContainsKey: FieldName.ContainsKey (e.g.: "PhoneBook.ContainsKey": "home")
        ELSE IF @key LIKE '%.ContainsKey'
        BEGIN
            DECLARE @dict_field_name NVARCHAR(450) = LEFT(@key, LEN(@key) - LEN('.ContainsKey'));
            DECLARE @dict_key NVARCHAR(450);
            DECLARE @dict_structure_id BIGINT;
            
            -- Get the key value (handle both {"$eq": "key"} and "key")
            IF @type = 5 AND JSON_VALUE(@value, '$."$eq"') IS NOT NULL
                SET @dict_key = JSON_VALUE(@value, '$."$eq"');
            ELSE
                SET @dict_key = @value;
            
            -- Remove quotes from string value
            SET @dict_key = REPLACE(REPLACE(@dict_key, '"', ''), '''', '');
            
            -- Find structure for dictionary field
            SELECT @dict_structure_id = _id
            FROM _structures 
            WHERE _id_scheme = @scheme_id 
              AND _name = @dict_field_name
              AND _id_parent IS NULL;
            
            IF @dict_structure_id IS NOT NULL
            BEGIN
                -- Build EXISTS condition
                -- Check BOTH cases:
                --   1) Dictionary<K, primitive>: records in the structure itself (_id_structure = dict_id)
                --   2) Dictionary<K, Class>: records in child structures (_id_parent = dict_id)
                INSERT INTO @conditions (condition) VALUES (
                    N'EXISTS (SELECT 1 FROM _values dv JOIN _structures s ON dv._id_structure = s._id ' +
                    N'WHERE dv._id_object = ' + @table_alias + N'._id ' +
                    N'AND (s._id = ' + CAST(@dict_structure_id AS NVARCHAR(20)) + N' OR s._id_parent = ' + CAST(@dict_structure_id AS NVARCHAR(20)) + N') ' +
                    N'AND dv._array_index = N''' + REPLACE(@dict_key, '''', '''''') + N''')'
                );
            END;
        END
        -- Props field
        ELSE IF @key NOT LIKE '$%'
        BEGIN
            IF @type = 5  -- object with operators
            BEGIN
                DECLARE op_cursor CURSOR LOCAL FAST_FORWARD FOR
                    SELECT [key], [value], [type] FROM OPENJSON(@value);
                
                OPEN op_cursor;
                FETCH NEXT FROM op_cursor INTO @op_name, @op_value, @op_type;
                
                WHILE @@FETCH_STATUS = 0
                BEGIN
                    SET @op_value_type = CASE @op_type WHEN 1 THEN 'string' WHEN 2 THEN 'number' WHEN 3 THEN 'boolean' WHEN 4 THEN 'array' ELSE 'string' END;
                    EXEC dbo.internal_build_exists @scheme_id, @key, @op_name, @op_value, @op_value_type, @table_alias, @exists_sql OUTPUT;
                    
                    IF @exists_sql IS NOT NULL AND @exists_sql <> N'' AND @exists_sql <> N'1=1'
                        INSERT INTO @conditions (condition) VALUES (@exists_sql);
                    
                    FETCH NEXT FROM op_cursor INTO @op_name, @op_value, @op_type;
                END;
                
                CLOSE op_cursor;
                DEALLOCATE op_cursor;
            END
            ELSE IF @type = 4  -- array - treat as $in
            BEGIN
                EXEC dbo.internal_build_exists @scheme_id, @key, '$in', @value, 'array', @table_alias, @exists_sql OUTPUT;
                IF @exists_sql IS NOT NULL AND @exists_sql <> N'' AND @exists_sql <> N'1=1'
                    INSERT INTO @conditions (condition) VALUES (@exists_sql);
            END
            ELSE
            BEGIN
                -- Simple value - treat as $eq
                SET @op_value_type = CASE @type WHEN 1 THEN 'string' WHEN 2 THEN 'number' WHEN 3 THEN 'boolean' ELSE 'string' END;
                EXEC dbo.internal_build_exists @scheme_id, @key, '$eq', @value, @op_value_type, @table_alias, @exists_sql OUTPUT;
                IF @exists_sql IS NOT NULL AND @exists_sql <> N'' AND @exists_sql <> N'1=1'
                    INSERT INTO @conditions (condition) VALUES (@exists_sql);
            END;
        END;
        
        FETCH NEXT FROM json_cursor INTO @key, @value, @type;
    END;
    
    CLOSE json_cursor;
    DEALLOCATE json_cursor;
    
    -- Combine all conditions (separator must be variable, not expression)
    SET @separator = N' ' + @logical_op + N' ';
    SELECT @result = STRING_AGG(condition, @separator)
    FROM @conditions;
    
    IF @result IS NULL SET @result = N'';
END;
GO

-- =====================================================
-- HELPER: build_facet_field_path
-- Returns full field path including parent structures
-- =====================================================
CREATE FUNCTION dbo.build_facet_field_path(
    @structure_id BIGINT,
    @scheme_id BIGINT
)
RETURNS NVARCHAR(500)
AS
BEGIN
    DECLARE @path NVARCHAR(500) = N'';
    DECLARE @current_id BIGINT = @structure_id;
    DECLARE @current_name NVARCHAR(450);
    DECLARE @parent_id BIGINT;
    DECLARE @collection_type BIGINT;
    DECLARE @depth INT = 0;
    
    WHILE @current_id IS NOT NULL AND @depth < 10
    BEGIN
        SELECT 
            @current_name = _name,
            @parent_id = _parent_structure_id,
            @collection_type = _collection_type
        FROM _scheme_metadata_cache
        WHERE _structure_id = @current_id AND _scheme_id = @scheme_id;
        
        IF @current_name IS NOT NULL
        BEGIN
            IF @path = N''
                SET @path = @current_name + CASE WHEN @collection_type IS NOT NULL THEN N'[]' ELSE N'' END;
            ELSE
                SET @path = @current_name + CASE WHEN @collection_type IS NOT NULL THEN N'[]' ELSE N'' END + N'.' + @path;
        END;
        
        SET @current_id = @parent_id;
        SET @depth = @depth + 1;
    END;
    
    RETURN @path;
END;
GO

-- =====================================================
-- get_facets: Get facet values for a scheme
-- Basic version for backward compatibility
-- =====================================================
CREATE PROCEDURE dbo.get_facets
    @scheme_id BIGINT,
    @result NVARCHAR(MAX) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Ensure metadata cache is populated
    IF NOT EXISTS (SELECT 1 FROM _scheme_metadata_cache WHERE _scheme_id = @scheme_id)
    BEGIN
        EXEC dbo.sync_metadata_cache_for_scheme @scheme_id;
    END;
    
    DECLARE @facets TABLE (field_name NVARCHAR(450), field_values NVARCHAR(MAX));
    
    INSERT INTO @facets (field_name, field_values)
    SELECT 
        c._name,
        N'[' + ISNULL(STRING_AGG(
            CASE 
                WHEN c.db_type = 'String' THEN N'"' + REPLACE(REPLACE(v._String, N'\', N'\\'), N'"', N'\"') + N'"'
                WHEN c.db_type = 'Long' AND c.type_semantic <> '_RObject' THEN CAST(v._Long AS NVARCHAR(30))
                WHEN c.db_type = 'Boolean' THEN CASE WHEN v._Boolean = 1 THEN 'true' ELSE 'false' END
                WHEN c.db_type = 'Double' THEN FORMAT(v._Double, 'G', 'en-US')
                ELSE N'"' + REPLACE(REPLACE(ISNULL(v._String, ''), N'\', N'\\'), N'"', N'\"') + N'"'
            END
        , N','), N'') + N']'
    FROM _scheme_metadata_cache c
    LEFT JOIN (
        SELECT DISTINCT v._id_structure, v._String, v._Long, v._Boolean, v._Double
        FROM _values v
        JOIN _objects o ON o._id = v._id_object
        WHERE o._id_scheme = @scheme_id
          AND v._array_index IS NULL
    ) v ON v._id_structure = c._structure_id
    WHERE c._scheme_id = @scheme_id
      AND c._parent_structure_id IS NULL
      AND c.db_type NOT IN ('Guid', 'ByteArray')
    GROUP BY c._name
    HAVING COUNT(DISTINCT ISNULL(v._String, CAST(v._Long AS NVARCHAR(30)))) <= 100;
    
    SELECT @result = N'{' + ISNULL(STRING_AGG(N'"' + field_name + N'":' + field_values, N','), N'') + N'}'
    FROM @facets;
    
    IF @result IS NULL SET @result = N'{}';
END;
GO

-- =====================================================
-- get_facets_extended: Get facet values with options
-- Supports: filtering, limiting, specific fields, nested fields
-- =====================================================
IF OBJECT_ID('dbo.get_facets_extended', 'P') IS NOT NULL DROP PROCEDURE dbo.get_facets_extended;
GO

CREATE PROCEDURE dbo.get_facets_extended
    @scheme_id BIGINT,
    @fields NVARCHAR(MAX) = NULL,         -- JSON array of field names to include, NULL = all
    @max_values_per_field INT = 100,       -- Max unique values per facet
    @include_counts BIT = 0,               -- Include count of objects per value
    @include_nested BIT = 0,               -- Include nested Class fields
    @filter_prefix NVARCHAR(100) = NULL,   -- Filter fields by prefix
    @result NVARCHAR(MAX) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Ensure metadata cache is populated
    IF NOT EXISTS (SELECT 1 FROM _scheme_metadata_cache WHERE _scheme_id = @scheme_id)
    BEGIN
        EXEC dbo.sync_metadata_cache_for_scheme @scheme_id;
    END;
    
    DECLARE @facets TABLE (
        field_path NVARCHAR(500), 
        field_values NVARCHAR(MAX),
        total_count INT
    );
    
    -- Build field list
    DECLARE @field_filter TABLE (field_name NVARCHAR(450));
    IF @fields IS NOT NULL AND @fields <> N'' AND @fields <> N'[]'
    BEGIN
        INSERT INTO @field_filter (field_name)
        SELECT [value] FROM OPENJSON(@fields);
    END;
    
    -- Get facets for root fields
    INSERT INTO @facets (field_path, field_values, total_count)
    SELECT 
        c._name + CASE WHEN c._collection_type IS NOT NULL THEN N'[]' ELSE N'' END,
        N'[' + ISNULL(STRING_AGG(
            CASE 
                WHEN c.db_type = 'String' THEN N'"' + REPLACE(REPLACE(v._String, N'\', N'\\'), N'"', N'\"') + N'"'
                WHEN c.db_type = 'Long' AND c.type_semantic <> '_RObject' THEN CAST(v._Long AS NVARCHAR(30))
                WHEN c.db_type = 'Boolean' THEN CASE WHEN v._Boolean = 1 THEN 'true' ELSE 'false' END
                WHEN c.db_type = 'Double' THEN FORMAT(v._Double, 'G', 'en-US')
                WHEN c.db_type = 'ListItem' THEN N'"' + REPLACE(REPLACE(ISNULL(li._value, ''), N'\', N'\\'), N'"', N'\"') + N'"'
                ELSE N'"' + REPLACE(REPLACE(ISNULL(v._String, ''), N'\', N'\\'), N'"', N'\"') + N'"'
            END
        , N','), N'') + N']',
        COUNT(DISTINCT v._id_object)
    FROM _scheme_metadata_cache c
    LEFT JOIN (
        SELECT DISTINCT v._id_structure, v._id_object, v._String, v._Long, v._Boolean, v._Double, v._listitem
        FROM _values v
        JOIN _objects o ON o._id = v._id_object
        WHERE o._id_scheme = @scheme_id
    ) v ON v._id_structure = c._structure_id
    LEFT JOIN _list_items li ON c.type_semantic = '_RListItem' AND li._id = v._listitem
    WHERE c._scheme_id = @scheme_id
      AND (@include_nested = 1 OR c._parent_structure_id IS NULL)
      AND c.db_type NOT IN ('Guid', 'ByteArray')
      AND (@filter_prefix IS NULL OR c._name LIKE @filter_prefix + N'%')
      AND (NOT EXISTS (SELECT 1 FROM @field_filter) OR c._name IN (SELECT field_name FROM @field_filter))
    GROUP BY c._name, c._collection_type, c.db_type, c.type_semantic
    HAVING COUNT(DISTINCT COALESCE(v._String, CAST(v._Long AS NVARCHAR(30)), li._value)) <= @max_values_per_field;
    
    -- Build result JSON
    IF @include_counts = 1
    BEGIN
        SELECT @result = N'{' + ISNULL(STRING_AGG(
            N'"' + field_path + N'":{"values":' + field_values + N',"count":' + CAST(total_count AS NVARCHAR(20)) + N'}'
        , N','), N'') + N'}'
        FROM @facets;
    END
    ELSE
    BEGIN
        SELECT @result = N'{' + ISNULL(STRING_AGG(N'"' + field_path + N'":' + field_values, N','), N'') + N'}'
        FROM @facets;
    END;
    
    IF @result IS NULL SET @result = N'{}';
END;
GO

-- =====================================================
-- MAIN: search_objects_with_facets
-- Full featured search with all operators
-- =====================================================
CREATE PROCEDURE dbo.search_objects_with_facets
    @scheme_id BIGINT,
    @facet_filters NVARCHAR(MAX) = NULL,
    @limit_count INT = NULL,
    @offset_count INT = 0,
    @order_by NVARCHAR(MAX) = NULL,
    @max_recursion_depth INT = 10,
    @include_facets BIT = 0,
    @distinct_hash BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Ensure metadata cache is populated
    IF NOT EXISTS (SELECT 1 FROM _scheme_metadata_cache WHERE _scheme_id = @scheme_id)
    BEGIN
        EXEC dbo.sync_metadata_cache_for_scheme @scheme_id;
    END;
    
    -- Parse filters
    DECLARE @where_clause NVARCHAR(MAX) = N'';
    IF @facet_filters IS NOT NULL AND @facet_filters <> N'' AND @facet_filters <> N'{}' AND @facet_filters <> N'null'
    BEGIN
        EXEC dbo.internal_parse_filters @scheme_id, @facet_filters, N'o', N'AND', 0, @where_clause OUTPUT;
    END;
    
    -- Build ORDER BY clause using unified function
    -- Format: [{"field": "Name", "direction": "ASC"}] - extended format from C#
    DECLARE @order_clause NVARCHAR(MAX) = N'ORDER BY o._id';
    
    IF @order_by IS NOT NULL AND @order_by <> N'' AND @order_by <> N'[]' AND @order_by <> N'null'
    BEGIN
        SET @order_clause = dbo.build_order_by_clause(@order_by, N'o');
        IF @order_clause IS NULL OR @order_clause = N''
            SET @order_clause = N'ORDER BY o._id';
    END;
    
    -- Build and execute query
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @count_sql NVARCHAR(MAX);
    DECLARE @total_count INT;
    
    SET @sql = N'INSERT INTO #temp_ids (_id) SELECT ' + CASE WHEN @distinct_hash = 1 THEN 'DISTINCT' ELSE '' END + N' o._id
        FROM _objects o
        WHERE o._id_scheme = @p_scheme_id
' +
        CASE WHEN @where_clause <> N'' THEN N' AND ' + @where_clause ELSE N'' END + N' ' +
        @order_clause;
    
    IF @limit_count IS NOT NULL AND @limit_count > 0
        SET @sql = @sql + N' OFFSET ' + CAST(@offset_count AS NVARCHAR(20)) + 
                         N' ROWS FETCH NEXT ' + CAST(@limit_count AS NVARCHAR(20)) + N' ROWS ONLY';
    ELSE
        SET @sql = @sql + N' OFFSET 0 ROWS';  -- Allow ORDER BY without FETCH for count queries
    
    SET @sql = @sql + N'; SELECT @p_count = @@ROWCOUNT;';
    
    -- Count query
    SET @count_sql = N'SELECT @p_total = COUNT(' + CASE WHEN @distinct_hash = 1 THEN 'DISTINCT o._hash' ELSE '*' END + N')
        FROM _objects o
        WHERE o._id_scheme = @p_scheme_id
' +
        CASE WHEN @where_clause <> N'' THEN N' AND ' + @where_clause ELSE N'' END;
    
    -- Execute (use _sort_order to preserve ORDER BY)
    CREATE TABLE #temp_ids (_sort_order INT IDENTITY(1,1), _id BIGINT);
    DECLARE @row_count INT;
    
    -- OPTIMIZATION: For CountAsync (limit = 0), only execute count query, skip temp table creation
    IF @limit_count IS NULL OR @limit_count > 0
    BEGIN
        EXEC sp_executesql @sql, N'@p_scheme_id BIGINT, @p_count INT OUTPUT', 
            @p_scheme_id = @scheme_id, @p_count = @row_count OUTPUT;
    END;
    
    EXEC sp_executesql @count_sql, N'@p_scheme_id BIGINT, @p_total INT OUTPUT', 
        @p_scheme_id = @scheme_id, @p_total = @total_count OUTPUT;
    
    -- Build result JSON using function (no cursor - much faster!)
    DECLARE @objects_json NVARCHAR(MAX) = N'[]';
    DECLARE @facets_json NVARCHAR(MAX) = N'{}';
    
    -- OPTIMIZATION: Skip loading objects if limit = 0 (CountAsync case)
    IF @limit_count IS NULL OR @limit_count > 0
    BEGIN
        -- STRING_AGG with WITHIN GROUP to preserve ORDER BY from INSERT
        SELECT @objects_json = N'[' + ISNULL(STRING_AGG(
            dbo.get_object_json(t._id, @max_recursion_depth), N','
        ) WITHIN GROUP (ORDER BY t._sort_order), N'') + N']'
        FROM #temp_ids t;
    END;
    
    IF @include_facets = 1
        EXEC dbo.get_facets @scheme_id, @facets_json OUTPUT;
    
    -- Return result with NULL protection
    SELECT N'{"objects":' + ISNULL(@objects_json, N'[]') + 
           N',"total_count":' + CAST(ISNULL(@total_count, 0) AS NVARCHAR(20)) + 
           N',"limit":' + ISNULL(CAST(@limit_count AS NVARCHAR(20)), N'null') + 
           N',"offset":' + CAST(ISNULL(@offset_count, 0) AS NVARCHAR(20)) + 
           N',"facets":' + ISNULL(@facets_json, N'[]') + N'}' AS result;
    
    DROP TABLE #temp_ids;
END;
GO

-- =====================================================
-- search_tree_objects_with_facets
-- Search children/descendants with filters
-- Supports MULTIPLE parent_ids (comma-separated string)
-- =====================================================
CREATE PROCEDURE dbo.search_tree_objects_with_facets
    @scheme_id BIGINT,
    @parent_ids NVARCHAR(MAX),  -- Comma-separated list of parent IDs (e.g. '123,456,789') or NULL/empty for global search
    @facet_filters NVARCHAR(MAX) = NULL,
    @limit_count INT = NULL,
    @offset_count INT = 0,
    @order_by NVARCHAR(MAX) = NULL,
    @max_depth INT = 10,
    @max_recursion_depth INT = 10,
    @include_facets BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    
    IF NOT EXISTS (SELECT 1 FROM _scheme_metadata_cache WHERE _scheme_id = @scheme_id)
        EXEC dbo.sync_metadata_cache_for_scheme @scheme_id;
    
    DECLARE @where_clause NVARCHAR(MAX) = N'';
    IF @facet_filters IS NOT NULL AND @facet_filters <> N'' AND @facet_filters <> N'{}' AND @facet_filters <> N'null'
        EXEC dbo.internal_parse_filters @scheme_id, @facet_filters, N'd', N'AND', 0, @where_clause OUTPUT;
    
    -- Build ORDER BY clause from @order_by parameter (use 'd' alias)
    DECLARE @order_clause NVARCHAR(MAX) = N'ORDER BY d._id';
    IF @order_by IS NOT NULL AND @order_by <> N'' AND @order_by <> N'[]' AND @order_by <> N'null'
    BEGIN
        SET @order_clause = dbo.build_order_by_clause(@order_by, N'd');
        IF @order_clause IS NULL OR @order_clause = N''
            SET @order_clause = N'ORDER BY d._id';
    END;
    
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @total_count INT;
    DECLARE @objects_json NVARCHAR(MAX);
    DECLARE @facets_json NVARCHAR(MAX) = N'{}';
    
    -- Use temp table with sort_order to preserve ORDER BY
    CREATE TABLE #tree_ids (_sort_order INT IDENTITY(1,1), _id BIGINT);
    
    -- Check if parent_ids is empty or NULL - global search mode
    DECLARE @has_parents BIT = CASE 
        WHEN @parent_ids IS NULL OR LTRIM(RTRIM(@parent_ids)) = N'' THEN 0 
        ELSE 1 
    END;
    
    -- CASE 1: Global search (no parent_ids) - search entire scheme with hierarchical filters
    IF @has_parents = 0
    BEGIN
        SET @sql = N'INSERT INTO #tree_ids (_id) SELECT d._id FROM _objects d
            WHERE d._id_scheme = @p_scheme
' +
            CASE WHEN @where_clause <> N'' THEN N' AND ' + @where_clause ELSE N'' END +
            N' ' + @order_clause;
        
        IF @limit_count IS NOT NULL AND @limit_count > 0
            SET @sql = @sql + N' OFFSET ' + CAST(@offset_count AS NVARCHAR(20)) + 
                             N' ROWS FETCH NEXT ' + CAST(@limit_count AS NVARCHAR(20)) + N' ROWS ONLY';
        ELSE
            SET @sql = @sql + N' OFFSET 0 ROWS';
    END
    -- CASE 2: Direct children only (max_depth = 1)
    ELSE IF @max_depth = 1
    BEGIN
        SET @sql = N'INSERT INTO #tree_ids (_id) SELECT d._id FROM _objects d
            WHERE d._id_scheme = @p_scheme 
              AND d._id_parent IN (SELECT CAST(value AS BIGINT) FROM STRING_SPLIT(@p_parents, N'','') WHERE LTRIM(RTRIM(value)) <> N'''')
' +
            CASE WHEN @where_clause <> N'' THEN N' AND ' + @where_clause ELSE N'' END +
            N' ' + @order_clause;
        
        IF @limit_count IS NOT NULL AND @limit_count > 0
            SET @sql = @sql + N' OFFSET ' + CAST(@offset_count AS NVARCHAR(20)) + 
                             N' ROWS FETCH NEXT ' + CAST(@limit_count AS NVARCHAR(20)) + N' ROWS ONLY';
        ELSE
            SET @sql = @sql + N' OFFSET 0 ROWS';
    END
    -- CASE 3: Recursive descendants from multiple parents
    ELSE
    BEGIN
        SET @sql = N';WITH descendants AS (
            SELECT CAST(value AS BIGINT) AS _id, 0 AS depth 
            FROM STRING_SPLIT(@p_parents, N'','') 
            WHERE LTRIM(RTRIM(value)) <> N''''
            UNION ALL
            SELECT o._id, d.depth + 1 FROM _objects o JOIN descendants d ON o._id_parent = d._id WHERE d.depth < ' + CAST(@max_depth AS NVARCHAR(10)) + N'
        )
        INSERT INTO #tree_ids (_id) SELECT d._id FROM descendants dt
        JOIN _objects d ON dt._id = d._id
        WHERE dt.depth > 0 AND d._id_scheme = @p_scheme
' +
        CASE WHEN @where_clause <> N'' THEN N' AND ' + @where_clause ELSE N'' END +
        N' ' + @order_clause;
        
        IF @limit_count IS NOT NULL AND @limit_count > 0
            SET @sql = @sql + N' OFFSET ' + CAST(@offset_count AS NVARCHAR(20)) + 
                             N' ROWS FETCH NEXT ' + CAST(@limit_count AS NVARCHAR(20)) + N' ROWS ONLY';
        ELSE
            SET @sql = @sql + N' OFFSET 0 ROWS';
    END;
    
    -- Execute the query
    EXEC sp_executesql @sql, N'@p_scheme BIGINT, @p_parents NVARCHAR(MAX)', @p_scheme = @scheme_id, @p_parents = @parent_ids;
    SELECT @total_count = COUNT(*) FROM #tree_ids;
    
    -- Build JSON using function with ORDER BY to preserve sort order!
    SET @objects_json = N'[]';
    
    -- OPTIMIZATION: Skip loading objects if limit = 0 (CountAsync case)
    IF @limit_count IS NULL OR @limit_count > 0
    BEGIN
        -- STRING_AGG with WITHIN GROUP to preserve order from INSERT
        SELECT @objects_json = N'[' + ISNULL(STRING_AGG(
            dbo.get_object_json(t._id, @max_recursion_depth), N','
        ) WITHIN GROUP (ORDER BY t._sort_order), N'') + N']'
        FROM #tree_ids t;
    END;
    
    IF @include_facets = 1
        EXEC dbo.get_facets @scheme_id, @facets_json OUTPUT;
    
    -- Return result with NULL protection
    SELECT N'{"objects":' + ISNULL(@objects_json, N'[]') + 
           N',"total_count":' + CAST(ISNULL(@total_count, 0) AS NVARCHAR(20)) + 
           N',"limit":' + ISNULL(CAST(@limit_count AS NVARCHAR(20)), N'null') + 
           N',"offset":' + CAST(ISNULL(@offset_count, 0) AS NVARCHAR(20)) + 
           N',"parent_ids":"' + ISNULL(@parent_ids, N'') + N'"' +
           N',"max_depth":' + CAST(ISNULL(@max_depth, 10) AS NVARCHAR(10)) + 
           N',"facets":' + ISNULL(@facets_json, N'[]') + N'}' AS result;
    
    DROP TABLE #tree_ids;
END;
GO

PRINT '============================================================='
PRINT 'FULL Facets Search Module Created!'
PRINT ''
PRINT 'MAIN PROCEDURES:'
PRINT '  search_objects_with_facets     - Search with JSON filters'
PRINT '  search_tree_objects_with_facets - Search in hierarchy'
PRINT '  get_facets                     - Get facet values for scheme'
PRINT ''
PRINT 'HELPER FUNCTIONS:'
PRINT '  get_object_level(object_id)         - Get hierarchy level'
PRINT '  is_ancestor_of(object_id, anc_id)   - Check ancestor'
PRINT '  is_descendant_of(object_id, desc_id) - Check descendant'
PRINT '  normalize_base_field_name(name)     - Map field names'
PRINT '  get_value_column_by_type(type,sem)  - Get value column'
PRINT ''
PRINT 'OPERATORS SUPPORTED:'
PRINT '  Comparison: $eq, $ne, $gt, $gte, $lt, $lte'
PRINT '  String: $contains, $startsWith, $endsWith (+ IgnoreCase)'
PRINT '  List: $in, $exists'
PRINT '  Logical: $and, $or, $not'
PRINT '  Array: $arrayContains, $arrayAny, $arrayEmpty, $arrayCount*'
PRINT '  Hierarchy: $isRoot, $isLeaf, $hasAncestor, $hasDescendant, $level'
PRINT ''
PRINT 'FIELD TYPES SUPPORTED:'
PRINT '  Base fields: 0$:name, 0$:date_create, etc.'
PRINT '  Simple fields: Status, Name'
PRINT '  Class fields: Contact.Name, Address.City'
PRINT '  Arrays: Tags[], Contacts[].Email'
PRINT '  Dictionary: PhoneBook[home], Settings[theme]'
PRINT ''
PRINT 'USAGE EXAMPLE:'
PRINT '  EXEC search_objects_with_facets '
PRINT '    @scheme_id = 1,'
PRINT '    @facet_filters = N''{"Status":{"$eq":"Active"},"$and":[{"Age":{"$gte":18}}]}'''
PRINT '  preview_facet_query            - Show generated SQL (debug)'
PRINT '============================================================='
GO

-- =====================================================
-- DEBUG: preview_facet_query
-- Shows generated SQL without execution (for debugging)
-- =====================================================
CREATE PROCEDURE dbo.preview_facet_query
    @scheme_id BIGINT,
    @facet_filters NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Diagnostics
    DECLARE @cache_count INT;
    DECLARE @scheme_exists BIT = 0;
    DECLARE @cache_populated BIT = 0;
    
    -- Check if scheme exists
    IF EXISTS (SELECT 1 FROM _schemes WHERE _id = @scheme_id)
        SET @scheme_exists = 1;
    
    -- Check cache before sync
    SELECT @cache_count = COUNT(*) FROM _scheme_metadata_cache WHERE _scheme_id = @scheme_id;
    
    -- Ensure metadata cache is populated
    IF @cache_count = 0
    BEGIN
        EXEC dbo.sync_metadata_cache_for_scheme @scheme_id;
        SELECT @cache_count = COUNT(*) FROM _scheme_metadata_cache WHERE _scheme_id = @scheme_id;
        IF @cache_count > 0
            SET @cache_populated = 1;
    END
    ELSE
        SET @cache_populated = 1;
    
    -- Parse filters
    DECLARE @where_clause NVARCHAR(MAX) = N'';
    IF @facet_filters IS NOT NULL AND @facet_filters <> N'' AND @facet_filters <> N'{}' AND @facet_filters <> N'null'
    BEGIN
        EXEC dbo.internal_parse_filters @scheme_id, @facet_filters, N'o', N'AND', 0, @where_clause OUTPUT;
    END;
    
    -- Build preview SQL
    DECLARE @preview_sql NVARCHAR(MAX);
    SET @preview_sql = N'SELECT o._id, o._name, o._id_scheme
FROM _objects o
WHERE o._id_scheme = ' + CAST(@scheme_id AS NVARCHAR(20)) + N'
';
    
    IF @where_clause IS NOT NULL AND @where_clause <> N''
        SET @preview_sql = @preview_sql + N'
  AND ' + @where_clause;
    
    SET @preview_sql = @preview_sql + N'
ORDER BY o._id';
    
    -- Return preview with diagnostics
    SELECT 
        @scheme_id AS scheme_id,
        @scheme_exists AS scheme_exists,
        @cache_count AS metadata_cache_entries,
        @cache_populated AS cache_ok,
        @facet_filters AS input_filters,
        @where_clause AS generated_where_clause,
        @preview_sql AS full_sql_preview;
    
    -- Also return available fields in cache for debugging
    SELECT 
        _name AS field_name,
        db_type,
        type_semantic,
        CASE WHEN _collection_type IS NOT NULL THEN 'Array/Dict' ELSE 'Simple' END AS field_type,
        CASE WHEN _parent_structure_id IS NULL THEN 'Root' ELSE 'Nested' END AS level
    FROM _scheme_metadata_cache
    WHERE _scheme_id = @scheme_id
    ORDER BY CASE WHEN _parent_structure_id IS NULL THEN 0 ELSE 1 END, _name;
END;
GO

PRINT 'preview_facet_query procedure created!'
GO

-- =====================================================
-- DEBUG: preview_tree_facet_query
-- Shows generated SQL for tree search (for debugging)
-- =====================================================
IF OBJECT_ID('dbo.preview_tree_facet_query', 'P') IS NOT NULL DROP PROCEDURE dbo.preview_tree_facet_query;
GO

CREATE PROCEDURE dbo.preview_tree_facet_query
    @scheme_id BIGINT,
    @parent_id BIGINT,
    @facet_filters NVARCHAR(MAX) = NULL,
    @max_depth INT = 10
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Diagnostics
    DECLARE @cache_count INT;
    DECLARE @scheme_exists BIT = 0;
    DECLARE @parent_exists BIT = 0;
    DECLARE @cache_populated BIT = 0;
    
    -- Check if scheme exists
    IF EXISTS (SELECT 1 FROM _schemes WHERE _id = @scheme_id)
        SET @scheme_exists = 1;
    
    -- Check if parent object exists (NULL = global search, always valid)
    IF @parent_id IS NULL
        SET @parent_exists = 1;
    ELSE IF EXISTS (SELECT 1 FROM _objects WHERE _id = @parent_id)
        SET @parent_exists = 1;
    
    -- Check cache
    SELECT @cache_count = COUNT(*) FROM _scheme_metadata_cache WHERE _scheme_id = @scheme_id;
    
    IF @cache_count = 0
    BEGIN
        EXEC dbo.sync_metadata_cache_for_scheme @scheme_id;
        SELECT @cache_count = COUNT(*) FROM _scheme_metadata_cache WHERE _scheme_id = @scheme_id;
        IF @cache_count > 0
            SET @cache_populated = 1;
    END
    ELSE
        SET @cache_populated = 1;
    
    -- Parse filters
    DECLARE @where_clause NVARCHAR(MAX) = N'';
    IF @facet_filters IS NOT NULL AND @facet_filters <> N'' AND @facet_filters <> N'{}' AND @facet_filters <> N'null'
    BEGIN
        EXEC dbo.internal_parse_filters @scheme_id, @facet_filters, N'd', N'AND', 0, @where_clause OUTPUT;
    END;
    
    -- Build preview SQL
    DECLARE @preview_sql NVARCHAR(MAX);
    
    -- CASE 1: Global search (no parent_id)
    IF @parent_id IS NULL
    BEGIN
        SET @preview_sql = N'-- Global search (no parent_id)
SELECT d._id, d._name, d._id_parent
FROM _objects d
WHERE d._id_scheme = ' + CAST(@scheme_id AS NVARCHAR(20)) + N'
';
    END
    -- CASE 2: Direct children only
    ELSE IF @max_depth = 1
    BEGIN
        SET @preview_sql = N'SELECT d._id, d._name, d._id_parent
FROM _objects d
WHERE d._id_scheme = ' + CAST(@scheme_id AS NVARCHAR(20)) + N'
  AND d._id_parent = ' + CAST(@parent_id AS NVARCHAR(20)) + N'
';
    END
    -- CASE 3: Recursive descendants with CTE
    ELSE
    BEGIN
        SET @preview_sql = N';WITH descendants AS (
    SELECT ' + CAST(@parent_id AS NVARCHAR(20)) + N' AS _id, 0 AS depth
    UNION ALL
    SELECT o._id, d.depth + 1 
    FROM _objects o 
    JOIN descendants d ON o._id_parent = d._id 
    WHERE d.depth < ' + CAST(@max_depth AS NVARCHAR(10)) + N'
)
SELECT d._id, d._name, d._id_parent, dt.depth
FROM descendants dt
JOIN _objects d ON dt._id = d._id
WHERE dt.depth > 0 
  AND d._id_scheme = ' + CAST(@scheme_id AS NVARCHAR(20)) + N'
';
    END;
    
    IF @where_clause IS NOT NULL AND @where_clause <> N''
        SET @preview_sql = @preview_sql + N'
  AND ' + @where_clause;
    
    SET @preview_sql = @preview_sql + N'
ORDER BY d._id';
    
    -- Return preview with diagnostics
    SELECT 
        @scheme_id AS scheme_id,
        @parent_id AS parent_id,
        @max_depth AS max_depth,
        @scheme_exists AS scheme_exists,
        @parent_exists AS parent_exists,
        @cache_count AS metadata_cache_entries,
        @cache_populated AS cache_ok,
        @facet_filters AS input_filters,
        @where_clause AS generated_where_clause,
        @preview_sql AS full_sql_preview;
    
    -- Return fields in cache
    SELECT 
        _name AS field_name,
        db_type,
        type_semantic,
        CASE WHEN _collection_type IS NOT NULL THEN 'Array/Dict' ELSE 'Simple' END AS field_type,
        CASE WHEN _parent_structure_id IS NULL THEN 'Root' ELSE 'Nested' END AS level
    FROM _scheme_metadata_cache
    WHERE _scheme_id = @scheme_id
    ORDER BY CASE WHEN _parent_structure_id IS NULL THEN 0 ELSE 1 END, _name;
END;
GO

PRINT 'preview_tree_facet_query procedure created!'
GO

-- =====================================================
-- SUMMARY AND USAGE EXAMPLES
-- =====================================================
PRINT '============================================================='
PRINT 'REDB FACETS SEARCH MODULE FOR MSSQL - COMPLETE!'
PRINT ''
PRINT 'MAIN PROCEDURES:'
PRINT '  search_objects_with_facets      - Full featured search with JSON filters'
PRINT '  search_tree_objects_with_facets - Search children/descendants'
PRINT '  get_facets                      - Get unique values for UI filters'
PRINT '  get_facets_extended             - Extended facets with options'
PRINT '  preview_facet_query             - Debug: show generated SQL'
PRINT '  preview_tree_facet_query        - Debug: show tree search SQL'
PRINT ''
PRINT 'HELPER FUNCTIONS:'
PRINT '  normalize_base_field_name       - Map C# names to SQL columns'
PRINT '  get_value_column_by_type        - Get _values column for db_type'
PRINT '  get_object_level                - Get hierarchy level (0=root)'
PRINT '  is_ancestor_of                  - Check if object is ancestor'
PRINT '  is_descendant_of                - Check if object is descendant'
PRINT '  build_facet_field_path          - Build full field path for facets'
PRINT ''
PRINT 'SUPPORTED OPERATORS:'
PRINT '  Comparison: $eq, $ne, $gt, $gte, $lt, $lte'
PRINT '  Lists:      $in, $notIn, $between'
PRINT '  String:     $contains, $startsWith, $endsWith (+IgnoreCase)'
PRINT '  Pattern:    $like, $likeIgnoreCase (MSSQL alternative to $regex)'
PRINT '  Full-Text:  $match, $matchPrefix (requires FTS catalog)'
PRINT '  Logical:    $and, $or, $not'
PRINT '  Array:      $arrayContains, $arrayAny, $arrayEmpty, $arrayCount*'
PRINT '  Array Ext:  $arrayFirst, $arrayLast, $arrayAt, $arrayStartsWith, $arrayEndsWith'
PRINT '  Array Agg:  $arraySum, $arrayAvg, $arrayMin, $arrayMax (+Gt/Gte/Lt/Lte)'
PRINT '  Hierarchy:  $isRoot, $isLeaf, $hasAncestor, $hasDescendant, $level'
PRINT '  Existence:  $exists'
PRINT ''
PRINT 'FIELD TYPES:'
PRINT '  Base fields (0$:): 0$:name, 0$:date_create, 0$:id_parent, etc.'
PRINT '  Simple fields:     Status, Name, Age'
PRINT '  Class fields:      Contact.Name, Address.City'
PRINT '  Arrays:            Tags[], Contacts[].Email'
PRINT '  Dictionary:        PhoneBook[home], Settings[theme]'
PRINT '  ListItem:          Status.Value, Roles[].Alias'
PRINT ''
PRINT 'SMART TYPE DETECTION:'
PRINT '  Numbers -> checks Long, Double, Numeric columns'
PRINT '  Booleans (true/false) -> checks _Boolean column'
PRINT '  Dates (YYYY-MM-DD...) -> checks _DateTimeOffset column'
PRINT '  GUIDs -> checks _Guid column'
PRINT '  Strings -> checks _String column'
PRINT ''
PRINT 'ORDER BY OPTIONS:'
PRINT '  Simple:     [{"field": "asc"}, {"0$:date_create": "desc"}]'
PRINT '  With nulls: [{"field": {"dir": "asc", "nulls": "first"}}]'
PRINT '============================================================='
GO

/*
===== USAGE EXAMPLES =====

-- 1. Basic search with filters:
EXEC search_objects_with_facets 
    @scheme_id = 1,
    @facet_filters = N'{"Status": {"$eq": "Active"}, "Age": {"$gte": 18}}';

-- 2. Complex AND/OR logic:
EXEC search_objects_with_facets 
    @scheme_id = 1,
    @facet_filters = N'{
        "$and": [
            {"Status": {"$eq": "Active"}},
            {"$or": [
                {"City": {"$eq": "Moscow"}},
                {"City": {"$eq": "SPb"}}
            ]}
        ]
    }';

-- 3. Array operators:
EXEC search_objects_with_facets 
    @scheme_id = 1,
    @facet_filters = N'{
        "Tags[]": {"$arrayContains": "vip"},
        "Scores[]": {"$arrayCountGt": 3}
    }';

-- 4. Class fields (nested):
EXEC search_objects_with_facets 
    @scheme_id = 1,
    @facet_filters = N'{
        "Contact.Name": {"$contains": "John"},
        "Address.City": {"$in": ["Moscow", "SPb"]}
    }';

-- 5. Hierarchical search:
EXEC search_objects_with_facets 
    @scheme_id = 1,
    @facet_filters = N'{"$isRoot": true, "$hasDescendant": {"scheme_id": 2}}';

-- 6. Base fields (RedbObject columns):
EXEC search_objects_with_facets 
    @scheme_id = 1,
    @facet_filters = N'{
        "0$:name": {"$startsWith": "Test"},
        "0$:date_create": {"$gte": "2024-01-01"}
    }';

-- 7. Dictionary access:
EXEC search_objects_with_facets 
    @scheme_id = 1,
    @facet_filters = N'{"PhoneBook[home]": {"$eq": "+7-999-123-45-67"}}';

-- 8. With sorting and pagination:
EXEC search_objects_with_facets 
    @scheme_id = 1,
    @facet_filters = N'{"Status": {"$eq": "Active"}}',
    @limit_count = 10,
    @offset_count = 0,
    @order_by = N'[{"0$:date_create": "desc"}]';

-- 9. Debug preview (shows generated SQL):
EXEC preview_facet_query 
    @scheme_id = 1,
    @facet_filters = N'{"Status": {"$eq": "Active"}}';

-- 10. Tree search (children of parent):
EXEC search_tree_objects_with_facets 
    @scheme_id = 1,
    @parent_id = 100,
    @max_depth = 1;  -- direct children only

-- 10a. Global tree search (all objects with hierarchical filters):
EXEC search_tree_objects_with_facets 
    @scheme_id = 1,
    @parent_id = NULL,  -- NULL = search entire scheme
    @facet_filters = N'{"$isRoot": true}';  -- find all root objects

-- 10b. Find all leaf objects (no children):
EXEC search_tree_objects_with_facets 
    @scheme_id = 1,
    @parent_id = NULL,
    @facet_filters = N'{"$isLeaf": true}';

-- 11. Between operator (date range):
EXEC search_objects_with_facets 
    @scheme_id = 1,
    @facet_filters = N'{"0$:date_create": {"$between": ["2024-01-01", "2024-12-31"]}}';

-- 12. Not In operator:
EXEC search_objects_with_facets 
    @scheme_id = 1,
    @facet_filters = N'{"Status": {"$notIn": ["Deleted", "Archived"]}}';

-- 13. Full-Text Search (requires FTS catalog):
EXEC search_objects_with_facets 
    @scheme_id = 1,
    @facet_filters = N'{"Description": {"$match": "important AND urgent"}}';

-- 14. LIKE patterns (alternative to regex):
EXEC search_objects_with_facets 
    @scheme_id = 1,
    @facet_filters = N'{"Email": {"$like": "%@company.com"}}';

-- 15. Sorting with NULLS handling:
EXEC search_objects_with_facets 
    @scheme_id = 1,
    @facet_filters = N'{}',
    @order_by = N'[{"Priority": {"dir": "asc", "nulls": "last"}}, {"0$:date_create": "desc"}]';

-- 16. Extended facets with options:
DECLARE @facets NVARCHAR(MAX);
EXEC get_facets_extended 
    @scheme_id = 1,
    @fields = N'["Status", "City", "Tags"]',
    @max_values_per_field = 50,
    @include_counts = 1,
    @result = @facets OUTPUT;
SELECT @facets;

-- 17. Tree search preview (debugging):
EXEC preview_tree_facet_query 
    @scheme_id = 1,
    @parent_id = 100,
    @max_depth = 3,
    @facet_filters = N'{"Status": {"$eq": "Active"}}';

-- 18. ListItem field search:
EXEC search_objects_with_facets 
    @scheme_id = 1,
    @facet_filters = N'{"Status.Value": {"$eq": "Active"}, "Roles[].Alias": {"$arrayContains": "Admin"}}';
*/
