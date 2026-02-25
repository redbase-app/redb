-- ====================================================================================================
-- JSON OBJECT FUNCTION (not procedure!)
-- MS SQL Server version - OPTIMIZED for use with SELECT + STRING_AGG
-- ====================================================================================================
-- Key difference from get_object_json_internal (procedure):
--   - This is a FUNCTION that can be called in SELECT statements
--   - Reads directly from _values table (no temp tables)
--   - Uses recursive function calls instead of cursors where possible
--   - Designed for batch processing: SELECT dbo.get_object_json(id, 10) FROM ...
-- ====================================================================================================


-- =====================================================
-- DROP EXISTING FUNCTION (if exists)
-- =====================================================
IF OBJECT_ID('dbo.get_object_json', 'FN') IS NOT NULL
    DROP FUNCTION dbo.get_object_json
GO

IF OBJECT_ID('dbo.build_properties', 'FN') IS NOT NULL
    DROP FUNCTION dbo.build_properties
GO

IF OBJECT_ID('dbo.build_field_json', 'FN') IS NOT NULL
    DROP FUNCTION dbo.build_field_json
GO

IF OBJECT_ID('dbo.escape_json_string', 'FN') IS NOT NULL
    DROP FUNCTION dbo.escape_json_string
GO

IF OBJECT_ID('dbo.build_listitem_json', 'FN') IS NOT NULL
    DROP FUNCTION dbo.build_listitem_json
GO

-- =====================================================
-- HELPER: Escape string for JSON
-- =====================================================
CREATE FUNCTION dbo.escape_json_string(@input NVARCHAR(MAX))
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @input IS NULL RETURN NULL;
    
    RETURN REPLACE(
        REPLACE(
            REPLACE(
                REPLACE(
                    REPLACE(@input, N'\', N'\\'),  -- Backslash first!
                    N'"', N'\"'),
                CHAR(13), N'\r'),
            CHAR(10), N'\n'),
        CHAR(9), N'\t');
END
GO

-- =====================================================
-- HELPER: Build ListItem JSON (DRY - used in multiple places)
-- =====================================================
CREATE FUNCTION dbo.build_listitem_json(@listitem_id BIGINT)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @listitem_id IS NULL RETURN NULL;
    
    RETURN (SELECT N'{"id":' + CAST(li._id AS NVARCHAR(20)) + 
                   N',"idList":' + CAST(li._id_list AS NVARCHAR(20)) + 
                   N',"value":' + CASE WHEN li._value IS NULL THEN N'null' 
                                       ELSE N'"' + dbo.escape_json_string(li._value) + N'"' END +
                   N',"alias":' + CASE WHEN li._alias IS NULL THEN N'null' 
                                       ELSE N'"' + dbo.escape_json_string(li._alias) + N'"' END +
                   N'}'
            FROM _list_items li WHERE li._id = @listitem_id);
END
GO

-- =====================================================
-- HELPER: Build single field value as JSON
-- =====================================================
CREATE FUNCTION dbo.build_field_json(
    @object_id BIGINT,
    @structure_id BIGINT,
    @scheme_id BIGINT,
    @parent_structure_id BIGINT,
    @field_name NVARCHAR(450),
    @db_type NVARCHAR(50),
    @type_semantic NVARCHAR(50),
    @collection_type BIGINT,
    @max_depth INT,
    @array_index NVARCHAR(430),
    @parent_value_id BIGINT
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    DECLARE @result NVARCHAR(MAX);
    DECLARE @is_array BIT = CASE WHEN @collection_type = -9223372036854775668 THEN 1 ELSE 0 END;
    DECLARE @is_dictionary BIT = CASE WHEN @collection_type = -9223372036854775667 THEN 1 ELSE 0 END;
    
    -- Get value for this field
    DECLARE @val_id BIGINT, @val_String NVARCHAR(MAX), @val_Long BIGINT, @val_Guid UNIQUEIDENTIFIER;
    DECLARE @val_Double FLOAT, @val_Numeric DECIMAL(38,18), @val_DateTimeOffset DATETIMEOFFSET;
    DECLARE @val_Boolean BIT, @val_ByteArray VARBINARY(MAX), @val_ListItem BIGINT, @val_Object BIGINT;
    
    -- Find the value record
    IF @parent_value_id IS NOT NULL
    BEGIN
        SELECT TOP 1 
            @val_id = _id, @val_String = _String, @val_Long = _Long, @val_Guid = _Guid,
            @val_Double = _Double, @val_Numeric = _Numeric, @val_DateTimeOffset = _DateTimeOffset,
            @val_Boolean = _Boolean, @val_ByteArray = _ByteArray, @val_ListItem = _ListItem, @val_Object = _Object
        FROM _values
        WHERE _id_object = @object_id AND _id_structure = @structure_id AND _array_parent_id = @parent_value_id;
    END
    ELSE IF @array_index IS NOT NULL
    BEGIN
        SELECT TOP 1 
            @val_id = _id, @val_String = _String, @val_Long = _Long, @val_Guid = _Guid,
            @val_Double = _Double, @val_Numeric = _Numeric, @val_DateTimeOffset = _DateTimeOffset,
            @val_Boolean = _Boolean, @val_ByteArray = _ByteArray, @val_ListItem = _ListItem, @val_Object = _Object
        FROM _values
        WHERE _id_object = @object_id AND _id_structure = @structure_id AND _array_index = @array_index;
    END
    ELSE
    BEGIN
        SELECT TOP 1 
            @val_id = _id, @val_String = _String, @val_Long = _Long, @val_Guid = _Guid,
            @val_Double = _Double, @val_Numeric = _Numeric, @val_DateTimeOffset = _DateTimeOffset,
            @val_Boolean = _Boolean, @val_ByteArray = _ByteArray, @val_ListItem = _ListItem, @val_Object = _Object
        FROM _values
        WHERE _id_object = @object_id AND _id_structure = @structure_id AND _array_index IS NULL AND _array_parent_id IS NULL;
    END
    
    -- =====================================================
    -- ARRAYS
    -- =====================================================
    IF @is_array = 1
    BEGIN
        DECLARE @base_value_id BIGINT;
        -- For nested arrays inside array/dict elements, base record has _array_parent_id = @parent_value_id
        IF @parent_value_id IS NOT NULL
            SELECT TOP 1 @base_value_id = _id FROM _values 
            WHERE _id_object = @object_id AND _id_structure = @structure_id 
              AND _array_index IS NULL AND _array_parent_id = @parent_value_id;
        ELSE
            SELECT TOP 1 @base_value_id = _id FROM _values 
            WHERE _id_object = @object_id AND _id_structure = @structure_id 
              AND _array_index IS NULL AND _array_parent_id IS NULL;
        
        IF @type_semantic = '_RObject'
        BEGIN
            -- Array of Object references - recursive call for each
            SELECT @result = N'[' + ISNULL(STRING_AGG(
                CASE WHEN v._Object IS NOT NULL AND @max_depth > 0 
                     THEN dbo.get_object_json(v._Object, @max_depth - 1)
                     ELSE N'null' END
            , N',') WITHIN GROUP (ORDER BY 
                CASE WHEN v._array_index LIKE '[0-9]%' AND ISNUMERIC(v._array_index) = 1 
                     THEN CAST(v._array_index AS INT) ELSE 2147483647 END, v._array_index
            ), N'') + N']'
            FROM _values v
            WHERE v._id_object = @object_id AND v._id_structure = @structure_id
              AND v._array_index IS NOT NULL
              AND (@base_value_id IS NULL OR v._array_parent_id = @base_value_id);
        END
        ELSE IF @type_semantic = 'Object'
        BEGIN
            -- Array of Class - recursive properties for each element
            SELECT @result = N'[' + ISNULL(STRING_AGG(
                dbo.build_properties(@object_id, @scheme_id, @max_depth, @structure_id, v._array_index, v._id)
            , N',') WITHIN GROUP (ORDER BY 
                CASE WHEN v._array_index LIKE '[0-9]%' AND ISNUMERIC(v._array_index) = 1 
                     THEN CAST(v._array_index AS INT) ELSE 2147483647 END, v._array_index
            ), N'') + N']'
            FROM _values v
            WHERE v._id_object = @object_id AND v._id_structure = @structure_id
              AND v._array_index IS NOT NULL
              AND (@base_value_id IS NULL OR v._array_parent_id = @base_value_id);
        END
        ELSE
        BEGIN
            -- Array of primitives (including ListItem)
            SELECT @result = N'[' + ISNULL(STRING_AGG(
                CASE 
                    WHEN @db_type = 'String' THEN 
                        CASE WHEN v._String IS NULL THEN N'null' ELSE N'"' + dbo.escape_json_string(v._String) + N'"' END
                    WHEN @db_type = 'Long' THEN 
                        CASE WHEN v._Long IS NULL THEN N'null' ELSE CAST(v._Long AS NVARCHAR(30)) END
                    WHEN @db_type = 'Guid' THEN 
                        CASE WHEN v._Guid IS NULL THEN N'null' ELSE N'"' + CAST(v._Guid AS NVARCHAR(50)) + N'"' END
                    WHEN @db_type = 'Double' THEN 
                        CASE WHEN v._Double IS NULL THEN N'null' ELSE FORMAT(v._Double, 'G', 'en-US') END
                    WHEN @db_type = 'Numeric' THEN 
                        CASE WHEN v._Numeric IS NULL THEN N'null' ELSE REPLACE(CAST(v._Numeric AS NVARCHAR(50)), N',', N'.') END
                    WHEN @db_type = 'DateTimeOffset' THEN 
                        CASE WHEN v._DateTimeOffset IS NULL THEN N'null' ELSE N'"' + CONVERT(NVARCHAR(50), v._DateTimeOffset, 127) + N'"' END
                    WHEN @db_type = 'Boolean' THEN 
                        CASE WHEN v._Boolean IS NULL THEN N'null' WHEN v._Boolean = 1 THEN N'true' ELSE N'false' END
                    WHEN @db_type = 'ListItem' THEN 
                        CASE WHEN v._ListItem IS NULL THEN N'null'
                             ELSE dbo.build_listitem_json(v._ListItem)
                        END
                    ELSE N'null'
                END
            , N',') WITHIN GROUP (ORDER BY 
                CASE WHEN v._array_index LIKE '[0-9]%' AND ISNUMERIC(v._array_index) = 1 
                     THEN CAST(v._array_index AS INT) ELSE 2147483647 END, v._array_index
            ), N'') + N']'
            FROM _values v
            WHERE v._id_object = @object_id AND v._id_structure = @structure_id
              AND v._array_index IS NOT NULL
              AND (@base_value_id IS NULL OR v._array_parent_id = @base_value_id);
        END
        
        RETURN @result;
    END
    
    -- =====================================================
    -- DICTIONARIES
    -- =====================================================
    IF @is_dictionary = 1
    BEGIN
        DECLARE @dict_base_id BIGINT;
        -- For nested dicts inside array/dict elements, base record has _array_parent_id = @parent_value_id
        IF @parent_value_id IS NOT NULL
            SELECT TOP 1 @dict_base_id = _id FROM _values 
            WHERE _id_object = @object_id AND _id_structure = @structure_id 
              AND _array_index IS NULL AND _array_parent_id = @parent_value_id;
        ELSE
            SELECT TOP 1 @dict_base_id = _id FROM _values 
            WHERE _id_object = @object_id AND _id_structure = @structure_id 
              AND _array_index IS NULL AND _array_parent_id IS NULL;
        
        IF @type_semantic = '_RObject'
        BEGIN
            -- Dictionary of Object references
            SELECT @result = N'{' + ISNULL(STRING_AGG(
                N'"' + dbo.escape_json_string(v._array_index) + N'":' +
                CASE WHEN v._Object IS NOT NULL AND @max_depth > 0 
                     THEN dbo.get_object_json(v._Object, @max_depth - 1)
                     ELSE N'null' END
            , N','), N'') + N'}'
            FROM _values v
            WHERE v._id_object = @object_id AND v._id_structure = @structure_id
              AND v._array_index IS NOT NULL
              AND (@dict_base_id IS NULL OR v._array_parent_id = @dict_base_id);
        END
        ELSE IF @type_semantic = 'Object'
        BEGIN
            -- Dictionary of Class
            SELECT @result = N'{' + ISNULL(STRING_AGG(
                N'"' + dbo.escape_json_string(v._array_index) + N'":' +
                dbo.build_properties(@object_id, @scheme_id, @max_depth, @structure_id, NULL, v._id)
            , N','), N'') + N'}'
            FROM _values v
            WHERE v._id_object = @object_id AND v._id_structure = @structure_id
              AND v._array_index IS NOT NULL
              AND (@dict_base_id IS NULL OR v._array_parent_id = @dict_base_id);
        END
        ELSE
        BEGIN
            -- Dictionary of primitives
            SELECT @result = N'{' + ISNULL(STRING_AGG(
                N'"' + dbo.escape_json_string(v._array_index) + N'":' +
                CASE 
                    WHEN @db_type = 'String' THEN 
                        CASE WHEN v._String IS NULL THEN N'null' ELSE N'"' + dbo.escape_json_string(v._String) + N'"' END
                    WHEN @db_type = 'Long' THEN 
                        CASE WHEN v._Long IS NULL THEN N'null' ELSE CAST(v._Long AS NVARCHAR(30)) END
                    WHEN @db_type = 'Guid' THEN 
                        CASE WHEN v._Guid IS NULL THEN N'null' ELSE N'"' + CAST(v._Guid AS NVARCHAR(50)) + N'"' END
                    WHEN @db_type = 'Double' THEN 
                        CASE WHEN v._Double IS NULL THEN N'null' ELSE FORMAT(v._Double, 'G', 'en-US') END
                    WHEN @db_type = 'Numeric' THEN 
                        CASE WHEN v._Numeric IS NULL THEN N'null' ELSE REPLACE(CAST(v._Numeric AS NVARCHAR(50)), N',', N'.') END
                    WHEN @db_type = 'DateTimeOffset' THEN 
                        CASE WHEN v._DateTimeOffset IS NULL THEN N'null' ELSE N'"' + CONVERT(NVARCHAR(50), v._DateTimeOffset, 127) + N'"' END
                    WHEN @db_type = 'Boolean' THEN 
                        CASE WHEN v._Boolean IS NULL THEN N'null' WHEN v._Boolean = 1 THEN N'true' ELSE N'false' END
                    ELSE N'null'
                END
            , N','), N'') + N'}'
            FROM _values v
            WHERE v._id_object = @object_id AND v._id_structure = @structure_id
              AND v._array_index IS NOT NULL
              AND (@dict_base_id IS NULL OR v._array_parent_id = @dict_base_id);
        END
        
        RETURN @result;
    END
    
    -- =====================================================
    -- _RObject reference (single)
    -- =====================================================
    IF @type_semantic = '_RObject'
    BEGIN
        IF @val_Object IS NOT NULL AND @max_depth > 0
            RETURN dbo.get_object_json(@val_Object, @max_depth - 1);
        RETURN NULL;
    END
    
    -- =====================================================
    -- Nested Class (hierarchical)
    -- =====================================================
    IF @type_semantic = 'Object'
    BEGIN
        IF @val_Guid IS NOT NULL
            RETURN dbo.build_properties(@object_id, @scheme_id, @max_depth, @structure_id, NULL, @val_id);
        RETURN NULL;
    END
    
    -- =====================================================
    -- PRIMITIVES
    -- =====================================================
    IF @val_id IS NULL RETURN NULL;
    
    SET @result = CASE 
        WHEN @db_type = 'String' THEN 
            CASE WHEN @val_String IS NULL THEN NULL ELSE N'"' + dbo.escape_json_string(@val_String) + N'"' END
        WHEN @db_type = 'Long' THEN 
            CASE 
                WHEN @val_ListItem IS NOT NULL THEN dbo.build_listitem_json(@val_ListItem)
                WHEN @val_Long IS NULL THEN NULL 
                ELSE CAST(@val_Long AS NVARCHAR(30)) 
            END
        WHEN @db_type = 'Guid' THEN 
            CASE WHEN @val_Guid IS NULL THEN NULL ELSE N'"' + CAST(@val_Guid AS NVARCHAR(50)) + N'"' END
        WHEN @db_type = 'Double' THEN 
            CASE WHEN @val_Double IS NULL THEN NULL ELSE FORMAT(@val_Double, 'G', 'en-US') END
        WHEN @db_type = 'Numeric' THEN 
            CASE WHEN @val_Numeric IS NULL THEN NULL ELSE REPLACE(CAST(@val_Numeric AS NVARCHAR(50)), N',', N'.') END
        WHEN @db_type = 'DateTimeOffset' THEN 
            CASE WHEN @val_DateTimeOffset IS NULL THEN NULL ELSE N'"' + CONVERT(NVARCHAR(50), @val_DateTimeOffset, 127) + N'"' END
        WHEN @db_type = 'Boolean' THEN 
            CASE WHEN @val_Boolean IS NULL THEN NULL WHEN @val_Boolean = 1 THEN N'true' ELSE N'false' END
        WHEN @db_type = 'ListItem' THEN dbo.build_listitem_json(@val_ListItem)
        ELSE NULL
    END;
    
    RETURN @result;
END
GO

-- =====================================================
-- HELPER: Build properties JSON for an object
-- =====================================================
CREATE FUNCTION dbo.build_properties(
    @object_id BIGINT,
    @scheme_id BIGINT,
    @max_depth INT,
    @parent_structure_id BIGINT,
    @array_index NVARCHAR(430),
    @parent_value_id BIGINT
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    DECLARE @result NVARCHAR(MAX);
    
    -- Protection against infinite recursion
    IF @max_depth < -100 RETURN N'{"error":"Max recursion depth reached"}';
    
    -- Build properties object from metadata cache
    -- Filter out NULL values to reduce JSON size (like PostgreSQL behavior)
    SELECT @result = N'{' + ISNULL((
        SELECT STRING_AGG(N'"' + dbo.escape_json_string(_name) + N'":' + field_value, N',') 
               WITHIN GROUP (ORDER BY _order, _structure_id)
        FROM (
            SELECT 
                c._order,
                c._structure_id,
                c._name,
                dbo.build_field_json(
                    @object_id, c._structure_id, @scheme_id, @parent_structure_id,
                    c._name, c.db_type, c.type_semantic, c._collection_type,
                    @max_depth, @array_index, @parent_value_id
                ) AS field_value
            FROM _scheme_metadata_cache c
            WHERE c._scheme_id = @scheme_id
              AND ((@parent_structure_id IS NULL AND c._parent_structure_id IS NULL) 
                   OR (@parent_structure_id IS NOT NULL AND c._parent_structure_id = @parent_structure_id))
        ) sub
        WHERE field_value IS NOT NULL
    ), N'') + N'}';
    
    RETURN ISNULL(@result, N'{}');
END
GO

-- =====================================================
-- MAIN FUNCTION: Get object as JSON
-- =====================================================
CREATE FUNCTION dbo.get_object_json(
    @object_id BIGINT,
    @max_depth INT = 10
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    DECLARE @result NVARCHAR(MAX);
    DECLARE @scheme_id BIGINT;
    DECLARE @base_json NVARCHAR(MAX);
    DECLARE @properties_json NVARCHAR(MAX);
    
    -- Check if object exists
    IF NOT EXISTS(SELECT 1 FROM _objects WHERE _id = @object_id)
        RETURN NULL;
    
    -- Get scheme_id and base fields
    SELECT 
        @scheme_id = o._id_scheme,
        @base_json = N'{' +
            N'"id":' + CAST(o._id AS NVARCHAR(20)) +
            N',"name":' + CASE WHEN o._name IS NULL THEN N'null' ELSE N'"' + dbo.escape_json_string(o._name) + N'"' END +
            N',"scheme_id":' + CAST(o._id_scheme AS NVARCHAR(20)) +
            N',"scheme_name":"' + dbo.escape_json_string(s._name) + N'"' +
            N',"parent_id":' + CASE WHEN o._id_parent IS NULL THEN N'null' ELSE CAST(o._id_parent AS NVARCHAR(20)) END +
            N',"owner_id":' + CAST(o._id_owner AS NVARCHAR(20)) +
            N',"who_change_id":' + CAST(o._id_who_change AS NVARCHAR(20)) +
            N',"date_create":"' + CONVERT(NVARCHAR(50), o._date_create, 127) + N'"' +
            N',"date_modify":"' + CONVERT(NVARCHAR(50), o._date_modify, 127) + N'"' +
            N',"date_begin":' + CASE WHEN o._date_begin IS NULL THEN N'null' ELSE N'"' + CONVERT(NVARCHAR(50), o._date_begin, 127) + N'"' END +
            N',"date_complete":' + CASE WHEN o._date_complete IS NULL THEN N'null' ELSE N'"' + CONVERT(NVARCHAR(50), o._date_complete, 127) + N'"' END +
            N',"key":' + CASE WHEN o._key IS NULL THEN N'null' ELSE CAST(o._key AS NVARCHAR(20)) END +
            N',"value_long":' + CASE WHEN o._value_long IS NULL THEN N'null' ELSE CAST(o._value_long AS NVARCHAR(20)) END +
            N',"value_string":' + CASE WHEN o._value_string IS NULL THEN N'null' ELSE N'"' + dbo.escape_json_string(o._value_string) + N'"' END +
            N',"value_guid":' + CASE WHEN o._value_guid IS NULL THEN N'null' ELSE N'"' + CAST(o._value_guid AS NVARCHAR(50)) + N'"' END +
            N',"note":' + CASE WHEN o._note IS NULL THEN N'null' ELSE N'"' + dbo.escape_json_string(o._note) + N'"' END +
            N',"value_bool":' + CASE WHEN o._value_bool IS NULL THEN N'null' WHEN o._value_bool = 1 THEN N'true' ELSE N'false' END +
            N',"value_double":' + CASE WHEN o._value_double IS NULL THEN N'null' ELSE FORMAT(o._value_double, 'G', 'en-US') END +
            N',"value_numeric":' + CASE WHEN o._value_numeric IS NULL THEN N'null' ELSE REPLACE(CAST(o._value_numeric AS NVARCHAR(50)), N',', N'.') END +
            N',"value_datetime":' + CASE WHEN o._value_datetime IS NULL THEN N'null' ELSE N'"' + CONVERT(NVARCHAR(50), o._value_datetime, 127) + N'"' END +
            N',"hash":' + CASE WHEN o._hash IS NULL THEN N'null' ELSE N'"' + CAST(o._hash AS NVARCHAR(50)) + N'"' END
    FROM _objects o
    INNER JOIN _schemes s ON s._id = o._id_scheme
    WHERE o._id = @object_id;
    
    -- If max_depth = 0, return only base fields
    IF @max_depth <= 0
        RETURN @base_json + N'}';
    
    -- Build properties
    SET @properties_json = dbo.build_properties(@object_id, @scheme_id, @max_depth, NULL, NULL, NULL);
    
    -- Combine base + properties
    RETURN @base_json + N',"properties":' + ISNULL(@properties_json, N'{}') + N'}';
END
GO

-- =====================================================
-- USAGE EXAMPLES
-- =====================================================
-- 
-- Single object:
--   SELECT dbo.get_object_json(123, 10);
--
-- Multiple objects (batch - FAST!):
--   SELECT dbo.get_object_json(o._id, 10) as json_object
--   FROM _objects o
--   WHERE o._id IN (123, 456, 789);
--
-- With aggregation:
--   SELECT '[' + STRING_AGG(dbo.get_object_json(t._id, 10), ',') + ']'
--   FROM #temp_ids t;
-- =====================================================

PRINT '=========================================';
PRINT 'JSON object FUNCTION created!';
PRINT '';
PRINT 'FUNCTION: dbo.get_object_json(@object_id, @max_depth)';
PRINT '  - Can be used in SELECT statements';
PRINT '  - Reads directly from _values (no temp tables)';
PRINT '  - Recursive for nested objects';
PRINT '  - Use with STRING_AGG for batch processing';
PRINT '=========================================';
GO

