-- ====================================================================================================
-- JSON OBJECT FUNCTIONS
-- MS SQL Server version
-- ====================================================================================================
-- Builds JSON representation of objects from EAV model
-- Supports: hierarchical Class fields, arrays, dictionaries, Object references
-- ====================================================================================================


-- =====================================================
-- DROP EXISTING OBJECTS
-- =====================================================

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'sp_get_object_json')
    DROP PROCEDURE [dbo].[sp_get_object_json]
GO

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'sp_get_object_json_internal')
    DROP PROCEDURE [dbo].[sp_get_object_json_internal]
GO

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'sp_build_hierarchical_properties')
    DROP PROCEDURE [dbo].[sp_build_hierarchical_properties]
GO

-- Drop temp table type if exists
IF EXISTS (SELECT * FROM sys.types WHERE name = 'ValuesTableType')
    DROP TYPE [dbo].[ValuesTableType]
GO

-- =====================================================
-- CREATE TABLE TYPE FOR VALUES (used in temp table)
-- =====================================================

-- Note: We use #all_values temp table instead of TVP for recursion support

-- =====================================================
-- HELPER: Build hierarchical properties from #all_values
-- =====================================================
-- IMPORTANT: Requires #all_values temp table to exist in session!
-- Called recursively by get_object_json

CREATE PROCEDURE [dbo].[sp_build_hierarchical_properties]
    @object_id BIGINT,
    @parent_structure_id BIGINT = NULL,
    @object_scheme_id BIGINT,
    @max_depth INT = 10,
    @array_index NVARCHAR(430) = NULL,
    @parent_value_id BIGINT = NULL,
    @result NVARCHAR(MAX) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @field_results TABLE (
        [field_name] NVARCHAR(450),
        [field_order] BIGINT,
        [field_value] NVARCHAR(MAX)
    );
    
    -- Variables for cursor
    DECLARE @structure_id BIGINT;
    DECLARE @field_name NVARCHAR(450);
    DECLARE @field_order BIGINT;
    DECLARE @collection_type BIGINT;
    DECLARE @is_array BIT;
    DECLARE @is_dictionary BIT;
    DECLARE @type_name NVARCHAR(450);
    DECLARE @db_type NVARCHAR(450);
    DECLARE @type_semantic NVARCHAR(450);
    
    -- Variables for current value
    DECLARE @current_value_id BIGINT;
    DECLARE @current_String NVARCHAR(MAX);
    DECLARE @current_Long BIGINT;
    DECLARE @current_Guid UNIQUEIDENTIFIER;
    DECLARE @current_Double FLOAT;
    DECLARE @current_Numeric DECIMAL(38,18);
    DECLARE @current_DateTimeOffset DATETIMEOFFSET;
    DECLARE @current_Boolean BIT;
    DECLARE @current_ByteArray VARBINARY(MAX);
    DECLARE @current_ListItem BIGINT;
    DECLARE @current_Object BIGINT;
    
    DECLARE @field_value NVARCHAR(MAX);
    DECLARE @base_array_value_id BIGINT;
    DECLARE @children_json NVARCHAR(MAX);
    DECLARE @temp_json NVARCHAR(MAX);
    DECLARE @next_depth INT;
    
    -- Variables for array/dictionary handling (must be declared at procedure level)
    DECLARE @elem_array_index NVARCHAR(430);
    DECLARE @elem_value_id BIGINT;
    DECLARE @ref_object_id BIGINT;
    DECLARE @dict_key NVARCHAR(430);
    DECLARE @dict_value_id BIGINT;
    DECLARE @dict_obj_key NVARCHAR(430);
    DECLARE @dict_obj_id BIGINT;
    
    -- Table variables for collecting results (must be declared at procedure level)
    DECLARE @array_elements TABLE ([idx] INT IDENTITY, [element_json] NVARCHAR(MAX));
    DECLARE @obj_array TABLE ([idx] INT IDENTITY, [obj_json] NVARCHAR(MAX));
    DECLARE @dict_elements TABLE ([key] NVARCHAR(430), [element_json] NVARCHAR(MAX));
    DECLARE @dict_obj TABLE ([key] NVARCHAR(430), [obj_json] NVARCHAR(MAX));
    
    -- Protection against infinite recursion
    IF @max_depth < -100
    BEGIN
        SET @result = N'{"error":"Max recursion depth reached"}';
        RETURN;
    END
    
    -- Auto-fill cache if empty
    IF NOT EXISTS(SELECT 1 FROM [dbo].[_scheme_metadata_cache] WHERE [_scheme_id] = @object_scheme_id)
    BEGIN
        EXEC [dbo].[sync_metadata_cache_for_scheme] @object_scheme_id;
    END
    
    -- Iterate through structures for current level
    DECLARE structure_cursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT 
            c.[_structure_id],
            c.[_name],
            c.[_order],
            c.[_collection_type],
            CASE WHEN c.[_collection_type] = -9223372036854775668 THEN 1 ELSE 0 END, -- Array type ID
            CASE WHEN c.[_collection_type] = -9223372036854775667 THEN 1 ELSE 0 END, -- Dictionary type ID
            c.[type_name],
            c.[db_type],
            c.[type_semantic]
        FROM [dbo].[_scheme_metadata_cache] c
        WHERE c.[_scheme_id] = @object_scheme_id
          AND ((@parent_structure_id IS NULL AND c.[_parent_structure_id] IS NULL) 
               OR (@parent_structure_id IS NOT NULL AND c.[_parent_structure_id] = @parent_structure_id))
        ORDER BY c.[_order], c.[_structure_id];
    
    OPEN structure_cursor;
    FETCH NEXT FROM structure_cursor INTO @structure_id, @field_name, @field_order, @collection_type,
                                          @is_array, @is_dictionary, @type_name, @db_type, @type_semantic;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @field_value = NULL;
        SET @current_value_id = NULL;
        SET @base_array_value_id = NULL;
        
        -- Find current value from #all_values
        IF @parent_value_id IS NOT NULL
        BEGIN
            -- For nested fields inside array/dictionary elements
            SELECT TOP 1
                @current_value_id = [_id],
                @current_String = [_String],
                @current_Long = [_Long],
                @current_Guid = [_Guid],
                @current_Double = [_Double],
                @current_Numeric = [_Numeric],
                @current_DateTimeOffset = [_DateTimeOffset],
                @current_Boolean = [_Boolean],
                @current_ByteArray = [_ByteArray],
                @current_ListItem = [_ListItem],
                @current_Object = [_Object]
            FROM #all_values
            WHERE [_id_object] = @object_id
              AND [_id_structure] = @structure_id
              AND [_array_parent_id] = @parent_value_id;
        END
        ELSE IF @array_index IS NOT NULL
        BEGIN
            -- For array/dictionary elements
            SELECT TOP 1
                @current_value_id = [_id],
                @current_String = [_String],
                @current_Long = [_Long],
                @current_Guid = [_Guid],
                @current_Double = [_Double],
                @current_Numeric = [_Numeric],
                @current_DateTimeOffset = [_DateTimeOffset],
                @current_Boolean = [_Boolean],
                @current_ByteArray = [_ByteArray],
                @current_ListItem = [_ListItem],
                @current_Object = [_Object]
            FROM #all_values
            WHERE [_id_object] = @object_id
              AND [_id_structure] = @structure_id
              AND [_array_index] = @array_index;
        END
        ELSE
        BEGIN
            -- For regular fields
            SELECT TOP 1
                @current_value_id = [_id],
                @current_String = [_String],
                @current_Long = [_Long],
                @current_Guid = [_Guid],
                @current_Double = [_Double],
                @current_Numeric = [_Numeric],
                @current_DateTimeOffset = [_DateTimeOffset],
                @current_Boolean = [_Boolean],
                @current_ByteArray = [_ByteArray],
                @current_ListItem = [_ListItem],
                @current_Object = [_Object]
            FROM #all_values
            WHERE [_id_object] = @object_id
              AND [_id_structure] = @structure_id
              AND [_array_index] IS NULL;
        END
        
        -- Get base array/dictionary record ID for collections
        IF @is_array = 1 OR @is_dictionary = 1
        BEGIN
            IF @parent_value_id IS NULL
            BEGIN
                SELECT TOP 1 @base_array_value_id = [_id]
                FROM #all_values
                WHERE [_id_object] = @object_id
                  AND [_id_structure] = @structure_id
                  AND [_array_index] IS NULL
                  AND [_array_parent_id] IS NULL;
            END
            ELSE
            BEGIN
                SELECT TOP 1 @base_array_value_id = [_id]
                FROM #all_values
                WHERE [_id_object] = @object_id
                  AND [_id_structure] = @structure_id
                  AND [_array_index] IS NULL
                  AND [_array_parent_id] = @parent_value_id;
            END
        END
        
        -- =====================================================
        -- ARRAYS
        -- =====================================================
        IF @is_array = 1
        BEGIN
            IF @type_semantic = 'Object' -- Array of Class fields
            BEGIN
                -- Build array of Class objects recursively
                DELETE FROM @array_elements;
                
                DECLARE elem_cursor CURSOR LOCAL FAST_FORWARD FOR
                    SELECT [_array_index], [_id]
                    FROM #all_values
                    WHERE [_id_object] = @object_id
                      AND [_id_structure] = @structure_id
                      AND [_array_index] IS NOT NULL
                      AND (@base_array_value_id IS NULL OR [_array_parent_id] = @base_array_value_id)
                    ORDER BY CASE WHEN [_array_index] LIKE '[0-9]%' AND ISNUMERIC([_array_index]) = 1 
                                  THEN CAST([_array_index] AS INT) ELSE 0 END, [_array_index];
                
                OPEN elem_cursor;
                FETCH NEXT FROM elem_cursor INTO @elem_array_index, @elem_value_id;
                
                WHILE @@FETCH_STATUS = 0
                BEGIN
                    EXEC [dbo].[sp_build_hierarchical_properties]
                        @object_id,
                        @structure_id,
                        @object_scheme_id,
                        @max_depth,
                        @elem_array_index,
                        @elem_value_id,
                        @temp_json OUTPUT;
                    
                    INSERT INTO @array_elements ([element_json]) VALUES (@temp_json);
                    FETCH NEXT FROM elem_cursor INTO @elem_array_index, @elem_value_id;
                END
                
                CLOSE elem_cursor;
                DEALLOCATE elem_cursor;
                
                -- Build JSON array
                SELECT @field_value = N'[' + ISNULL(STRING_AGG([element_json], N',') WITHIN GROUP (ORDER BY [idx]), N'') + N']'
                FROM @array_elements;
            END
            ELSE -- Array of primitives or _RObject references
            BEGIN
                -- Handle _RObject arrays with recursion (cannot use subquery in STRING_AGG)
                IF @type_semantic = '_RObject'
                BEGIN
                    DELETE FROM @obj_array;
                    
                    DECLARE obj_cursor CURSOR LOCAL FAST_FORWARD FOR
                        SELECT v.[_Object]
                        FROM #all_values v
                        WHERE v.[_id_object] = @object_id
                          AND v.[_id_structure] = @structure_id
                          AND v.[_array_index] IS NOT NULL
                          AND v.[_Object] IS NOT NULL
                          AND (@base_array_value_id IS NULL OR v.[_array_parent_id] = @base_array_value_id)
                        ORDER BY CASE WHEN v.[_array_index] LIKE '[0-9]%' AND ISNUMERIC(v.[_array_index]) = 1 
                                      THEN CAST(v.[_array_index] AS INT) ELSE 0 END, v.[_array_index];
                    
                    OPEN obj_cursor;
                    FETCH NEXT FROM obj_cursor INTO @ref_object_id;
                    
                    WHILE @@FETCH_STATUS = 0
                    BEGIN
                        SET @next_depth = @max_depth - 1;
                        EXEC [dbo].[sp_get_object_json_internal] @ref_object_id, @next_depth, @temp_json OUTPUT;
                        INSERT INTO @obj_array ([obj_json]) VALUES (@temp_json);
                        FETCH NEXT FROM obj_cursor INTO @ref_object_id;
                    END
                    
                    CLOSE obj_cursor;
                    DEALLOCATE obj_cursor;
                    
                    SELECT @field_value = N'[' + ISNULL(STRING_AGG([obj_json], N',') WITHIN GROUP (ORDER BY [idx]), N'') + N']'
                    FROM @obj_array;
                END
                ELSE
                BEGIN
                    -- Array of primitives (String, Long, Guid, Double, ListItem, etc.)
                    SELECT @field_value = N'[' + ISNULL(STRING_AGG(
                        CASE 
                            WHEN @db_type = 'String' THEN 
                                CASE WHEN v.[_String] IS NULL THEN N'null' ELSE N'"' + REPLACE(REPLACE(v.[_String], N'\', N'\\'), N'"', N'\"') + N'"' END
                            WHEN @db_type = 'Long' THEN 
                                CASE WHEN v.[_Long] IS NULL THEN N'null' ELSE CAST(v.[_Long] AS NVARCHAR(30)) END
                            WHEN @db_type = 'Guid' THEN 
                                CASE WHEN v.[_Guid] IS NULL THEN N'null' ELSE N'"' + CAST(v.[_Guid] AS NVARCHAR(50)) + N'"' END
                            WHEN @db_type = 'Double' THEN 
                                CASE WHEN v.[_Double] IS NULL THEN N'null' ELSE FORMAT(v.[_Double], 'G', 'en-US') END
                            WHEN @db_type = 'Numeric' THEN 
                                CASE WHEN v.[_Numeric] IS NULL THEN N'null' ELSE REPLACE(CAST(v.[_Numeric] AS NVARCHAR(50)), N',', N'.') END
                            WHEN @db_type = 'DateTimeOffset' THEN 
                                CASE WHEN v.[_DateTimeOffset] IS NULL THEN N'null' ELSE N'"' + CONVERT(NVARCHAR(50), v.[_DateTimeOffset], 127) + N'"' END
                            WHEN @db_type = 'Boolean' THEN 
                                CASE WHEN v.[_Boolean] IS NULL THEN N'null' WHEN v.[_Boolean] = 1 THEN N'true' ELSE N'false' END
                            WHEN @db_type = 'ListItem' THEN 
                                CASE WHEN v.[_ListItem] IS NULL THEN N'null'
                                     ELSE dbo.build_listitem_json(v.[_ListItem])
                                END
                            ELSE N'null'
                        END
                    , N',') WITHIN GROUP (ORDER BY 
                        CASE WHEN v.[_array_index] LIKE '[0-9]%' AND ISNUMERIC(v.[_array_index]) = 1 
                             THEN CAST(v.[_array_index] AS INT) ELSE 0 END, 
                        v.[_array_index]
                    ), N'') + N']'
                    FROM #all_values v
                    WHERE v.[_id_object] = @object_id
                      AND v.[_id_structure] = @structure_id
                      AND v.[_array_index] IS NOT NULL
                      AND (@base_array_value_id IS NULL OR v.[_array_parent_id] = @base_array_value_id);
                END
            END
        END
        -- =====================================================
        -- DICTIONARIES
        -- =====================================================
        ELSE IF @is_dictionary = 1
        BEGIN
            IF @type_semantic = 'Object' -- Dictionary of Class fields
            BEGIN
                DELETE FROM @dict_elements;
                
                DECLARE dict_cursor CURSOR LOCAL FAST_FORWARD FOR
                    SELECT [_array_index], [_id]
                    FROM #all_values
                    WHERE [_id_object] = @object_id
                      AND [_id_structure] = @structure_id
                      AND [_array_index] IS NOT NULL
                      AND (@base_array_value_id IS NULL OR [_array_parent_id] = @base_array_value_id);
                
                OPEN dict_cursor;
                FETCH NEXT FROM dict_cursor INTO @dict_key, @dict_value_id;
                
                WHILE @@FETCH_STATUS = 0
                BEGIN
                    EXEC [dbo].[sp_build_hierarchical_properties]
                        @object_id,
                        @structure_id,
                        @object_scheme_id,
                        @max_depth,
                        NULL,
                        @dict_value_id,
                        @temp_json OUTPUT;
                    
                    INSERT INTO @dict_elements ([key], [element_json]) VALUES (@dict_key, @temp_json);
                    FETCH NEXT FROM dict_cursor INTO @dict_key, @dict_value_id;
                END
                
                CLOSE dict_cursor;
                DEALLOCATE dict_cursor;
                
                -- Build JSON object
                SELECT @field_value = N'{' + ISNULL(STRING_AGG(
                    N'"' + REPLACE(REPLACE([key], N'\', N'\\'), N'"', N'\"') + N'":' + [element_json]
                , N','), N'') + N'}'
                FROM @dict_elements;
            END
            ELSE IF @type_semantic = '_RObject' -- Dictionary of Object references
            BEGIN
                DELETE FROM @dict_obj;
                
                DECLARE dict_obj_cursor CURSOR LOCAL FAST_FORWARD FOR
                    SELECT v.[_array_index], v.[_Object]
                    FROM #all_values v
                    WHERE v.[_id_object] = @object_id
                      AND v.[_id_structure] = @structure_id
                      AND v.[_array_index] IS NOT NULL
                      AND v.[_Object] IS NOT NULL
                      AND (@base_array_value_id IS NULL OR v.[_array_parent_id] = @base_array_value_id);
                
                OPEN dict_obj_cursor;
                FETCH NEXT FROM dict_obj_cursor INTO @dict_obj_key, @dict_obj_id;
                
                WHILE @@FETCH_STATUS = 0
                BEGIN
                    SET @next_depth = @max_depth - 1;
                    EXEC [dbo].[sp_get_object_json_internal] @dict_obj_id, @next_depth, @temp_json OUTPUT;
                    INSERT INTO @dict_obj ([key], [obj_json]) VALUES (@dict_obj_key, @temp_json);
                    FETCH NEXT FROM dict_obj_cursor INTO @dict_obj_key, @dict_obj_id;
                END
                
                CLOSE dict_obj_cursor;
                DEALLOCATE dict_obj_cursor;
                
                SELECT @field_value = N'{' + ISNULL(STRING_AGG(
                    N'"' + REPLACE(REPLACE([key], N'\', N'\\'), N'"', N'\"') + N'":' + [obj_json]
                , N','), N'') + N'}'
                FROM @dict_obj;
            END
            ELSE -- Dictionary of primitives
            BEGIN
                SELECT @field_value = N'{' + ISNULL(STRING_AGG(
                    N'"' + REPLACE(REPLACE(v.[_array_index], N'\', N'\\'), N'"', N'\"') + N'":' +
                    CASE 
                        WHEN @db_type = 'String' THEN 
                            CASE WHEN v.[_String] IS NULL THEN N'null' ELSE N'"' + REPLACE(REPLACE(v.[_String], N'\', N'\\'), N'"', N'\"') + N'"' END
                        WHEN @db_type = 'Long' THEN 
                            CASE WHEN v.[_Long] IS NULL THEN N'null' ELSE CAST(v.[_Long] AS NVARCHAR(30)) END
                        WHEN @db_type = 'Guid' THEN 
                            CASE WHEN v.[_Guid] IS NULL THEN N'null' ELSE N'"' + CAST(v.[_Guid] AS NVARCHAR(50)) + N'"' END
                        WHEN @db_type = 'Double' THEN 
                            CASE WHEN v.[_Double] IS NULL THEN N'null' ELSE FORMAT(v.[_Double], 'G', 'en-US') END
                        WHEN @db_type = 'Numeric' THEN 
                            CASE WHEN v.[_Numeric] IS NULL THEN N'null' ELSE REPLACE(CAST(v.[_Numeric] AS NVARCHAR(50)), N',', N'.') END
                        WHEN @db_type = 'DateTimeOffset' THEN 
                            CASE WHEN v.[_DateTimeOffset] IS NULL THEN N'null' ELSE N'"' + CONVERT(NVARCHAR(50), v.[_DateTimeOffset], 127) + N'"' END
                        WHEN @db_type = 'Boolean' THEN 
                            CASE WHEN v.[_Boolean] IS NULL THEN N'null' WHEN v.[_Boolean] = 1 THEN N'true' ELSE N'false' END
                        ELSE N'null'
                    END
                , N','), N'') + N'}'
                FROM #all_values v
                WHERE v.[_id_object] = @object_id
                  AND v.[_id_structure] = @structure_id
                  AND v.[_array_index] IS NOT NULL
                  AND (@base_array_value_id IS NULL OR v.[_array_parent_id] = @base_array_value_id);
            END
        END
        -- =====================================================
        -- OBJECT REFERENCE (_RObject)
        -- =====================================================
        ELSE IF @type_name = 'Object' AND @type_semantic = '_RObject'
        BEGIN
            IF @current_Object IS NOT NULL
            BEGIN
                SET @next_depth = @max_depth - 1;
                EXEC [dbo].[sp_get_object_json_internal] @current_Object, @next_depth, @field_value OUTPUT;
            END
        END
        -- =====================================================
        -- CLASS FIELD (hierarchical)
        -- =====================================================
        ELSE IF @type_semantic = 'Object'
        BEGIN
            IF @current_Guid IS NOT NULL
            BEGIN
                EXEC [dbo].[sp_build_hierarchical_properties]
                    @object_id,
                    @structure_id,
                    @object_scheme_id,
                    @max_depth,
                    NULL,
                    @current_value_id,
                    @field_value OUTPUT;
            END
        END
        -- =====================================================
        -- PRIMITIVE TYPES
        -- =====================================================
        ELSE IF @current_value_id IS NOT NULL
        BEGIN
            SET @field_value = CASE 
                WHEN @db_type = 'String' THEN 
                    CASE WHEN @current_String IS NULL THEN NULL 
                         ELSE N'"' + REPLACE(REPLACE(@current_String, N'\', N'\\'), N'"', N'\"') + N'"' END
                WHEN @db_type = 'Long' THEN 
                    CASE 
                        WHEN @current_ListItem IS NOT NULL THEN dbo.build_listitem_json(@current_ListItem)
                        WHEN @current_Long IS NULL THEN NULL 
                        ELSE CAST(@current_Long AS NVARCHAR(30)) 
                    END
                WHEN @db_type = 'Guid' THEN 
                    CASE WHEN @current_Guid IS NULL THEN NULL 
                         ELSE N'"' + CAST(@current_Guid AS NVARCHAR(50)) + N'"' END
                WHEN @db_type = 'Double' THEN 
                    CASE WHEN @current_Double IS NULL THEN NULL 
                         ELSE FORMAT(@current_Double, 'G', 'en-US') END
                WHEN @db_type = 'Numeric' THEN 
                    CASE WHEN @current_Numeric IS NULL THEN NULL 
                         ELSE REPLACE(CAST(@current_Numeric AS NVARCHAR(50)), N',', N'.') END
                WHEN @db_type = 'DateTimeOffset' THEN 
                    CASE WHEN @current_DateTimeOffset IS NULL THEN NULL 
                         ELSE N'"' + CONVERT(NVARCHAR(50), @current_DateTimeOffset, 127) + N'"' END
                WHEN @db_type = 'Boolean' THEN 
                    CASE WHEN @current_Boolean IS NULL THEN NULL 
                         WHEN @current_Boolean = 1 THEN N'true' ELSE N'false' END
                WHEN @db_type = 'ListItem' THEN dbo.build_listitem_json(@current_ListItem)
                WHEN @db_type = 'ByteArray' THEN
                    CASE WHEN @current_ByteArray IS NULL THEN NULL
                         ELSE N'"' + CAST(N'' AS XML).value('xs:base64Binary(sql:variable("@current_ByteArray"))', 'NVARCHAR(MAX)') + N'"'
                    END
                ELSE NULL
            END;
        END
        
        -- Add to results if not null
        IF @field_value IS NOT NULL
        BEGIN
            INSERT INTO @field_results ([field_name], [field_order], [field_value])
            VALUES (@field_name, @field_order, @field_value);
        END
        
        FETCH NEXT FROM structure_cursor INTO @structure_id, @field_name, @field_order, @collection_type,
                                              @is_array, @is_dictionary, @type_name, @db_type, @type_semantic;
    END
    
    CLOSE structure_cursor;
    DEALLOCATE structure_cursor;
    
    -- Build final JSON object
    SELECT @result = N'{' + ISNULL(STRING_AGG(
        N'"' + REPLACE(REPLACE([field_name], N'\', N'\\'), N'"', N'\"') + N'":' + [field_value]
    , N',') WITHIN GROUP (ORDER BY [field_order], [field_name]), N'') + N'}'
    FROM @field_results;
    
    IF @result IS NULL OR @result = N''
        SET @result = N'{}';
END
GO

-- =====================================================
-- MAIN: Get object as JSON (with OUTPUT for recursive calls)
-- =====================================================

CREATE PROCEDURE [dbo].[sp_get_object_json_internal]
    @object_id BIGINT,
    @max_depth INT = 10,
    @result NVARCHAR(MAX) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @object_scheme_id BIGINT;
    DECLARE @base_json NVARCHAR(MAX);
    DECLARE @properties_json NVARCHAR(MAX);
    DECLARE @is_root_call BIT = 0;
    
    -- Check if object exists - return NULL if not found (consistent with PostgreSQL)
    IF NOT EXISTS(SELECT 1 FROM [dbo].[_objects] WHERE [_id] = @object_id)
    BEGIN
        SET @result = NULL;
        RETURN;
    END
    
    -- Check if this is root call (need to create temp table)
    IF OBJECT_ID('tempdb..#all_values') IS NULL
    BEGIN
        SET @is_root_call = 1;
        
        -- Create temp table for all values
        CREATE TABLE #all_values (
            [_id] BIGINT,
            [_id_structure] BIGINT,
            [_id_object] BIGINT,
            [_String] NVARCHAR(MAX),
            [_Long] BIGINT,
            [_Guid] UNIQUEIDENTIFIER,
            [_Double] FLOAT,
            [_DateTimeOffset] DATETIMEOFFSET,
            [_Boolean] BIT,
            [_ByteArray] VARBINARY(MAX),
            [_Numeric] DECIMAL(38,18),
            [_ListItem] BIGINT,
            [_Object] BIGINT,
            [_array_parent_id] BIGINT,
            [_array_index] NVARCHAR(430)
        );
    END
    
    -- Load values for this object (always, for both root and recursive calls)
    -- Check if values for this object are already loaded
    IF NOT EXISTS(SELECT 1 FROM #all_values WHERE [_id_object] = @object_id)
    BEGIN
        INSERT INTO #all_values
        SELECT [_id], [_id_structure], [_id_object], [_String], [_Long], [_Guid], [_Double],
               [_DateTimeOffset], [_Boolean], [_ByteArray], [_Numeric], [_ListItem], [_Object],
               [_array_parent_id], [_array_index]
        FROM [dbo].[_values]
        WHERE [_id_object] = @object_id;
    END
    
    -- Get scheme_id
    SELECT @object_scheme_id = [_id_scheme]
    FROM [dbo].[_objects]
    WHERE [_id] = @object_id;
    
    -- Build base object JSON
    SELECT @base_json = N'{' +
        N'"id":' + CAST(o.[_id] AS NVARCHAR(20)) +
        N',"name":' + CASE WHEN o.[_name] IS NULL THEN N'null' ELSE N'"' + REPLACE(REPLACE(o.[_name], N'\', N'\\'), N'"', N'\"') + N'"' END +
        N',"scheme_id":' + CAST(o.[_id_scheme] AS NVARCHAR(20)) +
        N',"scheme_name":"' + REPLACE(REPLACE(s.[_name], N'\', N'\\'), N'"', N'\"') + N'"' +
        N',"parent_id":' + CASE WHEN o.[_id_parent] IS NULL THEN N'null' ELSE CAST(o.[_id_parent] AS NVARCHAR(20)) END +
        N',"owner_id":' + CAST(o.[_id_owner] AS NVARCHAR(20)) +
        N',"who_change_id":' + CAST(o.[_id_who_change] AS NVARCHAR(20)) +
        N',"date_create":"' + CONVERT(NVARCHAR(50), o.[_date_create], 127) + N'"' +
        N',"date_modify":"' + CONVERT(NVARCHAR(50), o.[_date_modify], 127) + N'"' +
        N',"date_begin":' + CASE WHEN o.[_date_begin] IS NULL THEN N'null' ELSE N'"' + CONVERT(NVARCHAR(50), o.[_date_begin], 127) + N'"' END +
        N',"date_complete":' + CASE WHEN o.[_date_complete] IS NULL THEN N'null' ELSE N'"' + CONVERT(NVARCHAR(50), o.[_date_complete], 127) + N'"' END +
        N',"key":' + CASE WHEN o.[_key] IS NULL THEN N'null' ELSE CAST(o.[_key] AS NVARCHAR(20)) END +
        N',"value_long":' + CASE WHEN o.[_value_long] IS NULL THEN N'null' ELSE CAST(o.[_value_long] AS NVARCHAR(20)) END +
        N',"value_string":' + CASE WHEN o.[_value_string] IS NULL THEN N'null' ELSE N'"' + REPLACE(REPLACE(o.[_value_string], N'\', N'\\'), N'"', N'\"') + N'"' END +
        N',"value_guid":' + CASE WHEN o.[_value_guid] IS NULL THEN N'null' ELSE N'"' + CAST(o.[_value_guid] AS NVARCHAR(50)) + N'"' END +
        N',"note":' + CASE WHEN o.[_note] IS NULL THEN N'null' ELSE N'"' + REPLACE(REPLACE(o.[_note], N'\', N'\\'), N'"', N'\"') + N'"' END +
        N',"value_bool":' + CASE WHEN o.[_value_bool] IS NULL THEN N'null' WHEN o.[_value_bool] = 1 THEN N'true' ELSE N'false' END +
        N',"value_double":' + CASE WHEN o.[_value_double] IS NULL THEN N'null' ELSE FORMAT(o.[_value_double], 'G', 'en-US') END +
        N',"value_numeric":' + CASE WHEN o.[_value_numeric] IS NULL THEN N'null' ELSE REPLACE(CAST(o.[_value_numeric] AS NVARCHAR(50)), N',', N'.') END +
        N',"value_datetime":' + CASE WHEN o.[_value_datetime] IS NULL THEN N'null' ELSE N'"' + CONVERT(NVARCHAR(50), o.[_value_datetime], 127) + N'"' END +
        N',"value_bytes":' + CASE WHEN o.[_value_bytes] IS NULL THEN N'null' ELSE N'"' + CAST(N'' AS XML).value('xs:base64Binary(sql:column("o.[_value_bytes]"))', 'NVARCHAR(MAX)') + N'"' END +
        N',"hash":' + CASE WHEN o.[_hash] IS NULL THEN N'null' ELSE N'"' + CAST(o.[_hash] AS NVARCHAR(50)) + N'"' END
    FROM [dbo].[_objects] o
    INNER JOIN [dbo].[_schemes] s ON s.[_id] = o.[_id_scheme]
    WHERE o.[_id] = @object_id;
    
    -- Check max_depth
    IF @max_depth <= 0
    BEGIN
        SET @result = @base_json + N'}';
        IF @is_root_call = 1 DROP TABLE #all_values;
        RETURN;
    END
    
    -- Build properties
    EXEC [dbo].[sp_build_hierarchical_properties]
        @object_id,
        NULL,
        @object_scheme_id,
        @max_depth,
        NULL,
        NULL,
        @properties_json OUTPUT;
    
    -- Combine base + properties
    SET @result = @base_json + N',"properties":' + ISNULL(@properties_json, N'{}') + N'}';
    
    -- Cleanup temp table if this was root call
    IF @is_root_call = 1
        DROP TABLE #all_values;
END
GO

-- =====================================================
-- PUBLIC WRAPPER: Get object as JSON with SELECT result
-- More efficient for C# - just ExecuteScalarAsync
-- =====================================================

CREATE PROCEDURE [dbo].[sp_get_object_json]
    @object_id BIGINT,
    @max_depth INT = 10
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @result NVARCHAR(MAX);
    EXEC [dbo].[sp_get_object_json_internal] @object_id, @max_depth, @result OUTPUT;
    SELECT @result AS json_result;
END
GO

-- =====================================================
-- USAGE EXAMPLES
-- =====================================================
-- 
-- Get object as JSON (simple - SELECT result):
--   EXEC get_object_json @object_id = 123;
--   EXEC get_object_json @object_id = 123, @max_depth = 2;
--
-- For recursive calls (internal use with OUTPUT):
--   DECLARE @json NVARCHAR(MAX);
--   EXEC sp_get_object_json_internal @object_id = 123, @result = @json OUTPUT;
-- =====================================================

PRINT '========================================='
PRINT 'JSON object functions created!'
PRINT '========================================='
GO

