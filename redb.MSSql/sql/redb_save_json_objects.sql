-- ============================================================
-- SAVE OBJECT FROM JSON (MS SQL Server)
-- Inverse of get_object_json: JSON -> _objects + _values
-- Strategy: DeleteInsert (delete all _values, then insert new)
-- ============================================================
-- Input format: same JSON structure as produced by get_object_json()
--
-- Two stored procedures (mirroring read architecture):
--   sp_save_object_json(@json NVARCHAR(MAX), @result_id BIGINT OUTPUT)   [mirror of get_object_json]
--   sp_save_hierarchical_properties(...)                                  [mirror of build_field_json/build_properties]
--
-- Supported field types (all branches from build_field_json):
--   1. Array of Class          (type_semantic='Object',  _is_array)
--   2. Array of _RObject refs  (type_semantic='_RObject', _is_array)
--   3. Array of primitives     (String/Long/Double/Guid/Boolean/Numeric/DateTime/ByteArray/ListItem)
--   4. Dictionary of Class     (type_semantic='Object',  _is_dictionary)
--   5. Dictionary of _RObject  (type_semantic='_RObject', _is_dictionary)
--   6. Dictionary of primitives
--   7. Single _RObject ref     (type_name='Object', type_semantic='_RObject')
--   8. Class field             (type_semantic='Object', marker _Guid)
--   9. ListItem                (db_type='ListItem', or Long backward-compat)
--  10. Primitive scalars       (String/Long/Double/Guid/Boolean/Numeric/DateTime/ByteArray)
-- ============================================================

-- Drop existing procedures
IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'sp_save_object_json')
    DROP PROCEDURE [dbo].[sp_save_object_json]
GO

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'sp_save_hierarchical_properties')
    DROP PROCEDURE [dbo].[sp_save_hierarchical_properties]
GO


-- ============================================================
-- 1. RECURSIVE PROPERTY WRITER
--    Mirror of build_field_json / build_properties (reader)
--    Walks _scheme_metadata_cache, extracts values from JSON,
--    INSERTs into _values with correct column routing
-- ============================================================
CREATE PROCEDURE [dbo].[sp_save_hierarchical_properties]
    @object_id BIGINT,              -- owning object _id
    @parent_structure_id BIGINT,    -- NULL for root level, structure _id for nested Class
    @scheme_id BIGINT,              -- scheme _id (for metadata cache lookup)
    @properties_json NVARCHAR(MAX), -- JSON object: {"Name":"Alice","Tags":[...],"Address":{...}}
    @parent_value_id BIGINT = NULL  -- NULL for root, parent _values._id for nested
AS
BEGIN
    SET NOCOUNT ON;

    -- Ensure metadata cache is populated (mirrors read function)
    IF NOT EXISTS(SELECT 1 FROM [dbo].[_scheme_metadata_cache] WHERE [_scheme_id] = @scheme_id)
    BEGIN
        EXEC [dbo].[sync_metadata_cache_for_scheme] @scheme_id;
    END

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

    -- Variables for JSON extraction
    DECLARE @field_json NVARCHAR(MAX);
    DECLARE @field_type NVARCHAR(50);
    DECLARE @v_value_id BIGINT;
    DECLARE @v_head_id BIGINT;
    DECLARE @v_element_id BIGINT;
    DECLARE @v_nested_id BIGINT;

    -- Variables for array/dictionary iteration
    DECLARE @arr_len INT;
    DECLARE @arr_idx INT;
    DECLARE @element_json NVARCHAR(MAX);
    DECLARE @element_type NVARCHAR(50);

    -- Variables for OPENJSON dictionary iteration
    DECLARE @dict_key NVARCHAR(430);
    DECLARE @dict_value NVARCHAR(MAX);
    DECLARE @dict_type INT;

    -- Walk structures for this level (same query & ordering as build_properties)
    DECLARE structure_cursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT
            c.[_structure_id],
            c.[_name],
            c.[_order],
            c.[_collection_type],
            CASE WHEN c.[_collection_type] = -9223372036854775668 THEN 1 ELSE 0 END,
            CASE WHEN c.[_collection_type] = -9223372036854775667 THEN 1 ELSE 0 END,
            c.[type_name],
            c.[db_type],
            c.[type_semantic]
        FROM [dbo].[_scheme_metadata_cache] c
        WHERE c.[_scheme_id] = @scheme_id
          AND ((@parent_structure_id IS NULL AND c.[_parent_structure_id] IS NULL)
               OR (@parent_structure_id IS NOT NULL AND c.[_parent_structure_id] = @parent_structure_id))
        ORDER BY c.[_order], c.[_structure_id];

    OPEN structure_cursor;
    FETCH NEXT FROM structure_cursor INTO @structure_id, @field_name, @field_order, @collection_type,
                                          @is_array, @is_dictionary, @type_name, @db_type, @type_semantic;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Extract field value using OPENJSON (NVARCHAR(MAX), no 4000-char JSON_VALUE limit)
        SET @field_json = NULL;
        SELECT @field_json = [value]
        FROM OPENJSON(@properties_json)
        WHERE [key] = @field_name;

        -- Skip if field not present or JSON null
        IF @field_json IS NULL OR @field_json = N'null'
        BEGIN
            FETCH NEXT FROM structure_cursor INTO @structure_id, @field_name, @field_order, @collection_type,
                                                  @is_array, @is_dictionary, @type_name, @db_type, @type_semantic;
            CONTINUE;
        END

        -- ================================================================
        -- DISPATCH BY TYPE
        -- Mirrors the CASE tree in build_field_json
        -- ================================================================

        IF @is_array = 1
        BEGIN
            -- ========================================================
            -- ARRAY FIELDS (branches 1-3)
            -- ========================================================

            -- Insert head record: marker "this array property exists"
            SET @v_head_id = NEXT VALUE FOR [dbo].[global_identity];
            INSERT INTO [dbo].[_values] ([_id], [_id_structure], [_id_object], [_array_index], [_array_parent_id])
            VALUES (@v_head_id, @structure_id, @object_id, NULL, @parent_value_id);

            -- Empty array [] -> head record alone (read returns [])
            SET @arr_len = (SELECT COUNT(*) FROM OPENJSON(@field_json));
            IF @arr_len = 0
            BEGIN
                FETCH NEXT FROM structure_cursor INTO @structure_id, @field_name, @field_order, @collection_type,
                                                      @is_array, @is_dictionary, @type_name, @db_type, @type_semantic;
                CONTINUE;
            END

            -- ----- Branch 1: Array of Class fields -----
            IF @type_semantic = 'Object'
            BEGIN
                DECLARE class_arr_cursor CURSOR LOCAL FAST_FORWARD FOR
                    SELECT CAST([key] AS INT), [value], [type]
                    FROM OPENJSON(@field_json)
                    ORDER BY CAST([key] AS INT);

                OPEN class_arr_cursor;
                DECLARE @ca_idx INT, @ca_val NVARCHAR(MAX), @ca_type INT;
                FETCH NEXT FROM class_arr_cursor INTO @ca_idx, @ca_val, @ca_type;

                WHILE @@FETCH_STATUS = 0
                BEGIN
                    IF @ca_val IS NOT NULL AND @ca_val != N'null'
                    BEGIN
                        -- Insert element record
                        SET @v_element_id = NEXT VALUE FOR [dbo].[global_identity];
                        INSERT INTO [dbo].[_values] ([_id], [_id_structure], [_id_object], [_array_index], [_array_parent_id])
                        VALUES (@v_element_id, @structure_id, @object_id, CAST(@ca_idx AS NVARCHAR(30)), @v_head_id);

                        -- Recurse into child properties of this array element
                        IF @ca_type = 5  -- JSON object
                        BEGIN
                            EXEC [dbo].[sp_save_hierarchical_properties]
                                @object_id,
                                @structure_id,
                                @scheme_id,
                                @ca_val,
                                @v_element_id;
                        END
                    END

                    FETCH NEXT FROM class_arr_cursor INTO @ca_idx, @ca_val, @ca_type;
                END

                CLOSE class_arr_cursor;
                DEALLOCATE class_arr_cursor;
            END

            -- ----- Branch 2: Array of Object references (_RObject) -----
            ELSE IF @type_semantic = '_RObject'
            BEGIN
                DECLARE robj_arr_cursor CURSOR LOCAL FAST_FORWARD FOR
                    SELECT CAST([key] AS INT), [value], [type]
                    FROM OPENJSON(@field_json)
                    ORDER BY CAST([key] AS INT);

                OPEN robj_arr_cursor;
                DECLARE @ra_idx INT, @ra_val NVARCHAR(MAX), @ra_type INT;
                FETCH NEXT FROM robj_arr_cursor INTO @ra_idx, @ra_val, @ra_type;

                WHILE @@FETCH_STATUS = 0
                BEGIN
                    SET @v_nested_id = NULL;

                    IF @ra_type = 5  -- JSON object -> recursive save
                    BEGIN
                        EXEC [dbo].[sp_save_object_json] @ra_val, @v_nested_id OUTPUT;
                    END
                    ELSE IF @ra_val IS NOT NULL AND @ra_val != N'null'
                    BEGIN
                        SET @v_nested_id = CAST(@ra_val AS BIGINT);
                    END

                    SET @v_element_id = NEXT VALUE FOR [dbo].[global_identity];
                    INSERT INTO [dbo].[_values] (
                        [_id], [_id_structure], [_id_object], [_Object], [_array_index], [_array_parent_id]
                    ) VALUES (
                        @v_element_id, @structure_id, @object_id,
                        @v_nested_id, CAST(@ra_idx AS NVARCHAR(30)), @v_head_id
                    );

                    FETCH NEXT FROM robj_arr_cursor INTO @ra_idx, @ra_val, @ra_type;
                END

                CLOSE robj_arr_cursor;
                DEALLOCATE robj_arr_cursor;
            END

            -- ----- Branch 3: Array of primitive types -----
            ELSE
            BEGIN
                DECLARE prim_arr_cursor CURSOR LOCAL FAST_FORWARD FOR
                    SELECT CAST([key] AS INT), [value], [type]
                    FROM OPENJSON(@field_json)
                    ORDER BY CAST([key] AS INT);

                OPEN prim_arr_cursor;
                DECLARE @pa_idx INT, @pa_val NVARCHAR(MAX), @pa_type INT;
                FETCH NEXT FROM prim_arr_cursor INTO @pa_idx, @pa_val, @pa_type;

                WHILE @@FETCH_STATUS = 0
                BEGIN
                    SET @v_element_id = NEXT VALUE FOR [dbo].[global_identity];

                    INSERT INTO [dbo].[_values] (
                        [_id], [_id_structure], [_id_object], [_array_index], [_array_parent_id],
                        [_String], [_Long], [_Guid], [_Double], [_Numeric],
                        [_DateTimeOffset], [_Boolean], [_ByteArray], [_ListItem]
                    ) VALUES (
                        @v_element_id, @structure_id, @object_id,
                        CAST(@pa_idx AS NVARCHAR(30)), @v_head_id,
                        -- Route value to correct column based on db_type
                        CASE WHEN @db_type = 'String' THEN @pa_val END,
                        CASE WHEN @db_type = 'Long' AND @pa_type != 5 THEN CAST(@pa_val AS BIGINT) END,
                        CASE WHEN @db_type = 'Guid' THEN CAST(@pa_val AS UNIQUEIDENTIFIER) END,
                        CASE WHEN @db_type = 'Double' THEN CAST(@pa_val AS FLOAT) END,
                        CASE WHEN @db_type = 'Numeric' THEN CAST(@pa_val AS DECIMAL(38,18)) END,
                        CASE WHEN @db_type IN ('DateTimeOffset', 'DateTime') THEN CAST(@pa_val AS DATETIMEOFFSET) END,
                        CASE WHEN @db_type = 'Boolean' THEN
                            CASE WHEN @pa_val = 'true' THEN 1 WHEN @pa_val = 'false' THEN 0 ELSE CAST(@pa_val AS BIT) END
                        END,
                        CASE WHEN @db_type = 'ByteArray' THEN CAST(N'' AS XML).value('xs:base64Binary(sql:variable("@pa_val"))', 'VARBINARY(MAX)') END,
                        -- _ListItem: ListItem type OR Long backward-compat (Long field with ListItem JSON)
                        CASE WHEN @db_type = 'ListItem' THEN
                                 CASE WHEN @pa_type = 5
                                      THEN CAST(JSON_VALUE(@pa_val, '$.id') AS BIGINT)
                                      ELSE CAST(@pa_val AS BIGINT) END
                             WHEN @db_type = 'Long' AND @pa_type = 5
                             THEN CAST(JSON_VALUE(@pa_val, '$.id') AS BIGINT)
                        END
                    );

                    FETCH NEXT FROM prim_arr_cursor INTO @pa_idx, @pa_val, @pa_type;
                END

                CLOSE prim_arr_cursor;
                DEALLOCATE prim_arr_cursor;
            END
        END

        ELSE IF @is_dictionary = 1
        BEGIN
            -- ========================================================
            -- DICTIONARY FIELDS (branches 4-6)
            -- ========================================================

            -- Insert head record (marker: "this dictionary property exists")
            SET @v_head_id = NEXT VALUE FOR [dbo].[global_identity];
            INSERT INTO [dbo].[_values] ([_id], [_id_structure], [_id_object], [_array_index], [_array_parent_id])
            VALUES (@v_head_id, @structure_id, @object_id, NULL, @parent_value_id);

            -- Empty dict {} -> head record alone (read returns {}); OPENJSON yields zero rows

            -- ----- Branch 4: Dictionary of Class fields -----
            IF @type_semantic = 'Object'
            BEGIN
                DECLARE class_dict_cursor CURSOR LOCAL FAST_FORWARD FOR
                    SELECT [key], [value], [type]
                    FROM OPENJSON(@field_json);

                OPEN class_dict_cursor;
                DECLARE @cd_key NVARCHAR(430), @cd_val NVARCHAR(MAX), @cd_type INT;
                FETCH NEXT FROM class_dict_cursor INTO @cd_key, @cd_val, @cd_type;

                WHILE @@FETCH_STATUS = 0
                BEGIN
                    SET @v_element_id = NEXT VALUE FOR [dbo].[global_identity];
                    INSERT INTO [dbo].[_values] ([_id], [_id_structure], [_id_object], [_array_index], [_array_parent_id])
                    VALUES (@v_element_id, @structure_id, @object_id, @cd_key, @v_head_id);

                    IF @cd_type = 5  -- JSON object
                    BEGIN
                        EXEC [dbo].[sp_save_hierarchical_properties]
                            @object_id,
                            @structure_id,
                            @scheme_id,
                            @cd_val,
                            @v_element_id;
                    END

                    FETCH NEXT FROM class_dict_cursor INTO @cd_key, @cd_val, @cd_type;
                END

                CLOSE class_dict_cursor;
                DEALLOCATE class_dict_cursor;
            END

            -- ----- Branch 5: Dictionary of Object references (_RObject) -----
            ELSE IF @type_semantic = '_RObject'
            BEGIN
                DECLARE robj_dict_cursor CURSOR LOCAL FAST_FORWARD FOR
                    SELECT [key], [value], [type]
                    FROM OPENJSON(@field_json);

                OPEN robj_dict_cursor;
                DECLARE @rd_key NVARCHAR(430), @rd_val NVARCHAR(MAX), @rd_type INT;
                FETCH NEXT FROM robj_dict_cursor INTO @rd_key, @rd_val, @rd_type;

                WHILE @@FETCH_STATUS = 0
                BEGIN
                    SET @v_nested_id = NULL;

                    IF @rd_type = 5  -- JSON object -> recursive save
                    BEGIN
                        EXEC [dbo].[sp_save_object_json] @rd_val, @v_nested_id OUTPUT;
                    END
                    ELSE IF @rd_val IS NOT NULL AND @rd_val != N'null'
                    BEGIN
                        SET @v_nested_id = CAST(@rd_val AS BIGINT);
                    END

                    SET @v_element_id = NEXT VALUE FOR [dbo].[global_identity];
                    INSERT INTO [dbo].[_values] (
                        [_id], [_id_structure], [_id_object], [_Object], [_array_index], [_array_parent_id]
                    ) VALUES (
                        @v_element_id, @structure_id, @object_id,
                        @v_nested_id, @rd_key, @v_head_id
                    );

                    FETCH NEXT FROM robj_dict_cursor INTO @rd_key, @rd_val, @rd_type;
                END

                CLOSE robj_dict_cursor;
                DEALLOCATE robj_dict_cursor;
            END

            -- ----- Branch 6: Dictionary of primitive types -----
            ELSE
            BEGIN
                DECLARE prim_dict_cursor CURSOR LOCAL FAST_FORWARD FOR
                    SELECT [key], [value], [type]
                    FROM OPENJSON(@field_json);

                OPEN prim_dict_cursor;
                DECLARE @pd_key NVARCHAR(430), @pd_val NVARCHAR(MAX), @pd_type INT;
                FETCH NEXT FROM prim_dict_cursor INTO @pd_key, @pd_val, @pd_type;

                WHILE @@FETCH_STATUS = 0
                BEGIN
                    SET @v_element_id = NEXT VALUE FOR [dbo].[global_identity];

                    INSERT INTO [dbo].[_values] (
                        [_id], [_id_structure], [_id_object], [_array_index], [_array_parent_id],
                        [_String], [_Long], [_Guid], [_Double], [_Numeric],
                        [_DateTimeOffset], [_Boolean], [_ByteArray], [_ListItem]
                    ) VALUES (
                        @v_element_id, @structure_id, @object_id,
                        @pd_key, @v_head_id,
                        CASE WHEN @db_type = 'String' THEN @pd_val END,
                        CASE WHEN @db_type = 'Long' THEN CAST(@pd_val AS BIGINT) END,
                        CASE WHEN @db_type = 'Guid' THEN CAST(@pd_val AS UNIQUEIDENTIFIER) END,
                        CASE WHEN @db_type = 'Double' THEN CAST(@pd_val AS FLOAT) END,
                        CASE WHEN @db_type = 'Numeric' THEN CAST(@pd_val AS DECIMAL(38,18)) END,
                        CASE WHEN @db_type IN ('DateTimeOffset', 'DateTime') THEN CAST(@pd_val AS DATETIMEOFFSET) END,
                        CASE WHEN @db_type = 'Boolean' THEN
                            CASE WHEN @pd_val = 'true' THEN 1 WHEN @pd_val = 'false' THEN 0 ELSE CAST(@pd_val AS BIT) END
                        END,
                        CASE WHEN @db_type = 'ByteArray' THEN CAST(N'' AS XML).value('xs:base64Binary(sql:variable("@pd_val"))', 'VARBINARY(MAX)') END,
                        CASE WHEN @db_type = 'ListItem' THEN
                                 CASE WHEN @pd_type = 5
                                      THEN CAST(JSON_VALUE(@pd_val, '$.id') AS BIGINT)
                                      ELSE CAST(@pd_val AS BIGINT) END
                        END
                    );

                    FETCH NEXT FROM prim_dict_cursor INTO @pd_key, @pd_val, @pd_type;
                END

                CLOSE prim_dict_cursor;
                DEALLOCATE prim_dict_cursor;
            END
        END

        ELSE IF @type_name = 'Object' AND @type_semantic = '_RObject'
        BEGIN
            -- ========================================================
            -- Branch 7: SINGLE OBJECT REFERENCE
            -- Mirror: get_object_json(current_Object, max_depth - 1)
            -- ========================================================
            SET @v_nested_id = NULL;
            SET @field_type = (SELECT CASE WHEN ISJSON(@field_json) = 1 AND LEFT(LTRIM(@field_json), 1) = '{' THEN 'object' ELSE 'scalar' END);

            IF @field_type = 'object'
            BEGIN
                -- Full nested object JSON -> save recursively, get ID
                EXEC [dbo].[sp_save_object_json] @field_json, @v_nested_id OUTPUT;
            END
            ELSE
            BEGIN
                -- Numeric ID reference
                SET @v_nested_id = CAST(@field_json AS BIGINT);
            END

            SET @v_value_id = NEXT VALUE FOR [dbo].[global_identity];
            INSERT INTO [dbo].[_values] ([_id], [_id_structure], [_id_object], [_Object], [_array_parent_id])
            VALUES (@v_value_id, @structure_id, @object_id, @v_nested_id, @parent_value_id);
        END

        ELSE IF @type_semantic = 'Object'
        BEGIN
            -- ========================================================
            -- Branch 8: CLASS FIELD (nested class with child properties)
            -- Mirror: WHEN _Guid IS NULL THEN NULL ELSE build_properties(...)
            -- ========================================================

            -- Insert marker record with _Guid (read checks: _Guid IS NULL -> class is null)
            SET @v_value_id = NEXT VALUE FOR [dbo].[global_identity];
            INSERT INTO [dbo].[_values] ([_id], [_id_structure], [_id_object], [_Guid], [_array_parent_id])
            VALUES (@v_value_id, @structure_id, @object_id, NEWID(), @parent_value_id);

            -- Recurse: save child properties under this class structure
            EXEC [dbo].[sp_save_hierarchical_properties]
                @object_id,
                @structure_id,
                @scheme_id,
                @field_json,
                @v_value_id;
        END

        ELSE
        BEGIN
            -- ========================================================
            -- Branches 9-10: PRIMITIVE SCALAR FIELDS + LISTITEM
            -- ========================================================
            SET @v_value_id = NEXT VALUE FOR [dbo].[global_identity];
            SET @field_type = (SELECT CASE WHEN ISJSON(@field_json) = 1 AND LEFT(LTRIM(@field_json), 1) = '{' THEN 'object' ELSE 'scalar' END);

            -- Branch 9: ListItem (db_type='ListItem', or Long backward-compat with ListItem JSON)
            IF @db_type = 'ListItem'
               OR (@db_type = 'Long' AND @field_type = 'object')
            BEGIN
                INSERT INTO [dbo].[_values] ([_id], [_id_structure], [_id_object], [_ListItem], [_array_parent_id])
                VALUES (@v_value_id, @structure_id, @object_id,
                        CAST(JSON_VALUE(@field_json, '$.id') AS BIGINT), @parent_value_id);
            END
            -- Branch 10: Standard primitive types
            ELSE
            BEGIN
                INSERT INTO [dbo].[_values] (
                    [_id], [_id_structure], [_id_object], [_array_parent_id],
                    [_String], [_Long], [_Guid], [_Double], [_Numeric],
                    [_DateTimeOffset], [_Boolean], [_ByteArray]
                ) VALUES (
                    @v_value_id, @structure_id, @object_id, @parent_value_id,
                    CASE WHEN @db_type = 'String' THEN @field_json END,
                    CASE WHEN @db_type = 'Long' THEN CAST(@field_json AS BIGINT) END,
                    CASE WHEN @db_type = 'Guid' THEN CAST(@field_json AS UNIQUEIDENTIFIER) END,
                    CASE WHEN @db_type = 'Double' THEN CAST(@field_json AS FLOAT) END,
                    CASE WHEN @db_type = 'Numeric' THEN CAST(@field_json AS DECIMAL(38,18)) END,
                    CASE WHEN @db_type IN ('DateTimeOffset', 'DateTime') THEN CAST(@field_json AS DATETIMEOFFSET) END,
                    CASE WHEN @db_type = 'Boolean' THEN
                        CASE WHEN @field_json = 'true' THEN 1 WHEN @field_json = 'false' THEN 0 ELSE CAST(@field_json AS BIT) END
                    END,
                    CASE WHEN @db_type = 'ByteArray' THEN CAST(N'' AS XML).value('xs:base64Binary(sql:variable("@field_json"))', 'VARBINARY(MAX)') END
                );
            END
        END

        FETCH NEXT FROM structure_cursor INTO @structure_id, @field_name, @field_order, @collection_type,
                                              @is_array, @is_dictionary, @type_name, @db_type, @type_semantic;
    END

    CLOSE structure_cursor;
    DEALLOCATE structure_cursor;
END
GO


-- ============================================================
-- 2. MAIN ENTRY POINT
--    Mirror of get_object_json (reader)
--    JSON -> UPSERT _objects -> DELETE _values -> save properties
-- ============================================================
CREATE PROCEDURE [dbo].[sp_save_object_json]
    @json_data NVARCHAR(MAX),
    @result_id BIGINT = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @object_id BIGINT;
    DECLARE @scheme_id BIGINT;
    DECLARE @properties_json NVARCHAR(MAX);
    DECLARE @exists BIT;
    DECLARE @v_bytes_b64 NVARCHAR(MAX);
    DECLARE @v_note NVARCHAR(MAX);
    DECLARE @v_value_string NVARCHAR(MAX);

    -- Extract required fields
    SET @object_id = CAST(JSON_VALUE(@json_data, '$.id') AS BIGINT);
    SET @scheme_id = CAST(JSON_VALUE(@json_data, '$.scheme_id') AS BIGINT);

    -- Extract NVARCHAR(MAX) fields via OPENJSON (no 4000-char JSON_VALUE limit)
    SELECT
        @v_note = note,
        @v_value_string = value_string,
        @v_bytes_b64 = value_bytes
    FROM OPENJSON(@json_data) WITH (
        note NVARCHAR(MAX) '$.note',
        value_string NVARCHAR(MAX) '$.value_string',
        value_bytes NVARCHAR(MAX) '$.value_bytes'
    );

    IF @scheme_id IS NULL
    BEGIN
        RAISERROR('sp_save_object_json: scheme_id is required', 16, 1);
        RETURN;
    END

    -- Verify scheme exists
    IF NOT EXISTS(SELECT 1 FROM [dbo].[_schemes] WHERE [_id] = @scheme_id)
    BEGIN
        RAISERROR('sp_save_object_json: scheme_id not found', 16, 1);
        RETURN;
    END

    -- Generate new ID for new objects (id = 0 or absent)
    IF @object_id IS NULL OR @object_id = 0
        SET @object_id = NEXT VALUE FOR [dbo].[global_identity];

    -- Check if object already exists
    SET @exists = CASE WHEN EXISTS(SELECT 1 FROM [dbo].[_objects] WHERE [_id] = @object_id) THEN 1 ELSE 0 END;

    IF @exists = 1
    BEGIN
        -- ===== UPDATE existing object =====
        -- _date_create is preserved (not updated)
        UPDATE [dbo].[_objects] SET
            [_id_scheme]       = @scheme_id,
            [_id_parent]       = CAST(JSON_VALUE(@json_data, '$.parent_id') AS BIGINT),
            [_id_owner]        = COALESCE(CAST(JSON_VALUE(@json_data, '$.owner_id') AS BIGINT), [_id_owner]),
            [_id_who_change]   = COALESCE(CAST(JSON_VALUE(@json_data, '$.who_change_id') AS BIGINT), [_id_who_change]),
            [_name]            = JSON_VALUE(@json_data, '$.name'),
            [_note]            = @v_note,
            [_key]             = CAST(JSON_VALUE(@json_data, '$.key') AS BIGINT),
            [_hash]            = CAST(JSON_VALUE(@json_data, '$.hash') AS UNIQUEIDENTIFIER),
            [_date_modify]     = SYSDATETIMEOFFSET(),
            [_date_begin]      = CAST(JSON_VALUE(@json_data, '$.date_begin') AS DATETIMEOFFSET),
            [_date_complete]   = CAST(JSON_VALUE(@json_data, '$.date_complete') AS DATETIMEOFFSET),
            [_value_long]      = CAST(JSON_VALUE(@json_data, '$.value_long') AS BIGINT),
            [_value_string]    = @v_value_string,
            [_value_guid]      = CAST(JSON_VALUE(@json_data, '$.value_guid') AS UNIQUEIDENTIFIER),
            [_value_bool]      = CASE WHEN JSON_VALUE(@json_data, '$.value_bool') = 'true' THEN 1
                                      WHEN JSON_VALUE(@json_data, '$.value_bool') = 'false' THEN 0
                                      ELSE NULL END,
            [_value_double]    = CAST(JSON_VALUE(@json_data, '$.value_double') AS FLOAT),
            [_value_numeric]   = CAST(JSON_VALUE(@json_data, '$.value_numeric') AS DECIMAL(38,18)),
            [_value_datetime]  = CAST(JSON_VALUE(@json_data, '$.value_datetime') AS DATETIMEOFFSET),
            [_value_bytes]     = CASE WHEN @v_bytes_b64 IS NOT NULL
                                      THEN CAST(N'' AS XML).value('xs:base64Binary(sql:variable("@v_bytes_b64"))', 'VARBINARY(MAX)')
                                      ELSE NULL END
        WHERE [_id] = @object_id;

        -- DeleteInsert strategy: remove all old values before inserting new
        DELETE FROM [dbo].[_values] WHERE [_id_object] = @object_id;
    END
    ELSE
    BEGIN
        -- ===== INSERT new object =====
        INSERT INTO [dbo].[_objects] (
            [_id], [_id_scheme], [_id_parent], [_id_owner], [_id_who_change],
            [_name], [_note], [_key], [_hash],
            [_date_create], [_date_modify], [_date_begin], [_date_complete],
            [_value_long], [_value_string], [_value_guid], [_value_bool],
            [_value_double], [_value_numeric], [_value_datetime], [_value_bytes]
        ) VALUES (
            @object_id,
            @scheme_id,
            CAST(JSON_VALUE(@json_data, '$.parent_id') AS BIGINT),
            COALESCE(CAST(JSON_VALUE(@json_data, '$.owner_id') AS BIGINT), 1),
            COALESCE(CAST(JSON_VALUE(@json_data, '$.who_change_id') AS BIGINT), 1),
            JSON_VALUE(@json_data, '$.name'),
            @v_note,
            CAST(JSON_VALUE(@json_data, '$.key') AS BIGINT),
            CAST(JSON_VALUE(@json_data, '$.hash') AS UNIQUEIDENTIFIER),
            COALESCE(CAST(JSON_VALUE(@json_data, '$.date_create') AS DATETIMEOFFSET), SYSDATETIMEOFFSET()),
            SYSDATETIMEOFFSET(),
            CAST(JSON_VALUE(@json_data, '$.date_begin') AS DATETIMEOFFSET),
            CAST(JSON_VALUE(@json_data, '$.date_complete') AS DATETIMEOFFSET),
            CAST(JSON_VALUE(@json_data, '$.value_long') AS BIGINT),
            @v_value_string,
            CAST(JSON_VALUE(@json_data, '$.value_guid') AS UNIQUEIDENTIFIER),
            CASE WHEN JSON_VALUE(@json_data, '$.value_bool') = 'true' THEN 1
                 WHEN JSON_VALUE(@json_data, '$.value_bool') = 'false' THEN 0
                 ELSE NULL END,
            CAST(JSON_VALUE(@json_data, '$.value_double') AS FLOAT),
            CAST(JSON_VALUE(@json_data, '$.value_numeric') AS DECIMAL(38,18)),
            CAST(JSON_VALUE(@json_data, '$.value_datetime') AS DATETIMEOFFSET),
            CASE WHEN @v_bytes_b64 IS NOT NULL
                 THEN CAST(N'' AS XML).value('xs:base64Binary(sql:variable("@v_bytes_b64"))', 'VARBINARY(MAX)')
                 ELSE NULL END
        );
    END

    -- Save properties if present
    SET @properties_json = JSON_QUERY(@json_data, '$.properties');
    IF @properties_json IS NOT NULL
    BEGIN
        EXEC [dbo].[sp_save_hierarchical_properties]
            @object_id,
            NULL,            -- root level (no parent structure)
            @scheme_id,
            @properties_json,
            NULL;            -- root level (no parent value)
    END

    SET @result_id = @object_id;
END
GO


-- ============================================================
-- USAGE EXAMPLES
-- ============================================================
--
-- Save/update object from JSON:
--   DECLARE @id BIGINT;
--   EXEC sp_save_object_json @json_data = N'{"id":123,"scheme_id":1,...,"properties":{...}}', @result_id = @id OUTPUT;
--   SELECT @id;
--
-- Insert new object (id=0 or absent -> auto-generate):
--   DECLARE @id BIGINT;
--   EXEC sp_save_object_json @json_data = N'{"id":0,"scheme_id":1,...}', @result_id = @id OUTPUT;
--   SELECT @id;
-- ============================================================

PRINT '========================================='
PRINT 'Save JSON object procedures created!'
