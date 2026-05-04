-- ============================================================
-- SAVE OBJECT FROM JSON (Postgres)
-- Inverse of get_object_json: JSON → _objects + _values
-- Strategy: DeleteInsert (delete all _values, then insert new)
-- ============================================================
-- Input format: same JSON structure as produced by get_object_json()
--
-- Two functions (mirroring read architecture):
--   save_object_json(jsonb) → bigint           [mirror of get_object_json]
--   save_hierarchical_properties(...)           [mirror of build_hierarchical_properties_optimized]
--
-- Supported field types (all branches from build_hierarchical_properties_optimized):
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

-- Drop existing functions (handle signature changes on re-deploy)
DROP FUNCTION IF EXISTS save_object_json(jsonb);
DROP FUNCTION IF EXISTS save_hierarchical_properties(bigint, bigint, bigint, jsonb, bigint);


-- ============================================================
-- 1. RECURSIVE PROPERTY WRITER
--    Mirror of build_hierarchical_properties_optimized (reader)
--    Walks _scheme_metadata_cache, extracts values from JSON,
--    INSERTs into _values with correct column routing
-- ============================================================
CREATE OR REPLACE FUNCTION save_hierarchical_properties(
    p_object_id bigint,            -- owning object _id
    p_parent_structure_id bigint,  -- NULL for root level, structure _id for nested Class
    p_scheme_id bigint,            -- scheme _id (for metadata cache lookup)
    p_properties_json jsonb,       -- JSON object: {"Name":"Alice","Tags":[...],"Address":{...}}
    p_parent_value_id bigint DEFAULT NULL  -- NULL for root, parent _values._id for nested
) RETURNS void
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    structure_record RECORD;
    field_json jsonb;
    v_value_id bigint;
    v_head_id bigint;
    v_element_id bigint;
    v_element jsonb;
    v_nested_id bigint;
    v_idx integer;
    v_dict_entry RECORD;
BEGIN
    -- Ensure metadata cache is populated (mirrors read function)
    IF NOT EXISTS(SELECT 1 FROM _scheme_metadata_cache WHERE _scheme_id = p_scheme_id LIMIT 1) THEN
        PERFORM sync_metadata_cache_for_scheme(p_scheme_id);
    END IF;

    -- Walk structures for this level
    -- Same query & ordering as build_hierarchical_properties_optimized
    FOR structure_record IN
        SELECT
            c._structure_id as structure_id,
            c._name as field_name,
            c._collection_type as collection_type,
            c._collection_type = -9223372036854775668 as _is_array,      -- Array type ID
            c._collection_type = -9223372036854775667 as _is_dictionary,  -- Dictionary type ID
            c.type_name,
            c.db_type,
            c.type_semantic
        FROM _scheme_metadata_cache c
        WHERE c._scheme_id = p_scheme_id
          AND ((p_parent_structure_id IS NULL AND c._parent_structure_id IS NULL)
               OR (p_parent_structure_id IS NOT NULL AND c._parent_structure_id = p_parent_structure_id))
        ORDER BY c._order, c._structure_id
    LOOP
        -- Extract field value from JSON by structure name
        field_json := p_properties_json -> structure_record.field_name;

        -- Skip if field not present (SQL NULL) or JSON null
        IF field_json IS NULL OR field_json = 'null'::jsonb THEN
            CONTINUE;
        END IF;

        -- ================================================================
        -- DISPATCH BY TYPE
        -- Mirrors the CASE tree in build_hierarchical_properties_optimized
        -- ================================================================

        IF structure_record._is_array THEN
            -- ========================================================
            -- ARRAY FIELDS  (branches 1-3)
            -- ========================================================
            IF jsonb_typeof(field_json) != 'array' THEN
                CONTINUE;  -- type mismatch guard
            END IF;

            -- Insert head record: marker "this array property exists"
            -- Read side: base_array_value_id IS NULL → property is null (not [])
            v_head_id := nextval('global_identity');
            INSERT INTO _values (_id, _id_structure, _id_object, _array_index, _array_parent_id)
            VALUES (v_head_id, structure_record.structure_id, p_object_id, NULL, p_parent_value_id);

            -- Empty array [] → head record alone (read returns [])
            IF jsonb_array_length(field_json) = 0 THEN
                CONTINUE;
            END IF;

            -- ----- Branch 1: Array of Class fields -----
            -- Mirror: build_hierarchical_properties_optimized recursion per element
            IF structure_record.type_semantic = 'Object' THEN
                FOR v_idx IN 0..jsonb_array_length(field_json) - 1
                LOOP
                    v_element := field_json -> v_idx;
                    IF v_element IS NULL OR v_element = 'null'::jsonb THEN
                        CONTINUE;
                    END IF;

                    -- Insert element record (_array_index = index, _array_parent_id = head)
                    v_element_id := nextval('global_identity');
                    INSERT INTO _values (_id, _id_structure, _id_object, _array_index, _array_parent_id)
                    VALUES (v_element_id, structure_record.structure_id, p_object_id,
                            v_idx::text, v_head_id);

                    -- Recurse into child properties of this array element
                    IF jsonb_typeof(v_element) = 'object' THEN
                        PERFORM save_hierarchical_properties(
                            p_object_id,
                            structure_record.structure_id,  -- parent structure = array structure
                            p_scheme_id,
                            v_element,
                            v_element_id  -- children's _array_parent_id → this element
                        );
                    END IF;
                END LOOP;

            -- ----- Branch 2: Array of Object references (_RObject) -----
            -- Mirror: get_object_json(v._Object, max_depth - 1) per element
            ELSIF structure_record.type_semantic = '_RObject' THEN
                FOR v_idx IN 0..jsonb_array_length(field_json) - 1
                LOOP
                    v_element := field_json -> v_idx;

                    -- Resolve: full object JSON → recursive save; number → ID directly
                    IF jsonb_typeof(v_element) = 'object' THEN
                        v_nested_id := save_object_json(v_element);
                    ELSIF v_element IS NOT NULL AND v_element != 'null'::jsonb THEN
                        v_nested_id := (v_element #>> '{}')::bigint;
                    ELSE
                        v_nested_id := NULL;
                    END IF;

                    v_element_id := nextval('global_identity');
                    INSERT INTO _values (
                        _id, _id_structure, _id_object, _Object, _array_index, _array_parent_id
                    ) VALUES (
                        v_element_id, structure_record.structure_id, p_object_id,
                        v_nested_id, v_idx::text, v_head_id
                    );
                END LOOP;

            -- ----- Branch 3: Array of primitive types -----
            -- Mirror: to_jsonb(v._String) / to_jsonb(v._Long) / ... per element
            ELSE
                FOR v_idx IN 0..jsonb_array_length(field_json) - 1
                LOOP
                    v_element := field_json -> v_idx;
                    v_element_id := nextval('global_identity');

                    INSERT INTO _values (
                        _id, _id_structure, _id_object, _array_index, _array_parent_id,
                        _String, _Long, _Guid, _Double, _Numeric,
                        _DateTimeOffset, _Boolean, _ByteArray, _ListItem
                    ) VALUES (
                        v_element_id, structure_record.structure_id, p_object_id,
                        v_idx::text, v_head_id,
                        -- Route value to correct column based on db_type
                        CASE WHEN structure_record.db_type = 'String'
                             THEN v_element #>> '{}' END,
                        CASE WHEN structure_record.db_type = 'Long'
                                  AND jsonb_typeof(v_element) != 'object'
                             THEN (v_element #>> '{}')::bigint END,
                        CASE WHEN structure_record.db_type = 'Guid'
                             THEN (v_element #>> '{}')::uuid END,
                        CASE WHEN structure_record.db_type = 'Double'
                             THEN (v_element #>> '{}')::float END,
                        CASE WHEN structure_record.db_type = 'Numeric'
                             THEN (v_element #>> '{}')::numeric END,
                        CASE WHEN structure_record.db_type IN ('DateTimeOffset', 'DateTime')
                             THEN (v_element #>> '{}')::timestamptz END,
                        CASE WHEN structure_record.db_type = 'Boolean'
                             THEN (v_element #>> '{}')::boolean END,
                        CASE WHEN structure_record.db_type = 'ByteArray'
                             THEN decode(v_element #>> '{}', 'base64') END,
                        -- _ListItem: ListItem type OR Long backward-compat (Long field with ListItem JSON)
                        CASE WHEN structure_record.db_type = 'ListItem' THEN
                                 CASE WHEN jsonb_typeof(v_element) = 'object'
                                      THEN (v_element ->> 'id')::bigint
                                      ELSE (v_element #>> '{}')::bigint END
                             WHEN structure_record.db_type = 'Long'
                                  AND jsonb_typeof(v_element) = 'object'
                             THEN (v_element ->> 'id')::bigint
                        END
                    );
                END LOOP;
            END IF;


        ELSIF structure_record._is_dictionary THEN
            -- ========================================================
            -- DICTIONARY FIELDS  (branches 4-6)
            -- ========================================================
            IF jsonb_typeof(field_json) != 'object' THEN
                CONTINUE;  -- type mismatch guard
            END IF;

            -- Insert head record (marker: "this dictionary property exists")
            v_head_id := nextval('global_identity');
            INSERT INTO _values (_id, _id_structure, _id_object, _array_index, _array_parent_id)
            VALUES (v_head_id, structure_record.structure_id, p_object_id, NULL, p_parent_value_id);

            -- Empty dict {} → head record alone (read returns {}); jsonb_each yields zero rows

            -- ----- Branch 4: Dictionary of Class fields -----
            -- Mirror: build_hierarchical_properties_optimized recursion per entry
            IF structure_record.type_semantic = 'Object' THEN
                FOR v_dict_entry IN SELECT * FROM jsonb_each(field_json)
                LOOP
                    v_element_id := nextval('global_identity');
                    INSERT INTO _values (_id, _id_structure, _id_object, _array_index, _array_parent_id)
                    VALUES (v_element_id, structure_record.structure_id, p_object_id,
                            v_dict_entry.key, v_head_id);

                    IF jsonb_typeof(v_dict_entry.value) = 'object' THEN
                        PERFORM save_hierarchical_properties(
                            p_object_id,
                            structure_record.structure_id,
                            p_scheme_id,
                            v_dict_entry.value,
                            v_element_id
                        );
                    END IF;
                END LOOP;

            -- ----- Branch 5: Dictionary of Object references (_RObject) -----
            -- Mirror: get_object_json(v._Object, max_depth - 1) per entry
            ELSIF structure_record.type_semantic = '_RObject' THEN
                FOR v_dict_entry IN SELECT * FROM jsonb_each(field_json)
                LOOP
                    IF jsonb_typeof(v_dict_entry.value) = 'object' THEN
                        v_nested_id := save_object_json(v_dict_entry.value);
                    ELSIF v_dict_entry.value IS NOT NULL AND v_dict_entry.value != 'null'::jsonb THEN
                        v_nested_id := (v_dict_entry.value #>> '{}')::bigint;
                    ELSE
                        v_nested_id := NULL;
                    END IF;

                    v_element_id := nextval('global_identity');
                    INSERT INTO _values (
                        _id, _id_structure, _id_object, _Object, _array_index, _array_parent_id
                    ) VALUES (
                        v_element_id, structure_record.structure_id, p_object_id,
                        v_nested_id, v_dict_entry.key, v_head_id
                    );
                END LOOP;

            -- ----- Branch 6: Dictionary of primitive types -----
            -- Mirror: jsonb_object_agg(key, to_jsonb(v._String / v._Long / ...))
            ELSE
                FOR v_dict_entry IN SELECT * FROM jsonb_each(field_json)
                LOOP
                    v_element_id := nextval('global_identity');

                    INSERT INTO _values (
                        _id, _id_structure, _id_object, _array_index, _array_parent_id,
                        _String, _Long, _Guid, _Double, _Numeric,
                        _DateTimeOffset, _Boolean, _ByteArray, _ListItem
                    ) VALUES (
                        v_element_id, structure_record.structure_id, p_object_id,
                        v_dict_entry.key, v_head_id,
                        CASE WHEN structure_record.db_type = 'String'
                             THEN v_dict_entry.value #>> '{}' END,
                        CASE WHEN structure_record.db_type = 'Long'
                             THEN (v_dict_entry.value #>> '{}')::bigint END,
                        CASE WHEN structure_record.db_type = 'Guid'
                             THEN (v_dict_entry.value #>> '{}')::uuid END,
                        CASE WHEN structure_record.db_type = 'Double'
                             THEN (v_dict_entry.value #>> '{}')::float END,
                        CASE WHEN structure_record.db_type = 'Numeric'
                             THEN (v_dict_entry.value #>> '{}')::numeric END,
                        CASE WHEN structure_record.db_type IN ('DateTimeOffset', 'DateTime')
                             THEN (v_dict_entry.value #>> '{}')::timestamptz END,
                        CASE WHEN structure_record.db_type = 'Boolean'
                             THEN (v_dict_entry.value #>> '{}')::boolean END,
                        CASE WHEN structure_record.db_type = 'ByteArray'
                             THEN decode(v_dict_entry.value #>> '{}', 'base64') END,
                        CASE WHEN structure_record.db_type = 'ListItem' THEN
                                 CASE WHEN jsonb_typeof(v_dict_entry.value) = 'object'
                                      THEN (v_dict_entry.value ->> 'id')::bigint
                                      ELSE (v_dict_entry.value #>> '{}')::bigint END
                        END
                    );
                END LOOP;
            END IF;


        ELSIF structure_record.type_name = 'Object' AND structure_record.type_semantic = '_RObject' THEN
            -- ========================================================
            -- Branch 7: SINGLE OBJECT REFERENCE
            -- Mirror: get_object_json(current_value_record._Object, max_depth - 1)
            -- ========================================================
            IF jsonb_typeof(field_json) = 'object' THEN
                -- Full nested object JSON → save recursively, get ID
                v_nested_id := save_object_json(field_json);
            ELSE
                -- Numeric ID reference
                v_nested_id := (field_json #>> '{}')::bigint;
            END IF;

            v_value_id := nextval('global_identity');
            INSERT INTO _values (_id, _id_structure, _id_object, _Object, _array_parent_id)
            VALUES (v_value_id, structure_record.structure_id, p_object_id,
                    v_nested_id, p_parent_value_id);


        ELSIF structure_record.type_semantic = 'Object' THEN
            -- ========================================================
            -- Branch 8: CLASS FIELD (nested class with child properties)
            -- Mirror: WHEN _Guid IS NULL THEN NULL ELSE build_hierarchical_...
            -- ========================================================
            IF jsonb_typeof(field_json) != 'object' THEN
                CONTINUE;  -- class fields must be JSON objects
            END IF;

            -- Insert marker record with _Guid (read checks: _Guid IS NULL → class is null)
            v_value_id := nextval('global_identity');
            INSERT INTO _values (_id, _id_structure, _id_object, _Guid, _array_parent_id)
            VALUES (v_value_id, structure_record.structure_id, p_object_id,
                    gen_random_uuid(), p_parent_value_id);

            -- Recurse: save child properties under this class structure
            PERFORM save_hierarchical_properties(
                p_object_id,
                structure_record.structure_id,  -- children are under this structure in _scheme_metadata_cache
                p_scheme_id,
                field_json,
                v_value_id  -- children's _array_parent_id → this marker record
            );


        ELSE
            -- ========================================================
            -- Branches 9-10: PRIMITIVE SCALAR FIELDS + LISTITEM
            -- Mirror: to_jsonb(v._String) / to_jsonb(v._Long) / build_listitem_jsonb / ...
            -- ========================================================
            v_value_id := nextval('global_identity');

            -- Branch 9: ListItem (db_type='ListItem', or Long backward-compat with ListItem JSON)
            IF structure_record.db_type = 'ListItem'
               OR (structure_record.db_type = 'Long' AND jsonb_typeof(field_json) = 'object') THEN

                INSERT INTO _values (_id, _id_structure, _id_object, _ListItem, _array_parent_id)
                VALUES (v_value_id, structure_record.structure_id, p_object_id,
                        (field_json ->> 'id')::bigint, p_parent_value_id);

            -- Branch 10: Standard primitive types
            ELSE
                INSERT INTO _values (
                    _id, _id_structure, _id_object, _array_parent_id,
                    _String, _Long, _Guid, _Double, _Numeric,
                    _DateTimeOffset, _Boolean, _ByteArray
                ) VALUES (
                    v_value_id, structure_record.structure_id, p_object_id, p_parent_value_id,
                    CASE WHEN structure_record.db_type = 'String'
                         THEN field_json #>> '{}' END,
                    CASE WHEN structure_record.db_type = 'Long'
                         THEN (field_json #>> '{}')::bigint END,
                    CASE WHEN structure_record.db_type = 'Guid'
                         THEN (field_json #>> '{}')::uuid END,
                    CASE WHEN structure_record.db_type = 'Double'
                         THEN (field_json #>> '{}')::float END,
                    CASE WHEN structure_record.db_type = 'Numeric'
                         THEN (field_json #>> '{}')::numeric END,
                    CASE WHEN structure_record.db_type IN ('DateTimeOffset', 'DateTime')
                         THEN (field_json #>> '{}')::timestamptz END,
                    CASE WHEN structure_record.db_type = 'Boolean'
                         THEN (field_json #>> '{}')::boolean END,
                    CASE WHEN structure_record.db_type = 'ByteArray'
                         THEN decode(field_json #>> '{}', 'base64') END
                );
            END IF;

        END IF;

    END LOOP;
END;
$BODY$;


-- ============================================================
-- 2. MAIN ENTRY POINT
--    Mirror of get_object_json (reader)
--    JSON → UPSERT _objects → DELETE _values → save properties
-- ============================================================
CREATE OR REPLACE FUNCTION save_object_json(
    p_json_data jsonb
) RETURNS bigint
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    v_object_id bigint;
    v_scheme_id bigint;
    v_properties jsonb;
    v_exists boolean;
BEGIN
    -- Extract required fields
    v_object_id := (p_json_data ->> 'id')::bigint;
    v_scheme_id := (p_json_data ->> 'scheme_id')::bigint;

    IF v_scheme_id IS NULL THEN
        RAISE EXCEPTION 'save_object_json: scheme_id is required';
    END IF;

    -- Verify scheme exists
    IF NOT EXISTS(SELECT 1 FROM _schemes WHERE _id = v_scheme_id) THEN
        RAISE EXCEPTION 'save_object_json: scheme_id=% not found', v_scheme_id;
    END IF;

    -- Generate new ID for new objects (id = 0 or absent)
    IF v_object_id IS NULL OR v_object_id = 0 THEN
        v_object_id := nextval('global_identity');
    END IF;

    -- Check if object already exists
    SELECT EXISTS(SELECT 1 FROM _objects WHERE _id = v_object_id) INTO v_exists;

    IF v_exists THEN
        -- ===== UPDATE existing object =====
        -- Mirror: base fields from get_object_json output
        -- Note: _date_create is preserved (not updated)
        UPDATE _objects SET
            _id_scheme       = v_scheme_id,
            _id_parent       = (p_json_data ->> 'parent_id')::bigint,
            _id_owner        = COALESCE((p_json_data ->> 'owner_id')::bigint, _id_owner),
            _id_who_change   = COALESCE((p_json_data ->> 'who_change_id')::bigint, _id_who_change),
            _name            = p_json_data ->> 'name',
            _note            = p_json_data ->> 'note',
            _key             = (p_json_data ->> 'key')::bigint,
            _hash            = (p_json_data ->> 'hash')::uuid,
            _date_modify     = now(),
            _date_begin      = (p_json_data ->> 'date_begin')::timestamptz,
            _date_complete   = (p_json_data ->> 'date_complete')::timestamptz,
            _value_long      = (p_json_data ->> 'value_long')::bigint,
            _value_string    = p_json_data ->> 'value_string',
            _value_guid      = (p_json_data ->> 'value_guid')::uuid,
            _value_bool      = (p_json_data ->> 'value_bool')::boolean,
            _value_double    = (p_json_data ->> 'value_double')::float,
            _value_numeric   = (p_json_data ->> 'value_numeric')::numeric,
            _value_datetime  = (p_json_data ->> 'value_datetime')::timestamptz,
            _value_bytes     = decode(p_json_data ->> 'value_bytes', 'base64')
        WHERE _id = v_object_id;

        -- DeleteInsert strategy: remove all old values before inserting new
        DELETE FROM _values WHERE _id_object = v_object_id;

    ELSE
        -- ===== INSERT new object =====
        INSERT INTO _objects (
            _id, _id_scheme, _id_parent, _id_owner, _id_who_change,
            _name, _note, _key, _hash,
            _date_create, _date_modify, _date_begin, _date_complete,
            _value_long, _value_string, _value_guid, _value_bool,
            _value_double, _value_numeric, _value_datetime, _value_bytes
        ) VALUES (
            v_object_id,
            v_scheme_id,
            (p_json_data ->> 'parent_id')::bigint,
            COALESCE((p_json_data ->> 'owner_id')::bigint, 1),
            COALESCE((p_json_data ->> 'who_change_id')::bigint, 1),
            p_json_data ->> 'name',
            p_json_data ->> 'note',
            (p_json_data ->> 'key')::bigint,
            (p_json_data ->> 'hash')::uuid,
            COALESCE((p_json_data ->> 'date_create')::timestamptz, now()),
            now(),
            (p_json_data ->> 'date_begin')::timestamptz,
            (p_json_data ->> 'date_complete')::timestamptz,
            (p_json_data ->> 'value_long')::bigint,
            p_json_data ->> 'value_string',
            (p_json_data ->> 'value_guid')::uuid,
            (p_json_data ->> 'value_bool')::boolean,
            (p_json_data ->> 'value_double')::float,
            (p_json_data ->> 'value_numeric')::numeric,
            (p_json_data ->> 'value_datetime')::timestamptz,
            decode(p_json_data ->> 'value_bytes', 'base64')
        );
    END IF;

    -- Save properties if present (not null, not JSON null)
    v_properties := p_json_data -> 'properties';
    IF v_properties IS NOT NULL AND v_properties != 'null'::jsonb THEN
        PERFORM save_hierarchical_properties(
            v_object_id,
            NULL,           -- root level (no parent structure)
            v_scheme_id,
            v_properties,
            NULL            -- root level (no parent value)
        );
    END IF;

    RETURN v_object_id;
END;
$BODY$;


-- ============================================================
-- DOCUMENTATION
-- ============================================================
COMMENT ON FUNCTION save_hierarchical_properties(bigint, bigint, bigint, jsonb, bigint) IS
'Recursive property writer — inverse of build_hierarchical_properties_optimized.
Walks _scheme_metadata_cache for the given scheme, extracts values from the properties JSON,
and INSERTs into _values with correct column routing based on db_type.
Handles all 10 field type branches:
  Arrays (Class / _RObject / primitives) with head+element records
  Dictionaries (Class / _RObject / primitives) with head+keyed-element records
  Single Object references (_RObject) — recursive save_object_json for nested objects
  Class fields — marker record with _Guid + recursive child properties
  ListItem — extract id from JSON object
  Primitives — type-based column routing (String/_Long/_Guid/_Double/etc.)
Mutual recursion: _RObject fields call save_object_json for nested objects.';

COMMENT ON FUNCTION save_object_json(jsonb) IS
'Save object from JSON — inverse of get_object_json.
Strategy: DeleteInsert (UPDATE/INSERT _objects, DELETE all _values, INSERT new _values).
Input: JSON in exactly the same format as produced by get_object_json().
Supports:
  New objects (id=0 or absent): generates ID via nextval(global_identity)
  Existing objects (id>0): UPDATE base fields, DELETE+re-INSERT values
  All property types: scalars, arrays, dictionaries, Class fields, Object references
  Nested objects in _RObject fields: saved recursively via mutual recursion
Returns: object _id (bigint).';
