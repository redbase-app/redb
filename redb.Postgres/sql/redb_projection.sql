-- ============================================================
-- PROJECTION FUNCTIONS: Optimized loading of only required fields
-- ============================================================
-- Result format:
--   - Class field → flat with paths: "Contact.Name": "John"
--   - Arrays → flat with indexes: "Items[0].Price": 100
--   - _RObject → NESTED object: "Author": { "Name": "Pushkin", ... }
-- ============================================================

-- ===== FUNCTION 1: Building a flat projection =====

DROP FUNCTION IF EXISTS build_flat_projection(bigint, bigint, jsonb, jsonb, integer, text, integer, bigint) CASCADE;

CREATE OR REPLACE FUNCTION build_flat_projection(
    p_object_id bigint,
    p_scheme_id bigint,
    p_projection_paths jsonb,
    p_all_values_json jsonb,
    p_max_depth integer DEFAULT 10,
    p_path_prefix text DEFAULT '',
    p_array_index integer DEFAULT NULL,
    p_parent_value_id bigint DEFAULT NULL
) RETURNS jsonb
LANGUAGE 'plpgsql'
COST 100
VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    result_json jsonb := '{}'::jsonb;
    proj_item jsonb;
    v_path text;
    v_structure_id bigint;
    v_is_array boolean;
    v_is_dictionary boolean;
    v_collection_type_name text;
    v_db_type text;
    v_type_semantic text;
    v_current_value jsonb;
    v_field_value jsonb;
    v_ref_object_id bigint;
    v_ref_scheme_id bigint;
    v_ref_values_json jsonb;
    v_ref_projection jsonb;
    v_array_element RECORD;
    v_base_array_value_id bigint;
    v_full_path text;
BEGIN
    IF p_max_depth < -100 THEN
        RETURN jsonb_build_object('_error', 'Max depth exceeded');
    END IF;
    
    FOR proj_item IN SELECT * FROM jsonb_array_elements(p_projection_paths)
    LOOP
        v_path := proj_item->>'path';
        v_structure_id := (proj_item->>'structure_id')::bigint;
        
        IF v_structure_id IS NULL THEN CONTINUE; END IF;
        
        -- Get collection info: is_array = true for both Array and Dictionary
        -- v_collection_type_name used to distinguish them ('Array' or 'Dictionary')
        SELECT 
            _collection_type IS NOT NULL,
            db_type, 
            type_semantic,
            CASE 
                WHEN _collection_type IS NOT NULL THEN 
                    (SELECT t._name FROM _types t WHERE t._id = _collection_type)
                ELSE NULL 
            END,
            -- ⭐ If path is not provided, take from metadata
            COALESCE(v_path, _name)
        INTO v_is_array, v_db_type, v_type_semantic, v_collection_type_name, v_path
        FROM _scheme_metadata_cache
        WHERE _scheme_id = p_scheme_id AND _structure_id = v_structure_id;
        
        IF NOT FOUND THEN CONTINUE; END IF;
        
        -- Determine if this is a Dictionary (string keys) vs Array (numeric keys)
        v_is_dictionary := (v_collection_type_name = 'Dictionary');
        
        v_full_path := CASE WHEN p_path_prefix = '' THEN v_path ELSE p_path_prefix || v_path END;
        
        IF p_parent_value_id IS NOT NULL THEN
            -- ⭐ Child field of Dictionary/Array: search by _array_parent_id
            SELECT jsonb_build_object(
                '_String', v._String, '_Long', v._Long, '_Guid', v._Guid,
                '_Double', v._Double, '_Numeric', v._Numeric,
                '_DateTimeOffset', v._DateTimeOffset, '_Boolean', v._Boolean,
                '_ByteArray', v._ByteArray, '_ListItem', v._ListItem, '_Object', v._Object
            ) INTO v_current_value
            FROM _values v
            WHERE v._id_object = p_object_id AND v._id_structure = v_structure_id
              AND v._array_parent_id = p_parent_value_id
            LIMIT 1;
        ELSIF p_array_index IS NOT NULL THEN
            -- ⭐ Dictionary/Array element: search by _array_index
            SELECT jsonb_build_object(
                '_String', v._String, '_Long', v._Long, '_Guid', v._Guid,
                '_Double', v._Double, '_Numeric', v._Numeric,
                '_DateTimeOffset', v._DateTimeOffset, '_Boolean', v._Boolean,
                '_ByteArray', v._ByteArray, '_ListItem', v._ListItem, '_Object', v._Object
            ) INTO v_current_value
            FROM _values v
            WHERE v._id_object = p_object_id AND v._id_structure = v_structure_id
              AND v._array_index = p_array_index::text
            LIMIT 1;
        ELSE
            -- ⭐ Regular field: take from cache
            v_current_value := p_all_values_json->v_structure_id::text;
        END IF;
        
        -- _RObject → NESTED object (reference to another RedbObject)
        IF v_type_semantic = '_RObject' THEN
            IF p_max_depth > 0 AND v_current_value IS NOT NULL THEN
                v_ref_object_id := (v_current_value->>'_Object')::bigint;
                IF v_ref_object_id IS NOT NULL THEN
                    SELECT _id_scheme INTO v_ref_scheme_id FROM _objects WHERE _id = v_ref_object_id;
                    
                    SELECT jsonb_object_agg(v._id_structure::text, jsonb_build_object(
                        '_String', v._String, '_Long', v._Long, '_Guid', v._Guid,
                        '_Double', v._Double, '_Numeric', v._Numeric,
                        '_DateTimeOffset', v._DateTimeOffset, '_Boolean', v._Boolean,
                        '_ByteArray', v._ByteArray, '_ListItem', v._ListItem, '_Object', v._Object
                    )) INTO v_ref_values_json
                    FROM _values v WHERE v._id_object = v_ref_object_id AND v._array_index IS NULL;
                    
                    v_ref_projection := proj_item->'nested';
                    IF v_ref_projection IS NOT NULL AND jsonb_array_length(v_ref_projection) > 0 THEN
                        v_field_value := build_flat_projection(
                            v_ref_object_id, v_ref_scheme_id, v_ref_projection,
                            COALESCE(v_ref_values_json, '{}'::jsonb), p_max_depth - 1, v_path, NULL, NULL
                        );
                    ELSE
                        v_field_value := jsonb_build_object('_id', v_ref_object_id);
                    END IF;
                    result_json := result_json || jsonb_build_object(v_path, v_field_value);
                END IF;
            ELSIF v_current_value IS NOT NULL AND (v_current_value->>'_Object') IS NOT NULL THEN
                result_json := result_json || jsonb_build_object(v_path, jsonb_build_object('_id', (v_current_value->>'_Object')::bigint));
            END IF;
        
        -- Arrays and Dictionaries (both have _array_index) — MUST BE BEFORE Object!
        -- Dictionary<K,V> can have type_semantic='Object' if V is a class
        ELSIF v_is_array THEN
            DECLARE
                v_dict_key text := proj_item->>'dict_key';  -- ⭐ Specific Dictionary key
            BEGIN
                v_base_array_value_id := NULL;
                IF p_parent_value_id IS NULL THEN
                    SELECT v._id INTO v_base_array_value_id FROM _values v
                    WHERE v._id_object = p_object_id AND v._id_structure = v_structure_id
                      AND v._array_index IS NULL AND v._array_parent_id IS NULL LIMIT 1;
                ELSE
                    SELECT v._id INTO v_base_array_value_id FROM _values v
                    WHERE v._id_object = p_object_id AND v._id_structure = v_structure_id
                      AND v._array_index IS NULL AND v._array_parent_id = p_parent_value_id LIMIT 1;
                END IF;
                
                FOR v_array_element IN
                    SELECT v._array_index, v._id,
                        CASE 
                            WHEN v_db_type = 'String' THEN to_jsonb(v._String)
                            WHEN v_db_type = 'Long' THEN to_jsonb(v._Long)
                            WHEN v_db_type = 'Guid' THEN to_jsonb(v._Guid)
                            WHEN v_db_type = 'Double' THEN to_jsonb(v._Double)
                            WHEN v_db_type = 'Numeric' THEN to_jsonb(v._Numeric)
                            WHEN v_db_type = 'DateTimeOffset' THEN to_jsonb(v._DateTimeOffset)
                            WHEN v_db_type = 'Boolean' THEN to_jsonb(v._Boolean)
                            ELSE NULL
                        END as value
                    FROM _values v
                    WHERE v._id_object = p_object_id AND v._id_structure = v_structure_id
                      AND v._array_index IS NOT NULL
                      AND (v_base_array_value_id IS NULL OR v._array_parent_id = v_base_array_value_id)
                      -- ⭐ Filter by specific Dictionary key (if provided)
                      AND (v_dict_key IS NULL OR v._array_index = v_dict_key)
                    ORDER BY 
                        CASE WHEN v_is_dictionary THEN v._array_index 
                             ELSE lpad(v._array_index, 10, '0') -- numeric sort for arrays
                        END
                LOOP
                -- Dictionary: path[key] or Array: path[index]
                v_full_path := v_path || '[' || v_array_element._array_index || ']';
                
                IF v_type_semantic = 'Object' THEN
                    v_ref_projection := proj_item->'nested';
                    IF v_ref_projection IS NOT NULL AND jsonb_array_length(v_ref_projection) > 0 THEN
                        -- ⭐ For Dictionary<K,Class> return hierarchical JSON: {"AddressBook": {"home": {...}}}
                        -- Use build_hierarchical_properties_optimized to build full nested object
                        DECLARE
                            v_all_values _values[];
                            v_nested_json jsonb;
                            v_existing_dict jsonb;
                        BEGIN
                            -- Load all object values for recursive function
                            SELECT array_agg(v) INTO v_all_values
                            FROM _values v WHERE v._id_object = p_object_id;
                            
                            -- Build full JSON for Dictionary element
                            v_nested_json := build_hierarchical_properties_optimized(
                                p_object_id,
                                v_structure_id,  -- parent = AddressBook structure
                                p_scheme_id,
                                COALESCE(v_all_values, ARRAY[]::_values[]),
                                p_max_depth - 1,
                                v_array_element._array_index,  -- array_index = 'home'
                                v_array_element._id  -- parent_value_id
                            );
                            
                            -- Add to hierarchical structure: {"AddressBook": {"home": {...}}}
                            v_existing_dict := result_json->v_path;
                            IF v_existing_dict IS NULL THEN
                                v_existing_dict := '{}'::jsonb;
                            END IF;
                            v_existing_dict := v_existing_dict || jsonb_build_object(v_array_element._array_index, v_nested_json);
                            result_json := result_json || jsonb_build_object(v_path, v_existing_dict);
                        END;
                    END IF;
                ELSIF v_array_element.value IS NOT NULL THEN
                    -- ⭐ For Dictionary return hierarchical JSON: {"PhoneBook": {"home": "..."}}
                    IF v_is_dictionary THEN
                        DECLARE
                            v_existing_dict jsonb;
                        BEGIN
                            v_existing_dict := result_json->v_path;
                            IF v_existing_dict IS NULL THEN
                                v_existing_dict := '{}'::jsonb;
                            END IF;
                            v_existing_dict := v_existing_dict || jsonb_build_object(v_array_element._array_index, v_array_element.value);
                            result_json := result_json || jsonb_build_object(v_path, v_existing_dict);
                        END;
                    ELSE
                        -- For Array — flat format: "Items[0]": value
                        result_json := result_json || jsonb_build_object(v_full_path, v_array_element.value);
                    END IF;
                END IF;
            END LOOP;
            END;  -- ⭐ Closing DECLARE block for v_dict_key
        
        -- ⭐ Object (Class) → flat with paths: Address1.City, Address1.Street
        -- NOT array/dictionary, but has nested fields
        ELSIF v_type_semantic = 'Object' THEN
            v_ref_projection := proj_item->'nested';
            IF v_ref_projection IS NOT NULL AND jsonb_array_length(v_ref_projection) > 0 THEN
                -- Has nested — recursively process child fields
                v_field_value := build_flat_projection(
                    p_object_id, p_scheme_id, v_ref_projection, p_all_values_json,
                    p_max_depth - 1, v_full_path || '.', NULL, NULL
                );
                result_json := result_json || v_field_value;
            END IF;
            
        -- Simple fields → flat with path
        ELSE
            v_field_value := CASE 
                WHEN v_db_type = 'String' AND v_current_value IS NOT NULL THEN to_jsonb(v_current_value->>'_String')
                WHEN v_db_type = 'Long' AND v_current_value IS NOT NULL THEN to_jsonb((v_current_value->>'_Long')::bigint)
                WHEN v_db_type = 'Guid' AND v_current_value IS NOT NULL THEN to_jsonb((v_current_value->>'_Guid')::uuid)
                WHEN v_db_type = 'Double' AND v_current_value IS NOT NULL THEN to_jsonb((v_current_value->>'_Double')::double precision)
                WHEN v_db_type = 'Numeric' AND v_current_value IS NOT NULL THEN to_jsonb((v_current_value->>'_Numeric')::numeric)
                WHEN v_db_type = 'DateTimeOffset' AND v_current_value IS NOT NULL THEN to_jsonb((v_current_value->>'_DateTimeOffset')::timestamptz)
                WHEN v_db_type = 'Boolean' AND v_current_value IS NOT NULL THEN to_jsonb((v_current_value->>'_Boolean')::boolean)
                WHEN v_db_type = 'ListItem' AND v_current_value IS NOT NULL AND (v_current_value->>'_ListItem')::bigint IS NOT NULL THEN
                    (SELECT jsonb_build_object('id', li._id, 'value', li._value, 'alias', li._alias)
                     FROM _list_items li WHERE li._id = (v_current_value->>'_ListItem')::bigint)
                ELSE NULL
            END;
            IF v_field_value IS NOT NULL THEN
                result_json := result_json || jsonb_build_object(v_full_path, v_field_value);
            END IF;
        END IF;
    END LOOP;
    
    RETURN result_json;
END;
$BODY$;

COMMENT ON FUNCTION build_flat_projection IS 'Flat projection: Class→path, array→index, _RObject→nested';


-- ===== FUNCTION 2: Search with projection =====

DROP FUNCTION IF EXISTS search_objects_with_projection(bigint, jsonb, jsonb, integer, integer, jsonb, integer) CASCADE;

CREATE OR REPLACE FUNCTION search_objects_with_projection(
    p_scheme_id bigint,
    p_filter_json jsonb DEFAULT NULL,
    p_projection_paths jsonb DEFAULT '[]'::jsonb,
    p_limit integer DEFAULT NULL,
    p_offset integer DEFAULT 0,
    p_order_by jsonb DEFAULT NULL,
    p_max_depth integer DEFAULT 10
) RETURNS jsonb
LANGUAGE 'plpgsql'
COST 500
VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    v_structure_ids bigint[];
    v_base_conditions text;
    v_hierarchical_conditions text;
    v_order_conditions text;
    v_final_where text;
    v_objects_result jsonb;
    v_total_count integer;
    v_query text;
BEGIN
    -- Extract structure_ids from projection_paths
    WITH RECURSIVE all_paths AS (
        SELECT (elem->>'structure_id')::bigint as sid FROM jsonb_array_elements(p_projection_paths) elem
        WHERE elem->>'structure_id' IS NOT NULL
        UNION ALL
        SELECT (nested_elem->>'structure_id')::bigint
        FROM jsonb_array_elements(p_projection_paths) elem, jsonb_array_elements(elem->'nested') nested_elem
        WHERE elem->'nested' IS NOT NULL AND nested_elem->>'structure_id' IS NOT NULL
    )
    SELECT ARRAY_AGG(DISTINCT sid) INTO v_structure_ids FROM all_paths WHERE sid IS NOT NULL;
    
    -- Filter conditions
    v_base_conditions := _build_single_facet_condition(p_filter_json, p_scheme_id, 'obj', 10);
    v_hierarchical_conditions := build_hierarchical_conditions(p_filter_json, 'obj');
    v_order_conditions := build_order_conditions(p_order_by, 'obj');
    v_final_where := format('WHERE obj._id_scheme = %s%s%s', p_scheme_id, COALESCE(v_base_conditions, ''), COALESCE(v_hierarchical_conditions, ''));
    
    EXECUTE format('SELECT COUNT(*) FROM _objects obj %s', v_final_where) INTO v_total_count;
    
    v_query := format('
        WITH filtered_objects AS (
            SELECT obj._id FROM _objects obj %s %s %s
        ),
        projected_values AS (
            SELECT v._id_object, jsonb_object_agg(v._id_structure::text, jsonb_build_object(
                ''_String'', v._String, ''_Long'', v._Long, ''_Guid'', v._Guid,
                ''_Double'', v._Double, ''_Numeric'', v._Numeric, ''_DateTimeOffset'', v._DateTimeOffset,
                ''_Boolean'', v._Boolean, ''_ByteArray'', v._ByteArray, ''_ListItem'', v._ListItem, ''_Object'', v._Object
            )) as values_json
            FROM _values v
            WHERE v._id_object IN (SELECT _id FROM filtered_objects) AND v._id_structure = ANY($1) AND v._array_index IS NULL
            GROUP BY v._id_object
        )
        SELECT jsonb_agg(jsonb_build_object(
            ''_id'', o._id, ''_name'', o._name, ''_scheme_id'', o._id_scheme, ''_hash'', o._hash,
            ''properties'', build_flat_projection(o._id, o._id_scheme, $2, COALESCE(pv.values_json, ''{}''::jsonb), %s, '''', NULL, NULL)
        ))
        FROM filtered_objects fo JOIN _objects o ON o._id = fo._id LEFT JOIN projected_values pv ON pv._id_object = o._id',
        v_final_where, v_order_conditions,
        CASE WHEN p_limit IS NULL OR p_limit >= 2000000000 THEN '' ELSE format('LIMIT %s OFFSET %s', p_limit, p_offset) END,
        p_max_depth
    );
    
    EXECUTE v_query INTO v_objects_result USING v_structure_ids, p_projection_paths;
    
    RETURN jsonb_build_object('objects', COALESCE(v_objects_result, '[]'::jsonb), 'total_count', v_total_count, 'limit', p_limit, 'offset', p_offset);
END;
$BODY$;

COMMENT ON FUNCTION search_objects_with_projection IS 'Search with flat projection. Loads only required structure_ids.';


-- ===== FUNCTION 3: Get ONE object with projection =====

DROP FUNCTION IF EXISTS get_object_with_projection(bigint, jsonb, integer) CASCADE;

CREATE OR REPLACE FUNCTION get_object_with_projection(
    p_object_id bigint,
    p_projection_paths jsonb,
    p_max_depth integer DEFAULT 10
) RETURNS jsonb
LANGUAGE 'plpgsql'
COST 100
VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    v_scheme_id bigint;
    v_values_json jsonb;
    v_structure_ids bigint[];
    v_result jsonb;
BEGIN
    -- Get object scheme_id
    SELECT _id_scheme INTO v_scheme_id FROM _objects WHERE _id = p_object_id;
    IF v_scheme_id IS NULL THEN
        RETURN jsonb_build_object('error', 'Object not found');
    END IF;
    
    -- Extract all structure_ids from projection_paths
    WITH RECURSIVE all_paths AS (
        SELECT (elem->>'structure_id')::bigint as sid FROM jsonb_array_elements(p_projection_paths) elem
        WHERE elem->>'structure_id' IS NOT NULL
        UNION ALL
        SELECT (nested_elem->>'structure_id')::bigint
        FROM jsonb_array_elements(p_projection_paths) elem, jsonb_array_elements(elem->'nested') nested_elem
        WHERE elem->'nested' IS NOT NULL AND nested_elem->>'structure_id' IS NOT NULL
    )
    SELECT ARRAY_AGG(DISTINCT sid) INTO v_structure_ids FROM all_paths WHERE sid IS NOT NULL;
    
    -- Load only required values
    SELECT jsonb_object_agg(v._id_structure::text, jsonb_build_object(
        '_String', v._String, '_Long', v._Long, '_Guid', v._Guid,
        '_Double', v._Double, '_Numeric', v._Numeric, '_DateTimeOffset', v._DateTimeOffset,
        '_Boolean', v._Boolean, '_ByteArray', v._ByteArray, '_ListItem', v._ListItem, '_Object', v._Object
    ))
    INTO v_values_json
    FROM _values v
    WHERE v._id_object = p_object_id 
      AND v._id_structure = ANY(v_structure_ids)
      AND v._array_index IS NULL;
    
    -- Build result
    SELECT jsonb_build_object(
        '_id', o._id,
        '_name', o._name,
        '_scheme_id', o._id_scheme,
        '_hash', o._hash,
        '_date_modify', o._date_modify,
        'properties', build_flat_projection(
            o._id, o._id_scheme, p_projection_paths,
            COALESCE(v_values_json, '{}'::jsonb), p_max_depth, '', NULL, NULL
        )
    )
    INTO v_result
    FROM _objects o WHERE o._id = p_object_id;
    
    RETURN v_result;
END;
$BODY$;

COMMENT ON FUNCTION get_object_with_projection IS 'Get one object with flat projection by ID.';


-- ===== FUNCTION 4: Search with projection by text paths (MAIN) =====
-- Accepts human-readable paths: ["Name", "AddressBook[home].City"]
-- SQL resolves them into structure_ids and builds nested projection

DROP FUNCTION IF EXISTS search_objects_with_projection_by_paths(bigint, jsonb, text[], integer, integer, jsonb, integer) CASCADE;

CREATE OR REPLACE FUNCTION search_objects_with_projection_by_paths(
    p_scheme_id bigint,
    p_filter_json jsonb DEFAULT NULL,
    p_field_paths text[] DEFAULT ARRAY[]::text[],
    p_limit integer DEFAULT NULL,
    p_offset integer DEFAULT 0,
    p_order_by jsonb DEFAULT NULL,
    p_max_depth integer DEFAULT 10
) RETURNS jsonb
LANGUAGE 'plpgsql'
COST 500
VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    v_projection_paths jsonb := '[]'::jsonb;
    v_path text;
    v_resolved record;
    v_nested_paths jsonb;
    v_parent_path text;
    v_parent_key text;
    v_child_field text;
    v_parent_entry jsonb;
    v_idx int;
    v_base_conditions text;
    v_hierarchical_conditions text;
    v_order_conditions text;
    v_final_where text;
    v_objects_result jsonb;
    v_total_count integer;
    v_query text;
    v_structure_ids bigint[] := ARRAY[]::bigint[];
BEGIN
    -- STEP 1: Parse text paths and build nested projection
    -- Input format: ["Name", "AddressBook[home].City", "AddressBook[home].Street"]
    -- Output format: [
    --   {"path": "Name", "structure_id": 123},
    --   {"path": "AddressBook", "structure_id": 456, "nested": [
    --     {"path": "City", "structure_id": 789},
    --     {"path": "Street", "structure_id": 790}
    --   ]}
    -- ]
    
    FOREACH v_path IN ARRAY p_field_paths
    LOOP
        -- Resolve path via existing function
        SELECT * INTO v_resolved FROM resolve_field_path(p_scheme_id, v_path);
        
        IF v_resolved.structure_id IS NULL THEN
            RAISE WARNING 'Field path "%" not found in scheme %, skipping', v_path, p_scheme_id;
            CONTINUE;
        END IF;
        
        v_structure_ids := array_append(v_structure_ids, v_resolved.structure_id);
        
        -- Check: is it a nested path with Dictionary key? (AddressBook[home].City)
        IF v_resolved.dict_key IS NOT NULL AND position('.' IN v_path) > 0 THEN
            -- Parse: "AddressBook[home].City" -> parent="AddressBook", key="home", child="City"
            v_parent_path := split_part(v_path, '[', 1);  -- "AddressBook"
            v_parent_key := v_resolved.dict_key;          -- "home"
            v_child_field := substring(v_path from '\]\.(.*)'::text);  -- "City" or "City.SubField"
            
            -- Find parent structure AddressBook
            DECLARE
                v_parent_structure_id bigint;
                v_parent_resolved record;
            BEGIN
                SELECT * INTO v_parent_resolved FROM resolve_field_path(p_scheme_id, v_parent_path);
                v_parent_structure_id := v_parent_resolved.structure_id;
                
                IF v_parent_structure_id IS NOT NULL THEN
                    -- Check if parent entry with same key already exists
                    v_idx := NULL;
                    FOR i IN 0..jsonb_array_length(v_projection_paths) - 1 LOOP
                        IF (v_projection_paths->i->>'structure_id')::bigint = v_parent_structure_id 
                           AND v_projection_paths->i->>'dict_key' = v_parent_key THEN
                            v_idx := i;
                            EXIT;
                        END IF;
                    END LOOP;
                    
                    IF v_idx IS NULL THEN
                        -- Create new entry for parent with nested and dict_key
                        v_projection_paths := v_projection_paths || jsonb_build_object(
                            'path', v_parent_path,
                            'structure_id', v_parent_structure_id,
                            'dict_key', v_parent_key,
                            'nested', jsonb_build_array(jsonb_build_object(
                                'path', split_part(v_child_field, '.', 1),
                                'structure_id', v_resolved.structure_id
                            ))
                        );
                        v_structure_ids := array_append(v_structure_ids, v_parent_structure_id);
                    ELSE
                        -- Add to existing nested
                        v_nested_paths := v_projection_paths->v_idx->'nested';
                        IF v_nested_paths IS NULL THEN
                            v_nested_paths := '[]'::jsonb;
                        END IF;
                        v_nested_paths := v_nested_paths || jsonb_build_object(
                            'path', split_part(v_child_field, '.', 1),
                            'structure_id', v_resolved.structure_id
                        );
                        v_projection_paths := jsonb_set(v_projection_paths, ARRAY[v_idx::text, 'nested'], v_nested_paths);
                    END IF;
                END IF;
            END;
        -- ⭐ NEW: Simple Dictionary key without nested fields (PhoneBook[home])
        ELSIF v_resolved.dict_key IS NOT NULL THEN
            v_parent_path := split_part(v_path, '[', 1);  -- "PhoneBook"
            v_parent_key := v_resolved.dict_key;          -- "home"
            
            -- Find the field structure itself PhoneBook
            DECLARE
                v_parent_structure_id bigint;
                v_parent_resolved record;
            BEGIN
                SELECT * INTO v_parent_resolved FROM resolve_field_path(p_scheme_id, v_parent_path);
                v_parent_structure_id := v_parent_resolved.structure_id;
                
                IF v_parent_structure_id IS NOT NULL THEN
                    -- Add entry for Dictionary with dict_key
                    v_projection_paths := v_projection_paths || jsonb_build_object(
                        'path', v_parent_path,
                        'structure_id', v_parent_structure_id,
                        'dict_key', v_parent_key
                    );
                    v_structure_ids := array_append(v_structure_ids, v_parent_structure_id);
                END IF;
            END;
        ELSE
            -- Check: is it a Class field (Object)? Automatically add nested for child fields
            DECLARE
                v_type_semantic text;
                v_child_nested jsonb := '[]'::jsonb;
                v_child record;
            BEGIN
                SELECT type_semantic INTO v_type_semantic
                FROM _scheme_metadata_cache
                WHERE _scheme_id = p_scheme_id AND _structure_id = v_resolved.structure_id;
                
                IF v_type_semantic = 'Object' THEN
                    -- ⭐ Class field: automatically add all child fields
                    FOR v_child IN
                        SELECT _structure_id, _name
                        FROM _scheme_metadata_cache
                        WHERE _scheme_id = p_scheme_id AND _parent_structure_id = v_resolved.structure_id
                    LOOP
                        v_child_nested := v_child_nested || jsonb_build_object(
                            'path', v_child._name,
                            'structure_id', v_child._structure_id
                        );
                        v_structure_ids := array_append(v_structure_ids, v_child._structure_id);
                    END LOOP;
                    
                    IF jsonb_array_length(v_child_nested) > 0 THEN
                        v_projection_paths := v_projection_paths || jsonb_build_object(
                            'path', v_path,
                            'structure_id', v_resolved.structure_id,
                            'nested', v_child_nested
                        );
                    ELSE
                        v_projection_paths := v_projection_paths || jsonb_build_object(
                            'path', v_path,
                            'structure_id', v_resolved.structure_id
                        );
                    END IF;
                ELSE
                    -- Simple path without nesting
                    v_projection_paths := v_projection_paths || jsonb_build_object(
                        'path', v_path,
                        'structure_id', v_resolved.structure_id
                    );
                END IF;
            END;
        END IF;
    END LOOP;
    
    -- If no paths — return empty result
    IF jsonb_array_length(v_projection_paths) = 0 THEN
        RETURN jsonb_build_object('objects', '[]'::jsonb, 'total_count', 0, 'limit', p_limit, 'offset', p_offset);
    END IF;
    
    -- STEP 2: Filter conditions
    v_base_conditions := _build_single_facet_condition(p_filter_json, p_scheme_id, 'obj', 10);
    v_hierarchical_conditions := build_hierarchical_conditions(p_filter_json, 'obj');
    v_order_conditions := build_order_conditions(p_order_by, 'obj');
    v_final_where := format('WHERE obj._id_scheme = %s%s%s', p_scheme_id, COALESCE(v_base_conditions, ''), COALESCE(v_hierarchical_conditions, ''));
    
    -- STEP 3: Calculate total_count
    EXECUTE format('SELECT COUNT(*) FROM _objects obj %s', v_final_where) INTO v_total_count;
    
    -- STEP 4: Main query
    v_query := format('
        WITH filtered_objects AS (
            SELECT obj._id FROM _objects obj %s %s %s
        ),
        projected_values AS (
            SELECT v._id_object, jsonb_object_agg(v._id_structure::text, jsonb_build_object(
                ''_String'', v._String, ''_Long'', v._Long, ''_Guid'', v._Guid,
                ''_Double'', v._Double, ''_Numeric'', v._Numeric, ''_DateTimeOffset'', v._DateTimeOffset,
                ''_Boolean'', v._Boolean, ''_ByteArray'', v._ByteArray, ''_ListItem'', v._ListItem, ''_Object'', v._Object
            )) as values_json
            FROM _values v
            WHERE v._id_object IN (SELECT _id FROM filtered_objects) 
              AND v._id_structure = ANY($1) 
              AND v._array_index IS NULL
            GROUP BY v._id_object
        )
        SELECT jsonb_agg(jsonb_build_object(
            ''id'', o._id, 
            ''name'', o._name, 
            ''scheme_id'', o._id_scheme, 
            ''scheme_name'', s._name,
            ''parent_id'', o._id_parent,
            ''owner_id'', o._id_owner,
            ''who_change_id'', o._id_who_change,
            ''date_create'', o._date_create,
            ''date_modify'', o._date_modify,
            ''date_begin'', o._date_begin,
            ''date_complete'', o._date_complete,
            ''key'', o._key,
            ''value_long'', o._value_long,
            ''value_string'', o._value_string,
            ''value_guid'', o._value_guid,
            ''note'', o._note,
            ''value_bool'', o._value_bool,
            ''value_double'', o._value_double,
            ''value_numeric'', o._value_numeric,
            ''value_datetime'', o._value_datetime,
            ''value_bytes'', o._value_bytes,
            ''hash'', o._hash,
            ''properties'', build_flat_projection(o._id, o._id_scheme, $2, COALESCE(pv.values_json, ''{}''::jsonb), %s, '''', NULL, NULL)
        ))
        FROM filtered_objects fo 
        JOIN _objects o ON o._id = fo._id 
        JOIN _schemes s ON s._id = o._id_scheme
        LEFT JOIN projected_values pv ON pv._id_object = o._id',
        v_final_where, 
        v_order_conditions,
        CASE WHEN p_limit IS NULL OR p_limit >= 2000000000 THEN '' ELSE format('LIMIT %s OFFSET %s', p_limit, p_offset) END,
        p_max_depth
    );
    
    EXECUTE v_query INTO v_objects_result USING v_structure_ids, v_projection_paths;
    
    RETURN jsonb_build_object(
        'objects', COALESCE(v_objects_result, '[]'::jsonb), 
        'total_count', v_total_count, 
        'limit', p_limit, 
        'offset', p_offset,
        'facets', '[]'::jsonb
    );
END;
$BODY$;

COMMENT ON FUNCTION search_objects_with_projection_by_paths IS 
'Search objects with projection by text paths (human-readable format).
Path examples:
  - "Name"                    simple field
  - "Address.City"            nested Class
  - "Items[].Price"           all array elements
  - "Items[0].Price"          first array element
  - "PhoneBook[home]"         specific Dictionary key
  - "AddressBook[home].City"  field inside Dictionary value

Usage:
  SELECT search_objects_with_projection_by_paths(
      4504439,                                           -- scheme_id
      ''{"Name": {"$ne": null}}''::jsonb,                -- filters
      ARRAY[''Name'', ''AddressBook[home].City''],       -- projection
      100, 0, NULL, 10
  );
';


-- ===== FUNCTION 5: Search with projection by array of structure_ids =====
-- Legacy version for backward compatibility with C#
-- Recommended to use search_objects_with_projection_by_paths

DROP FUNCTION IF EXISTS search_objects_with_projection_by_ids(bigint, jsonb, bigint[], integer, integer, jsonb, integer) CASCADE;

CREATE OR REPLACE FUNCTION search_objects_with_projection_by_ids(
    p_scheme_id bigint,
    p_filter_json jsonb DEFAULT NULL,
    p_structure_ids bigint[] DEFAULT ARRAY[]::bigint[],
    p_limit integer DEFAULT NULL,
    p_offset integer DEFAULT 0,
    p_order_by jsonb DEFAULT NULL,
    p_max_depth integer DEFAULT 10
) RETURNS jsonb
LANGUAGE 'plpgsql'
COST 500
VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    v_projection_paths jsonb;
    v_base_conditions text;
    v_hierarchical_conditions text;
    v_order_conditions text;
    v_final_where text;
    v_objects_result jsonb;
    v_total_count integer;
    v_query text;
BEGIN
    -- STEP 1: Build projection_paths from structure_ids using metadata
    -- Format: [{"path": "Article", "structure_id": 1011}, ...]
    SELECT jsonb_agg(
        jsonb_build_object(
            'path', _build_field_path(c._structure_id, p_scheme_id),
            'structure_id', c._structure_id
        )
    )
    INTO v_projection_paths
    FROM _scheme_metadata_cache c
    WHERE c._scheme_id = p_scheme_id
      AND c._structure_id = ANY(p_structure_ids);
    
    -- If no paths — return empty result
    IF v_projection_paths IS NULL OR jsonb_array_length(v_projection_paths) = 0 THEN
        v_projection_paths := '[]'::jsonb;
    END IF;
    
    -- STEP 2: Filter conditions
    v_base_conditions := _build_single_facet_condition(p_filter_json, p_scheme_id, 'obj', 10);
    v_hierarchical_conditions := build_hierarchical_conditions(p_filter_json, 'obj');
    v_order_conditions := build_order_conditions(p_order_by, 'obj');
    v_final_where := format('WHERE obj._id_scheme = %s%s%s', p_scheme_id, COALESCE(v_base_conditions, ''), COALESCE(v_hierarchical_conditions, ''));
    
    -- STEP 3: Calculate total_count
    EXECUTE format('SELECT COUNT(*) FROM _objects obj %s', v_final_where) INTO v_total_count;
    
    -- STEP 4: Main query
    v_query := format('
        WITH filtered_objects AS (
            SELECT obj._id FROM _objects obj %s %s %s
        ),
        projected_values AS (
            SELECT v._id_object, jsonb_object_agg(v._id_structure::text, jsonb_build_object(
                ''_String'', v._String, ''_Long'', v._Long, ''_Guid'', v._Guid,
                ''_Double'', v._Double, ''_Numeric'', v._Numeric, ''_DateTimeOffset'', v._DateTimeOffset,
                ''_Boolean'', v._Boolean, ''_ByteArray'', v._ByteArray, ''_ListItem'', v._ListItem, ''_Object'', v._Object
            )) as values_json
            FROM _values v
            WHERE v._id_object IN (SELECT _id FROM filtered_objects) 
              AND v._id_structure = ANY($1) 
              AND v._array_index IS NULL
            GROUP BY v._id_object
        )
        SELECT jsonb_agg(jsonb_build_object(
            ''id'', o._id, 
            ''name'', o._name, 
            ''scheme_id'', o._id_scheme, 
            ''scheme_name'', s._name,
            ''parent_id'', o._id_parent,
            ''owner_id'', o._id_owner,
            ''who_change_id'', o._id_who_change,
            ''date_create'', o._date_create,
            ''date_modify'', o._date_modify,
            ''key'', o._key,
            ''value_long'', o._value_long,
            ''value_string'', o._value_string,
            ''value_guid'', o._value_guid,
            ''note'', o._note,
            ''value_bool'', o._value_bool,
            ''value_double'', o._value_double,
            ''value_numeric'', o._value_numeric,
            ''value_datetime'', o._value_datetime,
            ''value_bytes'', o._value_bytes,
            ''hash'', o._hash,
            ''properties'', build_flat_projection(o._id, o._id_scheme, $2, COALESCE(pv.values_json, ''{}''::jsonb), %s, '''', NULL, NULL)
        ))
        FROM filtered_objects fo 
        JOIN _objects o ON o._id = fo._id 
        JOIN _schemes s ON s._id = o._id_scheme
        LEFT JOIN projected_values pv ON pv._id_object = o._id',
        v_final_where, 
        v_order_conditions,
        CASE WHEN p_limit IS NULL OR p_limit >= 2000000000 THEN '' ELSE format('LIMIT %s OFFSET %s', p_limit, p_offset) END,
        p_max_depth
    );
    
    EXECUTE v_query INTO v_objects_result USING p_structure_ids, v_projection_paths;
    
    RETURN jsonb_build_object(
        'objects', COALESCE(v_objects_result, '[]'::jsonb), 
        'total_count', v_total_count, 
        'limit', p_limit, 
        'offset', p_offset,
        'facets', '[]'::jsonb
    );
END;
$BODY$;

COMMENT ON FUNCTION search_objects_with_projection_by_ids IS 
'Simplified version of search_objects_with_projection for C#.
Accepts bigint[] structure_ids instead of JSONB paths.
SQL automatically builds paths from _scheme_metadata_cache.
Response format compatible with search_objects_with_facets.';


-- ===== HELPER FUNCTION: Build field path from structure_id =====

DROP FUNCTION IF EXISTS _build_field_path(bigint, bigint) CASCADE;

CREATE OR REPLACE FUNCTION _build_field_path(
    p_structure_id bigint,
    p_scheme_id bigint
) RETURNS text
LANGUAGE 'plpgsql'
COST 50
STABLE
AS $BODY$
DECLARE
    v_path text := '';
    v_current_id bigint := p_structure_id;
    v_name text;
    v_parent_id bigint;
BEGIN
    -- Recursively build path from structure_id to root
    LOOP
        SELECT _name, _parent_structure_id
        INTO v_name, v_parent_id
        FROM _scheme_metadata_cache
        WHERE _scheme_id = p_scheme_id AND _structure_id = v_current_id;
        
        IF NOT FOUND THEN
            EXIT;
        END IF;
        
        -- Add name to path
        IF v_path = '' THEN
            v_path := v_name;
        ELSE
            v_path := v_name || '.' || v_path;
        END IF;
        
        -- Move to parent
        IF v_parent_id IS NULL THEN
            EXIT;
        END IF;
        v_current_id := v_parent_id;
    END LOOP;
    
    RETURN v_path;
END;
$BODY$;

COMMENT ON FUNCTION _build_field_path IS 'Builds field path from structure_id (e.g., "Contact.Name")';


-- ============================================================
-- TEST EXAMPLES
-- ============================================================

/*
-- ===== STEP 0: Check that metadata cache is filled =====
SELECT * FROM warmup_all_metadata_caches();

-- ===== STEP 1: Find scheme_id and structure_ids for testing =====
-- View all schemes:
SELECT _id, _name FROM _schemes LIMIT 10;

-- View scheme structures (replace 1002 with your scheme):
SELECT 
    _structure_id, 
    _name, 
    _parent_structure_id,
    db_type, 
    type_semantic,
    _collection_type IS NOT NULL as is_array  -- _collection_type = Array/Dictionary/NULL
FROM _scheme_metadata_cache 
WHERE _scheme_id = 1002  -- your scheme
ORDER BY _parent_structure_id NULLS FIRST, _order;

-- ===== STEP 2: Simple test — one field =====
-- Replace structure_id with real one from your scheme
SELECT search_objects_with_projection(
    1002,                                           -- scheme_id
    NULL,                                           -- no filter
    '[{"path": "Age", "structure_id": 100}]'::jsonb, -- projection
    5,                                              -- limit
    0                                               -- offset
);

-- ===== STEP 3: Several simple fields =====
SELECT search_objects_with_projection(
    1002,
    NULL,
    '[
        {"path": "Age", "structure_id": 100},
        {"path": "Name", "structure_id": 101},
        {"path": "Stock", "structure_id": 102}
    ]'::jsonb,
    5, 0
);

-- ===== STEP 4: Class fields (nested in same object) =====
-- For example Contact — Class with fields Name, Email
-- Contact._structure_id = 200, Contact.Name._structure_id = 201
SELECT search_objects_with_projection(
    1002,
    NULL,
    '[
        {"path": "Age", "structure_id": 100},
        {"path": "Contact.Name", "structure_id": 201},
        {"path": "Contact.Email", "structure_id": 202}
    ]'::jsonb,
    5, 0
);
-- Expected result: {"Age": 30, "Contact.Name": "John", "Contact.Email": "j@mail.ru"}

-- ===== STEP 5: Simple type arrays =====
-- Scores — array of Long, structure_id = 300
SELECT search_objects_with_projection(
    1002,
    NULL,
    '[{"path": "Scores", "structure_id": 300}]'::jsonb,
    5, 0
);
-- Expected result: {"Scores[0]": 100, "Scores[1]": 85, "Scores[2]": 90}

-- ===== STEP 6: Class arrays (Items[].Price) =====
-- Items — array of Object, Items.Price — Long
SELECT search_objects_with_projection(
    1002,
    NULL,
    '[
        {"path": "Items", "structure_id": 400, "nested": [
            {"path": "Price", "structure_id": 401},
            {"path": "Name", "structure_id": 402}
        ]}
    ]'::jsonb,
    5, 0
);
-- Expected result: {"Items[0].Price": 100, "Items[0].Name": "Book", "Items[1].Price": 200, ...}

-- ===== STEP 7: _RObject (reference to another object) =====
-- Author — _RObject, references another object with its own scheme
SELECT search_objects_with_projection(
    1002,
    NULL,
    '[
        {"path": "Age", "structure_id": 100},
        {"path": "Author", "structure_id": 500, "nested": [
            {"path": "Name", "structure_id": 101},
            {"path": "Email", "structure_id": 102}
        ]}
    ]'::jsonb,
    5, 0
);
-- Expected result: {"Age": 30, "Author": {"Name": "Pushkin", "Email": "push@mail.ru"}}

-- ===== STEP 8: With filter =====
SELECT search_objects_with_projection(
    1002,
    '{"Age": {"$gt": 25}}'::jsonb,                  -- filter
    '[
        {"path": "Age", "structure_id": 100},
        {"path": "Name", "structure_id": 101}
    ]'::jsonb,
    10, 0
);

-- ===== STEP 9: Test build_flat_projection directly =====
-- First find an object:
SELECT _id, _id_scheme FROM _objects WHERE _id_scheme = 1002 LIMIT 1;

-- Then load its values:
WITH obj_values AS (
    SELECT jsonb_object_agg(
        v._id_structure::text,
        jsonb_build_object(
            '_String', v._String, '_Long', v._Long, '_Guid', v._Guid,
            '_Double', v._Double, '_Numeric', v._Numeric,
            '_DateTimeOffset', v._DateTimeOffset, '_Boolean', v._Boolean,
            '_ByteArray', v._ByteArray, '_ListItem', v._ListItem, '_Object', v._Object
        )
    ) as vals
    FROM _values v
    WHERE v._id_object = 12345  -- replace with real _id
      AND v._array_index IS NULL
)
SELECT build_flat_projection(
    12345,                                          -- object_id
    1002,                                           -- scheme_id
    '[{"path": "Age", "structure_id": 100}]'::jsonb,
    vals,
    10, '', NULL, NULL
) FROM obj_values;

-- ===== STEP 10: Performance comparison =====
-- Without projection (all fields):
EXPLAIN ANALYZE 
SELECT search_objects_with_facets_base(1002, NULL, 100, 0, NULL, 10);

-- With projection (only 2 fields):
EXPLAIN ANALYZE 
SELECT search_objects_with_projection(
    1002, NULL,
    '[{"path": "Age", "structure_id": 100}, {"path": "Name", "structure_id": 101}]'::jsonb,
    100, 0
);

-- ===== ERROR DIAGNOSTICS =====

-- Check that structure_id exists:
SELECT * FROM _scheme_metadata_cache 
WHERE _scheme_id = 1002 AND _structure_id = 100;

-- Check that data exists:
SELECT COUNT(*) FROM _values 
WHERE _id_structure = 100;

-- Check type_semantic for _RObject:
SELECT _structure_id, _name, type_semantic 
FROM _scheme_metadata_cache 
WHERE _scheme_id = 1002 AND type_semantic = '_RObject';
*/