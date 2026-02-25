DROP VIEW IF EXISTS v_objects_json;
DROP FUNCTION IF EXISTS get_object_json;
DROP FUNCTION IF EXISTS build_listitem_jsonb;
-- Drop old signatures with jsonb parameter (before optimization to _values[])
DROP FUNCTION IF EXISTS build_hierarchical_properties_optimized(bigint, bigint, bigint, jsonb, integer, integer, bigint);
DROP FUNCTION IF EXISTS build_hierarchical_properties_optimized(bigint, bigint, bigint, jsonb, integer, text, bigint);
-- Drop new signature with _values[] array (if exists)
DROP FUNCTION IF EXISTS build_hierarchical_properties_optimized(bigint, bigint, bigint, _values[], integer, text, bigint);

-- ===== HELPER: Build ListItem JSON (DRY - used in multiple places) =====
CREATE OR REPLACE FUNCTION build_listitem_jsonb(
    listitem_id bigint,
    max_depth integer DEFAULT 10
) RETURNS jsonb
LANGUAGE 'plpgsql'
COST 10
STABLE
AS $BODY$
BEGIN
    IF listitem_id IS NULL THEN
        RETURN NULL;
    END IF;
    
    RETURN (SELECT jsonb_build_object(
        'id', li._id,
        'idList', li._id_list,
        'value', li._value,
        'alias', li._alias,
        'object', CASE 
            WHEN li._id_object IS NOT NULL THEN
                get_object_json(li._id_object, GREATEST(0, max_depth - 1))
            ELSE NULL 
        END
    )
    FROM _list_items li
    WHERE li._id = listitem_id);
END;
$BODY$;

-- ===== OPTIMIZED FUNCTIONS =====

-- Optimized function for building hierarchical properties with preloaded values array
-- 🚀 OPTIMIZATION: Uses _values[] array instead of jsonb - all data in memory, no repeated table queries
CREATE OR REPLACE FUNCTION build_hierarchical_properties_optimized(
    object_id bigint,
    parent_structure_id bigint,
    object_scheme_id bigint,
    all_values _values[],  -- 🚀 Array of _values records instead of jsonb
    max_depth integer DEFAULT 10,
    array_index text DEFAULT NULL, -- Text to support Dictionary string keys
    parent_value_id bigint DEFAULT NULL -- ID of parent element for nested arrays
) RETURNS jsonb
LANGUAGE 'plpgsql'
COST 100
VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    result_json jsonb := '{}'::jsonb;
    structure_record RECORD;
    current_value_record _values;  -- 🚀 Typed record instead of jsonb
    field_value jsonb;
    base_array_value_id bigint; -- ID of base array record for recursion
BEGIN
    -- Protection against infinite recursion for Class fields (hierarchical structures)
    -- IMPORTANT: This function is for Class fields (Address.Street, Contacts[].Email)
    -- max_depth is NOT checked here - Class fields are always loaded completely!
    -- max_depth is controlled only in get_object_json() for Object references (_RObject)
    IF max_depth < -100 THEN
        -- Protection against anomalous recursion (practically impossible)
        RETURN jsonb_build_object('error', 'Max recursion depth reached for hierarchical fields');
    END IF;
    
    -- 🔥 AUTOMATIC CACHE CHECK AND POPULATION
    IF NOT EXISTS(SELECT 1 FROM _scheme_metadata_cache WHERE _scheme_id = object_scheme_id LIMIT 1) THEN
        -- Cache is empty for this scheme → populate automatically
        PERFORM sync_metadata_cache_for_scheme(object_scheme_id);
        -- Auto-population without NOTICE (use warmup_all_metadata_caches() for explicit warmup)
    END IF;
    
    -- Collect all structures for given parent_structure_id (NO JOIN with _values!)
    -- 🚀 OPTIMIZATION: Use _scheme_metadata_cache instead of JOIN _structures ← _types
    FOR structure_record IN
        SELECT 
            c._structure_id as structure_id,
            c._name as field_name,
            c._collection_type as collection_type,  -- NULL = scalar, Array ID = array, Dictionary ID = dictionary
            c._collection_type = -9223372036854775668 as _is_array,  -- Array type ID
            c._collection_type = -9223372036854775667 as _is_dictionary,  -- Dictionary type ID
            c.type_name,
            c.db_type,
            c.type_semantic
        FROM _scheme_metadata_cache c
        WHERE c._scheme_id = object_scheme_id
          AND ((parent_structure_id IS NULL AND c._parent_structure_id IS NULL) 
               OR (parent_structure_id IS NOT NULL AND c._parent_structure_id = parent_structure_id))
        ORDER BY c._order, c._structure_id
    LOOP
        -- 🚀 OPTIMIZATION: Search value from preloaded array using unnest()
        IF parent_value_id IS NOT NULL THEN
            -- For nested fields inside array/dictionary elements - find by _array_parent_id
            SELECT v.* INTO current_value_record
            FROM unnest(all_values) AS v
            WHERE v._id_structure = structure_record.structure_id
              AND v._array_parent_id = parent_value_id
            LIMIT 1;
        ELSIF array_index IS NOT NULL THEN
            -- For array/dictionary elements - find value with specific array_index
            SELECT v.* INTO current_value_record
            FROM unnest(all_values) AS v
            WHERE v._id_structure = structure_record.structure_id
              AND v._array_index = array_index
            LIMIT 1;
        ELSE
            -- For regular fields or root fields - find first match by structure_id
            SELECT v.* INTO current_value_record
            FROM unnest(all_values) AS v
            WHERE v._id_structure = structure_record.structure_id
              AND v._array_index IS NULL
            LIMIT 1;
        END IF;
        
        -- ✅ Get ID of base array record for recursion
        base_array_value_id := NULL; -- Reset before each field
        
        IF structure_record._is_array = true OR structure_record._is_dictionary = true THEN
            -- Find base array/dictionary record from preloaded data
            IF parent_value_id IS NULL THEN
                -- Root array/dictionary: _array_parent_id must be NULL
                SELECT v._id INTO base_array_value_id
                FROM unnest(all_values) AS v
                WHERE v._id_structure = structure_record.structure_id
                  AND v._array_index IS NULL
                  AND v._array_parent_id IS NULL
                LIMIT 1;
            ELSE
                -- Nested array/dictionary: _array_parent_id must match parent_value_id
                SELECT v._id INTO base_array_value_id
                FROM unnest(all_values) AS v
                WHERE v._id_structure = structure_record.structure_id
                  AND v._array_index IS NULL
                  AND v._array_parent_id = parent_value_id
                LIMIT 1;
            END IF;
        END IF;
        
        -- Determine field value based on its type and preloaded data
        field_value := CASE 
            -- If this is an array - process relationally through _array_index
            WHEN structure_record._is_array = true THEN
                CASE 
                    -- Array of Class fields - build from relational data recursively
                    WHEN structure_record.type_semantic = 'Object' THEN
                        (
                            WITH array_elements AS (
                                -- Find all array elements with their indices from preloaded data
                                SELECT 
                                    v._array_index,
                                    -- Safe numeric sorting: only for numeric indices (Array), text keys (Dictionary) sort as 0
                                    CASE WHEN v._array_index ~ '^[0-9]+$' THEN v._array_index::int ELSE 0 END as array_index_int,
                                    v._id as element_value_id,
                                    v._array_parent_id,
                                    build_hierarchical_properties_optimized(
                                        object_id, 
                                        structure_record.structure_id, 
                                        object_scheme_id, 
                                        all_values,  -- 🚀 Pass array, not jsonb
                                        max_depth,
                                        v._array_index,
                                        v._id
                                    ) as element_json
                                FROM unnest(all_values) AS v  -- 🚀 From memory array
                                WHERE v._id_structure = structure_record.structure_id
                                  AND v._array_index IS NOT NULL
                                  AND (base_array_value_id IS NULL OR v._array_parent_id = base_array_value_id)
                                ORDER BY array_index_int, v._array_index  -- numeric first, then text
                            )
                            SELECT CASE 
                                WHEN COUNT(*) = 0 THEN '[]'::jsonb  -- Empty array = []
                                ELSE jsonb_agg(element_json ORDER BY array_index_int)
                            END
                            FROM array_elements
                        )
                    -- Arrays of primitive types (String, Long, Boolean, etc.) - relationally
                    ELSE
                        (
                            SELECT CASE 
                                WHEN COUNT(*) = 0 THEN '[]'::jsonb  -- Empty array = []
                                ELSE jsonb_agg(
                                CASE 
                                    -- Object references (_RObject) - check by type_semantic
                                    WHEN structure_record.type_semantic = '_RObject' AND v._Object IS NOT NULL THEN
                                        get_object_json(v._Object, max_depth - 1)
                                    WHEN structure_record.db_type = 'String' THEN to_jsonb(v._String)
                                    WHEN structure_record.db_type = 'Long' THEN 
                                        -- If _ListItem is filled, process as ListItem (for backward compatibility)
                                        CASE 
                                            WHEN v._ListItem IS NOT NULL THEN
                                                build_listitem_jsonb(v._ListItem, max_depth)
                                            ELSE to_jsonb(v._Long)
                                        END
                                    WHEN structure_record.db_type = 'Guid' THEN to_jsonb(v._Guid)
                                    WHEN structure_record.db_type = 'Double' THEN to_jsonb(v._Double)
                                    WHEN structure_record.db_type = 'Numeric' THEN to_jsonb(v._Numeric)
                                    WHEN structure_record.db_type = 'DateTimeOffset' THEN to_jsonb(v._DateTimeOffset)
                                    WHEN structure_record.db_type = 'Boolean' THEN to_jsonb(v._Boolean)
                                    WHEN structure_record.db_type = 'ListItem' THEN
                                        build_listitem_jsonb(v._ListItem, max_depth)
                                    WHEN structure_record.db_type = 'ByteArray' THEN 
                                        to_jsonb(encode(decode(v._ByteArray::text, 'base64'), 'base64'))
                                    ELSE NULL
                                -- Safe sorting: numeric for Array, text for Dictionary
                                END ORDER BY CASE WHEN v._array_index ~ '^[0-9]+$' THEN v._array_index::int ELSE 0 END, v._array_index
                                )
                            END
                            FROM unnest(all_values) AS v  -- 🚀 From memory array
                            WHERE v._id_structure = structure_record.structure_id
                              AND v._array_index IS NOT NULL
                              AND (base_array_value_id IS NULL OR v._array_parent_id = base_array_value_id)
                        )
                END
            
            -- Dictionary<K,V> fields - return JSON object with keys
            WHEN structure_record._is_dictionary = true THEN
                CASE 
                    -- Dictionary of RedbObject references (_RObject)
                    WHEN structure_record.type_semantic = '_RObject' THEN
                        (
                            SELECT CASE 
                                WHEN COUNT(*) = 0 THEN '{}'::jsonb  -- Empty dictionary = {}
                                ELSE jsonb_object_agg(
                                    v._array_index,  -- Key as JSON key
                                    CASE 
                                        WHEN v._Object IS NOT NULL THEN get_object_json(v._Object, max_depth - 1)
                                        ELSE NULL
                                    END
                                )
                            END
                            FROM unnest(all_values) AS v
                            WHERE v._id_structure = structure_record.structure_id
                              AND v._array_index IS NOT NULL
                              AND (base_array_value_id IS NULL OR v._array_parent_id = base_array_value_id)
                        )
                    -- Dictionary of Class fields
                    WHEN structure_record.type_semantic = 'Object' THEN
                        (
                            SELECT CASE 
                                WHEN COUNT(*) = 0 THEN '{}'::jsonb  -- Empty dictionary = {}
                                ELSE jsonb_object_agg(
                                    v._array_index,  -- Key as JSON key
                                    build_hierarchical_properties_optimized(
                                        object_id, 
                                        structure_record.structure_id, 
                                        object_scheme_id, 
                                        all_values,
                                        max_depth,
                                        NULL,  -- array_index = NULL for nested Class fields!
                                        v._id  -- parent_value_id = element record ID
                                    )
                                )
                            END
                            FROM unnest(all_values) AS v
                            WHERE v._id_structure = structure_record.structure_id
                              AND v._array_index IS NOT NULL
                              AND (base_array_value_id IS NULL OR v._array_parent_id = base_array_value_id)
                        )
                    -- Dictionary of primitive types
                    ELSE
                        (
                            SELECT CASE 
                                WHEN COUNT(*) = 0 THEN '{}'::jsonb  -- Empty dictionary = {}
                                ELSE jsonb_object_agg(
                                    v._array_index,  -- Key as JSON key
                                    CASE 
                                        WHEN structure_record.db_type = 'String' THEN to_jsonb(v._String)
                                        WHEN structure_record.db_type = 'Long' THEN to_jsonb(v._Long)
                                        WHEN structure_record.db_type = 'Guid' THEN to_jsonb(v._Guid)
                                        WHEN structure_record.db_type = 'Double' THEN to_jsonb(v._Double)
                                        WHEN structure_record.db_type = 'Numeric' THEN to_jsonb(v._Numeric)
                                        WHEN structure_record.db_type = 'DateTimeOffset' THEN to_jsonb(v._DateTimeOffset)
                                        WHEN structure_record.db_type = 'Boolean' THEN to_jsonb(v._Boolean)
                                        ELSE NULL
                                    END
                                )
                            END
                            FROM unnest(all_values) AS v
                            WHERE v._id_structure = structure_record.structure_id
                              AND v._array_index IS NOT NULL
                              AND (base_array_value_id IS NULL OR v._array_parent_id = base_array_value_id)
                        )
                END
            
            -- Regular fields (not arrays or dictionaries)
            -- Object reference to another object
            WHEN structure_record.type_name = 'Object' AND structure_record.type_semantic = '_RObject' THEN
                CASE 
                    WHEN current_value_record._Object IS NOT NULL THEN 
                        get_object_json(current_value_record._Object, max_depth - 1)
                    ELSE NULL
                END
            
            -- Class field with hierarchical child fields  
            WHEN structure_record.type_semantic = 'Object' THEN
                CASE 
                    WHEN current_value_record._Guid IS NULL THEN 
                        NULL  -- Class field is truly NULL - don't build object
                    ELSE
                        build_hierarchical_properties_optimized(
                            object_id, 
                            structure_record.structure_id, 
                            object_scheme_id, 
                            all_values,  -- 🚀 Pass array, not jsonb
                            max_depth,  -- Don't decrease max_depth for Class fields!
                            NULL,  -- array_index = NULL for nested Class fields
                            current_value_record._id  -- IMPORTANT: pass ID of current Class field record!
                        )
                END
                
            -- Primitive types - direct access to typed record fields (no JSON parsing!)
            -- Check _id IS NOT NULL to verify record was found (prevents jsonb null instead of SQL NULL)
            WHEN structure_record.db_type = 'String' THEN 
                CASE WHEN current_value_record._id IS NOT NULL THEN to_jsonb(current_value_record._String) ELSE NULL END
            WHEN structure_record.db_type = 'Long' THEN 
                -- If _ListItem is filled, process as ListItem (for backward compatibility)
                CASE 
                    WHEN current_value_record._ListItem IS NOT NULL THEN 
                        -- This is ListItem saved in old schema with db_type=Long
                        build_listitem_jsonb(current_value_record._ListItem, max_depth)
                    WHEN current_value_record._id IS NOT NULL THEN 
                        to_jsonb(current_value_record._Long)
                    ELSE NULL 
                END
            WHEN structure_record.db_type = 'Guid' THEN 
                CASE WHEN current_value_record._id IS NOT NULL THEN to_jsonb(current_value_record._Guid) ELSE NULL END
            WHEN structure_record.db_type = 'Double' THEN 
                CASE WHEN current_value_record._id IS NOT NULL THEN to_jsonb(current_value_record._Double) ELSE NULL END
            WHEN structure_record.db_type = 'Numeric' THEN 
                CASE WHEN current_value_record._id IS NOT NULL THEN to_jsonb(current_value_record._Numeric) ELSE NULL END
            WHEN structure_record.db_type = 'DateTimeOffset' THEN 
                CASE WHEN current_value_record._id IS NOT NULL THEN to_jsonb(current_value_record._DateTimeOffset) ELSE NULL END
            WHEN structure_record.db_type = 'Boolean' THEN 
                CASE WHEN current_value_record._id IS NOT NULL THEN to_jsonb(current_value_record._Boolean) ELSE NULL END
            WHEN structure_record.db_type = 'ListItem' OR current_value_record._ListItem IS NOT NULL THEN 
                -- Process as ListItem if db_type=ListItem OR if _ListItem is filled (backward compatibility)
                CASE 
                    WHEN current_value_record._ListItem IS NOT NULL THEN 
                        build_listitem_jsonb(current_value_record._ListItem, max_depth)
                    ELSE NULL
                END
            WHEN structure_record.db_type = 'ByteArray' THEN 
                CASE 
                    WHEN current_value_record._ByteArray IS NOT NULL THEN 
                        to_jsonb(encode(decode(current_value_record._ByteArray::text, 'base64'), 'base64'))
                    ELSE NULL
                END
            ELSE NULL
        END;
        -- Add field to result only if value is not NULL
        IF field_value IS NOT NULL THEN
            result_json := result_json || jsonb_build_object(structure_record.field_name, field_value);
        END IF;
        
    END LOOP;
    
    RETURN result_json;
END;
$BODY$;

-- OPTIMIZED function for getting object in JSON format with preloaded values array
-- 🚀 OPTIMIZATION: Loads all _values into typed array - no JSON parsing overhead
CREATE OR REPLACE FUNCTION get_object_json(
    object_id bigint,
    max_depth integer DEFAULT 10
) RETURNS jsonb
LANGUAGE 'plpgsql'
COST 100
VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    result_json jsonb;
    object_exists boolean;
    base_info jsonb;
    properties_info jsonb;
    object_scheme_id bigint;
    all_values _values[];  -- 🚀 Typed array instead of jsonb
BEGIN
    -- Check if object exists - return NULL if not found
    SELECT EXISTS(SELECT 1 FROM _objects WHERE _id = object_id) INTO object_exists;
    
    IF NOT object_exists THEN
        RETURN NULL;
    END IF;
    
    -- Check recursion depth
    IF max_depth <= 0 THEN
        -- max_depth = 0: return ONLY base fields WITHOUT properties
        SELECT jsonb_build_object(
            'id', o._id,
            'name', o._name,
            'scheme_id', o._id_scheme,
            'scheme_name', sc._name,
            'parent_id', o._id_parent,
            'owner_id', o._id_owner,
            'who_change_id', o._id_who_change,
            'date_create', o._date_create,
            'date_modify', o._date_modify,
            'date_begin', o._date_begin,
            'date_complete', o._date_complete,
            'key', o._key,
            'value_long', o._value_long,
            'value_string', o._value_string,
            'value_guid', o._value_guid,
            'note', o._note,
            'value_bool', o._value_bool,
            'value_double', o._value_double,
            'value_numeric', o._value_numeric,
            'value_datetime', o._value_datetime,
            'value_bytes', o._value_bytes,
            'hash', o._hash
        ) INTO result_json
        FROM _objects o
        JOIN _schemes sc ON sc._id = o._id_scheme
        WHERE o._id = object_id;
        
        RETURN result_json;
    END IF;
    
    -- Collect base object info + get scheme_id
    SELECT jsonb_build_object(
        'id', o._id,
        'name', o._name,
        'scheme_id', o._id_scheme,
        'scheme_name', sc._name,
        'parent_id', o._id_parent,
        'owner_id', o._id_owner,
        'who_change_id', o._id_who_change,
        'date_create', o._date_create,
        'date_modify', o._date_modify,
        'date_begin', o._date_begin,
        'date_complete', o._date_complete,
        'key', o._key,
        'value_long', o._value_long,
        'value_string', o._value_string,
        'value_guid', o._value_guid,
        'note', o._note,
        'value_bool', o._value_bool,
        'value_double', o._value_double,
        'value_numeric', o._value_numeric,
        'value_datetime', o._value_datetime,
        'value_bytes', o._value_bytes,
        'hash', o._hash
    ), o._id_scheme
    INTO base_info, object_scheme_id
    FROM _objects o
    JOIN _schemes sc ON sc._id = o._id_scheme
    WHERE o._id = object_id;
    
    -- 🚀 OPTIMIZATION: Load ALL values into typed array - single query, no JSON overhead
    SELECT array_agg(v) INTO all_values
    FROM _values v
    WHERE v._id_object = object_id;
    
    -- Use optimized function with preloaded values array
    SELECT build_hierarchical_properties_optimized(
        object_id, 
        NULL, 
        object_scheme_id, 
        COALESCE(all_values, ARRAY[]::_values[]),  -- 🚀 Pass typed array
        max_depth,
        NULL, -- array_index = NULL for root fields
        NULL  -- parent_value_id = NULL for root level
    ) INTO properties_info;
    
    -- Combine base info with properties
    result_json := base_info || jsonb_build_object('properties', COALESCE(properties_info, '{}'::jsonb));
    
    RETURN result_json;
END;
$BODY$;

-- BULK-OPTIMIZED VIEW for batch object retrieval in JSON format  
-- CREATE OR REPLACE VIEW v_objects_json AS
-- WITH 
-- -- Stage 1: BULK load values (optimal - GROUP BY only by ID)
-- all_values AS (
--     SELECT 
--         o._id,
--         COALESCE(
--             jsonb_object_agg(
--                 v._id_structure::text, 
--                 jsonb_build_object(
--                     '_String', v._String,
--                     '_Long', v._Long,
--                     '_Guid', v._Guid,
--                     '_Double', v._Double,
--                     '_DateTimeOffset', v._DateTimeOffset,
--                     '_Boolean', v._Boolean,
--                     '_ByteArray', v._ByteArray,
--                     '_array_parent_id', v._array_parent_id,
--                     '_array_index', v._array_index
--                 )
--             ) FILTER (WHERE v._id IS NOT NULL),
--             '{}'::jsonb
--         ) as all_values_json
--     FROM _objects o
--     LEFT JOIN _values v ON v._id_object = o._id
--     GROUP BY o._id  -- GROUP BY only by ID (fast!)
-- ),
-- -- Stage 2: Join with _objects fields and build JSON
-- objects_with_json AS (
--     SELECT 
--         o.*,  -- All _objects fields with single asterisk (efficient)
--         -- Full object JSON with properties
--         jsonb_build_object(
--             'id', o._id,
--             'name', o._name,
--             'scheme_id', o._id_scheme,
--             'scheme_name', s._name,
--             'parent_id', o._id_parent,
--             'owner_id', o._id_owner,
--             'who_change_id', o._id_who_change,
--             'date_create', o._date_create,
--             'date_modify', o._date_modify,
--             'date_begin', o._date_begin,
--             'date_complete', o._date_complete,
--             'key', o._key,
--             'value_long', o._value_long,
--             'value_string', o._value_string,
--             'value_guid', o._value_guid,
--             'note', o._note,
--             'value_bool', o._value_bool,
--             'hash', o._hash,
--             'properties', 
--             build_hierarchical_properties_optimized(
--                 o._id, 
--                 NULL, 
--                 o._id_scheme, 
--                 av.all_values_json,  -- Use preloaded data
--                 10,
--                 NULL -- array_index = NULL for root fields
--             )
--         ) as object_json
--     FROM _objects o
--     JOIN _schemes s ON s._id = o._id_scheme  
--     JOIN all_values av ON av._id = o._id  -- JOIN with preloaded values
-- )
-- SELECT * FROM objects_with_json ORDER BY _id;

-- -- Comments for OPTIMIZED functions and VIEWs for object retrieval
-- COMMENT ON VIEW v_objects_json IS 'MAXIMALLY OPTIMIZED VIEW for object retrieval. Two-stage architecture: 1) BULK _values aggregation with GROUP BY only by _id (fast!) 2) JOIN ready data with _objects via o.* (efficient). Returns ALL original _objects fields as columns PLUS full JSON with properties. Avoids heavy GROUP BY on 17 fields. Perfect for integration and API. Supports hierarchical Class fields.';

COMMENT ON FUNCTION build_hierarchical_properties_optimized(bigint, bigint, bigint, _values[], integer, text, bigint) IS 'Optimized function for recursive building of hierarchical JSON structure with preloaded _values[] array.
OPTIMIZATION: Uses typed _values[] array instead of jsonb - all data in memory, no repeated table queries!
IMPORTANT: max_depth is NOT decreased for Class fields - they are always loaded completely as part of object structure.
max_depth controls ONLY the depth of Object references (_RObject) in get_object_json().
Supports:
Relational arrays of Class fields
Nested arrays (arrays inside array elements) via array_index and parent_value_id
NO JOIN with _values in loop - uses unnest() from memory array!
5-10x faster for objects with arrays due to zero table queries.';

COMMENT ON FUNCTION get_object_json(bigint, integer) IS 'OPTIMIZED function for getting object in JSON format with SMART recursion depth:
max_depth = 0: only base fields WITHOUT properties (fast)
max_depth >= 1: base fields + properties
OPTIMIZATION: Loads ALL _values into typed array - single query, no JSON parsing overhead!
IMPORTANT: max_depth controls depth of Object references (_RObject):
  - Object references are called with max_depth-1
  - Class fields (Address, Contacts) are ALWAYS loaded COMPLETELY (max_depth not decreased)
  - ListItem._id_object also called with max_depth-1 (like regular Object references)
Supports:
Hierarchical Class fields (Address.Street, Contacts[].Email) - always fully
Object references (_RObject) - controlled depth via max_depth
ListItem with _id_object (base fields of linked object)
Relational arrays of all types
Optimal for objects with 10+ fields and arrays.';

-- ===== SIMPLE VIEW FOR OBJECTS WITH JSON =====

-- Drop existing view if exists
DROP VIEW IF EXISTS v_objects_json;

-- COMMENTED OUT: v_objects_json is inefficient for bulk operations (calls get_object_json for each row)
-- For LoadAsync direct SELECT from _objects + LoadPropsForManyAsync (LAZY) or get_object_json batch via unnest (EAGER) is used

-- -- Simple view: all _objects fields + JSON via get_object_json
-- CREATE VIEW v_objects_json AS
-- SELECT 
--     o.*,  -- All _objects fields as is
--     get_object_json(o._id, 10) as object_json  -- JSON representation of object
-- FROM _objects o;
-- COMMENT ON VIEW v_objects_json IS 'Simple view for object retrieval: all _objects fields + full JSON via get_object_json. Convenient for viewing and debugging.';
