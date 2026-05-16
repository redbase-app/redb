-- =====================================================
-- REDB EAV AGGREGATION FUNCTIONS
-- Aggregations over EAV fields (_values)
-- Support: simple fields, nested Classes, arrays
-- =====================================================

-- Remove old functions
DROP FUNCTION IF EXISTS aggregate_field(bigint, text, text, jsonb);
DROP FUNCTION IF EXISTS resolve_field_path(bigint, text);

-- =====================================================
-- resolve_field_path: Finds structure_id by path
-- =====================================================
-- Supports:
--   "Price"                   - simple field
--   "Customer.Name"           - nested Class
--   "Items[].Price"           - array (ALL elements)
--   "Items[2].Price"          - array (SPECIFIC element with index 2)
--   "Contacts[].Address.City" - nested inside array
-- 
-- ⭐ USES _scheme_metadata_cache FOR SPEED!
-- 
-- Returns: structure_id, db_type, is_array, array_index (NULL = all, number = specific)
-- =====================================================
CREATE OR REPLACE FUNCTION resolve_field_path(
    p_scheme_id bigint,
    p_field_path text
)
RETURNS TABLE(structure_id bigint, db_type text, is_array boolean, array_index int, dict_key text, is_dictionary boolean)
LANGUAGE plpgsql
AS $BODY$
DECLARE
    v_segments text[];
    v_segment text;
    v_clean_segment text;
    v_current_parent_id bigint := NULL;
    v_structure_id bigint;
    v_db_type text;
    v_is_collection boolean := false;
    v_is_dictionary boolean := false;
    v_found_collection_type bigint;
    v_array_index int := NULL;
    v_dict_key text := NULL;
    v_index_match text[];
    v_key_match text[];
    v_collection_type_name text;
BEGIN
    -- ⭐ Extract array index if specified: Items[2] -> 2, Items[] -> NULL
    v_index_match := regexp_match(p_field_path, '\[(\d+)\]');
    IF v_index_match IS NOT NULL THEN
        v_array_index := v_index_match[1]::int;
    END IF;
    
    -- 🆕 Extract string Dictionary key: PhoneBook[home] -> 'home'
    v_key_match := regexp_match(p_field_path, '\[([A-Za-z_][A-Za-z0-9_-]*)\]');
    IF v_key_match IS NOT NULL THEN
        v_dict_key := v_key_match[1];
    END IF;
    
    -- Remove [] and [N] and [key] from path and split into segments
    -- "Items[].Price" -> ["Items", "Price"]
    -- "Items[2].Price" -> ["Items", "Price"]
    -- "PhoneBook[home]" -> ["PhoneBook"]
    v_segments := string_to_array(regexp_replace(p_field_path, '\[[^\]]*\]', '', 'g'), '.');
    
    -- Process each segment of the path
    FOREACH v_segment IN ARRAY v_segments
    LOOP
        v_clean_segment := trim(v_segment);
        IF v_clean_segment = '' THEN
            CONTINUE;
        END IF;
        
        -- ⭐ Search in _scheme_metadata_cache (fast!)
        SELECT c._structure_id, c.db_type, c._collection_type
        INTO v_structure_id, v_db_type, v_found_collection_type
        FROM _scheme_metadata_cache c
        WHERE c._scheme_id = p_scheme_id
          AND c._name = v_clean_segment
          AND (
              (v_current_parent_id IS NULL AND c._parent_structure_id IS NULL)
              OR c._parent_structure_id = v_current_parent_id
          )
        LIMIT 1;
        
        IF v_structure_id IS NULL THEN
            RAISE EXCEPTION 'Field segment "%" not found in path "%" (scheme=%). Check cache: SELECT * FROM warmup_all_metadata_caches();', 
                v_clean_segment, p_field_path, p_scheme_id;
        END IF;
        
        -- Check if it's a collection (array/dictionary)?
        IF v_found_collection_type IS NOT NULL THEN
            v_is_collection := true;
            -- Check collection type: Array or Dictionary
            SELECT t._name INTO v_collection_type_name 
            FROM _types t WHERE t._id = v_found_collection_type;
            IF v_collection_type_name = 'Dictionary' THEN
                v_is_dictionary := true;
            END IF;
        END IF;
        
        -- Move to the next level
        v_current_parent_id := v_structure_id;
    END LOOP;
    
    -- Return result
    structure_id := v_structure_id;
    db_type := v_db_type;
    is_array := v_is_collection OR (p_field_path ~ '\[[^\]]*\]');
    array_index := v_array_index;  -- NULL = all elements, number = specific index
    dict_key := v_dict_key;        -- 🆕 NULL = all keys, string = specific key
    is_dictionary := v_is_dictionary;
    RETURN NEXT;
END;
$BODY$;

-- =====================================================
-- aggregate_field: Aggregation over EAV field
-- =====================================================
-- Parameters:
--   p_scheme_id - Scheme ID
--   p_field_path - field path:
--                  "Price"             - simple field
--                  "Customer.Name"     - nested Class
--                  "Items[].Price"     - array (ALL elements)
--                  "Items[2].Price"    - array (SPECIFIC element)
--                  "PhoneBook[home]"   - Dictionary (SPECIFIC key)
--   p_function - SUM, AVG, MIN, MAX, COUNT
--   p_filter_json - JSON filter or null
-- 
-- Returns: numeric aggregation result
-- =====================================================
CREATE OR REPLACE FUNCTION aggregate_field(
    p_scheme_id bigint,
    p_field_path text,
    p_function text,
    p_filter_json jsonb DEFAULT NULL
)
RETURNS numeric
LANGUAGE plpgsql
AS $BODY$
DECLARE
    v_structure_id bigint;
    v_db_type text;
    v_is_array boolean;
    v_array_index int;
    v_dict_key text;
    v_is_dictionary boolean;
    v_result numeric;
    v_sql text;
    v_column_name text;
    v_object_ids bigint[];
    v_array_condition text;
BEGIN
    -- 1. Resolve field path (support for nested, arrays, dictionaries and specific indices/keys)
    SELECT r.structure_id, r.db_type, r.is_array, r.array_index, r.dict_key, r.is_dictionary
    INTO v_structure_id, v_db_type, v_is_array, v_array_index, v_dict_key, v_is_dictionary
    FROM resolve_field_path(p_scheme_id, p_field_path) r;
    
    IF v_structure_id IS NULL THEN
        RAISE EXCEPTION 'Field "%" not found in scheme %', p_field_path, p_scheme_id;
    END IF;
    
    -- 2. Determine column by data type
    v_column_name := CASE v_db_type
        WHEN 'Long' THEN '_Long'
        WHEN 'Double' THEN '_Double'
        WHEN 'Numeric' THEN '_Numeric'
        WHEN 'Int' THEN '_Long'
        WHEN 'Decimal' THEN '_Numeric'
        WHEN 'Money' THEN '_Numeric'
        ELSE '_Long'
    END;
    
    -- 3. Condition for collections: Dictionary/Array/Simple
    IF v_dict_key IS NOT NULL THEN
        -- 🆕 Dictionary with key: PhoneBook[home]
        v_array_condition := format('AND v._array_index = %L', v_dict_key);
    ELSIF v_is_array THEN
        IF v_array_index IS NOT NULL THEN
            -- Array with index: Items[2]
            v_array_condition := format('AND v._array_index = %L', v_array_index::text);
        ELSE
            -- Array without index: Items[] — all elements
            v_array_condition := '';
        END IF;
    ELSE
        -- Simple field: not a collection
        v_array_condition := 'AND v._array_index IS NULL';
    END IF;
    
    -- 4. If there is a filter - get object_ids via get_filtered_object_ids
    -- ⚡ OPTIMIZED: returns only bigint[] without JSON overhead!
    IF p_filter_json IS NOT NULL AND p_filter_json != 'null'::jsonb THEN
        BEGIN
            v_object_ids := get_filtered_object_ids(p_scheme_id, p_filter_json, 10);
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'aggregate_field ERROR: % (filter=%)', SQLERRM, p_filter_json;
            v_object_ids := NULL;
        END;
        
        IF v_object_ids IS NULL OR array_length(v_object_ids, 1) IS NULL THEN
            RETURN CASE p_function
                WHEN 'COUNT' THEN 0
                ELSE NULL
            END;
        END IF;
    END IF;
    
    -- 5. Perform aggregation
    IF v_object_ids IS NOT NULL THEN
        -- With filter by object_ids
        v_sql := format(
            'SELECT %s(%s::numeric) FROM _values v
             WHERE v._id_structure = $1 
               AND v._id_object = ANY($2)
               %s',
            p_function, v_column_name, v_array_condition
        );
        EXECUTE v_sql INTO v_result USING v_structure_id, v_object_ids;
    ELSE
        -- Without filter - all scheme objects
        v_sql := format(
            'SELECT %s(%s::numeric) FROM _values v
             JOIN _objects o ON o._id = v._id_object
             WHERE v._id_structure = $1 
               AND o._id_scheme = $2
               %s',
            p_function, v_column_name, v_array_condition
        );
        EXECUTE v_sql INTO v_result USING v_structure_id, p_scheme_id;
    END IF;
    
    RETURN v_result;
END;
$BODY$;

-- =====================================================
-- COMMENTS
-- =====================================================
COMMENT ON FUNCTION resolve_field_path(bigint, text) IS 
'Resolves EAV field path to structure_id.
Supports: simple fields, nested Classes, arrays, dictionaries.
⭐ Array modes:
  Items[]  → all elements (array_index = NULL)
  Items[2] → specific element (array_index = 2)
⭐ Dictionary modes:
  PhoneBook[]     → all keys (dict_key = NULL)
  PhoneBook[home] → specific key (dict_key = ''home'')
Returns: structure_id, db_type, is_array, array_index, dict_key, is_dictionary';

COMMENT ON FUNCTION aggregate_field(bigint, text, text, jsonb) IS 
'Aggregation over EAV field. Supports SUM, AVG, MIN, MAX, COUNT.
⚡ With filter: 2 queries (get_filtered_object_ids + aggregation) — optimized!
Without filter: 1 query.
⭐ Two array modes:
  Items[].Price  → aggregation over ALL elements
  Items[2].Price → aggregation only over element with index 2
Examples:
  SELECT aggregate_field(1002, ''Price'', ''SUM'', NULL);
  SELECT aggregate_field(1002, ''Items[].Amount'', ''SUM'', NULL);   -- all elements
  SELECT aggregate_field(1002, ''Items[0].Amount'', ''SUM'', NULL);  -- only first
  SELECT aggregate_field(1002, ''Customer.Rating'', ''AVG'', NULL);';

-- =====================================================
-- aggregate_batch: Multiple aggregations in ONE query
-- ⭐ Supports array indices: Items[].Price vs Items[2].Price
-- =====================================================
CREATE OR REPLACE FUNCTION aggregate_batch(
    p_scheme_id bigint,
    p_aggregations jsonb,
    p_filter_json jsonb DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
AS $BODY$
DECLARE
    v_agg record;
    v_select_parts text[] := ARRAY[]::text[];
    v_structure_ids bigint[] := ARRAY[]::bigint[];
    v_resolved record;
    v_object_ids bigint[];
    v_result jsonb;
    v_sql text;
    v_field text;
    v_func text;
    v_has_count boolean := false;
    v_array_condition text;
BEGIN
    -- 1. Resolve all fields and build SELECT parts
    FOR v_agg IN SELECT * FROM jsonb_array_elements(p_aggregations)
    LOOP
        v_field := v_agg.value->>'field';
        v_func := upper(v_agg.value->>'func');
        
        -- ⭐ COUNT(*) — special case, does not require structure_id
        IF v_func = 'COUNT' AND (v_field IS NULL OR v_field = '*' OR v_field = '') THEN
            v_has_count := true;
            v_select_parts := array_append(v_select_parts, format(
                '''%s'', COUNT(DISTINCT v._id_object)',
                v_agg.value->>'alias'
            ));
            CONTINUE;
        END IF;
        
        -- 🆕 Check for base field (prefix "0$:")
        IF v_field LIKE '0$:%' THEN
            -- ═══════════════════════════════════════════════════
            -- 🚀 BASE FIELD _objects (for SumRedbAsync, etc.)
            -- ═══════════════════════════════════════════════════
            DECLARE
                raw_field_name text := substring(v_field from 4);
                sql_column text := _normalize_base_field_name(raw_field_name);
            BEGIN
                IF sql_column IS NULL THEN
                    RAISE EXCEPTION 'aggregate_batch: Unknown base field "%"', raw_field_name;
                END IF;
                
                -- 🔥 Aggregation directly from _objects (without _values!)
                -- For MIN/MAX no need for ::numeric (they work with timestamp, text, etc.)
                -- For SUM/AVG need ::numeric (only for numeric fields)
                IF v_func IN ('SUM', 'AVG') THEN
                    v_select_parts := array_append(v_select_parts, format(
                        '''%s'', %s(o.%I::numeric)',
                        v_agg.value->>'alias',
                        v_func,
                        sql_column
                    ));
                ELSE
                    -- MIN, MAX, COUNT — work with any types
                    v_select_parts := array_append(v_select_parts, format(
                        '''%s'', %s(o.%I)',
                        v_agg.value->>'alias',
                        v_func,
                        sql_column
                    ));
                END IF;
                -- DO NOT add structure_id - not needed for base fields JOIN!
            END;
        ELSE
            -- ═══════════════════════════════════════════════════
            -- 📦 EAV FIELD (existing logic)
            -- ═══════════════════════════════════════════════════
            SELECT * INTO v_resolved FROM resolve_field_path(p_scheme_id, v_field);
        
        IF v_resolved.structure_id IS NOT NULL THEN
            v_structure_ids := array_append(v_structure_ids, v_resolved.structure_id);
            
            DECLARE
                v_column_name text;
            BEGIN
                v_column_name := CASE v_resolved.db_type
                    WHEN 'Long' THEN '_Long'
                    WHEN 'Double' THEN '_Double'
                    WHEN 'Numeric' THEN '_Numeric'
                    WHEN 'Int' THEN '_Long'
                    WHEN 'Decimal' THEN '_Numeric'
                    WHEN 'Money' THEN '_Numeric'
                    ELSE '_Long'
                END;
                
                -- ⭐ COLLECTION MODES: Array/Dictionary/Simple
                IF v_resolved.dict_key IS NOT NULL THEN
                    -- 🆕 Dictionary with key: PhoneBook[home]
                    v_array_condition := format(' AND v._array_index = %L', v_resolved.dict_key);
                ELSIF v_resolved.is_array THEN
                    IF v_resolved.array_index IS NOT NULL THEN
                        -- Array with index: Items[2]
                        v_array_condition := format(' AND v._array_index = %L', v_resolved.array_index::text);
                    ELSE
                        -- Array without index: Items[] — all elements
                        v_array_condition := '';
                    END IF;
                ELSE
                    -- Simple field: not a collection
                    v_array_condition := ' AND v._array_index IS NULL';
                END IF;
                
                v_select_parts := array_append(v_select_parts, format(
                    '''%s'', %s(CASE WHEN v._id_structure = %s%s THEN v.%s::numeric END)',
                    v_agg.value->>'alias',
                    v_func,
                    v_resolved.structure_id,
                    v_array_condition,
                    v_column_name
                ));
            END;
        END IF;
        END IF;  -- 🆕 Close IF for "0$:" vs EAV
    END LOOP;
    
    IF array_length(v_select_parts, 1) IS NULL THEN
        RETURN '{}'::jsonb;
    END IF;
    
    -- 2. Filter: get object_ids via get_filtered_object_ids
    -- ⚡ OPTIMIZED: returns only bigint[] without JSON overhead!
    IF p_filter_json IS NOT NULL THEN
        BEGIN
            v_object_ids := get_filtered_object_ids(p_scheme_id, p_filter_json, 10);
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'aggregate_batch ERROR: % (filter=%)', SQLERRM, p_filter_json;
            v_object_ids := NULL;
        END;
        
        IF v_object_ids IS NULL OR array_length(v_object_ids, 1) IS NULL THEN
            RETURN '{}'::jsonb;
        END IF;
    END IF;
    
    -- 3. Perform aggregation
    IF array_length(v_structure_ids, 1) IS NULL OR array_length(v_structure_ids, 1) = 0 THEN
        -- Only COUNT(*) without other aggregations
        IF v_object_ids IS NOT NULL THEN
            v_sql := format(
                'SELECT jsonb_build_object(%s) FROM _values v WHERE v._id_object = ANY($1)',
                array_to_string(v_select_parts, ', ')
            );
            EXECUTE v_sql INTO v_result USING v_object_ids;
        ELSE
            v_sql := format(
                'SELECT jsonb_build_object(%s) FROM _values v JOIN _objects o ON o._id = v._id_object WHERE o._id_scheme = $1',
                array_to_string(v_select_parts, ', ')
            );
            EXECUTE v_sql INTO v_result USING p_scheme_id;
        END IF;
    ELSE
        -- There are structure_ids — standard query
        IF v_object_ids IS NOT NULL THEN
            v_sql := format(
                'SELECT jsonb_build_object(%s) FROM _values v WHERE v._id_structure = ANY($1) AND v._id_object = ANY($2)',
                array_to_string(v_select_parts, ', ')
            );
            EXECUTE v_sql INTO v_result USING v_structure_ids, v_object_ids;
        ELSE
            v_sql := format(
                'SELECT jsonb_build_object(%s) FROM _values v JOIN _objects o ON o._id = v._id_object WHERE v._id_structure = ANY($1) AND o._id_scheme = $2',
                array_to_string(v_select_parts, ', ')
            );
            EXECUTE v_sql INTO v_result USING v_structure_ids, p_scheme_id;
        END IF;
    END IF;
    
    RETURN COALESCE(v_result, '{}'::jsonb);
END;
$BODY$;

COMMENT ON FUNCTION aggregate_batch(bigint, jsonb, jsonb) IS 
'Multiple aggregations in one call.
⚡ With filter: 2 queries (get_filtered_object_ids + aggregation) — optimized!
Without filter: 1 aggregation query.
⭐ Supports two array modes:
  Items[].Price  → aggregation over ALL elements
  Items[2].Price → only element with index 2
Example:
SELECT aggregate_batch(1002, 
  ''[{"field":"Stock","func":"SUM","alias":"TotalStock"},
     {"field":"Items[].Price","func":"SUM","alias":"AllPrices"},
     {"field":"Items[0].Price","func":"SUM","alias":"FirstPrice"}]''::jsonb, NULL);';

-- =====================================================
-- SQL PREVIEW functions (for debugging)
-- =====================================================

-- aggregate_batch_preview: Shows SQL that will be executed
CREATE OR REPLACE FUNCTION aggregate_batch_preview(
    p_scheme_id bigint,
    p_aggregations jsonb,
    p_filter_json jsonb DEFAULT NULL
)
RETURNS text
LANGUAGE plpgsql
AS $BODY$
DECLARE
    v_agg record;
    v_select_parts text[] := ARRAY[]::text[];
    v_structure_ids bigint[] := ARRAY[]::bigint[];
    v_resolved record;
    v_sql text;
    v_field text;
    v_func text;
    v_array_condition text;
BEGIN
    -- 1. Resolve all fields and build SELECT parts
    FOR v_agg IN SELECT * FROM jsonb_array_elements(p_aggregations)
    LOOP
        v_field := v_agg.value->>'field';
        v_func := upper(v_agg.value->>'func');
        
        IF v_func = 'COUNT' AND (v_field IS NULL OR v_field = '*' OR v_field = '') THEN
            v_select_parts := array_append(v_select_parts, format(
                'COUNT(DISTINCT v._id_object) AS "%s"',
                v_agg.value->>'alias'
            ));
            CONTINUE;
        END IF;
        
        SELECT * INTO v_resolved FROM resolve_field_path(p_scheme_id, v_field);
        
        IF v_resolved.structure_id IS NOT NULL THEN
            v_structure_ids := array_append(v_structure_ids, v_resolved.structure_id);
            
            DECLARE
                v_column_name text;
            BEGIN
                v_column_name := CASE v_resolved.db_type
                    WHEN 'Long' THEN '_Long'
                    WHEN 'Double' THEN '_Double'
                    WHEN 'Numeric' THEN '_Numeric'
                    WHEN 'Int' THEN '_Long'
                    WHEN 'Decimal' THEN '_Numeric'
                    WHEN 'Money' THEN '_Numeric'
                    ELSE '_Long'
                END;
                
                IF v_resolved.is_array THEN
                    IF v_resolved.array_index IS NOT NULL THEN
                        v_array_condition := format(' AND v._array_index = %L', v_resolved.array_index::text);
                    ELSE
                        v_array_condition := '';
                    END IF;
                ELSE
                    v_array_condition := ' AND v._array_index IS NULL';
                END IF;
                
                v_select_parts := array_append(v_select_parts, format(
                    '%s(CASE WHEN v._id_structure = %s%s THEN v.%s::numeric END) AS "%s" /* %s */',
                    v_func,
                    v_resolved.structure_id,
                    v_array_condition,
                    v_column_name,
                    v_agg.value->>'alias',
                    v_field
                ));
            END;
        END IF;
    END LOOP;
    
    IF array_length(v_select_parts, 1) IS NULL THEN
        RETURN '-- No aggregations to execute';
    END IF;
    
    -- 2. Form final SQL
    IF p_filter_json IS NOT NULL THEN
        -- With filter: show that there will be 2 queries
        v_sql := format(E'-- 📊 AGGREGATE BATCH SQL PREVIEW\n-- Scheme: %s\n-- Filter: %s\n\n-- ⚠️ Step 1: Getting object_ids via search_objects_with_facets_base\n-- WITH filtered AS (SELECT ... FROM search_objects_with_facets_base(...))\n\n-- Step 2: Aggregation by object_ids\nSELECT\n  %s\nFROM _values v\nWHERE v._id_structure = ANY(ARRAY[%s]::bigint[])\n  AND v._id_object = ANY(filtered_object_ids);',
            p_scheme_id,
            p_filter_json::text,
            array_to_string(v_select_parts, E',\n  '),
            array_to_string(v_structure_ids, ', ')
        );
    ELSE
        -- Without filter: one query
        v_sql := format(E'-- 📊 AGGREGATE BATCH SQL PREVIEW\n-- Scheme: %s\n-- Filter: NULL\n\nSELECT\n  %s\nFROM _values v\nJOIN _objects o ON o._id = v._id_object\nWHERE v._id_structure = ANY(ARRAY[%s]::bigint[])\n  AND o._id_scheme = %s;',
            p_scheme_id,
            array_to_string(v_select_parts, E',\n  '),
            array_to_string(v_structure_ids, ', '),
            p_scheme_id
        );
    END IF;
    
    RETURN v_sql;
END;
$BODY$;

COMMENT ON FUNCTION aggregate_batch_preview(bigint, jsonb, jsonb) IS 
'🔍 SQL Preview for aggregations. Shows what SQL will be executed in aggregate_batch().
Analog of ToSqlStringAsync() / ToQueryString() from EF Core.
Example:
SELECT aggregate_batch_preview(1002, 
  ''[{"field":"Stock","func":"SUM","alias":"TotalStock"},
     {"field":"Scores1[0]","func":"AVG","alias":"AvgFirst"}]''::jsonb, NULL);';

-- =====================================================
-- TEST QUERIES
-- =====================================================
/*
-- Single
SELECT aggregate_field(1002, 'Stock', 'SUM', NULL);

-- ⭐ Arrays: ALL elements
SELECT aggregate_field(1002, 'Items[].Price', 'SUM', NULL);

-- ⭐ Arrays: SPECIFIC element
SELECT aggregate_field(1002, 'Items[0].Price', 'SUM', NULL);  -- first
SELECT aggregate_field(1002, 'Items[2].Price', 'AVG', NULL);  -- third

-- ⭐ BATCH: Multiple in ONE query
SELECT aggregate_batch(1002, 
    '[{"field":"Stock","func":"SUM","alias":"TotalStock"},
      {"field":"Age","func":"AVG","alias":"AvgAge"},
      {"field":"Items[].Price","func":"SUM","alias":"AllItemsPrice"},
      {"field":"Items[0].Price","func":"SUM","alias":"FirstItemPrice"}]'::jsonb,
    NULL);

-- 🔍 SQL PREVIEW (for debugging)
SELECT aggregate_batch_preview(1002, 
    '[{"field":"Stock","func":"SUM","alias":"TotalStock"},
      {"field":"Scores1[]","func":"SUM","alias":"AllScores"},
      {"field":"Scores1[0]","func":"AVG","alias":"AvgFirst"}]'::jsonb,
    NULL);
*/