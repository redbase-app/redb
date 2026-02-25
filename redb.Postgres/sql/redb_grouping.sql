-- =====================================================
-- REDB EAV GROUPING FUNCTIONS
-- GroupBy aggregations for EAV model
-- =====================================================

-- =====================================================
-- aggregate_grouped: GroupBy with aggregations
-- =====================================================
-- Parameters:
--   p_scheme_id     - Scheme ID
--   p_group_fields  - JSON array of grouping fields:
--                     [{"field":"Category","alias":"Category"}]
--                     [{"field":"Address.City","alias":"City"}]
--   p_aggregations  - JSON array of aggregations (like in aggregate_batch):
--                     [{"field":"Stock","func":"SUM","alias":"TotalStock"}]
--   p_filter_json   - JSON filter (optional)
--
-- Returns: jsonb array of groups
--   [{"Category":"Electronics","TotalStock":1500},...]
-- =====================================================

DROP FUNCTION IF EXISTS aggregate_grouped(bigint, jsonb, jsonb, jsonb);

CREATE OR REPLACE FUNCTION aggregate_grouped(
    p_scheme_id bigint,
    p_group_fields jsonb,
    p_aggregations jsonb,
    p_filter_json jsonb DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
AS $BODY$
DECLARE
    v_group_field record;
    v_agg record;
    v_resolved record;
    
    -- SQL parts
    v_select_parts text[] := ARRAY[]::text[];
    v_join_parts text[] := ARRAY[]::text[];
    v_group_parts text[] := ARRAY[]::text[];
    
    -- Counters for unique aliases
    v_join_idx int := 0;
    v_field text;
    v_alias text;
    v_func text;
    v_column_name text;
    v_join_alias text;
    v_array_condition text;
    
    -- Filter
    v_object_ids bigint[];
    v_where_clause text := '';
    
    -- Result
    v_sql text;
    v_result jsonb;
BEGIN
    -- =========================================
    -- 1. Process grouping fields
    -- =========================================
    FOR v_group_field IN SELECT * FROM jsonb_array_elements(p_group_fields)
    LOOP
        v_field := v_group_field.value->>'field';
        v_alias := COALESCE(v_group_field.value->>'alias', v_field);
        
        -- 🆕 Check for base field (prefix "0$:")
        IF v_field LIKE '0$:%' THEN
            -- ═══════════════════════════════════════════════════
            -- BASE FIELD from _objects (scheme_id, parent_id, etc.)
            -- ═══════════════════════════════════════════════════
            DECLARE
                raw_field_name text := substring(v_field from 4);  -- remove '0$:'
                sql_column text := _normalize_base_field_name(raw_field_name);
            BEGIN
                IF sql_column IS NULL THEN
                    RAISE EXCEPTION 'aggregate_grouped: Unknown base field "%"', raw_field_name;
                END IF;
                
                -- SELECT part: directly from _objects
                v_select_parts := array_append(v_select_parts, 
                    format('o.%I AS "%s"', sql_column, v_alias));
                
                -- GROUP BY part: directly from _objects
                v_group_parts := array_append(v_group_parts, 
                    format('o.%I', sql_column));
                
                -- NO JOIN needed for base fields!
            END;
        ELSE
            -- ═══════════════════════════════════════════════════
            -- 📦 EAV FIELD from _values (+ Dictionary support)
            -- ═══════════════════════════════════════════════════
            -- Resolve path to field
            SELECT * INTO v_resolved FROM resolve_field_path(p_scheme_id, v_field);
            
            IF v_resolved.structure_id IS NULL THEN
                RAISE EXCEPTION 'Group field "%" not found in scheme %', v_field, p_scheme_id;
            END IF;
            
            v_join_idx := v_join_idx + 1;
            v_join_alias := 'g' || v_join_idx;
            
            -- Determine column by type
            v_column_name := CASE v_resolved.db_type
                WHEN 'Long' THEN '_Long'
                WHEN 'Double' THEN '_Double'
                WHEN 'Numeric' THEN '_Numeric'
                WHEN 'Int' THEN '_Long'
                WHEN 'String' THEN '_String'
                WHEN 'Bool' THEN '_Bool'
                WHEN 'DateTime' THEN '_DateTime'
                WHEN 'ListItem' THEN '_ListItem'
                ELSE '_String'
            END;
            
            -- SELECT part
            v_select_parts := array_append(v_select_parts, 
                format('%s.%s AS "%s"', v_join_alias, v_column_name, v_alias));
            
            -- JOIN part (+ Dictionary/Array support)
            IF v_resolved.dict_key IS NOT NULL THEN
                -- Dictionary with key: PhoneBook[home]
                v_join_parts := array_append(v_join_parts,
                    format('JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index = %L',
                        v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias, v_resolved.dict_key));
            ELSIF v_resolved.array_index IS NOT NULL THEN
                -- Array with index: Items[2]
                v_join_parts := array_append(v_join_parts,
                    format('JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index = %L',
                        v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias, v_resolved.array_index::text));
            ELSE
                -- Simple field
                v_join_parts := array_append(v_join_parts,
                    format('JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index IS NULL',
                        v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias));
            END IF;
            
            -- GROUP BY part
            v_group_parts := array_append(v_group_parts, 
                format('%s.%s', v_join_alias, v_column_name));
        END IF;
    END LOOP;
    
    -- =========================================
    -- 2. Process aggregations
    -- =========================================
    FOR v_agg IN SELECT * FROM jsonb_array_elements(p_aggregations)
    LOOP
        v_field := v_agg.value->>'field';
        v_func := upper(v_agg.value->>'func');
        v_alias := COALESCE(v_agg.value->>'alias', v_func || '_' || COALESCE(v_field, 'count'));
        
        -- COUNT(*) - special case
        IF v_func = 'COUNT' AND (v_field IS NULL OR v_field = '*' OR v_field = '') THEN
            v_select_parts := array_append(v_select_parts,
                format('COUNT(DISTINCT o._id) AS "%s"', v_alias));
            CONTINUE;
        END IF;
        
        -- 🆕 Check for base field (prefix "0$:")
        IF v_field LIKE '0$:%' THEN
            -- BASE FIELD for aggregation
            DECLARE
                raw_field_name text := substring(v_field from 4);
                sql_column text := _normalize_base_field_name(raw_field_name);
            BEGIN
                IF sql_column IS NULL THEN
                    RAISE EXCEPTION 'aggregate_grouped: Unknown base field for aggregation "%"', raw_field_name;
                END IF;
                
                -- SELECT with aggregation directly from _objects
                -- For MIN/MAX ::numeric not needed (they work with timestamp, text, etc.)
                -- For SUM/AVG ::numeric needed (only for numeric fields)
                IF v_func IN ('SUM', 'AVG') THEN
                    v_select_parts := array_append(v_select_parts,
                        format('%s(o.%I::numeric) AS "%s"', v_func, sql_column, v_alias));
                ELSE
                    -- MIN, MAX, COUNT — work with any types
                    v_select_parts := array_append(v_select_parts,
                        format('%s(o.%I) AS "%s"', v_func, sql_column, v_alias));
                END IF;
                -- NO JOIN needed for base fields!
            END;
        ELSE
            -- EAV FIELD (+ Dictionary support)
            -- Resolve field path
            SELECT * INTO v_resolved FROM resolve_field_path(p_scheme_id, v_field);
            
            IF v_resolved.structure_id IS NULL THEN
                RAISE EXCEPTION 'Aggregation field "%" not found in scheme %', v_field, p_scheme_id;
            END IF;
            
            v_join_idx := v_join_idx + 1;
            v_join_alias := 'a' || v_join_idx;
            
            -- Determine column by type
            v_column_name := CASE v_resolved.db_type
                WHEN 'Long' THEN '_Long'
                WHEN 'Double' THEN '_Double'
                WHEN 'Numeric' THEN '_Numeric'
                WHEN 'Int' THEN '_Long'
                WHEN 'Decimal' THEN '_Numeric'
                WHEN 'Money' THEN '_Numeric'
                ELSE '_Long'
            END;
            
            -- Condition for Dictionary/Array/Simple field
            IF v_resolved.dict_key IS NOT NULL THEN
                -- Dictionary with key: PhoneBook[home]
                v_array_condition := format(' AND %s._array_index = %L', v_join_alias, v_resolved.dict_key);
            ELSIF v_resolved.is_array THEN
                IF v_resolved.array_index IS NOT NULL THEN
                    -- Array with index: Items[2]
                    v_array_condition := format(' AND %s._array_index = %L', v_join_alias, v_resolved.array_index::text);
                ELSE
                    v_array_condition := '';  -- all array/dictionary elements
                END IF;
            ELSE
                v_array_condition := format(' AND %s._array_index IS NULL', v_join_alias);
            END IF;
            
            -- JOIN part for aggregation
            v_join_parts := array_append(v_join_parts,
                format('LEFT JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s%s',
                    v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_array_condition));
            
            -- SELECT part with aggregation
            v_select_parts := array_append(v_select_parts,
                format('%s(%s.%s::numeric) AS "%s"', v_func, v_join_alias, v_column_name, v_alias));
        END IF;
    END LOOP;
    
    -- =========================================
    -- 3. Process filter
    -- =========================================
    IF p_filter_json IS NOT NULL AND p_filter_json != 'null'::jsonb THEN
        BEGIN
            v_object_ids := get_filtered_object_ids(p_scheme_id, p_filter_json, 10);
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'aggregate_grouped filter error: %', SQLERRM;
            v_object_ids := NULL;
        END;
        
        IF v_object_ids IS NULL OR array_length(v_object_ids, 1) IS NULL THEN
            RETURN '[]'::jsonb;
        END IF;
        
        v_where_clause := format(' AND o._id = ANY(ARRAY[%s]::bigint[])', 
            array_to_string(v_object_ids, ','));
    END IF;
    
    -- =========================================
    -- 4. Build and execute SQL
    -- =========================================
    -- 🆕 CHECK: If no grouping (v_group_parts empty), do not add GROUP BY and ORDER BY
    IF array_length(v_group_parts, 1) IS NULL OR array_length(v_group_parts, 1) = 0 THEN
        -- WITHOUT grouping (for simple aggregations like SumRedbAsync)
        v_sql := format(
            'SELECT jsonb_agg(row_to_json(t)) FROM (
                SELECT %s
                FROM _objects o
                %s
                WHERE o._id_scheme = %s%s
            ) t',
            array_to_string(v_select_parts, ', '),
            array_to_string(v_join_parts, ' '),
            p_scheme_id,
            v_where_clause
        );
    ELSE
        -- WITH grouping (regular GroupBy)
        v_sql := format(
            'SELECT jsonb_agg(row_to_json(t)) FROM (
                SELECT %s
                FROM _objects o
                %s
                WHERE o._id_scheme = %s%s
                GROUP BY %s
                ORDER BY %s
            ) t',
            array_to_string(v_select_parts, ', '),
            array_to_string(v_join_parts, ' '),
            p_scheme_id,
            v_where_clause,
            array_to_string(v_group_parts, ', '),
            array_to_string(v_group_parts, ', ')
        );
    END IF;
    
    -- DEBUG: uncomment for debugging
    -- RAISE NOTICE 'aggregate_grouped SQL: %', v_sql;
    
    EXECUTE v_sql INTO v_result;
    
    RETURN COALESCE(v_result, '[]'::jsonb);
END;
$BODY$;

COMMENT ON FUNCTION aggregate_grouped(bigint, jsonb, jsonb, jsonb) IS 
'GroupBy aggregation for EAV.
Supports: simple fields, nested paths, arrays Items[2], dictionaries PhoneBook[home].
Examples:
  -- Simple grouping
  SELECT aggregate_grouped(1002, 
    ''[{"field":"Tag","alias":"Tag"}]''::jsonb,
    ''[{"field":"Stock","func":"SUM","alias":"TotalStock"},{"field":"*","func":"COUNT","alias":"Count"}]''::jsonb,
    NULL);
    
  -- Nested path
  SELECT aggregate_grouped(1002,
    ''[{"field":"Address.City","alias":"City"}]''::jsonb,
    ''[{"field":"Age","func":"AVG","alias":"AvgAge"}]''::jsonb,
    NULL);
    
  -- Dictionary: grouping by dictionary value
  SELECT aggregate_grouped(1002,
    ''[{"field":"PhoneBook[home]","alias":"HomePhone"}]''::jsonb,
    ''[{"field":"*","func":"COUNT","alias":"Count"}]''::jsonb,
    NULL);
    
  -- Multiple keys
  SELECT aggregate_grouped(1002,
    ''[{"field":"Tag","alias":"Tag"},{"field":"Age","alias":"Age"}]''::jsonb,
    ''[{"field":"Stock","func":"SUM","alias":"Total"}]''::jsonb,
    NULL);';

-- =====================================================
-- aggregate_grouped_preview: SQL preview for debugging
-- =====================================================
CREATE OR REPLACE FUNCTION aggregate_grouped_preview(
    p_scheme_id bigint,
    p_group_fields jsonb,
    p_aggregations jsonb,
    p_filter_json jsonb DEFAULT NULL
)
RETURNS text
LANGUAGE plpgsql
AS $BODY$
DECLARE
    v_group_field record;
    v_agg record;
    v_resolved record;
    v_select_parts text[] := ARRAY[]::text[];
    v_join_parts text[] := ARRAY[]::text[];
    v_group_parts text[] := ARRAY[]::text[];
    v_join_idx int := 0;
    v_field text;
    v_alias text;
    v_func text;
    v_column_name text;
    v_join_alias text;
    v_array_condition text;
BEGIN
    -- Grouping fields
    FOR v_group_field IN SELECT * FROM jsonb_array_elements(p_group_fields)
    LOOP
        v_field := v_group_field.value->>'field';
        v_alias := COALESCE(v_group_field.value->>'alias', v_field);
        SELECT * INTO v_resolved FROM resolve_field_path(p_scheme_id, v_field);
        
        v_join_idx := v_join_idx + 1;
        v_join_alias := 'g' || v_join_idx;
        v_column_name := CASE v_resolved.db_type
            WHEN 'Long' THEN '_Long' WHEN 'String' THEN '_String' WHEN 'ListItem' THEN '_ListItem' ELSE '_String' END;
        
        v_select_parts := array_append(v_select_parts, format('%s.%s AS "%s"', v_join_alias, v_column_name, v_alias));
        
        -- 🆕 Dictionary/Array/Simple support
        IF v_resolved.dict_key IS NOT NULL THEN
            v_join_parts := array_append(v_join_parts, format('JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index = %L', 
                v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias, v_resolved.dict_key));
        ELSIF v_resolved.array_index IS NOT NULL THEN
            v_join_parts := array_append(v_join_parts, format('JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index = %L', 
                v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias, v_resolved.array_index::text));
        ELSE
            v_join_parts := array_append(v_join_parts, format('JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index IS NULL', 
                v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias));
        END IF;
        
        v_group_parts := array_append(v_group_parts, format('%s.%s', v_join_alias, v_column_name));
    END LOOP;
    
    -- Aggregations
    FOR v_agg IN SELECT * FROM jsonb_array_elements(p_aggregations)
    LOOP
        v_field := v_agg.value->>'field';
        v_func := upper(v_agg.value->>'func');
        v_alias := COALESCE(v_agg.value->>'alias', v_func);
        
        IF v_func = 'COUNT' AND (v_field IS NULL OR v_field = '*') THEN
            v_select_parts := array_append(v_select_parts, format('COUNT(DISTINCT o._id) AS "%s"', v_alias));
            CONTINUE;
        END IF;
        
        SELECT * INTO v_resolved FROM resolve_field_path(p_scheme_id, v_field);
        v_join_idx := v_join_idx + 1;
        v_join_alias := 'a' || v_join_idx;
        v_column_name := CASE v_resolved.db_type WHEN 'Long' THEN '_Long' ELSE '_Numeric' END;
        
        -- 🆕 Dictionary/Array/Simple support
        IF v_resolved.dict_key IS NOT NULL THEN
            v_join_parts := array_append(v_join_parts, format('LEFT JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index = %L',
                v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias, v_resolved.dict_key));
        ELSIF v_resolved.array_index IS NOT NULL THEN
            v_join_parts := array_append(v_join_parts, format('LEFT JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index = %L',
                v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias, v_resolved.array_index::text));
        ELSE
            v_join_parts := array_append(v_join_parts, format('LEFT JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index IS NULL',
                v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias));
        END IF;
        
        v_select_parts := array_append(v_select_parts, format('%s(%s.%s) AS "%s"', v_func, v_join_alias, v_column_name, v_alias));
    END LOOP;
    
    RETURN format(
        'SELECT %s FROM _objects o %s WHERE o._id_scheme = %s GROUP BY %s',
        array_to_string(v_select_parts, ', '),
        array_to_string(v_join_parts, ' '),
        p_scheme_id,
        array_to_string(v_group_parts, ', ')
    );
END;
$BODY$;

-- =====================================================
-- aggregate_array_grouped: GroupBy by array elements
-- =====================================================
-- Parameters:
--   p_scheme_id      - Scheme ID
--   p_array_path     - path to array (e.g. "Items")
--   p_group_fields   - JSON: [{"field":"Category","alias":"Category"}]
--   p_aggregations   - JSON: [{"field":"Price","func":"SUM","alias":"Total"}]
--   p_filter_json    - filter (optional)
--
-- Example:
--   SELECT aggregate_array_grouped(
--     1002,
--     'Items',
--     '[{"field":"Category","alias":"Category"}]',
--     '[{"field":"Price","func":"SUM","alias":"TotalPrice"}]',
--     NULL
--   );
-- =====================================================

DROP FUNCTION IF EXISTS aggregate_array_grouped(bigint, text, jsonb, jsonb, jsonb);

CREATE OR REPLACE FUNCTION aggregate_array_grouped(
    p_scheme_id bigint,
    p_array_path text,
    p_group_fields jsonb,
    p_aggregations jsonb,
    p_filter_json jsonb DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
AS $BODY$
DECLARE
    v_field record;
    v_agg record;
    v_resolved record;
    v_array_struct_id bigint;
    
    v_select_parts text[] := ARRAY[]::text[];
    v_group_parts text[] := ARRAY[]::text[];
    v_join_parts text[] := ARRAY[]::text[];
    
    v_join_idx int := 0;
    v_field_path text;
    v_alias text;
    v_func text;
    v_column_name text;
    v_join_alias text;
    
    v_sql text;
    v_result jsonb;
BEGIN
    -- 1. Get structure_id of array via resolve_field_path
    SELECT r.structure_id INTO v_array_struct_id
    FROM resolve_field_path(p_scheme_id, p_array_path) r;
    
    IF v_array_struct_id IS NULL THEN
        RAISE EXCEPTION 'Array "%" not found in scheme %', p_array_path, p_scheme_id;
    END IF;
    
    -- 2. Grouping fields (from array element)
    FOR v_field IN SELECT * FROM jsonb_array_elements(p_group_fields)
    LOOP
        v_field_path := v_field.value->>'field';
        v_alias := COALESCE(v_field.value->>'alias', v_field_path);
        
        -- Path inside element: Contacts.Type -> search Contacts[].Type
        SELECT * INTO v_resolved 
        FROM resolve_field_path(p_scheme_id, p_array_path || '[].' || v_field_path);
        
        IF v_resolved.structure_id IS NULL THEN
            RAISE WARNING 'aggregate_array_grouped: field "%" not found!', v_field_path;
            CONTINUE;
        END IF;
        
        v_join_idx := v_join_idx + 1;
        v_join_alias := 'g' || v_join_idx;
        v_column_name := CASE v_resolved.db_type
            WHEN 'Long' THEN '_Long'
            WHEN 'String' THEN '_String'
            WHEN 'ListItem' THEN '_ListItem'
            ELSE '_String'
        END;
        
        -- JOIN: for nested business object fields link via _array_parent_id
        v_join_parts := array_append(v_join_parts,
            format('LEFT JOIN _values %s ON %s._id_object = arr._id_object AND %s._id_structure = %s AND %s._array_parent_id = arr._id',
                v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias));
        
        v_select_parts := array_append(v_select_parts, format('%s.%s AS "%s"', v_join_alias, v_column_name, v_alias));
        v_group_parts := array_append(v_group_parts, format('%s.%s', v_join_alias, v_column_name));
    END LOOP;
    
    -- 3. Aggregations
    FOR v_agg IN SELECT * FROM jsonb_array_elements(p_aggregations)
    LOOP
        v_field_path := v_agg.value->>'field';
        v_func := upper(v_agg.value->>'func');
        v_alias := COALESCE(v_agg.value->>'alias', v_func);
        
        IF v_func = 'COUNT' AND (v_field_path IS NULL OR v_field_path = '*') THEN
            v_select_parts := array_append(v_select_parts, format('COUNT(*) AS "%s"', v_alias));
            CONTINUE;
        END IF;
        
        SELECT * INTO v_resolved 
        FROM resolve_field_path(p_scheme_id, p_array_path || '[].' || v_field_path);
        
        IF v_resolved.structure_id IS NULL THEN
            CONTINUE;
        END IF;
        
        v_join_idx := v_join_idx + 1;
        v_join_alias := 'a' || v_join_idx;
        v_column_name := CASE v_resolved.db_type
            WHEN 'Long' THEN '_Long'
            WHEN 'Double' THEN '_Double'
            WHEN 'Numeric' THEN '_Numeric'
            ELSE '_Long'
        END;
        
        -- JOIN: for nested business object fields link via _array_parent_id
        v_join_parts := array_append(v_join_parts,
            format('LEFT JOIN _values %s ON %s._id_object = arr._id_object AND %s._id_structure = %s AND %s._array_parent_id = arr._id',
                v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias));
        
        v_select_parts := array_append(v_select_parts, format('%s(%s.%s) AS "%s"', v_func, v_join_alias, v_column_name, v_alias));
    END LOOP;
    
    -- 4. Assemble SQL with array expansion
    v_sql := format(
        'SELECT jsonb_agg(row_to_json(t)) FROM (
            SELECT %s
            FROM _values arr
            JOIN _objects o ON o._id = arr._id_object
            %s
            WHERE o._id_scheme = %s 
              AND arr._id_structure = %s 
              AND arr._array_index IS NOT NULL
            GROUP BY %s
        ) t',
        array_to_string(v_select_parts, ', '),
        array_to_string(v_join_parts, ' '),
        p_scheme_id,
        v_array_struct_id,
        array_to_string(v_group_parts, ', ')
    );
    
    -- 🔍 DEBUG: uncomment for debugging
    -- RETURN jsonb_build_object('debug_sql', v_sql, 'array_struct_id', v_array_struct_id);
    
    EXECUTE v_sql INTO v_result;
    
    RETURN COALESCE(v_result, '[]'::jsonb);
END;
$BODY$;

COMMENT ON FUNCTION aggregate_array_grouped IS 
'GroupBy aggregation by EAV array elements.
Example: GroupBy Items[].Category with SUM(Items[].Price)';