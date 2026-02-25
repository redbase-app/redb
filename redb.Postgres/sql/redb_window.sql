
-- =====================================================
-- REDB EAV WINDOW FUNCTIONS
-- Window functions for EAV model
-- ROW_NUMBER, RANK, SUM OVER, etc.
-- =====================================================

-- =====================================================
-- query_with_window: Query with window functions
-- =====================================================
-- Parameters:
--   p_scheme_id      - Scheme ID
--   p_select_fields  - JSON array of fields for SELECT:
--                      [{"field":"Name","alias":"Name"}]
--   p_window_funcs   - JSON array of window functions:
--                      [{"func":"ROW_NUMBER","alias":"Rank"}]
--   p_partition_by   - JSON array of fields for PARTITION BY:
--                      [{"field":"Category"}]
--   p_order_by       - JSON array for ORDER BY inside window:
--                      [{"field":"Stock","dir":"DESC"}]
--   p_filter_json    - JSON filter (optional)
--   p_limit          - Record limit
--
-- Returns: jsonb array of objects with window functions
-- =====================================================
DROP FUNCTION IF EXISTS query_with_window(bigint, jsonb, jsonb, jsonb, jsonb, jsonb, integer);
DROP FUNCTION IF EXISTS query_with_window(bigint, jsonb, jsonb, jsonb, jsonb, jsonb, integer, jsonb);

CREATE OR REPLACE FUNCTION query_with_window(
    p_scheme_id bigint,
    p_select_fields jsonb,
    p_window_funcs jsonb,
    p_partition_by jsonb DEFAULT '[]'::jsonb,
    p_order_by jsonb DEFAULT '[]'::jsonb,
    p_filter_json jsonb DEFAULT NULL,
    p_limit integer DEFAULT 1000,
    p_frame_json jsonb DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
AS $BODY$
DECLARE
    v_field record;
    v_func record;
    v_resolved record;
    
    v_select_parts text[] := ARRAY[]::text[];
    v_join_parts text[] := ARRAY[]::text[];
    v_partition_parts text[] := ARRAY[]::text[];
    v_order_parts text[] := ARRAY[]::text[];
    
    v_join_idx int := 0;
    v_field_path text;
    v_alias text;
    v_column_name text;
    v_join_alias text;
    v_func_name text;
    v_dir text;
    
    v_object_ids bigint[];
    v_where_clause text := '';
    v_over_clause text;
    v_buckets int;
    v_frame_clause text := '';
    v_frame_type text;
    v_start_kind text;
    v_start_offset int;
    v_end_kind text;
    v_end_offset int;
    
    v_sql text;
    v_result jsonb;
BEGIN
    -- =========================================
    -- 1. Base SELECT fields
    -- =========================================
    -- Always include id and name
    v_select_parts := array_append(v_select_parts, 'o._id AS "id"');
    v_select_parts := array_append(v_select_parts, 'o._name AS "name"');
    
    -- Fields from Props or base fields
    FOR v_field IN SELECT * FROM jsonb_array_elements(p_select_fields)
    LOOP
        v_field_path := v_field.value->>'field';
        v_alias := COALESCE(v_field.value->>'alias', v_field_path);
        
        -- Check for base field (prefix "0$:")
        IF v_field_path LIKE '0$:%' THEN
            -- BASE FIELD from _objects (Name, Id, SchemeId, etc.)
            DECLARE
                raw_field_name text := substring(v_field_path from 4);
                sql_column text := _normalize_base_field_name(raw_field_name);
            BEGIN
                IF sql_column IS NULL THEN
                    RAISE EXCEPTION 'query_with_window SELECT: Unknown base field "%"', raw_field_name;
                END IF;
                
                -- SELECT directly from _objects (WITHOUT JOIN!)
                v_select_parts := array_append(v_select_parts, 
                    format('o.%I AS "%s"', sql_column, v_alias));
            END;
        ELSE
            -- EAV FIELD from _values (existing logic + Dictionary support)
            SELECT * INTO v_resolved FROM resolve_field_path(p_scheme_id, v_field_path);
            IF v_resolved.structure_id IS NULL THEN
                CONTINUE;
            END IF;
            
            v_join_idx := v_join_idx + 1;
            v_join_alias := 's' || v_join_idx;
            
            v_column_name := CASE v_resolved.db_type
                WHEN 'Long' THEN '_Long'
                WHEN 'String' THEN '_String'
                WHEN 'Double' THEN '_Double'
                WHEN 'Numeric' THEN '_Numeric'
                ELSE '_String'
            END;
            
            v_select_parts := array_append(v_select_parts, 
                format('%s.%s AS "%s"', v_join_alias, v_column_name, v_alias));
            
            -- 🆕 Dictionary support: PhoneBook[home] -> _array_index = 'home'
            IF v_resolved.dict_key IS NOT NULL THEN
                v_join_parts := array_append(v_join_parts,
                    format('LEFT JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index = %L',
                        v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias, v_resolved.dict_key));
            ELSIF v_resolved.array_index IS NOT NULL THEN
                -- Array with specific index: Items[2]
                v_join_parts := array_append(v_join_parts,
                    format('LEFT JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index = %L',
                        v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias, v_resolved.array_index::text));
            ELSE
                -- Simple field (not collection)
                v_join_parts := array_append(v_join_parts,
                    format('LEFT JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index IS NULL',
                        v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias));
            END IF;
        END IF;
    END LOOP;
    
    -- =========================================
    -- 2. PARTITION BY
    -- =========================================
    FOR v_field IN SELECT * FROM jsonb_array_elements(p_partition_by)
    LOOP
        v_field_path := v_field.value->>'field';
        
        -- Check for base field (prefix "0$:")
        IF v_field_path LIKE '0$:%' THEN
            -- BASE FIELD from _objects
            DECLARE
                raw_field_name text := substring(v_field_path from 4);
                sql_column text := _normalize_base_field_name(raw_field_name);
            BEGIN
                IF sql_column IS NULL THEN
                    RAISE EXCEPTION 'query_with_window PARTITION BY: Unknown base field "%"', raw_field_name;
                END IF;
                
                v_partition_parts := array_append(v_partition_parts, 
                    format('o.%I', sql_column));
                -- NO JOIN needed for base fields!
            END;
        ELSE
            -- EAV FIELD (existing logic + Dictionary support)
            SELECT * INTO v_resolved FROM resolve_field_path(p_scheme_id, v_field_path);
            IF v_resolved.structure_id IS NULL THEN
                CONTINUE;
            END IF;
            
            v_join_idx := v_join_idx + 1;
            v_join_alias := 'p' || v_join_idx;
            
            v_column_name := CASE v_resolved.db_type
                WHEN 'Long' THEN '_Long'
                WHEN 'String' THEN '_String'
                ELSE '_String'
            END;
            
            -- 🆕 Dictionary support: PhoneBook[home] -> _array_index = 'home'
            IF v_resolved.dict_key IS NOT NULL THEN
                v_join_parts := array_append(v_join_parts,
                    format('LEFT JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index = %L',
                        v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias, v_resolved.dict_key));
            ELSIF v_resolved.array_index IS NOT NULL THEN
                v_join_parts := array_append(v_join_parts,
                    format('LEFT JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index = %L',
                        v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias, v_resolved.array_index::text));
            ELSE
                v_join_parts := array_append(v_join_parts,
                    format('LEFT JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index IS NULL',
                        v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias));
            END IF;
            v_partition_parts := array_append(v_partition_parts, 
                format('%s.%s', v_join_alias, v_column_name));
        END IF;
    END LOOP;
    
    -- =========================================
    -- 3. ORDER BY inside window
    -- =========================================
    FOR v_field IN SELECT * FROM jsonb_array_elements(p_order_by)
    LOOP
        v_field_path := v_field.value->>'field';
        v_dir := COALESCE(upper(v_field.value->>'dir'), 'ASC');
        
        -- Check for base field (prefix "0$:")
        IF v_field_path LIKE '0$:%' THEN
            -- BASE FIELD from _objects
            DECLARE
                raw_field_name text := substring(v_field_path from 4);
                sql_column text := _normalize_base_field_name(raw_field_name);
            BEGIN
                IF sql_column IS NULL THEN
                    RAISE EXCEPTION 'query_with_window ORDER BY: Unknown base field "%"', raw_field_name;
                END IF;
                
                v_order_parts := array_append(v_order_parts, 
                    format('o.%I %s', sql_column, v_dir));
                -- NO JOIN needed for base fields!
            END;
        ELSE
            -- EAV FIELD (existing logic + Dictionary support)
            SELECT * INTO v_resolved FROM resolve_field_path(p_scheme_id, v_field_path);
            IF v_resolved.structure_id IS NULL THEN
                CONTINUE;
            END IF;
            
            v_join_idx := v_join_idx + 1;
            v_join_alias := 'w' || v_join_idx;
            
            v_column_name := CASE v_resolved.db_type
                WHEN 'Long' THEN '_Long'
                WHEN 'Double' THEN '_Double'
                WHEN 'Numeric' THEN '_Numeric'
                ELSE '_String'
            END;
            
            -- 🆕 Dictionary support: PhoneBook[home] -> _array_index = 'home'
            IF v_resolved.dict_key IS NOT NULL THEN
                v_join_parts := array_append(v_join_parts,
                    format('LEFT JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index = %L',
                        v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias, v_resolved.dict_key));
            ELSIF v_resolved.array_index IS NOT NULL THEN
                v_join_parts := array_append(v_join_parts,
                    format('LEFT JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index = %L',
                        v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias, v_resolved.array_index::text));
            ELSE
                v_join_parts := array_append(v_join_parts,
                    format('LEFT JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index IS NULL',
                        v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias));
            END IF;
            v_order_parts := array_append(v_order_parts, 
                format('%s.%s %s', v_join_alias, v_column_name, v_dir));
        END IF;
    END LOOP;
    
    -- =========================================
    -- 4. Build OVER clause (with ROWS BETWEEN support)
    -- =========================================
    -- Parse frame if exists
    IF p_frame_json IS NOT NULL AND p_frame_json != 'null'::jsonb THEN
        v_frame_type := COALESCE(upper(p_frame_json->>'type'), 'ROWS');
        v_start_kind := upper(p_frame_json->'start'->>'kind');
        v_start_offset := COALESCE((p_frame_json->'start'->>'offset')::int, 0);
        v_end_kind := upper(p_frame_json->'end'->>'kind');
        v_end_offset := COALESCE((p_frame_json->'end'->>'offset')::int, 0);
        
        -- Form ROWS/RANGE BETWEEN ... AND ...
        v_frame_clause := v_frame_type || ' BETWEEN ';
        
        -- Start bound
        v_frame_clause := v_frame_clause || CASE v_start_kind
            WHEN 'UNBOUNDEDPRECEDING' THEN 'UNBOUNDED PRECEDING'
            WHEN 'CURRENTROW' THEN 'CURRENT ROW'
            WHEN 'PRECEDING' THEN v_start_offset::text || ' PRECEDING'
            WHEN 'FOLLOWING' THEN v_start_offset::text || ' FOLLOWING'
            ELSE 'UNBOUNDED PRECEDING'
        END;
        
        v_frame_clause := v_frame_clause || ' AND ';
        
        -- End bound
        v_frame_clause := v_frame_clause || CASE v_end_kind
            WHEN 'UNBOUNDEDFOLLOWING' THEN 'UNBOUNDED FOLLOWING'
            WHEN 'CURRENTROW' THEN 'CURRENT ROW'
            WHEN 'PRECEDING' THEN v_end_offset::text || ' PRECEDING'
            WHEN 'FOLLOWING' THEN v_end_offset::text || ' FOLLOWING'
            ELSE 'CURRENT ROW'
        END;
    END IF;
    
    v_over_clause := 'OVER (';
    IF array_length(v_partition_parts, 1) > 0 THEN
        v_over_clause := v_over_clause || 'PARTITION BY ' || array_to_string(v_partition_parts, ', ');
    END IF;
    IF array_length(v_order_parts, 1) > 0 THEN
        IF array_length(v_partition_parts, 1) > 0 THEN
            v_over_clause := v_over_clause || ' ';
        END IF;
        v_over_clause := v_over_clause || 'ORDER BY ' || array_to_string(v_order_parts, ', ');
    END IF;
    -- Add frame clause if exists
    IF v_frame_clause != '' THEN
        IF array_length(v_order_parts, 1) > 0 OR array_length(v_partition_parts, 1) > 0 THEN
            v_over_clause := v_over_clause || ' ';
        END IF;
        v_over_clause := v_over_clause || v_frame_clause;
    END IF;
    v_over_clause := v_over_clause || ')';
    
    -- =========================================
    -- 5. Window functions
    -- =========================================
    FOR v_func IN SELECT * FROM jsonb_array_elements(p_window_funcs)
    LOOP
        v_func_name := upper(v_func.value->>'func');
        v_alias := COALESCE(v_func.value->>'alias', v_func_name);
        v_field_path := v_func.value->>'field';
        
        CASE v_func_name
            -- Ranking functions (without field)
            WHEN 'ROW_NUMBER' THEN
                v_select_parts := array_append(v_select_parts, 
                    format('ROW_NUMBER() %s AS "%s"', v_over_clause, v_alias));
            WHEN 'RANK' THEN
                v_select_parts := array_append(v_select_parts, 
                    format('RANK() %s AS "%s"', v_over_clause, v_alias));
            WHEN 'DENSE_RANK' THEN
                v_select_parts := array_append(v_select_parts, 
                    format('DENSE_RANK() %s AS "%s"', v_over_clause, v_alias));
            WHEN 'COUNT' THEN
                v_select_parts := array_append(v_select_parts, 
                    format('COUNT(*) %s AS "%s"', v_over_clause, v_alias));
            
            -- NTILE(n) - split into n buckets
            WHEN 'NTILE' THEN
                v_buckets := COALESCE((v_func.value->>'buckets')::int, 4);
                v_select_parts := array_append(v_select_parts, 
                    format('NTILE(%s) %s AS "%s"', v_buckets, v_over_clause, v_alias));
                    
            -- Aggregate functions with field (SUM, AVG, MIN, MAX) + Dictionary support
            WHEN 'SUM', 'AVG', 'MIN', 'MAX' THEN
                IF v_field_path IS NOT NULL AND v_field_path != '' THEN
                    SELECT * INTO v_resolved FROM resolve_field_path(p_scheme_id, v_field_path);
                    IF v_resolved.structure_id IS NOT NULL THEN
                        v_join_idx := v_join_idx + 1;
                        v_join_alias := 'f' || v_join_idx;
                        v_column_name := CASE v_resolved.db_type
                            WHEN 'Long' THEN '_Long'
                            WHEN 'Double' THEN '_Double'
                            WHEN 'Numeric' THEN '_Numeric'
                            ELSE '_Long'
                        END;
                        -- 🆕 Dictionary support
                        IF v_resolved.dict_key IS NOT NULL THEN
                            v_join_parts := array_append(v_join_parts,
                                format('LEFT JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index = %L',
                                    v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias, v_resolved.dict_key));
                        ELSIF v_resolved.array_index IS NOT NULL THEN
                            v_join_parts := array_append(v_join_parts,
                                format('LEFT JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index = %L',
                                    v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias, v_resolved.array_index::text));
                        ELSE
                            v_join_parts := array_append(v_join_parts,
                                format('LEFT JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index IS NULL',
                                    v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias));
                        END IF;
                        v_select_parts := array_append(v_select_parts,
                            format('%s(%s.%s) %s AS "%s"', v_func_name, v_join_alias, v_column_name, v_over_clause, v_alias));
                    END IF;
                END IF;
                
            -- Offset functions (LAG, LEAD, FIRST_VALUE, LAST_VALUE) + Dictionary support
            WHEN 'LAG', 'LEAD', 'FIRST_VALUE', 'LAST_VALUE' THEN
                IF v_field_path IS NOT NULL AND v_field_path != '' THEN
                    SELECT * INTO v_resolved FROM resolve_field_path(p_scheme_id, v_field_path);
                    IF v_resolved.structure_id IS NOT NULL THEN
                        v_join_idx := v_join_idx + 1;
                        v_join_alias := 'l' || v_join_idx;
                        v_column_name := CASE v_resolved.db_type
                            WHEN 'Long' THEN '_Long'
                            WHEN 'Double' THEN '_Double'
                            WHEN 'Numeric' THEN '_Numeric'
                            WHEN 'String' THEN '_String'
                            ELSE '_Long'
                        END;
                        -- 🆕 Dictionary support
                        IF v_resolved.dict_key IS NOT NULL THEN
                            v_join_parts := array_append(v_join_parts,
                                format('LEFT JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index = %L',
                                    v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias, v_resolved.dict_key));
                        ELSIF v_resolved.array_index IS NOT NULL THEN
                            v_join_parts := array_append(v_join_parts,
                                format('LEFT JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index = %L',
                                    v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias, v_resolved.array_index::text));
                        ELSE
                            v_join_parts := array_append(v_join_parts,
                                format('LEFT JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index IS NULL',
                                    v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias));
                        END IF;
                        v_select_parts := array_append(v_select_parts,
                            format('%s(%s.%s) %s AS "%s"', v_func_name, v_join_alias, v_column_name, v_over_clause, v_alias));
                    END IF;
                END IF;
                
            ELSE
                -- Skip unknown functions
                NULL;
        END CASE;
    END LOOP;
    
    -- =========================================
    -- 6. Filter
    -- =========================================
    IF p_filter_json IS NOT NULL AND p_filter_json != 'null'::jsonb THEN
        BEGIN
            v_object_ids := get_filtered_object_ids(p_scheme_id, p_filter_json, 10);
        EXCEPTION WHEN OTHERS THEN
            v_object_ids := NULL;
        END;
        
        IF v_object_ids IS NOT NULL AND array_length(v_object_ids, 1) > 0 THEN
            v_where_clause := format(' AND o._id = ANY(ARRAY[%s]::bigint[])', 
                array_to_string(v_object_ids, ','));
        END IF;
    END IF;
    
    -- =========================================
    -- 7. Assemble and execute SQL
    -- =========================================
    v_sql := format(
        'SELECT jsonb_agg(row_to_json(t)) FROM (
            SELECT %s
            FROM _objects o
            %s
            WHERE o._id_scheme = %s%s
            LIMIT %s
        ) t',
        array_to_string(v_select_parts, ', '),
        array_to_string(v_join_parts, ' '),
        p_scheme_id,
        v_where_clause,
        p_limit
    );
    
    EXECUTE v_sql INTO v_result;
    
    RETURN COALESCE(v_result, '[]'::jsonb);
END;
$BODY$;

COMMENT ON FUNCTION query_with_window IS 
'Query with window functions for EAV.
Supports: simple fields, arrays Items[2], dictionaries PhoneBook[home].
Example (simple fields):
  SELECT query_with_window(
    1002,
    ''[{"field":"Name","alias":"Name"},{"field":"Stock","alias":"Stock"}]''::jsonb,
    ''[{"func":"ROW_NUMBER","alias":"Rank"}]''::jsonb,
    ''[{"field":"Tag"}]''::jsonb,
    ''[{"field":"Stock","dir":"DESC"}]''::jsonb,
    NULL,
    100
  );
Example (Dictionary):
  SELECT query_with_window(
    1002,
    ''[{"field":"PhoneBook[home]","alias":"HomePhone"}]''::jsonb,
    ''[{"func":"ROW_NUMBER","alias":"Rank"}]''::jsonb,
    ''[]''::jsonb,
    ''[{"field":"PhoneBook[home]","dir":"ASC"}]''::jsonb,
    NULL,
    100
  );';
