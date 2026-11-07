-- =====================================================================
-- 26_pvt_array_groupby.sql
-- ---------------------------------------------------------------------
-- Array-element GROUP BY orchestrator. Sister of 22_pvt_groupby.sql but
-- operates on a flat element-level subquery (LEFT JOINs on _values
-- keyed via _array_parent_id), so it intentionally bypasses the
-- object-pivot CTE machinery.
--
--   pvt_build_array_groupby_sql(
--       p_scheme_id     bigint,
--       p_array_path    text,        -- e.g. 'Items'
--       p_filter        jsonb,       -- optional outer object filter (PVT shape); when
--                                    -- non-empty, compiled via pvt_build_query_sql and
--                                    -- applied as arr._id_object IN (<filtered ids>).
--       p_group_by      jsonb,       -- non-empty array of {field, alias?}
--       p_aggregations  jsonb,       -- optional array of legacy agg entries
--                                    -- ({field, func, alias}) - same shape as
--                                    -- aggregate_array_grouped consumes
--       p_having        jsonb,       -- optional bool expression (PVT shape:
--                                    -- $and/$or/$not + $gt/$gte/$lt/$lte/$eq/$ne
--                                    -- with $sum/$count/$avg/$min/$max/$field/$const)
--       p_order         jsonb,       -- optional [{field, asc?}] over outer aliases
--       p_limit         integer,
--       p_offset        integer
--   ) RETURNS text
--
-- Inner subquery shape:
--   SELECT g1.<typed_col> AS "Field1", a1.<typed_col> AS "Field2", ...
--     FROM _values arr
--     JOIN _objects o ON o._id = arr._id_object
--     LEFT JOIN _values g1 ON g1._id_object = arr._id_object
--                         AND g1._id_structure = <sid>
--                         AND g1._array_parent_id = arr._id
--     ...
--    WHERE o._id_scheme = <s> AND arr._id_structure = <arr_sid>
--      AND arr._array_index IS NOT NULL
--
-- Outer query: SELECT/GROUP BY/HAVING/ORDER/LIMIT/OFFSET over inner.
--
-- HAVING reuses pvt_build_bool_expr against a fields-map populated for
-- non-base ('props' kind) entries, so $field references resolve to the
-- bare quoted alias emitted by the inner subquery.
-- =====================================================================

DROP FUNCTION IF EXISTS pvt_build_array_groupby_sql(bigint, text, jsonb, jsonb, jsonb, jsonb, jsonb, integer, integer);

CREATE OR REPLACE FUNCTION pvt_build_array_groupby_sql(
    p_scheme_id    bigint,
    p_array_path   text,
    p_filter       jsonb    DEFAULT NULL,
    p_group_by     jsonb    DEFAULT NULL,
    p_aggregations jsonb    DEFAULT NULL,
    p_having       jsonb    DEFAULT NULL,
    p_order        jsonb    DEFAULT NULL,
    p_limit        integer  DEFAULT NULL,
    p_offset       integer  DEFAULT 0
)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_arr_sid       bigint;
    v_grp_entry     jsonb;
    v_agg_entry     jsonb;
    v_ord_entry     jsonb;
    v_field_path    text;
    v_alias         text;
    v_func          text;
    v_resolved      record;
    v_join_alias    text;
    v_col_name      text;
    v_join_idx      integer := 0;
    v_join_parts    text[] := ARRAY[]::text[];
    v_inner_select  text[] := ARRAY[]::text[];
    v_fields_map    jsonb := '{}'::jsonb;
    v_select_parts  text[] := ARRAY[]::text[];
    v_group_parts   text[] := ARRAY[]::text[];
    v_alias_seen    text[] := ARRAY[]::text[];
    v_ord_parts     text[] := ARRAY[]::text[];
    v_having_sql    text := '';
    v_paging        text := '';
    v_inner_sql     text;
    v_filter_sql    text;
    v_filter_clause text := '';
BEGIN
    IF p_scheme_id IS NULL THEN
        RAISE EXCEPTION 'pvt_build_array_groupby_sql: p_scheme_id is required';
    END IF;
    IF p_array_path IS NULL OR p_array_path = '' THEN
        RAISE EXCEPTION 'pvt_build_array_groupby_sql: p_array_path is required';
    END IF;
    IF p_group_by IS NULL
       OR jsonb_typeof(p_group_by) <> 'array'
       OR jsonb_array_length(p_group_by) = 0 THEN
        RAISE EXCEPTION 'pvt_build_array_groupby_sql: p_group_by must be a non-empty JSON array';
    END IF;
    -- Outer object filter: compile via pvt_build_query_sql into an
    -- "_id IN (...)" clause applied at the inner level (before unnest).
    IF p_filter IS NOT NULL
       AND jsonb_typeof(p_filter) = 'object'
       AND p_filter <> '{}'::jsonb THEN
        v_filter_sql := pvt_build_query_sql(
            p_scheme_id => p_scheme_id,
            p_filter    => p_filter,
            p_limit     => NULL,
            p_offset    => 0,
            p_order     => NULL,
            p_max_depth => NULL,
            p_distinct  => false);
        v_filter_clause := format(
            E'\n  AND arr._id_object IN (SELECT _id FROM (%s) _filt)',
            v_filter_sql);
    END IF;

    -- Resolve array structure id
    SELECT r.structure_id INTO v_arr_sid
    FROM pvt_resolve_field_path_table(p_scheme_id, p_array_path) r;
    IF v_arr_sid IS NULL THEN
        RAISE EXCEPTION 'pvt_build_array_groupby_sql: array "%" not found in scheme %',
            p_array_path, p_scheme_id;
    END IF;

    -- ---- group_by entries: register joins, inner aliases, outer GROUP BY parts
    FOR v_grp_entry IN SELECT value FROM jsonb_array_elements(p_group_by) LOOP
        v_field_path := v_grp_entry->>'field';
        IF v_field_path IS NULL OR v_field_path = '' THEN
            RAISE EXCEPTION 'pvt_build_array_groupby_sql: group_by entry missing "field"';
        END IF;
        v_alias := COALESCE(v_grp_entry->>'alias', v_field_path);
        IF v_alias = ANY(v_alias_seen) THEN
            RAISE EXCEPTION 'pvt_build_array_groupby_sql: duplicate alias "%"', v_alias;
        END IF;

        SELECT r.structure_id, r.db_type INTO v_resolved
        FROM pvt_resolve_field_path_table(p_scheme_id, p_array_path || '[].' || v_field_path) r;
        IF v_resolved.structure_id IS NULL THEN
            RAISE EXCEPTION 'pvt_build_array_groupby_sql: group field "%" not found inside array "%"',
                v_field_path, p_array_path;
        END IF;

        v_join_idx := v_join_idx + 1;
        v_join_alias := 'g' || v_join_idx::text;
        v_col_name := pvt_db_type_to_value_column(v_resolved.db_type);
        IF v_col_name IS NULL THEN
            RAISE EXCEPTION 'pvt_build_array_groupby_sql: unsupported db_type "%" for field "%"',
                v_resolved.db_type, v_field_path;
        END IF;

        v_join_parts := v_join_parts || format(
            'LEFT JOIN _values %I ON %I._id_object = arr._id_object AND %I._id_structure = %s AND %I._array_parent_id = arr._id',
            v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias);
        -- NOTE: typed value columns (_String/_Long/...) are declared unquoted in DDL
        -- and therefore case-folded to lowercase by PG. Reference them via %s (bare),
        -- never %I (quoted), to avoid 42703 column-not-found errors.
        v_inner_select := v_inner_select || format('%I.%s AS %I',
            v_join_alias, v_col_name, v_field_path);
        v_fields_map := v_fields_map || jsonb_build_object(
            v_field_path,
            jsonb_build_object('kind', 'props', 'db_type', v_resolved.db_type));

        v_select_parts := v_select_parts || (quote_ident(v_field_path) || ' AS ' || quote_ident(v_alias));
        v_group_parts  := v_group_parts  || quote_ident(v_field_path);
        v_alias_seen   := v_alias_seen   || v_alias;
    END LOOP;

    -- ---- aggregations
    IF p_aggregations IS NOT NULL
       AND jsonb_typeof(p_aggregations) = 'array' THEN
        FOR v_agg_entry IN SELECT value FROM jsonb_array_elements(p_aggregations) LOOP
            v_field_path := v_agg_entry->>'field';
            v_func := upper(COALESCE(v_agg_entry->>'func', ''));
            v_alias := COALESCE(v_agg_entry->>'alias', v_func);
            IF v_alias = '' THEN
                RAISE EXCEPTION 'pvt_build_array_groupby_sql: aggregation missing func';
            END IF;
            IF v_alias = ANY(v_alias_seen) THEN
                RAISE EXCEPTION 'pvt_build_array_groupby_sql: duplicate alias "%"', v_alias;
            END IF;

            IF v_func = 'COUNT' AND (v_field_path IS NULL OR v_field_path = '*') THEN
                v_select_parts := v_select_parts || ('COUNT(*) AS ' || quote_ident(v_alias));
                v_alias_seen := v_alias_seen || v_alias;
                CONTINUE;
            END IF;

            -- Add element-field join if not already projected by group_by
            IF NOT (v_fields_map ? v_field_path) THEN
                SELECT r.structure_id, r.db_type INTO v_resolved
                FROM pvt_resolve_field_path_table(p_scheme_id, p_array_path || '[].' || v_field_path) r;
                IF v_resolved.structure_id IS NULL THEN
                    RAISE EXCEPTION 'pvt_build_array_groupby_sql: agg field "%" not found inside array "%"',
                        v_field_path, p_array_path;
                END IF;
                v_join_idx := v_join_idx + 1;
                v_join_alias := 'a' || v_join_idx::text;
                v_col_name := pvt_db_type_to_value_column(v_resolved.db_type);
                IF v_col_name IS NULL THEN
                    RAISE EXCEPTION 'pvt_build_array_groupby_sql: unsupported db_type "%" for agg field "%"',
                        v_resolved.db_type, v_field_path;
                END IF;
                v_join_parts := v_join_parts || format(
                    'LEFT JOIN _values %I ON %I._id_object = arr._id_object AND %I._id_structure = %s AND %I._array_parent_id = arr._id',
                    v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias);
                -- See note above: %s (bare) for typed value columns.
                v_inner_select := v_inner_select || format('%I.%s AS %I',
                    v_join_alias, v_col_name, v_field_path);
                v_fields_map := v_fields_map || jsonb_build_object(
                    v_field_path,
                    jsonb_build_object('kind', 'props', 'db_type', v_resolved.db_type));
            END IF;

            v_select_parts := v_select_parts || (
                v_func || '(' || quote_ident(v_field_path) || ') AS ' || quote_ident(v_alias));
            v_alias_seen := v_alias_seen || v_alias;
        END LOOP;
    END IF;

    -- ---- HAVING: pre-collect $field refs, register joins, then translate
    IF p_having IS NOT NULL
       AND jsonb_typeof(p_having) = 'object'
       AND p_having <> '{}'::jsonb THEN
        FOR v_field_path IN
            SELECT DISTINCT n FROM pvt_expr_field_names(p_having) n WHERE n IS NOT NULL
        LOOP
            IF v_fields_map ? v_field_path THEN
                CONTINUE;
            END IF;
            SELECT r.structure_id, r.db_type INTO v_resolved
            FROM pvt_resolve_field_path_table(p_scheme_id, p_array_path || '[].' || v_field_path) r;
            IF v_resolved.structure_id IS NULL THEN
                RAISE EXCEPTION 'pvt_build_array_groupby_sql: HAVING field "%" not found inside array "%"',
                    v_field_path, p_array_path;
            END IF;
            v_join_idx := v_join_idx + 1;
            v_join_alias := 'h' || v_join_idx::text;
            v_col_name := pvt_db_type_to_value_column(v_resolved.db_type);
            IF v_col_name IS NULL THEN
                RAISE EXCEPTION 'pvt_build_array_groupby_sql: unsupported db_type "%" for HAVING field "%"',
                    v_resolved.db_type, v_field_path;
            END IF;
            v_join_parts := v_join_parts || format(
                'LEFT JOIN _values %I ON %I._id_object = arr._id_object AND %I._id_structure = %s AND %I._array_parent_id = arr._id',
                v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias);
            -- See note above: %s (bare) for typed value columns.
            v_inner_select := v_inner_select || format('%I.%s AS %I',
                v_join_alias, v_col_name, v_field_path);
            v_fields_map := v_fields_map || jsonb_build_object(
                v_field_path,
                jsonb_build_object('kind', 'props', 'db_type', v_resolved.db_type));
        END LOOP;

        v_having_sql := E'\nHAVING ' || pvt_build_bool_expr(p_having, v_fields_map, '');
    END IF;

    -- ---- ORDER BY (over outer aliases; minimal asc/desc support)
    IF p_order IS NOT NULL
       AND jsonb_typeof(p_order) = 'array'
       AND jsonb_array_length(p_order) > 0 THEN
        FOR v_ord_entry IN SELECT value FROM jsonb_array_elements(p_order) LOOP
            v_alias := v_ord_entry->>'field';
            IF v_alias IS NULL OR v_alias = '' THEN
                RAISE EXCEPTION 'pvt_build_array_groupby_sql: order entry missing "field"';
            END IF;
            v_ord_parts := v_ord_parts || (
                quote_ident(v_alias) ||
                CASE WHEN COALESCE((v_ord_entry->>'asc')::boolean, true) THEN '' ELSE ' DESC' END);
        END LOOP;
        v_paging := E'\nORDER BY ' || array_to_string(v_ord_parts, ', ');
    END IF;

    IF p_limit IS NOT NULL AND p_limit >= 0 THEN
        v_paging := v_paging || E'\nLIMIT ' || p_limit::text;
    END IF;
    IF COALESCE(p_offset, 0) > 0 THEN
        v_paging := v_paging || E'\nOFFSET ' || p_offset::text;
    END IF;

    -- ---- Assemble
    v_inner_sql := format(
        E'SELECT %s\nFROM _values arr\nJOIN _objects o ON o._id = arr._id_object\n%s\nWHERE o._id_scheme = %s AND arr._id_structure = %s AND arr._array_index IS NOT NULL%s',
        array_to_string(v_inner_select, ', '),
        array_to_string(v_join_parts, E'\n'),
        p_scheme_id::text,
        v_arr_sid::text,
        v_filter_clause);

    RETURN format(
        E'SELECT %s\nFROM (\n%s\n) elements\nGROUP BY %s%s%s',
        array_to_string(v_select_parts, ', '),
        v_inner_sql,
        array_to_string(v_group_parts, ', '),
        v_having_sql,
        v_paging);
END;
$BODY$;

COMMENT ON FUNCTION pvt_build_array_groupby_sql(bigint, text, jsonb, jsonb, jsonb, jsonb, jsonb, integer, integer) IS
'Builds a GROUP BY query over array elements. Element fields are projected into an inner subquery via LEFT JOINs on _values keyed by _array_parent_id; the outer query groups by inner aliases and applies HAVING through pvt_build_bool_expr.';
