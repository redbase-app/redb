-- =====================================================================
-- 23_pvt_window.sql
-- ---------------------------------------------------------------------
-- Window functions over PVT source. Window calls are scalar expressions
-- carrying an OVER (...) clause; the compiler is exposed both as an
-- explicit pvt_build_window_expr() entry point and as a `$over` scalar-
-- expression key so callers can drop windows anywhere a scalar fits
-- (PostgreSQL itself enforces SELECT/ORDER-BY only at execution time).
--
-- Window node shape:
--   {
--     "func":          "row_number" | "rank" | "dense_rank"
--                    | "percent_rank" | "cume_dist" | "ntile"
--                    | "lag" | "lead" | "first_value" | "last_value"
--                    | "nth_value"
--                    | "sum" | "avg" | "min" | "max" | "count",
--     "args":          [<scalar-expr>, ...]?,  -- function arguments
--     "partition_by":  [<order-shape-entry>, ...]?,
--     "order_by":      [<order-shape-entry-with-direction>, ...]?,
--     "frame": {
--         "type": "rows" | "range" | "groups",
--         "start": <bound>,
--         "end":   <bound>?,
--         "exclude": "current_row" | "group" | "ties" | "no_others"?
--     }?
--   }
--
-- Frame bound forms:
--   "unbounded_preceding"
--   "current_row"
--   "unbounded_following"
--   { "preceding": <int> }
--   { "following": <int> }
--
--   pvt_build_window_sql(
--       p_scheme_id    bigint,
--       p_filter       jsonb,
--       p_select       jsonb,    -- non-empty array of select entries
--       p_order        jsonb,
--       p_limit        integer,
--       p_offset       integer,
--       p_source_mode  text,
--       p_tree_ids     bigint[],
--       p_max_depth    integer,
--       p_include_seed boolean,
--       p_polymorphic  boolean
--   ) RETURNS text
--
-- Select entries follow the ORDER-BY shape: {field|field_path|$expr,
-- alias?}. Aggregates and window expressions are emitted via $expr.
-- =====================================================================


-- ---------- pvt_window_field_names ------------------------------------
-- Recursively yield every field path referenced inside a window node
-- (args + partition_by + order_by). Used by orchestrators to extend the
-- fields map fed into pvt_build_cte_sql.
CREATE OR REPLACE FUNCTION pvt_window_field_names(p_node jsonb)
RETURNS SETOF text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_elem jsonb;
BEGIN
    IF p_node IS NULL OR jsonb_typeof(p_node) <> 'object' THEN
        RETURN;
    END IF;

    IF p_node ? 'args' AND jsonb_typeof(p_node->'args') = 'array' THEN
        FOR v_elem IN SELECT value FROM jsonb_array_elements(p_node->'args') LOOP
            RETURN QUERY SELECT pvt_expr_field_names(v_elem);
        END LOOP;
    END IF;

    IF p_node ? 'partition_by' AND jsonb_typeof(p_node->'partition_by') = 'array' THEN
        FOR v_elem IN SELECT value FROM jsonb_array_elements(p_node->'partition_by') LOOP
            IF v_elem ? '$expr' THEN
                RETURN QUERY SELECT pvt_expr_field_names(v_elem->'$expr');
            ELSIF v_elem ? 'field' OR v_elem ? 'field_path' THEN
                RETURN NEXT COALESCE(v_elem->>'field', v_elem->>'field_path');
            END IF;
        END LOOP;
    END IF;

    IF p_node ? 'order_by' AND jsonb_typeof(p_node->'order_by') = 'array' THEN
        FOR v_elem IN SELECT value FROM jsonb_array_elements(p_node->'order_by') LOOP
            IF v_elem ? '$expr' THEN
                RETURN QUERY SELECT pvt_expr_field_names(v_elem->'$expr');
            ELSIF v_elem ? 'field' OR v_elem ? 'field_path' THEN
                RETURN NEXT COALESCE(v_elem->>'field', v_elem->>'field_path');
            END IF;
        END LOOP;
    END IF;
END;
$BODY$;

COMMENT ON FUNCTION pvt_window_field_names(jsonb) IS
    'Recursively yields every field path referenced inside a window node (args + partition_by + order_by). Used by pvt_build_window_sql to extend the fields map.';


-- ---------- _pvt_compile_frame_bound (internal) -----------------------
CREATE OR REPLACE FUNCTION _pvt_compile_frame_bound(p_bound jsonb, p_label text)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
AS $BODY$
DECLARE
    v_t text;
    v_n bigint;
BEGIN
    IF p_bound IS NULL OR jsonb_typeof(p_bound) = 'null' THEN
        RAISE EXCEPTION '_pvt_compile_frame_bound: frame.% is required', p_label;
    END IF;
    v_t := jsonb_typeof(p_bound);
    IF v_t = 'string' THEN
        DECLARE v_s text := lower(p_bound #>> '{}'); BEGIN
            IF v_s = 'unbounded_preceding' THEN RETURN 'UNBOUNDED PRECEDING'; END IF;
            IF v_s = 'current_row'         THEN RETURN 'CURRENT ROW';         END IF;
            IF v_s = 'unbounded_following' THEN RETURN 'UNBOUNDED FOLLOWING'; END IF;
            RAISE EXCEPTION '_pvt_compile_frame_bound: invalid string bound "%"', v_s;
        END;
    END IF;
    IF v_t = 'object' THEN
        IF p_bound ? 'preceding' THEN
            v_n := (p_bound->>'preceding')::bigint;
            RETURN v_n::text || ' PRECEDING';
        END IF;
        IF p_bound ? 'following' THEN
            v_n := (p_bound->>'following')::bigint;
            RETURN v_n::text || ' FOLLOWING';
        END IF;
    END IF;
    RAISE EXCEPTION '_pvt_compile_frame_bound: bound must be "unbounded_preceding"/"current_row"/"unbounded_following" or {preceding|following: N}, got %', p_bound::text;
END;
$BODY$;


-- ---------- _pvt_compile_window_over (internal) -----------------------
-- Emit `OVER (PARTITION BY ... ORDER BY ... ROWS BETWEEN ... AND ...)`.
-- Returns the empty string when no window keys are present (caller emits
-- `OVER ()` for those, e.g. unframed ROW_NUMBER).
CREATE OR REPLACE FUNCTION _pvt_compile_window_over(
    p_node        jsonb,
    p_fields      jsonb,
    p_base_prefix text
)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_parts        text[] := ARRAY[]::text[];
    v_pb_parts     text[] := ARRAY[]::text[];
    v_elem         jsonb;
    v_order_sql    text;
    v_frame        jsonb;
    v_frame_kw     text;
    v_frame_start  text;
    v_frame_end    text;
    v_frame_excl   text;
    v_frame_sql    text;
    v_synth_order  jsonb;
BEGIN
    -- PARTITION BY -------------------------------------------------
    IF p_node ? 'partition_by'
       AND jsonb_typeof(p_node->'partition_by') = 'array'
       AND jsonb_array_length(p_node->'partition_by') > 0 THEN
        FOR v_elem IN SELECT value FROM jsonb_array_elements(p_node->'partition_by') LOOP
            v_pb_parts := v_pb_parts || _pvt_compile_order_col(v_elem, p_fields, p_base_prefix, true);
        END LOOP;
        v_parts := v_parts || ('PARTITION BY ' || array_to_string(v_pb_parts, ', '));
    END IF;

    -- ORDER BY (reuse pvt_build_order_conditions to keep direction/
    -- NULLS handling consistent; strip the leading "ORDER BY " prefix
    -- because we are inside OVER (...)).
    IF p_node ? 'order_by'
       AND jsonb_typeof(p_node->'order_by') = 'array'
       AND jsonb_array_length(p_node->'order_by') > 0 THEN
        v_synth_order := p_node->'order_by';
        v_order_sql := pvt_build_order_conditions(v_synth_order, p_fields, p_base_prefix, NULL);
        -- pvt_build_order_conditions returns either '' or E'\nORDER BY ...'.
        IF v_order_sql IS NOT NULL AND v_order_sql <> '' THEN
            v_parts := v_parts || ltrim(v_order_sql, E'\n');
        END IF;
    END IF;

    -- FRAME ---------------------------------------------------------
    IF p_node ? 'frame' THEN
        v_frame := p_node->'frame';
        IF jsonb_typeof(v_frame) <> 'object' THEN
            RAISE EXCEPTION '_pvt_compile_window_over: frame must be an object';
        END IF;
        v_frame_kw := upper(COALESCE(v_frame->>'type', 'rows'));
        IF v_frame_kw NOT IN ('ROWS', 'RANGE', 'GROUPS') THEN
            RAISE EXCEPTION '_pvt_compile_window_over: frame.type must be rows|range|groups (got %)', v_frame_kw;
        END IF;
        v_frame_start := _pvt_compile_frame_bound(v_frame->'start', 'start');
        IF v_frame ? 'end' AND jsonb_typeof(v_frame->'end') <> 'null' THEN
            v_frame_end := _pvt_compile_frame_bound(v_frame->'end', 'end');
            v_frame_sql := v_frame_kw || ' BETWEEN ' || v_frame_start || ' AND ' || v_frame_end;
        ELSE
            v_frame_sql := v_frame_kw || ' ' || v_frame_start;
        END IF;
        IF v_frame ? 'exclude' AND jsonb_typeof(v_frame->'exclude') = 'string' THEN
            v_frame_excl := lower(v_frame->>'exclude');
            v_frame_sql := v_frame_sql || ' EXCLUDE ' || CASE v_frame_excl
                WHEN 'current_row' THEN 'CURRENT ROW'
                WHEN 'group'       THEN 'GROUP'
                WHEN 'ties'        THEN 'TIES'
                WHEN 'no_others'   THEN 'NO OTHERS'
                ELSE NULL
            END;
            IF v_frame_excl NOT IN ('current_row', 'group', 'ties', 'no_others') THEN
                RAISE EXCEPTION '_pvt_compile_window_over: frame.exclude must be current_row|group|ties|no_others (got %)', v_frame_excl;
            END IF;
        END IF;
        v_parts := v_parts || v_frame_sql;
    END IF;

    IF array_length(v_parts, 1) IS NULL THEN
        RETURN 'OVER ()';
    END IF;
    RETURN 'OVER (' || array_to_string(v_parts, ' ') || ')';
END;
$BODY$;


-- ---------- pvt_build_window_expr -------------------------------------
CREATE OR REPLACE FUNCTION pvt_build_window_expr(
    p_node        jsonb,
    p_fields      jsonb,
    p_base_prefix text DEFAULT ''
)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_func     text;
    v_args     jsonb;
    v_args_sql text[] := ARRAY[]::text[];
    v_elem     jsonb;
    v_arg_sql  text;
    v_call     text;
    v_over     text;
    v_distinct boolean := false;
    v_filter   jsonb;
    v_filt_sql text := '';
BEGIN
    IF p_node IS NULL OR jsonb_typeof(p_node) <> 'object' THEN
        RAISE EXCEPTION 'pvt_build_window_expr: window node must be an object';
    END IF;
    IF NOT (p_node ? 'func') OR jsonb_typeof(p_node->'func') <> 'string' THEN
        RAISE EXCEPTION 'pvt_build_window_expr: window node must include "func" (string)';
    END IF;

    v_func := lower(p_node->>'func');
    IF v_func NOT IN (
        'row_number', 'rank', 'dense_rank', 'percent_rank', 'cume_dist',
        'ntile', 'lag', 'lead', 'first_value', 'last_value', 'nth_value',
        'sum', 'avg', 'min', 'max', 'count'
    ) THEN
        RAISE EXCEPTION 'pvt_build_window_expr: unsupported window func "%"', v_func;
    END IF;

    -- Build the argument list. count(*) is a special shorthand.
    IF p_node ? 'args' THEN
        v_args := p_node->'args';
        IF v_func = 'count'
           AND jsonb_typeof(v_args) = 'array'
           AND jsonb_array_length(v_args) = 1
           AND jsonb_typeof(v_args->0) = 'string'
           AND (v_args->0 #>> '{}') = '*' THEN
            v_call := 'COUNT(*)';
        ELSE
            IF jsonb_typeof(v_args) <> 'array' THEN
                RAISE EXCEPTION 'pvt_build_window_expr: args must be an array (got %)', jsonb_typeof(v_args);
            END IF;
            FOR v_elem IN SELECT value FROM jsonb_array_elements(v_args) LOOP
                v_arg_sql := pvt_build_scalar_expr(v_elem, p_fields, p_base_prefix);
                v_args_sql := v_args_sql || v_arg_sql;
            END LOOP;
            v_call := upper(v_func) || '(' || array_to_string(v_args_sql, ', ') || ')';
        END IF;
    ELSE
        IF v_func IN ('ntile', 'lag', 'lead', 'first_value', 'last_value', 'nth_value') THEN
            RAISE EXCEPTION 'pvt_build_window_expr: % requires "args"', v_func;
        END IF;
        IF v_func IN ('sum', 'avg', 'min', 'max', 'count') THEN
            RAISE EXCEPTION 'pvt_build_window_expr: aggregate window % requires "args"', v_func;
        END IF;
        v_call := upper(v_func) || '()';
    END IF;

    -- Optional FILTER (WHERE ...) for aggregate windows.
    IF p_node ? 'filter'
       AND v_func IN ('sum', 'avg', 'min', 'max', 'count') THEN
        v_filter := p_node->'filter';
        v_filt_sql := ' FILTER (WHERE ' || pvt_build_bool_expr(v_filter, p_fields, p_base_prefix) || ')';
    END IF;

    v_over := _pvt_compile_window_over(p_node, p_fields, p_base_prefix);
    RETURN v_call || v_filt_sql || ' ' || v_over;
END;
$BODY$;

COMMENT ON FUNCTION pvt_build_window_expr(jsonb, jsonb, text) IS
    'Compiles a window-function node into a SQL fragment `FUNC(args) [FILTER (WHERE ...)] OVER (...)`. Supports ranking, navigation (lag/lead/first_value/...), and aggregate windows with optional FILTER. Frame spec: rows|range|groups + start/end bounds + optional exclude.';


-- ---------- Hook $over into pvt_build_scalar_expr ---------------------
-- We can't extend scalar_expr from this file, but $over is recognised by
-- the existing dispatcher's "no matching operator" branch via a tiny
-- shim wrapper: callers may use either {"$over": <window_node>} or a
-- bare window node passed directly to pvt_build_window_expr.
-- The shim is registered as a scalar-expr operator by replacing the
-- final RAISE in pvt_build_scalar_expr -- see 17_pvt_expr.sql tail.


-- ---------------------------------------------------------------------
-- pvt_build_window_sql
-- ---------------------------------------------------------------------
DROP FUNCTION IF EXISTS pvt_build_window_sql(bigint, jsonb, jsonb, jsonb, integer, integer, text, bigint[], integer, boolean, boolean);

CREATE OR REPLACE FUNCTION pvt_build_window_sql(
    p_scheme_id    bigint,
    p_filter       jsonb,
    p_select       jsonb,
    p_order        jsonb    DEFAULT NULL,
    p_limit        integer  DEFAULT NULL,
    p_offset       integer  DEFAULT 0,
    p_source_mode  text     DEFAULT 'flat',
    p_tree_ids     bigint[] DEFAULT NULL,
    p_max_depth    integer  DEFAULT NULL,
    p_include_seed boolean  DEFAULT true,
    p_polymorphic  boolean  DEFAULT true
)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_fields        jsonb;
    v_extra         jsonb;
    v_extra_names   text[];
    v_cte_sql       text;
    v_where_sql     text;
    v_order_sql     text;
    v_paging        text := '';
    v_force_outer   boolean;
    v_narrow        boolean;
    v_push_sql      text;
    v_residual      jsonb;
    v_outer_filter  jsonb;
    v_prefix        text;
    v_sel_entry     jsonb;
    v_sel_idx       integer := 0;
    v_sel_col       text;
    v_sel_alias     text;
    v_sel_seen      text[] := ARRAY[]::text[];
    v_select_parts  text[] := ARRAY[]::text[];
    v_select_sql    text;
    v_win_node      jsonb;
BEGIN
    IF p_scheme_id IS NULL THEN
        RAISE EXCEPTION 'pvt_build_window_sql: p_scheme_id is required';
    END IF;
    IF p_select IS NULL
       OR jsonb_typeof(p_select) <> 'array'
       OR jsonb_array_length(p_select) = 0 THEN
        RAISE EXCEPTION 'pvt_build_window_sql: p_select must be a non-empty JSON array';
    END IF;

    -- 1. Collect fields from filter + select + order.
    v_fields := pvt_collect_fields(
        p_scheme_id,
        p_filter,
        CASE WHEN p_order IS NULL THEN p_select ELSE p_select || p_order END,
        false);

    -- 2. Extend with fields referenced inside window args / partition /
    --    order. Walk every $expr in the select array and harvest from
    --    nested $over nodes via pvt_window_field_names. (Regular $field
    --    refs inside $expr are already covered by pvt_collect_fields.)
    v_extra_names := ARRAY(
        SELECT DISTINCT n
          FROM (
            SELECT pvt_window_field_names(e->'$expr'->'$over') AS n
              FROM jsonb_array_elements(p_select) e
             WHERE e ? '$expr' AND (e->'$expr') ? '$over'
          ) z WHERE n IS NOT NULL);
    v_extra := pvt_collect_extra_fields(p_scheme_id, v_extra_names);
    v_fields := v_fields || v_extra;

    -- 3. Base pushdown.
    SELECT s.v_pushdown_sql, s.v_residual_filter
      INTO v_push_sql, v_residual
      FROM pvt_split_filter(p_filter, v_fields) s;
    v_outer_filter := v_residual;

    -- 4. Narrow vs wide. Tree modes always go wide (CTE keyed on _id).
    v_force_outer := pvt_has_absence_check(v_outer_filter) OR (v_fields = '{}'::jsonb);
    v_narrow := p_source_mode = 'flat'
                AND NOT v_force_outer
                AND NOT EXISTS (
                    SELECT 1
                      FROM jsonb_each(v_fields) AS e(k, val)
                     WHERE (val->>'dict_key') IS NOT NULL
                       AND (val->>'parent_sid') IS NOT NULL);
    v_prefix := CASE WHEN v_narrow THEN 'o.' ELSE '' END;

    -- 5. Shape A shortcut.
    IF v_outer_filter IS NULL
       AND p_source_mode = 'flat'
       AND (p_tree_ids IS NULL OR array_length(p_tree_ids, 1) IS NULL)
       AND NOT EXISTS (
            SELECT 1
              FROM jsonb_each(v_fields) AS e(k, val)
             WHERE COALESCE(val->>'kind', '') <> 'base'
       ) THEN
        v_prefix := 'o.';
        v_narrow := false;
    ELSE
        v_cte_sql := pvt_build_cte_sql(
            p_scheme_id    => p_scheme_id,
            p_fields       => v_fields,
            p_source_mode  => p_source_mode,
            p_tree_ids     => p_tree_ids,
            p_max_depth    => p_max_depth,
            p_force_outer  => v_force_outer,
            p_extra_where  => v_push_sql,
            p_narrow       => v_narrow,
            p_include_seed => p_include_seed,
            p_polymorphic  => p_polymorphic
        );
    END IF;

    -- 6. Build select projection.
    FOR v_sel_entry IN SELECT value FROM jsonb_array_elements(p_select) LOOP
        v_sel_idx := v_sel_idx + 1;
        -- $over shortcut: {"$over": <win>} as the entry's expression.
        IF v_sel_entry ? '$expr' AND jsonb_typeof(v_sel_entry->'$expr') = 'object'
           AND (v_sel_entry->'$expr') ? '$over' THEN
            v_win_node := (v_sel_entry->'$expr')->'$over';
            v_sel_col := pvt_build_window_expr(v_win_node, v_fields, v_prefix);
        ELSE
            v_sel_col := _pvt_compile_order_col(v_sel_entry, v_fields, v_prefix, true);
        END IF;
        v_sel_alias := CASE
            WHEN v_sel_entry ? 'alias' AND jsonb_typeof(v_sel_entry->'alias') = 'string'
                THEN v_sel_entry->>'alias'
            WHEN v_sel_entry ? 'field'
                THEN v_sel_entry->>'field'
            WHEN v_sel_entry ? 'field_path'
                THEN v_sel_entry->>'field_path'
            ELSE '_sel_' || v_sel_idx::text
        END;
        IF v_sel_alias = ANY(v_sel_seen) THEN
            RAISE EXCEPTION 'pvt_build_window_sql: duplicate select alias "%"', v_sel_alias;
        END IF;
        v_sel_seen := v_sel_seen || v_sel_alias;
        v_select_parts := v_select_parts || (v_sel_col || ' AS ' || quote_ident(v_sel_alias));
    END LOOP;
    v_select_sql := array_to_string(v_select_parts, ', ');

    -- 7. WHERE / ORDER BY / paging.
    v_where_sql := pvt_build_where_from_json(v_outer_filter, v_fields, v_prefix);
    v_order_sql := pvt_build_order_conditions(p_order, v_fields, v_prefix, NULL);

    IF p_limit IS NOT NULL AND p_limit >= 0 THEN
        v_paging := v_paging || E'\nLIMIT ' || p_limit::text;
    END IF;
    IF COALESCE(p_offset, 0) > 0 THEN
        v_paging := v_paging || E'\nOFFSET ' || p_offset::text;
    END IF;

    -- 8. Assemble.
    IF v_cte_sql IS NULL THEN
        RETURN 'SELECT ' || v_select_sql
            || ' FROM _objects o'
            || E'\nWHERE o._id_scheme = ' || p_scheme_id::text
            || CASE WHEN v_push_sql IS NOT NULL AND v_push_sql <> ''
                    THEN ' AND ' || v_push_sql ELSE '' END
            || v_order_sql
            || v_paging;
    END IF;

    IF v_narrow THEN
        RETURN v_cte_sql
            || E'\nSELECT ' || v_select_sql
            || ' FROM _pvt_cte'
            || E'\nJOIN _objects o ON o._id = _pvt_cte._id_object'
            || E'\nWHERE ' || v_where_sql
            || v_order_sql
            || v_paging;
    END IF;

    RETURN v_cte_sql
        || E'\nSELECT ' || v_select_sql
        || ' FROM _pvt_cte'
        || E'\nWHERE ' || v_where_sql
        || v_order_sql
        || v_paging;
END;
$BODY$;

COMMENT ON FUNCTION pvt_build_window_sql(bigint, jsonb, jsonb, jsonb, integer, integer, text, bigint[], integer, boolean, boolean) IS
    'Window-function orchestrator. p_select is a non-empty array of {field|field_path|$expr, alias?} entries; window expressions are written as {alias, $expr: {$over: {func, args, partition_by, order_by, frame}}}. Tree variants reuse the same p_source_mode / p_tree_ids / p_max_depth / p_include_seed / p_polymorphic plumbing as pvt_build_query_sql.';
