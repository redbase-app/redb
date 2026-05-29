-- =====================================================================
-- 22_pvt_groupby.sql
-- ---------------------------------------------------------------------
-- GROUP BY orchestrator: project group keys + aggregates, with optional
-- HAVING / ORDER BY / LIMIT / OFFSET clauses. Sister of files 20 and 21
-- - reuses the same field-collection, push-down and CTE-shape decisions.
--
--   pvt_build_groupby_sql(
--       p_scheme_id    bigint,
--       p_filter       jsonb,
--       p_group_by     jsonb,    -- non-empty array of {field|$expr,alias?}
--       p_aggregations jsonb,    -- optional array of agg entries (see 19)
--       p_having       jsonb,    -- optional boolean-expression node
--       p_order        jsonb,    -- optional ORDER BY entries
--       p_limit        integer,
--       p_offset       integer,
--       p_source_mode  text,
--       p_tree_ids     bigint[],
--       p_max_depth    integer,
--       p_include_seed boolean,
--       p_polymorphic  boolean
--   ) RETURNS text
--
-- Generated shape (narrow / wide selection mirrors files 20-21):
--
--   SELECT <grp1_sql> AS "<a1>", ..., <agg1_sql> AS "<aa1>", ...
--     FROM (CTE or _objects)
--    WHERE <residual>
--    GROUP BY <grp1_sql>, <grp2_sql>, ...
--   [HAVING <having_sql>]
--   [ORDER BY ...] [LIMIT ...] [OFFSET ...]
--
-- HAVING is a free feature over Pro: Pro currently emits zero HAVING
-- support, so any predicate over an aggregate had to be done client-side.
-- In HAVING, ORDER BY (and select-side projections), aggregates may be
-- written inline as {"$count":"*"} / {"$sum":{"$field":"X"}} thanks to
-- pvt_build_scalar_expr's aggregate-passthrough extension; alias refs
-- are not supported -- repeat the expression (PostgreSQL is fine with
-- HAVING on raw aggregate expressions).
-- =====================================================================

DROP FUNCTION IF EXISTS pvt_build_groupby_sql(bigint, jsonb, jsonb, jsonb, jsonb, jsonb, integer, integer, text, bigint[], integer, boolean, boolean);

CREATE OR REPLACE FUNCTION pvt_build_groupby_sql(
    p_scheme_id    bigint,
    p_filter       jsonb,
    p_group_by     jsonb,
    p_aggregations jsonb    DEFAULT NULL,
    p_having       jsonb    DEFAULT NULL,
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
    v_having_sql    text := '';
    v_order_sql     text;
    v_paging        text := '';
    v_force_outer   boolean;
    v_narrow        boolean;
    v_push_sql      text;
    v_residual      jsonb;
    v_outer_filter  jsonb;
    v_prefix        text;
    v_grp_entry     jsonb;
    v_grp_idx       integer := 0;
    v_grp_col       text;
    v_grp_alias     text;
    v_grp_seen      text[] := ARRAY[]::text[];
    v_select_parts  text[] := ARRAY[]::text[];
    v_groupby_parts text[] := ARRAY[]::text[];
    v_select_sql    text;
    v_groupby_sql   text;
    -- Pro-parity inline GROUP BY: when the field set is pure scalar
    -- pivot (no base/nested-dict/simple-dict/array refs) AND filter is
    -- narrow-eligible, skip the CTE wrapper and emit
    -- `SELECT pvt.<grp>, agg(...) FROM (<inline pivot>) pvt GROUP BY pvt.<grp>`.
    -- The inline pivot lifts `v._array_index IS NULL` out of every
    -- column FILTER into a single outer WHERE -- index-friendly at scale.
    v_inline        boolean := false;
    v_inline_sql    text;
    v_inline_cols   text := '';
    v_inline_sids   text := '';
    v_inline_li_join boolean := false;
    v_inline_first  boolean := true;
    v_fname         text;
    v_fmeta         jsonb;
BEGIN
    IF p_scheme_id IS NULL THEN
        RAISE EXCEPTION 'pvt_build_groupby_sql: p_scheme_id is required';
    END IF;
    IF p_group_by IS NULL
       OR jsonb_typeof(p_group_by) <> 'array'
       OR jsonb_array_length(p_group_by) = 0 THEN
        RAISE EXCEPTION 'pvt_build_groupby_sql: p_group_by must be a non-empty JSON array';
    END IF;

    -- 1. Collect fields from filter + group_by (piped via order channel)
    --    + ORDER BY entries.
    v_fields := pvt_collect_fields(
        p_scheme_id,
        p_filter,
        CASE
            WHEN p_order IS NULL THEN p_group_by
            ELSE p_group_by || p_order
        END,
        false);

    -- 2. Extend with fields from aggregations + HAVING.
    v_extra_names := ARRAY(
        SELECT DISTINCT n FROM (
            SELECT pvt_aggregations_field_names(p_aggregations) AS n
            UNION ALL
            SELECT pvt_expr_field_names(p_having) AS n
        ) z WHERE n IS NOT NULL);
    v_extra := pvt_collect_extra_fields(p_scheme_id, v_extra_names);
    v_fields := v_fields || v_extra;

    -- 3. Base pushdown.
    SELECT s.v_pushdown_sql, s.v_residual_filter
      INTO v_push_sql, v_residual
      FROM pvt_split_filter(p_filter, v_fields) s;
    v_outer_filter := v_residual;

    -- 4. Narrow vs wide. Tree modes always go wide (pvt_build_cte_sql
    -- emits base-cols CTE keyed on _id in tree mode, not _id_object).
    v_force_outer := pvt_has_absence_check(v_outer_filter) OR (v_fields = '{}'::jsonb);
    v_narrow := p_source_mode = 'flat'
                AND NOT v_force_outer
                AND NOT EXISTS (
                    SELECT 1
                      FROM jsonb_each(v_fields) AS e(k, val)
                     WHERE (val->>'dict_key') IS NOT NULL
                       AND (val->>'parent_sid') IS NOT NULL);
    v_prefix := CASE WHEN v_narrow THEN 'o.' ELSE '' END;

    -- 5. Shape A shortcut: pure-base flat group-by, no aggregates touch
    --    PROPS. Skip CTE entirely.
    IF v_outer_filter IS NULL
       AND p_source_mode = 'flat'
       AND (p_tree_ids IS NULL OR array_length(p_tree_ids, 1) IS NULL)
       AND NOT EXISTS (
            SELECT 1
              FROM jsonb_each(v_fields) AS e(k, val)
             WHERE COALESCE(val->>'kind', '') <> 'base'
       ) THEN
        v_prefix := 'o.';
        v_narrow := false;   -- mark so assembly below picks the "no CTE" branch
    ELSE
        -- 5b. Pro-parity inline GROUP BY eligibility. Pure scalar pivot,
        --     narrow shape, no tree, no base refs anywhere (filter,
        --     group_by, having, order, aggregations -- all live in v_fields).
        v_inline := v_narrow
                    AND p_source_mode = 'flat'
                    AND (p_tree_ids IS NULL OR array_length(p_tree_ids, 1) IS NULL)
                    AND NOT EXISTS (
                        SELECT 1 FROM jsonb_each(v_fields) AS e(k, val)
                         WHERE COALESCE(val->>'kind', '') = 'base'
                    )
                    AND NOT EXISTS (
                        SELECT 1 FROM jsonb_each(v_fields) AS e(k, val)
                         WHERE COALESCE(val->>'kind', '') <> 'base'
                           AND (
                                COALESCE((val->>'is_array')::boolean, false)
                             OR (val->>'dict_key') IS NOT NULL
                             OR (val->>'parent_sid') IS NOT NULL
                             OR COALESCE((val->>'length_modifier')::boolean, false)
                           )
                    );

        IF v_inline THEN
            -- Build inline pivot subquery: project _id + every scalar
            -- pivot column WITHOUT `_array_index IS NULL` per-FILTER
            -- (hoisted to outer WHERE). Cuts FILTER overhead and lets
            -- the planner use a single ANY()-index scan.
            FOR v_fname, v_fmeta IN SELECT key, value FROM jsonb_each(v_fields) LOOP
                IF COALESCE(v_fmeta->>'kind', '') = 'base' THEN CONTINUE; END IF;
                IF NOT v_inline_first THEN
                    v_inline_cols := v_inline_cols || ',' || E'\n        ';
                ELSE
                    v_inline_cols := E',\n        ';
                    v_inline_first := false;
                END IF;
                v_inline_cols := v_inline_cols
                              || pvt_build_column_expr(v_fname, v_fmeta, true);
                IF v_inline_sids <> '' THEN
                    v_inline_sids := v_inline_sids || ', ';
                END IF;
                v_inline_sids := v_inline_sids || (v_fmeta->>'sid');
                IF (v_fmeta->>'list_item_prop') IN ('Value', 'Alias') THEN
                    v_inline_li_join := true;
                END IF;
            END LOOP;
            v_inline_sql :=
                E'SELECT\n        o._id' || v_inline_cols ||
                E'\n    FROM _objects o' ||
                E'\n    INNER JOIN _values v ON v._id_object = o._id' ||
                CASE WHEN v_inline_li_join
                     THEN E'\n    LEFT JOIN _list_items li ON li._id = v._ListItem'
                     ELSE '' END ||
                E'\n    WHERE o._id_scheme = ' || p_scheme_id::text ||
                CASE WHEN v_push_sql IS NOT NULL AND v_push_sql <> ''
                     THEN ' AND ' || v_push_sql ELSE '' END ||
                E'\n      AND v._id_structure = ANY(ARRAY[' || v_inline_sids || ']::bigint[])' ||
                E'\n      AND v._array_index IS NULL' ||
                E'\n    GROUP BY o._id';
            v_prefix := 'pvt.';
        ELSE
            -- 6. Build CTE (legacy path: mixed types / tree / base refs).
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
    END IF;

    -- 7. Build group-key projection + GROUP BY clause.
    FOR v_grp_entry IN SELECT value FROM jsonb_array_elements(p_group_by) LOOP
        v_grp_idx := v_grp_idx + 1;
        v_grp_col := _pvt_compile_order_col(v_grp_entry, v_fields, v_prefix, true);
        v_grp_alias := CASE
            WHEN v_grp_entry ? 'alias' AND jsonb_typeof(v_grp_entry->'alias') = 'string'
                THEN v_grp_entry->>'alias'
            WHEN v_grp_entry ? 'field'
                THEN v_grp_entry->>'field'
            WHEN v_grp_entry ? 'field_path'
                THEN v_grp_entry->>'field_path'
            ELSE '_grp_' || v_grp_idx::text
        END;
        IF v_grp_alias = ANY(v_grp_seen) THEN
            RAISE EXCEPTION 'pvt_build_groupby_sql: duplicate group-by alias "%"', v_grp_alias;
        END IF;
        v_grp_seen := v_grp_seen || v_grp_alias;
        v_select_parts  := v_select_parts  || (v_grp_col || ' AS ' || quote_ident(v_grp_alias));
        v_groupby_parts := v_groupby_parts || v_grp_col;
    END LOOP;

    -- 8. Append aggregate projection.
    IF p_aggregations IS NOT NULL
       AND jsonb_typeof(p_aggregations) = 'array'
       AND jsonb_array_length(p_aggregations) > 0 THEN
        v_select_parts := v_select_parts || pvt_build_agg_projection(p_aggregations, v_fields, v_prefix);
    END IF;

    v_select_sql  := array_to_string(v_select_parts,  ', ');
    v_groupby_sql := array_to_string(v_groupby_parts, ', ');

    -- 9. WHERE / HAVING / ORDER BY / paging.
    v_where_sql := pvt_build_where_from_json(v_outer_filter, v_fields, v_prefix);
    IF p_having IS NOT NULL AND p_having <> '{}'::jsonb THEN
        v_having_sql := E'\nHAVING ' || pvt_build_bool_expr(p_having, v_fields, v_prefix);
    END IF;
    v_order_sql := pvt_build_order_conditions(p_order, v_fields, v_prefix, NULL);

    IF p_limit IS NOT NULL AND p_limit >= 0 THEN
        v_paging := v_paging || E'\nLIMIT ' || p_limit::text;
    END IF;
    IF COALESCE(p_offset, 0) > 0 THEN
        v_paging := v_paging || E'\nOFFSET ' || p_offset::text;
    END IF;

    -- 10. Assemble. Shape A: SELECT ... FROM _objects o. Inline: SELECT
    --     ... FROM (<inline pivot>) pvt (Pro-parity). Narrow: from
    --     _pvt_cte JOIN _objects o. Wide: from _pvt_cte.
    IF v_cte_sql IS NULL AND NOT v_inline THEN
        -- Shape A: pure-base flat.
        RETURN 'SELECT ' || v_select_sql
            || ' FROM _objects o'
            || E'\nWHERE o._id_scheme = ' || p_scheme_id::text
            || CASE WHEN v_push_sql IS NOT NULL AND v_push_sql <> ''
                    THEN ' AND ' || v_push_sql ELSE '' END
            || E'\nGROUP BY ' || v_groupby_sql
            || v_having_sql
            || v_order_sql
            || v_paging;
    END IF;

    IF v_inline THEN
        -- Pro-parity inline GROUP BY: no CTE, pivot is a derived table.
        RETURN 'SELECT ' || v_select_sql
            || E'\n  FROM (' || v_inline_sql || E'\n  ) pvt'
            || CASE WHEN v_where_sql = 'TRUE' THEN '' ELSE E'\n WHERE ' || v_where_sql END
            || E'\n GROUP BY ' || v_groupby_sql
            || v_having_sql
            || v_order_sql
            || v_paging;
    END IF;

    IF v_narrow THEN
        RETURN v_cte_sql
            || E'\nSELECT ' || v_select_sql
            || ' FROM _pvt_cte'
            || E'\nJOIN _objects o ON o._id = _pvt_cte._id_object'
            || E'\nWHERE ' || v_where_sql
            || E'\nGROUP BY ' || v_groupby_sql
            || v_having_sql
            || v_order_sql
            || v_paging;
    END IF;

    RETURN v_cte_sql
        || E'\nSELECT ' || v_select_sql
        || ' FROM _pvt_cte'
        || E'\nWHERE ' || v_where_sql
        || E'\nGROUP BY ' || v_groupby_sql
        || v_having_sql
        || v_order_sql
        || v_paging;
END;
$BODY$;

COMMENT ON FUNCTION pvt_build_groupby_sql(bigint, jsonb, jsonb, jsonb, jsonb, jsonb, integer, integer, text, bigint[], integer, boolean, boolean) IS
    'GROUP BY orchestrator. p_group_by is a non-empty array of {field|field_path|$expr, alias?} entries (ORDER-BY-shape minus direction). p_aggregations is an optional array of {alias, $<func>: <expr>|"*", distinct?, filter?} entries. p_having is an optional boolean-expression node where aggregates can be referenced inline via {"$count":"*"} / {"$sum":{"$field":"X"}} (alias refs not supported). HAVING support is a free feature over Pro.';
