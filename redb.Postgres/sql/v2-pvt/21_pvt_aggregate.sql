-- =====================================================================
-- 21_pvt_aggregate.sql
-- ---------------------------------------------------------------------
-- Terminal aggregations: produce a single-row result of N aggregates
-- over a (filtered) PVT source. Sister of pvt_build_query_sql (file 20)
-- - same source-mode / tree / push-down plumbing, different projection
-- and no LIMIT/OFFSET/ORDER BY/DISTINCT (a single row has no order).
--
--   pvt_build_aggregate_sql(
--       p_scheme_id    bigint,
--       p_filter       jsonb,
--       p_aggregations jsonb,         -- non-empty array of agg entries
--       p_source_mode  text,
--       p_tree_ids     bigint[],
--       p_max_depth    integer,
--       p_include_seed boolean,
--       p_polymorphic  boolean
--   ) RETURNS text
--
-- Generated shape (Shape A vs Shape B/C selection mirrors file 20):
--
--   Shape A (pure-base filter, no PROPS refs, flat mode):
--      SELECT <agg1> AS "<a1>", ... FROM _objects o
--       WHERE o._id_scheme = X [AND <push>]
--
--   Shape B narrow:
--      <pvt_build_cte_sql narrow output>
--      SELECT <agg1> AS "<a1>", ...
--        FROM _pvt_cte JOIN _objects o ON o._id = _pvt_cte._id_object
--       WHERE <residual with base refs as o.*>
--
--   Shape C wide:
--      <pvt_build_cte_sql wide output>
--      SELECT <agg1> AS "<a1>", ... FROM _pvt_cte
--       WHERE <residual>
-- =====================================================================


-- ---------- pvt_collect_extra_fields ----------------------------------
-- Resolve metadata for a flat array of field paths. Used by aggregation
-- / window orchestrators to register fields referenced inside aggregate
-- operands / window arguments (which don't fit the field|field_path|
-- $expr shape consumed by pvt_collect_fields).
CREATE OR REPLACE FUNCTION pvt_collect_extra_fields(
    p_scheme_id bigint,
    p_paths     text[]
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_path   text;
    v_meta   jsonb;
    v_result jsonb := '{}'::jsonb;
BEGIN
    IF p_scheme_id IS NULL THEN
        RAISE EXCEPTION 'pvt_collect_extra_fields: p_scheme_id is required';
    END IF;
    IF p_paths IS NULL OR array_length(p_paths, 1) IS NULL THEN
        RETURN v_result;
    END IF;

    FOREACH v_path IN ARRAY p_paths LOOP
        IF v_path IS NULL OR v_path = '' THEN
            CONTINUE;
        END IF;
        v_meta := pvt_resolve_field_path(p_scheme_id, v_path);
        v_result := v_result || jsonb_build_object(v_path, v_meta);
    END LOOP;
    RETURN v_result;
END;
$BODY$;

COMMENT ON FUNCTION pvt_collect_extra_fields(bigint, text[]) IS
    'Resolve metadata for a flat array of field paths and merge into a JSONB fields map. Used to register fields referenced inside aggregate operands / window arguments that do not fit the order/filter shapes consumed by pvt_collect_fields.';


-- ---------- pvt_aggregations_field_names ------------------------------
-- Yield every field path referenced inside an aggregation array (operands
-- + per-aggregate filters). Used by orchestrators to extend the fields
-- map fed into pvt_build_cte_sql.
CREATE OR REPLACE FUNCTION pvt_aggregations_field_names(p_aggs jsonb)
RETURNS SETOF text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_entry jsonb;
BEGIN
    IF p_aggs IS NULL OR jsonb_typeof(p_aggs) <> 'array' THEN
        RETURN;
    END IF;
    FOR v_entry IN SELECT value FROM jsonb_array_elements(p_aggs) LOOP
        RETURN QUERY SELECT pvt_agg_entry_field_names(v_entry);
    END LOOP;
END;
$BODY$;

COMMENT ON FUNCTION pvt_aggregations_field_names(jsonb) IS
    'Yields every field path referenced inside an array of aggregate entries (operands + filters). Empty when p_aggs is null or non-array.';


-- ---------------------------------------------------------------------
-- pvt_build_aggregate_sql
-- ---------------------------------------------------------------------
DROP FUNCTION IF EXISTS pvt_build_aggregate_sql(bigint, jsonb, jsonb, text, bigint[], integer, boolean, boolean);

CREATE OR REPLACE FUNCTION pvt_build_aggregate_sql(
    p_scheme_id    bigint,
    p_filter       jsonb,
    p_aggregations jsonb,
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
    v_fields      jsonb;
    v_extra       jsonb;
    v_extra_names text[];
    v_cte_sql     text;
    v_where_sql   text;
    v_force_outer boolean;
    v_narrow      boolean;
    v_push_sql      text;
    v_residual      jsonb;
    v_outer_filter  jsonb;
    v_proj          text;
BEGIN
    IF p_scheme_id IS NULL THEN
        RAISE EXCEPTION 'pvt_build_aggregate_sql: p_scheme_id is required';
    END IF;
    IF p_aggregations IS NULL
       OR jsonb_typeof(p_aggregations) <> 'array'
       OR jsonb_array_length(p_aggregations) = 0 THEN
        RAISE EXCEPTION 'pvt_build_aggregate_sql: p_aggregations must be a non-empty JSON array';
    END IF;

    -- 1. Collect fields from filter (no order entries for aggregations).
    v_fields := pvt_collect_fields(p_scheme_id, p_filter, NULL, false);

    -- 2. Extend with fields referenced inside aggregate operands /
    --    per-aggregate filters.
    v_extra_names := ARRAY(SELECT DISTINCT n
                             FROM pvt_aggregations_field_names(p_aggregations) AS n
                            WHERE n IS NOT NULL);
    v_extra := pvt_collect_extra_fields(p_scheme_id, v_extra_names);
    v_fields := v_fields || v_extra;

    -- 3. Base pushdown.
    SELECT s.v_pushdown_sql, s.v_residual_filter
      INTO v_push_sql, v_residual
      FROM pvt_split_filter(p_filter, v_fields) s;
    v_outer_filter := v_residual;

    -- 4. Shape A shortcut: pure-base flat aggregation.
    IF v_outer_filter IS NULL
       AND p_source_mode = 'flat'
       AND (p_tree_ids IS NULL OR array_length(p_tree_ids, 1) IS NULL)
       AND NOT EXISTS (
            SELECT 1
              FROM jsonb_each(v_fields) AS e(k, val)
             WHERE COALESCE(val->>'kind', '') <> 'base'
       ) THEN
        v_proj := pvt_build_agg_projection(p_aggregations, v_fields, 'o.');
        RETURN 'SELECT ' || v_proj
            || ' FROM _objects o'
            || E'\nWHERE o._id_scheme = ' || p_scheme_id::text
            || CASE WHEN v_push_sql IS NOT NULL AND v_push_sql <> ''
                    THEN ' AND ' || v_push_sql ELSE '' END;
    END IF;

    -- 5. Narrow vs wide decision (mirrors file 20). Tree modes always go
    -- wide because pvt_build_cte_sql ignores narrow=true in tree mode
    -- and emits the base-cols CTE keyed on _id (not _id_object).
    v_force_outer := pvt_has_absence_check(v_outer_filter) OR (v_fields = '{}'::jsonb);
    v_narrow := p_source_mode = 'flat'
                AND NOT v_force_outer
                AND NOT EXISTS (
                    SELECT 1
                      FROM jsonb_each(v_fields) AS e(k, val)
                     WHERE (val->>'dict_key') IS NOT NULL
                       AND (val->>'parent_sid') IS NOT NULL);

    -- 6. Build CTE.
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

    -- 7. Residual WHERE + projection (prefix depends on shape).
    v_where_sql := pvt_build_where_from_json(
        v_outer_filter, v_fields,
        CASE WHEN v_narrow THEN 'o.' ELSE '' END);
    v_proj := pvt_build_agg_projection(
        p_aggregations, v_fields,
        CASE WHEN v_narrow THEN 'o.' ELSE '' END);

    IF v_narrow THEN
        RETURN v_cte_sql
            || E'\nSELECT ' || v_proj
            || ' FROM _pvt_cte'
            || E'\nJOIN _objects o ON o._id = _pvt_cte._id_object'
            || E'\nWHERE ' || v_where_sql;
    END IF;

    RETURN v_cte_sql
        || E'\nSELECT ' || v_proj
        || ' FROM _pvt_cte'
        || E'\nWHERE ' || v_where_sql;
END;
$BODY$;

COMMENT ON FUNCTION pvt_build_aggregate_sql(bigint, jsonb, jsonb, text, bigint[], integer, boolean, boolean) IS
    'Terminal aggregation orchestrator: emits a single-row SELECT projecting N aggregate columns over the PVT source. p_aggregations is a non-empty JSON array of {alias, $<func>: <expr>|"*", distinct?, filter?} entries. Tree variants reuse the same p_source_mode / p_tree_ids / p_max_depth / p_include_seed / p_polymorphic plumbing as pvt_build_query_sql.';
