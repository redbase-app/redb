-- =====================================================================
-- 20_pvt_build_query_sql.sql
-- ---------------------------------------------------------------------
-- Factor-1 orchestrator: produce the full SQL string for a base-fields
-- search query. Pure SQL generation, no execution. The result is used
-- both by pvt_search_objects_base (via EXECUTE) and by the SQL preview
-- entry point.
--
-- Function:
--   pvt_build_query_sql(
--       p_scheme_id    bigint,
--       p_filter       jsonb,
--       p_limit        integer,
--       p_offset       integer,
--       p_order        jsonb,
--       p_max_depth    integer,
--       p_distinct     boolean,
--       p_source_mode  text,        -- flat | tree | tree_descendants | tree_children | tree_roots | tree_leaves | tree_ancestors
--       p_tree_ids     bigint[],
--       p_include_seed boolean,     -- false strips seed from tree_descendants
--       p_polymorphic  boolean      -- false ANDs scheme_id into recursive walks
--   ) RETURNS text
--
-- Generated shape (depending on filter content):
--
--   Shape A (pure base, no props referenced) -- emitted directly:
--      SELECT [DISTINCT] _id FROM _objects o
--       WHERE o._id_scheme = X [AND <pushed_base_predicate>]
--       [ORDER BY ...] [LIMIT ... OFFSET ...]
--
--   Shape B narrow (props referenced, no null-check, no tree,
--                   no nested-dict groups) -- Pro-parity outer:
--      WITH _pvt_cte AS (
--          SELECT v._id_object, <pivot cols>
--          FROM _values v
--          WHERE v._id_structure = ANY(ARRAY[<sids>]::bigint[])
--            AND v._id_object IN (SELECT _id FROM _objects
--                                  WHERE _id_scheme=X [AND <push>])
--          GROUP BY v._id_object
--      )
--      SELECT [DISTINCT] o._id
--        FROM _pvt_cte
--        JOIN _objects o ON o._id = _pvt_cte._id_object
--       WHERE <residual with base refs rewritten to o.*>
--       [ORDER BY ...] [LIMIT ... OFFSET ...]
--
--   Shape C wide (null-check, tree mode, or nested-dict groups) --
--                                             legacy wide CTE outer:
--      <pvt_build_cte_sql wide output>
--      SELECT [DISTINCT] _id FROM _pvt_cte
--       WHERE <pvt_build_where_from_json (bare base refs)>
--       [ORDER BY ...] [LIMIT ... OFFSET ...]
-- =====================================================================

-- Signature evolved in v0.6.0 (added p_include_seed + p_polymorphic) and
-- v0.6.1 (added p_distinct_on for SELECT DISTINCT ON parity).
-- CREATE OR REPLACE cannot change argument lists, so drop legacy forms.
DROP FUNCTION IF EXISTS pvt_build_query_sql(bigint, jsonb, integer, integer, jsonb, integer, boolean);
DROP FUNCTION IF EXISTS pvt_build_query_sql(bigint, jsonb, integer, integer, jsonb, integer, boolean, text, bigint[]);
DROP FUNCTION IF EXISTS pvt_build_query_sql(bigint, jsonb, integer, integer, jsonb, integer, boolean, text, bigint[], boolean, boolean);

CREATE OR REPLACE FUNCTION pvt_build_query_sql(
    p_scheme_id    bigint,
    p_filter       jsonb    DEFAULT NULL,
    p_limit        integer  DEFAULT NULL,
    p_offset       integer  DEFAULT 0,
    p_order        jsonb    DEFAULT NULL,
    p_max_depth    integer  DEFAULT NULL,
    p_distinct     boolean  DEFAULT false,
    p_source_mode  text     DEFAULT 'flat',
    p_tree_ids     bigint[] DEFAULT NULL,
    p_include_seed boolean  DEFAULT true,
    p_polymorphic  boolean  DEFAULT true,
    p_distinct_on  jsonb    DEFAULT NULL
)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_fields    jsonb;
    v_cte_sql   text;
    v_where_sql text;
    v_order_sql text;
    v_paging    text := '';
    v_force_outer boolean;
    v_narrow      boolean;
    v_push_sql      text;
    v_residual      jsonb;
    v_outer_filter  jsonb;
    v_residual_sql  text;
    v_can_push      boolean;
BEGIN
    IF p_scheme_id IS NULL THEN
        RAISE EXCEPTION 'pvt_build_query_sql: p_scheme_id is required';
    END IF;

    -- DISTINCT and DISTINCT ON are mutually exclusive; PostgreSQL does
    -- not allow combining them in a single SELECT.
    IF p_distinct AND p_distinct_on IS NOT NULL
       AND jsonb_typeof(p_distinct_on) = 'array'
       AND jsonb_array_length(p_distinct_on) > 0 THEN
        RAISE EXCEPTION 'pvt_build_query_sql: p_distinct and p_distinct_on are mutually exclusive';
    END IF;

    -- 1. Resolve metadata for every referenced field. DISTINCT ON
    --    entries share shape with ORDER BY entries (field|field_path|$expr),
    --    so we concat them into the order channel for collection only;
    --    actual rendering happens via separate builders.
    v_fields := pvt_collect_fields(
        p_scheme_id,
        p_filter,
        CASE
            WHEN p_distinct_on IS NULL THEN p_order
            WHEN p_order       IS NULL THEN p_distinct_on
            ELSE p_order || p_distinct_on
        END,
        false);

    -- 2. Pro-style base pushdown: peel off base/hierarchical predicates
    --    so they can be AND-ed inside the pivot CTE (before JOIN/GROUP
    --    BY) instead of after it. Indexes on _objects(_id_scheme,
    --    _date_*, _id_parent, _id_owner, ...) become usable.
    SELECT s.v_pushdown_sql, s.v_residual_filter
      INTO v_push_sql, v_residual
      FROM pvt_split_filter(p_filter, v_fields) s;

    -- The outer WHERE walker only sees the residual; if pushdown
    -- swallowed the entire filter the outer becomes 'TRUE'.
    v_outer_filter := v_residual;

    -- 2b. Shape A shortcut: pure-base flat query. When the filter
    --     references zero PROPS fields (only base / hierarchical),
    --     split swallowed everything (residual = NULL), and we are
    --     not in tree mode, the whole query collapses to a plain
    --     SELECT over _objects -- no CTE, no JOIN with _values, no
    --     GROUP BY. This is the same shape Pro emits for queries
    --     like `WhereRedb(o => o._id > 0)`.
    IF v_outer_filter IS NULL
       AND p_source_mode = 'flat'
       AND (p_tree_ids IS NULL OR array_length(p_tree_ids, 1) IS NULL)
       AND NOT EXISTS (
            SELECT 1
              FROM jsonb_each(v_fields) AS e(k, val)
             WHERE COALESCE(val->>'kind', '') <> 'base'
       ) THEN

        v_order_sql := pvt_build_order_conditions(p_order, v_fields, '', p_distinct_on);

        IF p_limit IS NOT NULL AND p_limit >= 0 THEN
            v_paging := v_paging || E'\nLIMIT ' || p_limit::text;
        END IF;
        IF COALESCE(p_offset, 0) > 0 THEN
            v_paging := v_paging || E'\nOFFSET ' || p_offset::text;
        END IF;

        RETURN 'SELECT '
               || CASE WHEN p_distinct THEN 'DISTINCT ' ELSE '' END
               || pvt_build_distinct_on_select(p_distinct_on, v_fields, 'o.')
               || '_id FROM _objects o'
               || E'\nWHERE o._id_scheme = ' || p_scheme_id::text
               || CASE WHEN v_push_sql IS NOT NULL AND v_push_sql <> ''
                       THEN ' AND ' || v_push_sql
                       ELSE '' END
               || v_order_sql
               || v_paging;
    END IF;

    -- 3. Decide LEFT vs INNER JOIN on _values: use LEFT only when the
    --    RESIDUAL filter explicitly tests for ABSENCE (i.e. needs to
    --    surface rows that have no matching _values record). Base-only
    --    null tests are already pushed; $notNull/{$ne:null} are handled
    --    correctly by INNER JOIN (it drops absent rows, which is the
    --    desired outcome). See pvt_has_absence_check for the exact set.
    --    Also force outer when v_fields contains zero PROPS entries
    --    (only base/hierarchical refs from order/distinct_on): the
    --    narrow shape requires a non-empty pivot SID array, otherwise
    --    pvt_build_cte_sql silently downgrades the inner CTE to wide
    --    and the outer 'JOIN _objects o ON o._id = _pvt_cte._id_object'
    --    references a column the wide CTE does not project.
    v_force_outer := pvt_has_absence_check(v_outer_filter)
                     OR (v_fields = '{}'::jsonb)
                     OR NOT EXISTS (
                         SELECT 1 FROM jsonb_each(v_fields) AS e(k, val)
                          WHERE COALESCE(val->>'kind', '') <> 'base'
                     );

    -- 3b. Decide narrow vs wide CTE shape. Narrow Pro-parity shape
    --     applies when (a) we don't need LEFT JOIN (no absence check)
    --     and (b) the field set is NOT mixed (scalar pivot + nested-dict
    --     in the same query). Pure scalar pivot -> narrow _values scan.
    --     Pure nested-dict -> nested-only optimization (pvt_build_cte_sql
    --     skips the heavy pi wrapper and folds the object-set restriction
    --     into the nested CTE). Tree mode IS compatible in both. In
    --     narrow mode the outer SELECT JOINs _objects to expose base
    --     columns for residual / order refs.
    DECLARE
        v_has_nested boolean;
        v_has_scalar boolean;
    BEGIN
        v_has_nested := EXISTS (
            SELECT 1 FROM jsonb_each(v_fields) AS e(k, val)
             WHERE (val->>'dict_key') IS NOT NULL
               AND (val->>'parent_sid') IS NOT NULL);
        v_has_scalar := EXISTS (
            SELECT 1 FROM jsonb_each(v_fields) AS e(k, val)
             WHERE COALESCE(val->>'kind', '') <> 'base'
               AND NOT (COALESCE((val->>'length_modifier')::boolean, false))
               AND NOT ((val->>'dict_key') IS NOT NULL AND (val->>'parent_sid') IS NOT NULL));
        v_narrow := NOT v_force_outer
                    AND NOT (v_has_nested AND v_has_scalar);
    END;

    -- 4. Pro-parity residual WHERE pushdown: when the outer filter
    --    references only pivoted columns (no base/hierarchical refs),
    --    render it now and feed to pvt_build_cte_sql so it wraps
    --    _pvt_cte body as `SELECT * FROM (<pivot>) pvt WHERE <residual>`.
    --    The planner then prunes rows BEFORE the outer JOIN _objects.
    --    Mixed filters keep the legacy outer WHERE (base refs can only
    --    be resolved via the `o` alias the JOIN exposes).
    v_can_push := v_narrow
                  AND v_outer_filter IS NOT NULL
                  AND v_outer_filter <> '{}'::jsonb
                  AND NOT pvt_filter_has_base_refs(v_outer_filter, v_fields);
    IF v_can_push THEN
        v_residual_sql := pvt_build_where_from_json(v_outer_filter, v_fields, '');
    ELSE
        v_residual_sql := NULL;
    END IF;

    -- 5. Build the CTE (with optional base + residual pushdown).
    v_cte_sql := pvt_build_cte_sql(
        p_scheme_id      => p_scheme_id,
        p_fields         => v_fields,
        p_source_mode    => p_source_mode,
        p_tree_ids       => p_tree_ids,
        p_max_depth      => p_max_depth,
        p_force_outer    => v_force_outer,
        p_extra_where    => v_push_sql,
        p_narrow         => v_narrow,
        p_include_seed   => p_include_seed,
        p_polymorphic    => p_polymorphic,
        p_residual_where => v_residual_sql
    );

    -- 6. Build outer predicates and ordering. In narrow shape base
    --    columns live on _objects (alias `o`), so base references in
    --    the residual / order BY must be emitted as `o.<col>`. In wide
    --    shape base columns are projected into _pvt_cte unaliased and
    --    references stay bare (legacy behavior). When residual was
    --    pushed inside _pvt_cte we skip the outer WHERE.
    IF v_can_push THEN
        v_where_sql := 'TRUE';
    ELSE
        v_where_sql := pvt_build_where_from_json(
            v_outer_filter, v_fields,
            CASE WHEN v_narrow THEN 'o.' ELSE '' END);
    END IF;
    v_order_sql := pvt_build_order_conditions(
        p_order, v_fields,
        CASE WHEN v_narrow THEN 'o.' ELSE '' END,
        p_distinct_on);

    -- 6. Paging.
    IF p_limit IS NOT NULL AND p_limit >= 0 THEN
        v_paging := v_paging || E'\nLIMIT ' || p_limit::text;
    END IF;
    IF COALESCE(p_offset, 0) > 0 THEN
        v_paging := v_paging || E'\nOFFSET ' || p_offset::text;
    END IF;

    -- 7. Assemble outer SELECT. Narrow shape JOINs _objects so the
    --    residual WHERE / ORDER BY can address base columns via the
    --    `o` alias; wide shape selects directly from _pvt_cte.
    IF v_narrow THEN
        RETURN v_cte_sql
            || E'\nSELECT '
            || CASE WHEN p_distinct THEN 'DISTINCT ' ELSE '' END
            || pvt_build_distinct_on_select(p_distinct_on, v_fields, 'o.')
            || 'o._id FROM _pvt_cte'
            || E'\nJOIN _objects o ON o._id = _pvt_cte._id_object'
            || CASE WHEN v_where_sql = 'TRUE' THEN '' ELSE E'\nWHERE ' || v_where_sql END
            || v_order_sql
            || v_paging;
    END IF;

    RETURN v_cte_sql
        || E'\nSELECT '
        || CASE WHEN p_distinct THEN 'DISTINCT ' ELSE '' END
        || pvt_build_distinct_on_select(p_distinct_on, v_fields, '')
        || '_id FROM _pvt_cte'
        || CASE WHEN v_where_sql = 'TRUE' THEN '' ELSE E'\nWHERE ' || v_where_sql END
        || v_order_sql
        || v_paging;
END;
$BODY$;

COMMENT ON FUNCTION pvt_build_query_sql(bigint, jsonb, integer, integer, jsonb, integer, boolean, text, bigint[], boolean, boolean, jsonb) IS
    'Factor-1 of the PVT engine: pure SQL generation. Composes pvt_collect_fields + pvt_build_cte_sql + pvt_build_where_from_json + pvt_build_order_conditions into a complete SELECT statement that yields a single _id column. p_source_mode: flat | tree | tree_descendants | tree_children | tree_roots | tree_leaves | tree_ancestors. p_include_seed=false strips seed objects from tree_descendants. p_polymorphic=false restricts recursive walks to p_scheme_id (default polymorphic matches Pro). p_distinct_on accepts the same {field|field_path|$expr} entries as p_order; its expressions are rendered as SELECT DISTINCT ON (...) and auto-prepended ASC to ORDER BY (PostgreSQL requirement). Mutually exclusive with p_distinct.';
