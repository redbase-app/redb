-- =====================================================================
-- 24_pvt_projection.sql
-- ---------------------------------------------------------------------
-- Generic column projection ("Select" shaping). Sister of
-- pvt_build_query_sql (file 20): same source-mode / tree / push-down
-- plumbing, but instead of returning a single `_id` column the outer
-- SELECT yields an arbitrary list of typed columns built from the
-- entries in p_projection.
--
-- A projection entry has one of the following shapes:
--
--   { "field": "Name" }                                 -- bare field
--   { "field_path": "Address.City", "alias": "city" }   -- nested path
--   { "alias": "yr",
--     "$expr": { "$year": { "$field": "HireDate" } } }  -- expression
--   { "alias": "kind",
--     "$expr": { "$case": [
--         { "when": {"$gt":[{"$field":"Age"},{"$const":18}]},
--           "then": {"$const":"adult"} },
--         { "else": {"$const":"kid"} } ] } }
--
-- Functions:
--   pvt_projection_field_names(p_projection)
--       -> SETOF text  (every field path referenced by projection
--                       entries, for pvt_collect_extra_fields)
--   pvt_build_projection(p_projection, p_fields, p_base_prefix)
--       -> text         (a `<expr> AS "<alias>", ...` fragment, no
--                        leading SELECT)
--   pvt_build_projection_sql(...) -> text  (full SELECT orchestrator,
--                                           Shape A / B narrow / C wide)
--
-- Free-over-Pro angle: projections can carry arbitrary scalar-expr
-- nodes ($case / $coalesce / $cast / $year / $power / $concat / ...);
-- Pro's projection path is limited to bare field references plus a
-- handful of arithmetic / string ops on primitives.
-- =====================================================================


-- ---------- pvt_projection_field_names --------------------------------
-- Yield every field path referenced inside a projection array. Used by
-- the orchestrator to extend the fields map fed into pvt_build_cte_sql
-- via pvt_collect_extra_fields.
CREATE OR REPLACE FUNCTION pvt_projection_field_names(p_projection jsonb)
RETURNS SETOF text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_entry jsonb;
    v_path  text;
BEGIN
    IF p_projection IS NULL OR jsonb_typeof(p_projection) <> 'array' THEN
        RETURN;
    END IF;
    FOR v_entry IN SELECT value FROM jsonb_array_elements(p_projection) LOOP
        IF v_entry IS NULL OR jsonb_typeof(v_entry) <> 'object' THEN
            CONTINUE;
        END IF;
        IF v_entry ? 'field' AND jsonb_typeof(v_entry->'field') = 'string' THEN
            v_path := v_entry->>'field';
            IF v_path IS NOT NULL AND v_path <> '' THEN
                RETURN NEXT v_path;
            END IF;
        END IF;
        IF v_entry ? 'field_path' AND jsonb_typeof(v_entry->'field_path') = 'string' THEN
            v_path := v_entry->>'field_path';
            IF v_path IS NOT NULL AND v_path <> '' THEN
                RETURN NEXT v_path;
            END IF;
        END IF;
        IF v_entry ? '$expr' THEN
            RETURN QUERY SELECT pvt_expr_field_names(v_entry->'$expr');
        END IF;
    END LOOP;
END;
$BODY$;

COMMENT ON FUNCTION pvt_projection_field_names(jsonb) IS
    'Yields every field path referenced inside a projection array (bare field / field_path entries plus $expr operands). Empty when p_projection is null or non-array.';


-- ---------- pvt_build_projection --------------------------------------
-- Compile a projection array into a SELECT-list fragment.
-- Aliases default to: the field/field_path value (when present) or
-- `_proj_<i>` (positional). Duplicate aliases raise.
CREATE OR REPLACE FUNCTION pvt_build_projection(
    p_projection  jsonb,
    p_fields      jsonb,
    p_base_prefix text DEFAULT ''
)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_entry  jsonb;
    v_alias  text;
    v_expr   jsonb;
    v_sql    text;
    v_seen   text[] := ARRAY[]::text[];
    v_parts  text[] := ARRAY[]::text[];
    v_idx    integer := 0;
    v_keys   integer;
BEGIN
    IF p_projection IS NULL
       OR jsonb_typeof(p_projection) <> 'array'
       OR jsonb_array_length(p_projection) = 0 THEN
        RAISE EXCEPTION 'pvt_build_projection: p_projection must be a non-empty JSON array';
    END IF;

    FOR v_entry IN SELECT value FROM jsonb_array_elements(p_projection) LOOP
        v_idx := v_idx + 1;
        IF v_entry IS NULL OR jsonb_typeof(v_entry) <> 'object' THEN
            RAISE EXCEPTION 'pvt_build_projection: entry #% is not a JSON object', v_idx;
        END IF;

        -- Exactly one source key per entry: field | field_path | $expr.
        v_keys := (CASE WHEN v_entry ? 'field'      THEN 1 ELSE 0 END)
                + (CASE WHEN v_entry ? 'field_path' THEN 1 ELSE 0 END)
                + (CASE WHEN v_entry ? '$expr'      THEN 1 ELSE 0 END);
        IF v_keys = 0 THEN
            RAISE EXCEPTION 'pvt_build_projection: entry #% has no source key (field|field_path|$expr): %', v_idx, v_entry::text;
        END IF;
        IF v_keys > 1 THEN
            RAISE EXCEPTION 'pvt_build_projection: entry #% has more than one source key (field|field_path|$expr): %', v_idx, v_entry::text;
        END IF;

        -- Determine alias.
        IF v_entry ? 'alias' AND jsonb_typeof(v_entry->'alias') = 'string' THEN
            v_alias := v_entry->>'alias';
        ELSIF v_entry ? 'field' THEN
            v_alias := v_entry->>'field';
        ELSIF v_entry ? 'field_path' THEN
            v_alias := v_entry->>'field_path';
        ELSE
            v_alias := '_proj_' || v_idx::text;
        END IF;
        IF v_alias IS NULL OR v_alias = '' THEN
            RAISE EXCEPTION 'pvt_build_projection: entry #% has empty alias', v_idx;
        END IF;
        IF v_alias = ANY(v_seen) THEN
            RAISE EXCEPTION 'pvt_build_projection: duplicate alias "%"', v_alias;
        END IF;
        v_seen := v_seen || v_alias;

        -- Normalize source to a scalar-expr node.
        IF v_entry ? '$expr' THEN
            v_expr := v_entry->'$expr';
        ELSIF v_entry ? 'field' THEN
            v_expr := jsonb_build_object('$field', v_entry->>'field');
        ELSE
            v_expr := jsonb_build_object('$field', v_entry->>'field_path');
        END IF;

        v_sql := pvt_build_scalar_expr(v_expr, p_fields, p_base_prefix);
        v_parts := v_parts || (v_sql || ' AS ' || quote_ident(v_alias));
    END LOOP;

    RETURN array_to_string(v_parts, ', ');
END;
$BODY$;

COMMENT ON FUNCTION pvt_build_projection(jsonb, jsonb, text) IS
    'Compiles a projection array into a SELECT-list fragment `<expr> AS "<alias>", ...`. Supports {field} | {field_path} | {$expr:<scalar-expr-node>} entries. Aliases default to the field name or `_proj_<i>` (positional).';


-- =====================================================================
-- pvt_build_projection_sql
-- ---------------------------------------------------------------------
-- Full orchestrator: produce a complete SELECT statement that yields
-- the requested projection. Mirrors pvt_build_query_sql for filter /
-- order / distinct / tree / push-down handling; the only difference is
-- the SELECT list comes from p_projection instead of being a fixed
-- `_id`.
-- =====================================================================
DROP FUNCTION IF EXISTS pvt_build_projection_sql(bigint, jsonb, jsonb, integer, integer, jsonb, integer, boolean, text, bigint[], boolean, boolean, jsonb);

CREATE OR REPLACE FUNCTION pvt_build_projection_sql(
    p_scheme_id    bigint,
    p_projection   jsonb,
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
    v_proj          text;
    v_prefix        text;
BEGIN
    IF p_scheme_id IS NULL THEN
        RAISE EXCEPTION 'pvt_build_projection_sql: p_scheme_id is required';
    END IF;
    IF p_projection IS NULL
       OR jsonb_typeof(p_projection) <> 'array'
       OR jsonb_array_length(p_projection) = 0 THEN
        RAISE EXCEPTION 'pvt_build_projection_sql: p_projection must be a non-empty JSON array';
    END IF;

    -- DISTINCT and DISTINCT ON are mutually exclusive.
    IF p_distinct AND p_distinct_on IS NOT NULL
       AND jsonb_typeof(p_distinct_on) = 'array'
       AND jsonb_array_length(p_distinct_on) > 0 THEN
        RAISE EXCEPTION 'pvt_build_projection_sql: p_distinct and p_distinct_on are mutually exclusive';
    END IF;

    -- 1. Collect fields from filter + order + distinct_on (same channel
    --    as pvt_build_query_sql), then extend with projection refs.
    v_fields := pvt_collect_fields(
        p_scheme_id,
        p_filter,
        CASE
            WHEN p_distinct_on IS NULL THEN p_order
            WHEN p_order       IS NULL THEN p_distinct_on
            ELSE p_order || p_distinct_on
        END,
        false);

    v_extra_names := ARRAY(SELECT DISTINCT n
                             FROM pvt_projection_field_names(p_projection) AS n
                            WHERE n IS NOT NULL);
    v_extra := pvt_collect_extra_fields(p_scheme_id, v_extra_names);
    v_fields := v_fields || v_extra;

    -- 2. Base pushdown.
    SELECT s.v_pushdown_sql, s.v_residual_filter
      INTO v_push_sql, v_residual
      FROM pvt_split_filter(p_filter, v_fields) s;
    v_outer_filter := v_residual;

    -- 3. Shape A shortcut: pure-base flat projection.
    IF v_outer_filter IS NULL
       AND p_source_mode = 'flat'
       AND (p_tree_ids IS NULL OR array_length(p_tree_ids, 1) IS NULL)
       AND NOT EXISTS (
            SELECT 1
              FROM jsonb_each(v_fields) AS e(k, val)
             WHERE COALESCE(val->>'kind', '') <> 'base'
       ) THEN

        v_order_sql := pvt_build_order_conditions(p_order, v_fields, 'o.', p_distinct_on);
        v_proj := pvt_build_projection(p_projection, v_fields, 'o.');

        IF p_limit IS NOT NULL AND p_limit >= 0 THEN
            v_paging := v_paging || E'\nLIMIT ' || p_limit::text;
        END IF;
        IF COALESCE(p_offset, 0) > 0 THEN
            v_paging := v_paging || E'\nOFFSET ' || p_offset::text;
        END IF;

        RETURN 'SELECT '
               || CASE WHEN p_distinct THEN 'DISTINCT ' ELSE '' END
               || pvt_build_distinct_on_select(p_distinct_on, v_fields, 'o.')
               || v_proj
               || ' FROM _objects o'
               || E'\nWHERE o._id_scheme = ' || p_scheme_id::text
               || CASE WHEN v_push_sql IS NOT NULL AND v_push_sql <> ''
                       THEN ' AND ' || v_push_sql ELSE '' END
               || v_order_sql
               || v_paging;
    END IF;

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

    -- 5. Build CTE.
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

    -- 6. WHERE / ORDER / projection (prefix depends on shape).
    v_where_sql := pvt_build_where_from_json(v_outer_filter, v_fields, v_prefix);
    v_order_sql := pvt_build_order_conditions(p_order, v_fields, v_prefix, p_distinct_on);
    v_proj      := pvt_build_projection(p_projection, v_fields, v_prefix);

    -- 7. Paging.
    IF p_limit IS NOT NULL AND p_limit >= 0 THEN
        v_paging := v_paging || E'\nLIMIT ' || p_limit::text;
    END IF;
    IF COALESCE(p_offset, 0) > 0 THEN
        v_paging := v_paging || E'\nOFFSET ' || p_offset::text;
    END IF;

    -- 8. Assemble outer SELECT.
    IF v_narrow THEN
        RETURN v_cte_sql
            || E'\nSELECT '
            || CASE WHEN p_distinct THEN 'DISTINCT ' ELSE '' END
            || pvt_build_distinct_on_select(p_distinct_on, v_fields, v_prefix)
            || v_proj
            || ' FROM _pvt_cte'
            || E'\nJOIN _objects o ON o._id = _pvt_cte._id_object'
            || E'\nWHERE ' || v_where_sql
            || v_order_sql
            || v_paging;
    END IF;

    RETURN v_cte_sql
        || E'\nSELECT '
        || CASE WHEN p_distinct THEN 'DISTINCT ' ELSE '' END
        || pvt_build_distinct_on_select(p_distinct_on, v_fields, v_prefix)
        || v_proj
        || ' FROM _pvt_cte'
        || E'\nWHERE ' || v_where_sql
        || v_order_sql
        || v_paging;
END;
$BODY$;

COMMENT ON FUNCTION pvt_build_projection_sql(bigint, jsonb, jsonb, integer, integer, jsonb, integer, boolean, text, bigint[], boolean, boolean, jsonb) IS
    'Factor-1 projection orchestrator. Composes pvt_collect_fields + pvt_collect_extra_fields + pvt_build_cte_sql + pvt_build_where_from_json + pvt_build_order_conditions + pvt_build_projection into a complete SELECT statement with arbitrary projected columns. p_projection accepts entries {field} | {field_path} | {alias?, $expr:<scalar-expr-node>}. Filter / order / distinct / source-mode / pushdown semantics are identical to pvt_build_query_sql.';
