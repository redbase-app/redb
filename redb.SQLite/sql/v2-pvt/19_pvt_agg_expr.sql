-- =====================================================================
-- 19_pvt_agg_expr.sql
-- ---------------------------------------------------------------------
-- Aggregate-expression compiler used by terminal aggregations (21),
-- GROUP BY projections / HAVING (22) and window aggregate flavours (23).
-- A single aggregate entry has the shape:
--
--   {
--     "alias":   "<column alias, optional>",
--     "$<func>": "*" | <scalar-expr-node>,
--     "distinct": true | false,   -- optional, default false
--     "filter":   <bool-expr-node> -- optional, emits FILTER (WHERE ...)
--   }
--
-- Where `<func>` is one of: count / sum / avg / min / max / string_agg /
-- bool_and / bool_or.
--
-- `string_agg` requires a 2-element scalar-expr array `[value, separator]`
-- because Postgres demands a separator argument.
--
-- Free extras over Pro (which only knows COUNT/SUM/AVG/MIN/MAX without
-- DISTINCT, without FILTER, and without string_agg):
--   * `distinct: true` -> COUNT(DISTINCT expr) etc.
--   * `filter:   <bool>` -> COUNT(...) FILTER (WHERE ...) (per-aggregate
--     conditional aggregation; HAVING-of-one-aggregate)
--   * string_agg / bool_and / bool_or
--
-- All entries return text fragments without the `AS "<alias>"` suffix;
-- callers append aliasing themselves so the same compiler is reusable
-- inside HAVING / OVER / nested expressions.
-- =====================================================================


-- ---------- pvt_agg_entry_field_names ---------------------------------
-- Yield every field path referenced inside an aggregate entry. Used by
-- pvt_collect_fields callers (orchestrators of 21/22/23 wrap each entry
-- as `{"$expr": <entry-as-bag-of-nodes>}` and rely on this helper to
-- enumerate field refs in $<func> operand + filter).
CREATE OR REPLACE FUNCTION pvt_agg_entry_field_names(p_entry jsonb)
RETURNS SETOF text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    k text;
    v jsonb;
BEGIN
    IF p_entry IS NULL OR jsonb_typeof(p_entry) <> 'object' THEN
        RETURN;
    END IF;

    FOR k, v IN SELECT key, value FROM jsonb_each(p_entry) LOOP
        IF k IN ('alias', 'distinct') THEN
            CONTINUE;
        END IF;
        IF k = 'filter' THEN
            RETURN QUERY SELECT pvt_expr_field_names(v);
            CONTINUE;
        END IF;
        IF left(k, 1) <> '$' THEN
            CONTINUE;
        END IF;
        -- $<func> operand: bare "*" yields nothing; otherwise full expr walk.
        IF jsonb_typeof(v) = 'string' AND (v #>> '{}') = '*' THEN
            CONTINUE;
        END IF;
        RETURN QUERY SELECT pvt_expr_field_names(v);
    END LOOP;
END;
$BODY$;


-- ---------- pvt_build_agg_expr ----------------------------------------
-- Compile a single aggregate entry into a SQL fragment (without alias).
-- Validates exactly one $<func> key per entry and dispatches by name.
CREATE OR REPLACE FUNCTION pvt_build_agg_expr(
    p_entry       jsonb,
    p_fields      jsonb,
    p_base_prefix text DEFAULT ''
)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_op        text;
    v_arg       jsonb;
    v_distinct  boolean := false;
    v_filter    jsonb;
    v_inner     text;
    v_func      text;
    v_filt_sql  text := '';
    v_keys      int;
    v_args      jsonb;
    v_elem      jsonb;
    v_sep       text;
    v_fld_name  text;
    v_fld_meta  jsonb;
    v_is_array  boolean := false;
    v_col_expr  text;
BEGIN
    IF p_entry IS NULL OR jsonb_typeof(p_entry) <> 'object' THEN
        RAISE EXCEPTION 'pvt_build_agg_expr: entry must be a JSON object';
    END IF;

    -- Pick the single $-key inside the entry (alias/distinct/filter
    -- are sidecars, not operators).
    SELECT key INTO v_op
      FROM jsonb_object_keys(p_entry) AS t(key)
     WHERE left(key, 1) = '$'
     LIMIT 1;
    IF v_op IS NULL THEN
        RAISE EXCEPTION 'pvt_build_agg_expr: entry has no $<func> key: %', p_entry::text;
    END IF;

    -- Sanity: no more than one $-key per entry.
    SELECT count(*) INTO v_keys
      FROM jsonb_object_keys(p_entry) AS t(key)
     WHERE left(key, 1) = '$';
    IF v_keys > 1 THEN
        RAISE EXCEPTION 'pvt_build_agg_expr: entry has multiple $<func> keys (%): %',
            v_keys, p_entry::text;
    END IF;

    v_arg := p_entry -> v_op;
    IF p_entry ? 'distinct' AND jsonb_typeof(p_entry->'distinct') = 'boolean' THEN
        v_distinct := (p_entry->>'distinct')::boolean;
    END IF;
    IF p_entry ? 'filter' THEN
        v_filter := p_entry->'filter';
        v_filt_sql := ' FILTER (WHERE ' || pvt_build_bool_expr(v_filter, p_fields, p_base_prefix) || ')';
    END IF;

    -- ---------------- COUNT ----------------------------------------
    IF lower(v_op) = '$count' THEN
        -- "*" shorthand for COUNT(*) - DISTINCT meaningless here.
        IF jsonb_typeof(v_arg) = 'string' AND (v_arg #>> '{}') = '*' THEN
            IF v_distinct THEN
                RAISE EXCEPTION 'pvt_build_agg_expr: $count "*" with distinct=true is not allowed';
            END IF;
            RETURN 'COUNT(*)' || v_filt_sql;
        END IF;

        -- Detect array-typed $field operand (e.g. {"$count":{"$field":"SkillLevels[]"}})
        -- and route through per-row unnest so $count returns total element count
        -- across all rows (rows × array_length), matching Pro semantics.
        v_is_array := false;
        IF jsonb_typeof(v_arg) = 'object' AND v_arg ? '$field' THEN
            v_fld_name := v_arg->>'$field';
            v_fld_meta := p_fields -> v_fld_name;
            IF v_fld_meta IS NOT NULL
               AND COALESCE((v_fld_meta->>'is_array')::boolean, false)
               AND COALESCE(v_fld_meta->>'kind','') <> 'base' THEN
                v_is_array := true;
            END IF;
        END IF;

        IF v_is_array THEN
            IF v_distinct THEN
                RAISE EXCEPTION
                    'pvt_build_agg_expr: distinct=true is not supported for $count over array field "%"', v_fld_name;
            END IF;
            v_col_expr := pvt_build_scalar_expr(v_arg, p_fields, p_base_prefix);
            -- COUNT over unnested elements = SUM(per-row element count).
            -- COALESCE handles empty/NULL array (array_length returns NULL).
            RETURN 'SUM(COALESCE(array_length(' || v_col_expr || ', 1), 0))::bigint' || v_filt_sql;
        END IF;

        v_inner := pvt_build_scalar_expr(v_arg, p_fields, p_base_prefix);
        RETURN 'COUNT(' || CASE WHEN v_distinct THEN 'DISTINCT ' ELSE '' END || v_inner || ')' || v_filt_sql;
    END IF;

    -- ---------------- SUM / AVG / MIN / MAX ------------------------
    IF lower(v_op) IN ('$sum', '$avg', '$min', '$max') THEN
        v_func := CASE lower(v_op)
            WHEN '$sum' THEN 'SUM'
            WHEN '$avg' THEN 'AVG'
            WHEN '$min' THEN 'MIN'
            WHEN '$max' THEN 'MAX'
        END;

        -- Detect array-typed $field operand (e.g. {"$sum":{"$field":"SkillLevels[]"}})
        -- and route through per-row unnest so PG can aggregate element values
        -- instead of failing with "function sum(<type>[]) does not exist".
        v_is_array := false;
        IF jsonb_typeof(v_arg) = 'object' AND v_arg ? '$field' THEN
            v_fld_name := v_arg->>'$field';
            v_fld_meta := p_fields -> v_fld_name;
            IF v_fld_meta IS NOT NULL
               AND COALESCE((v_fld_meta->>'is_array')::boolean, false)
               AND COALESCE(v_fld_meta->>'kind','') <> 'base' THEN
                v_is_array := true;
            END IF;
        END IF;

        IF v_is_array THEN
            IF v_distinct THEN
                RAISE EXCEPTION
                    'pvt_build_agg_expr: distinct=true is not supported for aggregate over array field "%"', v_fld_name;
            END IF;
            v_col_expr := pvt_build_scalar_expr(v_arg, p_fields, p_base_prefix);
            IF lower(v_op) = '$avg' THEN
                -- AVG over all elements across all rows = SUM(elements) / COUNT(elements).
                -- Per-row sum and per-row count are computed in subqueries; outer SUMs
                -- combine them. NULLIF guards an all-empty-array result.
                RETURN '(SUM((SELECT SUM(_x) FROM unnest(' || v_col_expr || ') AS _x))::numeric'
                    || ' / NULLIF(SUM(COALESCE(array_length(' || v_col_expr || ', 1), 0)), 0))'
                    || v_filt_sql;
            ELSE
                -- SUM/MIN/MAX of aggregates is associative -> outer FUNC over per-row FUNC.
                RETURN v_func || '((SELECT ' || v_func || '(_x) FROM unnest('
                    || v_col_expr || ') AS _x))' || v_filt_sql;
            END IF;
        END IF;

        v_inner := pvt_build_scalar_expr(v_arg, p_fields, p_base_prefix);
        RETURN v_func || '(' || CASE WHEN v_distinct THEN 'DISTINCT ' ELSE '' END || v_inner || ')' || v_filt_sql;
    END IF;

    -- ---------------- STRING_AGG -----------------------------------
    -- { "$string_agg": [value, separator] }
    IF lower(v_op) = '$string_agg' THEN
        IF jsonb_typeof(v_arg) <> 'array' OR jsonb_array_length(v_arg) <> 2 THEN
            RAISE EXCEPTION 'pvt_build_agg_expr: $string_agg expects [value, separator]';
        END IF;
        v_inner := pvt_build_scalar_expr(v_arg->0, p_fields, p_base_prefix);
        v_sep   := pvt_build_scalar_expr(v_arg->1, p_fields, p_base_prefix);
        RETURN 'STRING_AGG(' || CASE WHEN v_distinct THEN 'DISTINCT ' ELSE '' END
            || v_inner || ', ' || v_sep || ')' || v_filt_sql;
    END IF;

    -- ---------------- BOOL_AND / BOOL_OR ---------------------------
    IF lower(v_op) IN ('$bool_and', '$bool_or') THEN
        v_func := CASE lower(v_op)
            WHEN '$bool_and' THEN 'BOOL_AND'
            WHEN '$bool_or'  THEN 'BOOL_OR'
        END;
        v_inner := pvt_build_scalar_expr(v_arg, p_fields, p_base_prefix);
        RETURN v_func || '(' || v_inner || ')' || v_filt_sql;
    END IF;

    RAISE EXCEPTION 'pvt_build_agg_expr: unsupported aggregate "%"', v_op;
END;
$BODY$;

COMMENT ON FUNCTION pvt_build_agg_expr(jsonb, jsonb, text) IS
    'Compiles a single aggregate entry into a SQL fragment. Supports $count|$sum|$avg|$min|$max|$string_agg|$bool_and|$bool_or with optional distinct=true and per-aggregate filter={...}. Free extras over Pro: DISTINCT modifier and FILTER clause.';


-- ---------- pvt_build_agg_projection ----------------------------------
-- Compile an array of aggregate entries into a SQL SELECT-list fragment
-- like `agg1 AS "alias1", agg2 AS "alias2", ...`. Aliases default to
-- positional `_agg_<i>` when omitted; duplicates raise.
CREATE OR REPLACE FUNCTION pvt_build_agg_projection(
    p_aggs        jsonb,
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
    v_sql    text;
    v_seen   text[] := ARRAY[]::text[];
    v_parts  text[] := ARRAY[]::text[];
    v_idx    integer := 0;
BEGIN
    IF p_aggs IS NULL OR jsonb_typeof(p_aggs) <> 'array' OR jsonb_array_length(p_aggs) = 0 THEN
        RAISE EXCEPTION 'pvt_build_agg_projection: p_aggs must be a non-empty JSON array';
    END IF;

    FOR v_entry IN SELECT value FROM jsonb_array_elements(p_aggs) LOOP
        v_idx := v_idx + 1;
        v_alias := CASE WHEN v_entry ? 'alias' AND jsonb_typeof(v_entry->'alias') = 'string'
                        THEN v_entry->>'alias'
                        ELSE '_agg_' || v_idx::text
                   END;
        IF v_alias = ANY(v_seen) THEN
            RAISE EXCEPTION 'pvt_build_agg_projection: duplicate alias "%"', v_alias;
        END IF;
        v_seen := v_seen || v_alias;
        v_sql := pvt_build_agg_expr(v_entry, p_fields, p_base_prefix);
        v_parts := v_parts || (v_sql || ' AS ' || quote_ident(v_alias));
    END LOOP;
    RETURN array_to_string(v_parts, ', ');
END;
$BODY$;

COMMENT ON FUNCTION pvt_build_agg_projection(jsonb, jsonb, text) IS
    'Compiles an array of aggregate entries into a `<sql> AS "<alias>", ...` projection fragment. Aliases default to positional `_agg_<i>`. Used by terminal aggregations and GROUP BY projections.';
