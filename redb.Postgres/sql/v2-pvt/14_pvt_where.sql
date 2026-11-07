-- =====================================================================
-- 14_pvt_where.sql
-- ---------------------------------------------------------------------
-- Recursive walker: turns the full filter JSON into a single WHERE
-- expression. Column references default to bare names (matching the
-- legacy wide _pvt_cte shape), but when p_base_prefix='o.' is passed
-- the walker rewrites base-field references to `o.<system_col>` so the
-- predicate composes against `FROM _objects o` directly (used by the
-- narrow Pro-shape outer SELECT that JOINs _objects on the pivot CTE).
--
-- Function:
--   pvt_build_where_from_json(p_filter jsonb, p_fields jsonb,
--                             p_base_prefix text DEFAULT '') RETURNS text
--
-- Grammar:
--   { "$and": [ ... ] }              ->  ( ... AND ... )
--   { "$or":  [ ... ] }              ->  ( ... OR  ... )
--   { "$not": { ... }   }            ->  NOT ( ... )
--   { "<field>": <op_json>, ... }    ->  AND of per-field conditions
--
-- An empty filter (NULL or {}) returns the SQL literal 'TRUE'.
-- =====================================================================

-- Signature evolved in v0.5.0 (added p_base_prefix for narrow Pro-shape
-- outer SELECT). CREATE OR REPLACE cannot change argument lists.
DROP FUNCTION IF EXISTS pvt_build_where_from_json(jsonb, jsonb);

CREATE OR REPLACE FUNCTION pvt_build_where_from_json(
    p_filter      jsonb,
    p_fields      jsonb,
    p_base_prefix text DEFAULT ''
)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    k        text;
    v        jsonb;
    elem     jsonb;
    parts    text[] := ARRAY[]::text[];
    children text[] := ARRAY[]::text[];
    meta     jsonb;
    v_normalized text;
    v_peek   text;
BEGIN
    IF p_filter IS NULL OR p_filter = '{}'::jsonb THEN
        RETURN 'TRUE';
    END IF;
    IF jsonb_typeof(p_filter) <> 'object' THEN
        RAISE EXCEPTION 'pvt_build_where_from_json: filter must be a JSON object (got %)', jsonb_typeof(p_filter);
    END IF;

    FOR k, v IN SELECT key, value FROM jsonb_each(p_filter) LOOP

        -- ---------- logical $and ----------
        IF lower(k) = '$and' THEN
            IF jsonb_typeof(v) <> 'array' THEN
                RAISE EXCEPTION '$and expects an array';
            END IF;
            children := ARRAY[]::text[];
            FOR elem IN SELECT value FROM jsonb_array_elements(v) LOOP
                children := children || pvt_build_where_from_json(elem, p_fields, p_base_prefix);
            END LOOP;
            IF array_length(children, 1) IS NULL THEN
                parts := parts || 'TRUE';
            ELSE
                parts := parts || ('(' || array_to_string(children, ' AND ') || ')');
            END IF;

        -- ---------- logical $or ----------
        ELSIF lower(k) = '$or' THEN
            IF jsonb_typeof(v) <> 'array' THEN
                RAISE EXCEPTION '$or expects an array';
            END IF;
            children := ARRAY[]::text[];
            FOR elem IN SELECT value FROM jsonb_array_elements(v) LOOP
                children := children || pvt_build_where_from_json(elem, p_fields, p_base_prefix);
            END LOOP;
            IF array_length(children, 1) IS NULL THEN
                parts := parts || 'FALSE';
            ELSE
                parts := parts || ('(' || array_to_string(children, ' OR ') || ')');
            END IF;

        -- ---------- logical $not ----------
        ELSIF lower(k) = '$not' THEN
            parts := parts || ('NOT (' || pvt_build_where_from_json(v, p_fields, p_base_prefix) || ')');

        -- ---------- hierarchical (tree) operators ----------
        ELSIF lower(k) IN ('$hasancestor', '$hasdescendant', '$level', '$isroot', '$isleaf') THEN
            -- Delegate to forked legacy helper. It operates on _objects
            -- columns which are all present inside the PVT CTE under
            -- their original names, so the predicates compose cleanly.
            parts := parts || pvt_build_hierarchical_conditions(
                jsonb_build_object(k, v),
                'o'  -- legacy expects an alias prefix; CTE column names match _objects column names
            );

        -- ---------- expression-form predicate ----------
        --   { "$gt": [exprL, exprR] }, { "$between": [L, lo, hi] }, etc.
        -- Disambiguated from `{ "$gt": value }` shorthand by the fact
        -- that the shorthand never appears at filter-level: it always
        -- lives inside `{ "<field>": { ... } }`.
        ELSIF lower(k) IN (
            '$eq', '$ne', '$lt', '$lte', '$gt', '$gte',
            '$like', '$ilike',
            '$in', '$nin', '$between',
            '$null', '$notnull', '$isnull', '$exists',
            '$contains', '$startswith', '$endswith',
            '$containsignorecase', '$startswithignorecase', '$endswithignorecase',
            '$regex', '$iregex', '$notregex', '$inotregex',
            '$fts'
        ) THEN
            parts := parts || pvt_build_expr_predicate(k, v, p_fields, p_base_prefix);

        -- ---------- $expr: arbitrary boolean expression --------------
        --   { "$expr": <bool-expr-node> } — full pvt_build_bool_expr,
        --   including $and/$or/$not, and any predicate compiled by
        --   pvt_build_expr_predicate. Useful for cross-field arithmetic
        --   filters and for $regex / $fts on multiple fields.
        ELSIF lower(k) = '$expr' THEN
            parts := parts || pvt_build_bool_expr(v, p_fields, p_base_prefix);

        -- ---------- field leaf ----------
        ELSE
            -- Pro-style ContainsKey normalization: rewrite `Dict.ContainsKey`
            -- to `Dict[<key>]` and look up the resolved metadata under the
            -- rewritten name. When the rewrite fired, the predicate
            -- collapses to `<col> IS NOT NULL` on the dict pivot column.
            v_peek       := pvt_peek_contains_key_value(v);
            v_normalized := pvt_normalize_field_name(k, v_peek);
            meta := p_fields -> v_normalized;
            IF meta IS NULL THEN
                RAISE EXCEPTION
                    'pvt_build_where_from_json: field "%" (normalized "%") has no metadata. Did pvt_collect_fields miss it?',
                    k, v_normalized;
            END IF;
            IF v_normalized <> k
               AND COALESCE((meta->>'was_contains_key')::boolean, false) THEN
                parts := parts || format('%I IS NOT NULL', v_normalized);
            ELSE
                parts := parts || pvt_build_field_condition(v_normalized, meta, v, p_base_prefix);
            END IF;
        END IF;
    END LOOP;

    IF array_length(parts, 1) IS NULL THEN
        RETURN 'TRUE';
    END IF;
    IF array_length(parts, 1) = 1 THEN
        RETURN parts[1];
    END IF;
    RETURN '(' || array_to_string(parts, ' AND ') || ')';
END;
$BODY$;

COMMENT ON FUNCTION pvt_build_where_from_json(jsonb, jsonb, text) IS
    'Recursive filter walker. Emits a SQL WHERE expression. With default p_base_prefix='''' base columns are referenced bare (matches the legacy wide CTE shape); with p_base_prefix=''o.'' base references are emitted as `o.<system_col>` so the predicate composes against `FROM _objects o` in the narrow Pro-shape outer SELECT.';
