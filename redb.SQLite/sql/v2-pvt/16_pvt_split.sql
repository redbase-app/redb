-- =====================================================================
-- 16_pvt_split.sql
-- ---------------------------------------------------------------------
-- Pro-parity "base pushdown" optimizer. Walks the filter JSON and
-- splits it into two parts:
--
--   * pushdown_sql       -- a SQL predicate over `_objects o.*` that is
--                           safe to AND with `o._id_scheme = X` inside
--                           the pivot CTE; lets PG use indexes on
--                           system columns BEFORE the JOIN with _values
--                           and the GROUP BY agg.
--   * residual_filter    -- the JSON subtree that still needs to be
--                           evaluated against pivot/props columns AFTER
--                           the CTE materializes; fed to the regular
--                           pvt_build_where_from_json walker.
--
-- Function:
--   pvt_split_filter(
--       p_filter jsonb,
--       p_fields jsonb,
--       OUT v_pushdown_sql    text,
--       OUT v_residual_filter jsonb
--   )
--
-- Splitting rules (conservative — never changes semantics):
--   leaf {f: ops}              base/hierarchical -> push,
--                              else                -> residual.
--   {$and: [c1, c2, ...]}      split each child; AND the push parts;
--                              residual = $and of non-null residuals
--                              (degraded to single child or NULL).
--   {$or:  [c1, c2, ...]}      pushable ONLY when every child is fully
--                              base (residual_i is NULL for all i).
--                              Mixed-leaf $or stays entirely in residual.
--   {$not: c}                  pushable ONLY when c is fully base.
--                              Otherwise stays entirely in residual.
--   top-level multi-key object treated as implicit $and of singletons.
--
-- Hierarchical operators ($hasAncestor / $hasDescendant / $level /
-- $isRoot / $isLeaf) are always pushable because the legacy helper
-- already emits SQL over the `o` alias of _objects.
-- =====================================================================

CREATE OR REPLACE FUNCTION pvt_split_filter(
    p_filter             jsonb,
    p_fields             jsonb,
    OUT v_pushdown_sql    text,
    OUT v_residual_filter jsonb
)
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    k                 text;
    v                 jsonb;
    elem              jsonb;
    child_push        text;
    child_residual    jsonb;
    push_parts        text[] := ARRAY[]::text[];
    residual_children jsonb  := '[]'::jsonb;
    is_logical        boolean := false;
    is_field_object   boolean := false;
    key_count         integer;
    v_peek            text;
    v_normalized      text;
    v_meta            jsonb;
    v_kind            text;
    v_op_key          text;
    v_singleton       jsonb;
BEGIN
    v_pushdown_sql    := NULL;
    v_residual_filter := NULL;

    -- Empty filter: nothing to push, nothing to filter.
    IF p_filter IS NULL OR p_filter = '{}'::jsonb THEN
        RETURN;
    END IF;

    IF jsonb_typeof(p_filter) <> 'object' THEN
        RAISE EXCEPTION 'pvt_split_filter: filter must be a JSON object (got %)', jsonb_typeof(p_filter);
    END IF;

    SELECT count(*) INTO key_count FROM jsonb_object_keys(p_filter);

    -- Detect logical operator at this node.
    SELECT key INTO v_op_key
      FROM jsonb_object_keys(p_filter) AS k(key)
     WHERE lower(k.key) IN ('$and', '$or', '$not');

    -- A node is "logical" only when it is a singleton {$and|$or|$not: ...}.
    -- Mixing {$and: [...], "Age": 18} is legal in the outer parser
    -- (treated as implicit AND), so re-route those through the multi-key
    -- path below.
    IF v_op_key IS NOT NULL AND key_count = 1 THEN
        is_logical := true;
    END IF;

    -- ============================================================
    -- 1) Logical singleton: $and / $or / $not
    -- ============================================================
    IF is_logical THEN
        v := p_filter -> v_op_key;

        IF lower(v_op_key) = '$and' THEN
            IF jsonb_typeof(v) <> 'array' THEN
                RAISE EXCEPTION 'pvt_split_filter: $and expects an array';
            END IF;
            FOR elem IN SELECT value FROM jsonb_array_elements(v) LOOP
                SELECT s.v_pushdown_sql, s.v_residual_filter
                  INTO child_push, child_residual
                  FROM pvt_split_filter(elem, p_fields) s;
                IF child_push IS NOT NULL THEN
                    push_parts := push_parts || child_push;
                END IF;
                IF child_residual IS NOT NULL THEN
                    residual_children := residual_children || jsonb_build_array(child_residual);
                END IF;
            END LOOP;

            IF array_length(push_parts, 1) IS NOT NULL THEN
                IF array_length(push_parts, 1) = 1 THEN
                    v_pushdown_sql := push_parts[1];
                ELSE
                    v_pushdown_sql := '(' || array_to_string(push_parts, ' AND ') || ')';
                END IF;
            END IF;

            IF jsonb_array_length(residual_children) > 0 THEN
                IF jsonb_array_length(residual_children) = 1 THEN
                    v_residual_filter := residual_children -> 0;
                ELSE
                    v_residual_filter := jsonb_build_object('$and', residual_children);
                END IF;
            END IF;
            RETURN;
        END IF;

        IF lower(v_op_key) = '$or' THEN
            IF jsonb_typeof(v) <> 'array' THEN
                RAISE EXCEPTION 'pvt_split_filter: $or expects an array';
            END IF;
            -- Pushable only if EVERY child is fully base (residual=NULL).
            -- Otherwise the whole $or must stay in residual unchanged,
            -- else we would tighten the predicate inside the CTE.
            DECLARE
                all_pushable boolean := true;
                or_pushes    text[]  := ARRAY[]::text[];
            BEGIN
                FOR elem IN SELECT value FROM jsonb_array_elements(v) LOOP
                    SELECT s.v_pushdown_sql, s.v_residual_filter
                      INTO child_push, child_residual
                      FROM pvt_split_filter(elem, p_fields) s;
                    IF child_residual IS NOT NULL OR child_push IS NULL THEN
                        all_pushable := false;
                        EXIT;
                    END IF;
                    or_pushes := or_pushes || child_push;
                END LOOP;

                IF all_pushable AND array_length(or_pushes, 1) IS NOT NULL THEN
                    v_pushdown_sql := '(' || array_to_string(or_pushes, ' OR ') || ')';
                    v_residual_filter := NULL;
                ELSE
                    v_pushdown_sql := NULL;
                    v_residual_filter := p_filter;
                END IF;
            END;
            RETURN;
        END IF;

        IF lower(v_op_key) = '$not' THEN
            SELECT s.v_pushdown_sql, s.v_residual_filter
              INTO child_push, child_residual
              FROM pvt_split_filter(v, p_fields) s;
            IF child_residual IS NULL AND child_push IS NOT NULL THEN
                v_pushdown_sql    := 'NOT (' || child_push || ')';
                v_residual_filter := NULL;
            ELSE
                v_pushdown_sql    := NULL;
                v_residual_filter := p_filter;
            END IF;
            RETURN;
        END IF;
    END IF;

    -- ============================================================
    -- 2) Multi-key object: implicit $and. Split each {k: v} singleton
    --    and reassemble.
    -- ============================================================
    IF key_count > 1 THEN
        FOR k, v IN SELECT key, value FROM jsonb_each(p_filter) LOOP
            v_singleton := jsonb_build_object(k, v);
            SELECT s.v_pushdown_sql, s.v_residual_filter
              INTO child_push, child_residual
              FROM pvt_split_filter(v_singleton, p_fields) s;
            IF child_push IS NOT NULL THEN
                push_parts := push_parts || child_push;
            END IF;
            IF child_residual IS NOT NULL THEN
                residual_children := residual_children || jsonb_build_array(child_residual);
            END IF;
        END LOOP;

        IF array_length(push_parts, 1) IS NOT NULL THEN
            IF array_length(push_parts, 1) = 1 THEN
                v_pushdown_sql := push_parts[1];
            ELSE
                v_pushdown_sql := '(' || array_to_string(push_parts, ' AND ') || ')';
            END IF;
        END IF;

        IF jsonb_array_length(residual_children) > 0 THEN
            IF jsonb_array_length(residual_children) = 1 THEN
                v_residual_filter := residual_children -> 0;
            ELSE
                v_residual_filter := jsonb_build_object('$and', residual_children);
            END IF;
        END IF;
        RETURN;
    END IF;

    -- ============================================================
    -- 3) Single-key leaf: either a hierarchical op or a field leaf.
    -- ============================================================
    SELECT key, value INTO k, v FROM jsonb_each(p_filter);

    -- Hierarchical operators -> always pushable. Reuse legacy helper.
    -- The legacy helper returns conditions with a leading ` AND ` so
    -- they can be concatenated to a pre-existing WHERE; we strip it
    -- here so the caller controls AND-joining.
    IF lower(k) IN ('$hasancestor', '$hasdescendant', '$level', '$isroot', '$isleaf', '$childrenof') THEN
        v_pushdown_sql    := pvt_build_hierarchical_conditions(
            jsonb_build_object(k, v),
            'o');
        IF v_pushdown_sql IS NOT NULL THEN
            v_pushdown_sql := regexp_replace(v_pushdown_sql, '^\s*AND\s+', '');
            v_pushdown_sql := trim(v_pushdown_sql);
            IF v_pushdown_sql = '' THEN
                v_pushdown_sql := NULL;
            END IF;
        END IF;
        v_residual_filter := NULL;
        RETURN;
    END IF;

    -- Expression-form predicate ($eq/$ne/$lt/.../$between/$in/...).
    -- Pushable iff every $field referenced inside resolves to a base
    -- column; otherwise the whole node stays in residual unchanged.
    IF lower(k) IN (
        '$eq', '$ne', '$lt', '$lte', '$gt', '$gte',
        '$like', '$ilike',
        '$in', '$nin', '$between',
        '$null', '$notnull', '$isnull', '$exists',
        '$contains', '$startswith', '$endswith',
        '$containsignorecase', '$startswithignorecase', '$endswithignorecase'
    ) THEN
        IF pvt_expr_is_base_only(v, p_fields) THEN
            v_pushdown_sql    := pvt_build_expr_predicate(k, v, p_fields, 'o.');
            v_residual_filter := NULL;
        ELSE
            v_pushdown_sql    := NULL;
            v_residual_filter := p_filter;
        END IF;
        RETURN;
    END IF;

    -- Top-level $expr: arbitrary boolean expression. Pushable iff every
    -- $field reference inside resolves to a base column; otherwise the
    -- whole node stays in residual unchanged.
    IF lower(k) = '$expr' THEN
        IF pvt_expr_is_base_only(v, p_fields) THEN
            v_pushdown_sql    := pvt_build_bool_expr(v, p_fields, 'o.');
            v_residual_filter := NULL;
        ELSE
            v_pushdown_sql    := NULL;
            v_residual_filter := p_filter;
        END IF;
        RETURN;
    END IF;

    -- Field leaf -> look up metadata; push only if kind = 'base'.
    v_peek       := pvt_peek_contains_key_value(v);
    v_normalized := pvt_normalize_field_name(k, v_peek);
    v_meta       := p_fields -> v_normalized;
    IF v_meta IS NULL THEN
        -- Unknown field: leave to the outer walker so its RAISE fires
        -- with the existing, descriptive error message.
        v_pushdown_sql    := NULL;
        v_residual_filter := p_filter;
        RETURN;
    END IF;

    v_kind := v_meta->>'kind';
    IF v_kind = 'base' THEN
        -- ContainsKey rewrites are dict-pivot only, never base.
        v_pushdown_sql    := pvt_build_field_condition(
            v_normalized, v_meta, v, 'o.');
        v_residual_filter := NULL;
    ELSE
        v_pushdown_sql    := NULL;
        v_residual_filter := p_filter;
    END IF;
END;
$BODY$;

COMMENT ON FUNCTION pvt_split_filter(jsonb, jsonb) IS
    'Splits a filter tree into (pushdown_sql over _objects o, residual_filter for outer CTE WHERE). Conservative: $or/$not are pushed only when every leaf inside is a base/hierarchical predicate; mixed branches stay in residual unchanged so semantics never change.';

-- =====================================================================
-- pvt_filter_has_base_refs: scan a residual filter and report whether it
-- references any base/hierarchical column. Used to gate WHERE pushdown
-- into _pvt_cte (Pro-parity perf): when false, the residual is safe to
-- evaluate inside the CTE wrapper that exposes only pivoted columns.
-- =====================================================================
CREATE OR REPLACE FUNCTION pvt_filter_has_base_refs(p_filter jsonb, p_fields jsonb)
RETURNS boolean
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    k text; v jsonb; elem jsonb;
    v_norm text; v_meta jsonb; v_peek text;
BEGIN
    IF p_filter IS NULL OR p_filter = '{}'::jsonb THEN
        RETURN false;
    END IF;
    IF jsonb_typeof(p_filter) <> 'object' THEN
        RETURN true;  -- conservative
    END IF;
    FOR k, v IN SELECT key, value FROM jsonb_each(p_filter) LOOP
        IF lower(k) IN ('$hasancestor','$hasdescendant','$level','$isroot','$isleaf') THEN
            RETURN true;
        END IF;
        IF lower(k) IN ('$and','$or') THEN
            IF jsonb_typeof(v) = 'array' THEN
                FOR elem IN SELECT value FROM jsonb_array_elements(v) LOOP
                    IF pvt_filter_has_base_refs(elem, p_fields) THEN RETURN true; END IF;
                END LOOP;
            END IF;
            CONTINUE;
        END IF;
        IF lower(k) = '$not' THEN
            IF pvt_filter_has_base_refs(v, p_fields) THEN RETURN true; END IF;
            CONTINUE;
        END IF;
        IF left(k, 1) = '$' THEN
            -- $expr / expression-form ops: conservative -> true
            RETURN true;
        END IF;
        -- field leaf
        v_peek := pvt_peek_contains_key_value(v);
        v_norm := pvt_normalize_field_name(k, v_peek);
        v_meta := p_fields -> v_norm;
        IF v_meta IS NULL THEN
            RETURN true;  -- conservative
        END IF;
        IF (v_meta->>'kind') = 'base' THEN
            RETURN true;
        END IF;
    END LOOP;
    RETURN false;
END;
$BODY$;

COMMENT ON FUNCTION pvt_filter_has_base_refs(jsonb, jsonb) IS
    'Returns true when the residual filter references any base column or hierarchical operator. Used to gate WHERE pushdown into the _pvt_cte wrapper (which projects only pivoted columns).';
