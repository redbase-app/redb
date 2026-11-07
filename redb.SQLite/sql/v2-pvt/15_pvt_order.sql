-- =====================================================================
-- 15_pvt_order.sql
-- ---------------------------------------------------------------------
-- Build an ORDER BY clause for the outer SELECT, referencing CTE
-- columns. Returns the empty string when no ordering is requested.
--
-- Function:
--   pvt_build_order_conditions(p_order jsonb, p_fields jsonb,
--                              p_base_prefix text DEFAULT '') RETURNS text
--
-- Input shape (accepted variants):
--   [
--     { "field": "FullName", "dir": "asc"  },
--     { "field": "Age",      "dir": "desc", "nulls": "last" }
--   ]
--   [
--     { "field": "FullName",      "direction": "ASC"  },     -- legacy C# emits this
--     { "field_path": "Age",      "direction": "DESC" }      -- legacy SQL emits this
--   ]
--   [
--     { "$expr": {"$mul":[{"$field":"Age"},{"$const":2}]},  -- arithmetic
--       "dir": "desc" },
--     { "$expr": {"$upper":{"$field":"FirstName"}},          -- function
--       "dir": "asc", "nulls": "last" }
--   ]
--
-- ORDER BY always runs on the OUTER SELECT (over the materialized CTE
-- row + JOIN _objects o in narrow Pro shape). Expression compilation
-- is delegated to pvt_build_scalar_expr from 17_pvt_expr.sql so the
-- same $field/$const/$add/.../$upper/.../$cast grammar that WHERE
-- accepts works in ORDER BY too. Base fields are emitted as
-- p_base_prefix || quote_ident(<system_col>), pivot fields as
-- quote_ident(<name>) (= the CTE column).
-- =====================================================================

-- Signature evolved in v0.6.0 (expression-form $expr support) and
-- v0.6.1 (p_distinct_on prepend for SELECT DISTINCT ON parity).
-- CREATE OR REPLACE cannot change argument lists, so drop legacy forms.
DROP FUNCTION IF EXISTS pvt_build_order_conditions(jsonb, jsonb);
DROP FUNCTION IF EXISTS pvt_build_order_conditions(jsonb, jsonb, text);
DROP FUNCTION IF EXISTS pvt_build_order_conditions(jsonb, jsonb, text, jsonb);
DROP FUNCTION IF EXISTS _pvt_compile_order_col(jsonb, jsonb, text);


-- ---------- internal helper: compile one entry column expression ------
-- Shared by ORDER BY, DISTINCT ON, GROUP BY and window PARTITION BY
-- builders. Returns the column-side SQL fragment (no direction, no
-- NULLS, no DISTINCT ON wrapping).
--
-- p_listitem_as_id (v0.4.x): controls how ListItem.Value / .Alias are
-- emitted. Pivot columns store raw `_ListItem` ids (parity with Pro);
-- ORDER BY / DISTINCT ON need the dereferenced string so that sorting
-- has a meaningful order, while GROUP BY / SELECT projection keep the
-- id directly. Pass `true` from GROUP BY-style builders.
CREATE OR REPLACE FUNCTION _pvt_compile_order_col(
    p_elem            jsonb,
    p_fields          jsonb,
    p_base_prefix     text,
    p_listitem_as_id  boolean DEFAULT false
)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_field   text;
    v_meta    jsonb;
    v_li_prop text;
    v_is_arr  boolean;
    v_col     text;
BEGIN
    IF p_elem IS NULL OR jsonb_typeof(p_elem) <> 'object' THEN
        RAISE EXCEPTION '_pvt_compile_order_col: each entry must be a JSON object (got %)', jsonb_typeof(p_elem);
    END IF;

    IF p_elem ? '$expr' THEN
        RETURN pvt_build_scalar_expr(p_elem->'$expr', p_fields, p_base_prefix);
    END IF;

    v_field := COALESCE(p_elem->>'field', p_elem->>'field_path');
    IF v_field IS NULL OR v_field = '' THEN
        RAISE EXCEPTION '_pvt_compile_order_col: entry must include "field", "field_path" or "$expr"';
    END IF;

    v_meta := p_fields -> v_field;
    IF v_meta IS NULL THEN
        RAISE EXCEPTION
            '_pvt_compile_order_col: field "%" has no metadata. Did pvt_collect_fields miss it?', v_field;
    END IF;

    IF v_meta->>'kind' = 'base' THEN
        RETURN p_base_prefix || quote_ident(v_meta->>'column');
    END IF;

    v_col     := quote_ident(v_field);
    v_li_prop := v_meta->>'list_item_prop';
    v_is_arr  := COALESCE((v_meta->>'is_array')::boolean, false);

    -- ListItem.Value / .Alias scalar: pivot column already holds the
    -- dereferenced _list_items._value / _alias string (see
    -- pvt_build_column_expr), so ORDER BY / GROUP BY just reference
    -- the column directly. No extra _list_items lookup needed.
    RETURN v_col;
END;
$BODY$;


-- ---------- pvt_build_distinct_on_select ------------------------------
-- Render a "DISTINCT ON (expr1, expr2) " SELECT-list prefix from a JSON
-- array of {field|$expr} entries. Returns the empty string when
-- p_distinct_on is NULL/empty. Used by the orchestrator inside the
-- three outer-SELECT shapes (Shape A, narrow, wide).
CREATE OR REPLACE FUNCTION pvt_build_distinct_on_select(
    p_distinct_on jsonb,
    p_fields      jsonb,
    p_base_prefix text DEFAULT ''
)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    elem  jsonb;
    parts text[] := ARRAY[]::text[];
BEGIN
    IF p_distinct_on IS NULL
       OR jsonb_typeof(p_distinct_on) <> 'array'
       OR jsonb_array_length(p_distinct_on) = 0 THEN
        RETURN '';
    END IF;
    FOR elem IN SELECT value FROM jsonb_array_elements(p_distinct_on) LOOP
        parts := parts || _pvt_compile_order_col(elem, p_fields, p_base_prefix);
    END LOOP;
    RETURN 'DISTINCT ON (' || array_to_string(parts, ', ') || ') ';
END;
$BODY$;

COMMENT ON FUNCTION pvt_build_distinct_on_select(jsonb, jsonb, text) IS
    'Renders a "DISTINCT ON (expr1, expr2) " SELECT-list prefix from a JSON array of {field|$expr} entries. PostgreSQL requires these expressions to also appear at the head of ORDER BY -- the caller pairs this with pvt_build_order_conditions(..., p_distinct_on) which auto-prepends them.';


CREATE OR REPLACE FUNCTION pvt_build_order_conditions(
    p_order       jsonb,
    p_fields      jsonb,
    p_base_prefix text  DEFAULT '',
    p_distinct_on jsonb DEFAULT NULL
)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    elem  jsonb;
    dir   text;
    nulls text;
    parts text[] := ARRAY[]::text[];
    v_col text;
BEGIN
    -- ---------- DISTINCT ON prefix --------------------------------
    -- PostgreSQL requires every DISTINCT ON expression to appear at
    -- the head of ORDER BY (with matching direction). We always emit
    -- them ASC -- callers needing the opposite must include the same
    -- expression explicitly in p_order with their desired direction
    -- before any other ordering keys, in which case PG will use that
    -- direction and we MUST NOT double-add (deduplicate by textual
    -- equality of the compiled fragment).
    IF p_distinct_on IS NOT NULL
       AND jsonb_typeof(p_distinct_on) = 'array'
       AND jsonb_array_length(p_distinct_on) > 0 THEN
        FOR elem IN SELECT value FROM jsonb_array_elements(p_distinct_on) LOOP
            v_col := _pvt_compile_order_col(elem, p_fields, p_base_prefix);
            parts := parts || (v_col || ' ASC');
        END LOOP;
    END IF;

    IF p_order IS NULL OR jsonb_typeof(p_order) <> 'array'
       OR jsonb_array_length(p_order) = 0 THEN
        IF array_length(parts, 1) IS NULL THEN
            RETURN '';
        END IF;
        RETURN E'\nORDER BY ' || array_to_string(parts, ', ');
    END IF;

    FOR elem IN SELECT value FROM jsonb_array_elements(p_order) LOOP
        dir   := lower(COALESCE(elem->>'dir', elem->>'direction', 'asc'));
        nulls := lower(COALESCE(elem->>'nulls', ''));

        IF dir NOT IN ('asc', 'desc') THEN
            RAISE EXCEPTION 'pvt_build_order_conditions: invalid direction "%" (asc|desc only)', dir;
        END IF;

        v_col := _pvt_compile_order_col(elem, p_fields, p_base_prefix);

        parts := parts || (
            v_col
            || ' ' || upper(dir)
            || CASE
                WHEN nulls = 'first' THEN ' NULLS FIRST'
                WHEN nulls = 'last'  THEN ' NULLS LAST'
                ELSE ''
               END
        );
    END LOOP;

    RETURN E'\nORDER BY ' || array_to_string(parts, ', ');
END;
$BODY$;

COMMENT ON FUNCTION pvt_build_order_conditions(jsonb, jsonb, text, jsonb) IS
    'Builds an ORDER BY clause for the outer SELECT. Accepts plain {"field"|"field_path","dir","nulls"} entries and Pro-parity {"$expr":<scalar-expr>,"dir","nulls"} entries; expressions are compiled via pvt_build_scalar_expr. Base fields use p_base_prefix (default '''', or ''o.'' for the narrow Pro-shape outer that JOINs _objects). When p_distinct_on is provided its expressions are auto-prepended ASC (PostgreSQL DISTINCT ON requirement). Returns empty string when both p_order and p_distinct_on are empty.';
