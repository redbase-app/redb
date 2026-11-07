-- =====================================================================
-- 17_pvt_expr.sql
-- ---------------------------------------------------------------------
-- Expression engine for the free PVT pipeline. Implements arithmetic,
-- string and Math.* style functions, parenthesized nesting and full
-- expression-form predicates --- matching Pro's `WhereRedb` /
-- `IRedbTreeQueryable` capability (Pro emits the same SQL by walking
-- the LINQ AST via ExpressionToSqlCompiler; here we accept a JSON AST
-- shaped 1:1 with that compiler's output).
--
-- ---------------------------------------------------------------------
-- Scalar expression grammar (returns a SQL fragment):
--
--   { "$field":  "<path>" }                    -> "PathColumn"
--                                                or 'o."_id"' under pushdown
--   { "$const":  <json scalar> }               -> quoted literal
--   { "$add":    [a, b, ...] }                 -> (a + b + ...)
--   { "$sub":    [a, b] }                      -> (a - b)
--   { "$mul":    [a, b, ...] }                 -> (a * b * ...)
--   { "$div":    [a, b] }                      -> (a / b)
--   { "$mod":    [a, b] }                      -> (a % b)
--   { "$neg":    a }                           -> (-a)
--   { "$abs":    a }                           -> ABS(a)
--   { "$round":  [a, digits] }                 -> ROUND(a, digits)
--   { "$floor":  a }                           -> FLOOR(a)
--   { "$ceil":   a }                           -> CEIL(a)
--   { "$min":    [a, b, ...] }                 -> LEAST(a, b, ...)
--   { "$max":    [a, b, ...] }                 -> GREATEST(a, b, ...)
--   { "$upper":  a }                           -> UPPER(a)
--   { "$lower":  a }                           -> LOWER(a)
--   { "$trim":   a }                           -> TRIM(a)
--   { "$length": a }                           -> LENGTH(a) (str) or
--                                                COALESCE(array_length(a,1),0)
--   { "$concat": [a, b, ...] }                 -> (a || b || ...)
--   { "$coalesce":[a, b, ...] }                -> COALESCE(a, b, ...)
--   { "$cast":   ["<sqltype>", a] }            -> (a)::<sqltype>
--
-- Expression-form predicate grammar (returns a SQL boolean fragment):
--
--   { "$eq" | "$ne" | "$lt" | "$lte" | "$gt" | "$gte":
--                [exprL, exprR] }              -> (L op R)
--   { "$like"  | "$ilike":
--                [exprL, exprR] }              -> (L LIKE / ILIKE R)
--   { "$contains" | "$startsWith" | "$endsWith":
--                [exprL, "literal"] }          -> sugar over LIKE
--   { "$containsIgnoreCase" | "$startsWithIgnoreCase" | "$endsWithIgnoreCase":
--                [exprL, "literal"] }          -> sugar over ILIKE
--   { "$in" | "$nin":
--                [exprL, [v1, v2, ...]] }      -> L [NOT] IN (...)
--   { "$between":
--                [exprL, low, high] }          -> L BETWEEN low AND high
--   { "$null"  | "$notNull"  | "$isNull":
--                expr | true }                 -> expr IS [NOT] NULL
--
-- Push-down classification: pvt_expr_is_base_only(node, fields) returns
-- true iff every `$field` reference inside resolves to kind='base'. SQL
-- functions and arithmetic are pushable by construction (they map 1:1
-- to PG operators).
-- =====================================================================


-- ---------- pvt_expr_field_names --------------------------------------
-- Yield every `$field` reference inside a scalar expression node.
-- Used by pvt_collect_fields so metadata is resolved for them.
CREATE OR REPLACE FUNCTION pvt_expr_field_names(p_node jsonb)
RETURNS SETOF text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    k    text;
    v    jsonb;
    elem jsonb;
BEGIN
    IF p_node IS NULL THEN
        RETURN;
    END IF;

    -- Array node (predicate args or n-ary operand list): walk each element.
    IF jsonb_typeof(p_node) = 'array' THEN
        FOR elem IN SELECT value FROM jsonb_array_elements(p_node) LOOP
            RETURN QUERY SELECT pvt_expr_field_names(elem);
        END LOOP;
        RETURN;
    END IF;

    -- Scalar literal: nothing to harvest.
    IF jsonb_typeof(p_node) <> 'object' THEN
        RETURN;
    END IF;

    IF p_node ? '$field' THEN
        RETURN NEXT p_node->>'$field';
        RETURN;
    END IF;
    IF p_node ? '$const' THEN
        RETURN;
    END IF;

    -- Generic recursion over every operand.
    FOR k, v IN SELECT key, value FROM jsonb_each(p_node) LOOP
        IF left(k, 1) <> '$' THEN
            CONTINUE;
        END IF;
        -- $case has named branches {when,then,else}: descend into values only.
        IF lower(k) = '$case' AND jsonb_typeof(v) = 'array' THEN
            FOR elem IN SELECT value FROM jsonb_array_elements(v) LOOP
                IF jsonb_typeof(elem) = 'object' THEN
                    IF elem ? 'when' THEN
                        RETURN QUERY SELECT pvt_expr_field_names(elem->'when');
                    END IF;
                    IF elem ? 'then' THEN
                        RETURN QUERY SELECT pvt_expr_field_names(elem->'then');
                    END IF;
                    IF elem ? 'else' THEN
                        RETURN QUERY SELECT pvt_expr_field_names(elem->'else');
                    END IF;
                END IF;
            END LOOP;
            CONTINUE;
        END IF;
        -- $fts object form has `query` (expression) + `fields` array;
        -- `language` is a literal config name and must not be walked.
        IF lower(k) = '$fts' AND jsonb_typeof(v) = 'object' THEN
            IF v ? 'query'  THEN RETURN QUERY SELECT pvt_expr_field_names(v->'query'); END IF;
            IF v ? 'fields' AND jsonb_typeof(v->'fields') = 'array' THEN
                FOR elem IN SELECT value FROM jsonb_array_elements(v->'fields') LOOP
                    RETURN QUERY SELECT pvt_expr_field_names(elem);
                END LOOP;
            END IF;
            CONTINUE;
        END IF;
        -- $cast: ["<sql-type>", expr] -- type literal must not be walked.
        IF lower(k) = '$cast' AND jsonb_typeof(v) = 'array' AND jsonb_array_length(v) >= 2 THEN
            RETURN QUERY SELECT pvt_expr_field_names(v->1);
            CONTINUE;
        END IF;
        -- Date helpers whose first array slot is a unit string literal.
        IF lower(k) IN ('$dateadd', '$datesub', '$datediff', '$datetrunc')
           AND jsonb_typeof(v) = 'array' AND jsonb_array_length(v) >= 1 THEN
            FOR elem IN SELECT value FROM jsonb_array_elements(v) WITH ORDINALITY AS t(value, ord)
                          WHERE ord >= 2 LOOP
                RETURN QUERY SELECT pvt_expr_field_names(elem);
            END LOOP;
            CONTINUE;
        END IF;
        IF jsonb_typeof(v) = 'array' THEN
            FOR elem IN SELECT value FROM jsonb_array_elements(v) LOOP
                RETURN QUERY SELECT pvt_expr_field_names(elem);
            END LOOP;
        ELSIF jsonb_typeof(v) = 'object' THEN
            RETURN QUERY SELECT pvt_expr_field_names(v);
        END IF;
    END LOOP;
END;
$BODY$;

COMMENT ON FUNCTION pvt_expr_field_names(jsonb) IS
    'Yields every "$field" path referenced inside a scalar expression node (recursive). Empty for $const / non-object nodes.';


-- ---------- pvt_expr_is_base_only -------------------------------------
-- Recursive check: every `$field` inside the node maps to a metadata
-- entry with kind='base'. Used by the pushdown splitter to decide
-- whether an expression predicate can move inside the pivot CTE.
CREATE OR REPLACE FUNCTION pvt_expr_is_base_only(
    p_node   jsonb,
    p_fields jsonb
)
RETURNS boolean
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_name text;
    v_meta jsonb;
BEGIN
    IF p_node IS NULL THEN
        RETURN true;
    END IF;
    FOR v_name IN SELECT pvt_expr_field_names(p_node) LOOP
        v_meta := p_fields -> v_name;
        IF v_meta IS NULL OR v_meta->>'kind' <> 'base' THEN
            RETURN false;
        END IF;
    END LOOP;
    RETURN true;
END;
$BODY$;

COMMENT ON FUNCTION pvt_expr_is_base_only(jsonb, jsonb) IS
    'True iff every $field inside the expression resolves (per pvt_collect_fields) to kind=base. Pushable predicates run inside the pivot CTE on _objects o.*.';


-- ---------- pvt_format_const ------------------------------------------
-- Convert a JSON scalar to a SQL literal. Strings are single-quoted
-- with `''` escaping; booleans/null/numbers map natively. No casts:
-- callers wrap with `$cast` when an explicit type is needed.
CREATE OR REPLACE FUNCTION pvt_format_const(p_value jsonb)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
AS $BODY$
DECLARE
    v_t text;
BEGIN
    IF p_value IS NULL OR jsonb_typeof(p_value) = 'null' THEN
        RETURN 'NULL';
    END IF;
    v_t := jsonb_typeof(p_value);
    IF v_t = 'string'  THEN RETURN quote_literal(p_value #>> '{}'); END IF;
    IF v_t = 'number'  THEN RETURN p_value::text;                    END IF;
    IF v_t = 'boolean' THEN RETURN p_value::text;                    END IF;
    -- Arrays and objects are not scalar literals.
    RAISE EXCEPTION
        'pvt_format_const: unsupported JSON type % for scalar literal (use $const for scalars only)', v_t;
END;
$BODY$;


-- ---------- pvt_build_scalar_expr -------------------------------------
-- Recursive scalar-expression compiler. Always wraps binary/unary
-- results in parentheses so operator precedence is preserved through
-- nesting.
CREATE OR REPLACE FUNCTION pvt_build_scalar_expr(
    p_node        jsonb,
    p_fields      jsonb,
    p_base_prefix text DEFAULT ''
)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_name      text;
    v_meta      jsonb;
    v_kind      text;
    v_args      jsonb;
    v_parts     text[];
    v_elem      jsonb;
    v_op_key    text;
    v_n         integer;
    v_a         text;
    v_b         text;
    v_type      text;
BEGIN
    IF p_node IS NULL OR jsonb_typeof(p_node) = 'null' THEN
        RETURN 'NULL';
    END IF;

    -- Bare JSON scalar / array literal: tolerate it as shorthand for
    -- $const (saves callers from wrapping every literal).
    IF jsonb_typeof(p_node) <> 'object' THEN
        IF jsonb_typeof(p_node) = 'array' THEN
            RAISE EXCEPTION
                'pvt_build_scalar_expr: bare JSON array is not a scalar expression (wrap each element or use a list-aware operator like $in)';
        END IF;
        RETURN pvt_format_const(p_node);
    END IF;

    -- ---------------- $field ---------------------------------------
    IF p_node ? '$field' THEN
        v_name := p_node->>'$field';
        v_meta := p_fields -> v_name;
        IF v_meta IS NULL THEN
            RAISE EXCEPTION
                'pvt_build_scalar_expr: $field "%" has no metadata (did pvt_collect_fields miss it?)', v_name;
        END IF;
        v_kind := v_meta->>'kind';
        IF v_kind = 'base' THEN
            RETURN p_base_prefix || quote_ident(v_meta->>'column');
        END IF;
        RETURN quote_ident(v_name);
    END IF;

    -- ---------------- $const ---------------------------------------
    IF p_node ? '$const' THEN
        RETURN pvt_format_const(p_node->'$const');
    END IF;

    -- Dispatch on the single $-key.
    SELECT key INTO v_op_key FROM jsonb_object_keys(p_node) AS t(key) WHERE left(key,1) = '$' LIMIT 1;
    IF v_op_key IS NULL THEN
        RAISE EXCEPTION 'pvt_build_scalar_expr: no operator key in node: %', p_node::text;
    END IF;
    v_args := p_node -> v_op_key;

    -- ---------------- Aggregate passthrough ------------------------
    -- Aggregate expressions ($count/$sum/$avg/$min*/$max*/$string_agg/
    -- $bool_and/$bool_or) appearing inside a scalar context delegate to
    -- pvt_build_agg_expr. This makes HAVING / ORDER BY / window args
    -- agg-aware without a separate compiler stack. PostgreSQL itself
    -- rejects aggregates in WHERE / GROUP BY so this is not a footgun.
    -- $min and $max overlap with n-ary LEAST/GREATEST helpers; the
    -- aggregate form is recognised only when the operand is a single
    -- non-array scalar-expression node (or "*"). LEAST/GREATEST callers
    -- always pass an array of >=1 operands.
    IF lower(v_op_key) IN ('$count', '$sum', '$avg', '$string_agg', '$bool_and', '$bool_or')
       OR (lower(v_op_key) IN ('$min', '$max')
           AND jsonb_typeof(v_args) <> 'array') THEN
        RETURN pvt_build_agg_expr(p_node, p_fields, p_base_prefix);
    END IF;

    -- ---------------- Window passthrough ---------------------------
    -- {"$over": <window-node>} delegates to pvt_build_window_expr.
    -- PostgreSQL itself rejects window calls outside SELECT / ORDER BY.
    IF lower(v_op_key) = '$over' THEN
        RETURN pvt_build_window_expr(v_args, p_fields, p_base_prefix);
    END IF;

    -- N-ary helpers --------------------------------------------------
    IF lower(v_op_key) IN ('$add', '$sub', '$mul', '$div', '$mod', '$concat', '$min', '$max', '$coalesce') THEN
        IF jsonb_typeof(v_args) <> 'array' THEN
            RAISE EXCEPTION 'pvt_build_scalar_expr: % expects a JSON array of operands', v_op_key;
        END IF;
        v_n := jsonb_array_length(v_args);
        IF v_n < 1 THEN
            RAISE EXCEPTION 'pvt_build_scalar_expr: % expects at least 1 operand', v_op_key;
        END IF;
        IF lower(v_op_key) IN ('$sub', '$div', '$mod') AND v_n <> 2 THEN
            RAISE EXCEPTION 'pvt_build_scalar_expr: % expects exactly 2 operands (got %)', v_op_key, v_n;
        END IF;
        v_parts := ARRAY[]::text[];
        FOR v_elem IN SELECT value FROM jsonb_array_elements(v_args) LOOP
            v_parts := v_parts || pvt_build_scalar_expr(v_elem, p_fields, p_base_prefix);
        END LOOP;
        IF lower(v_op_key) = '$add'      THEN RETURN '(' || array_to_string(v_parts, ' + ')  || ')'; END IF;
        IF lower(v_op_key) = '$sub'      THEN RETURN '(' || v_parts[1] || ' - ' || v_parts[2] || ')'; END IF;
        IF lower(v_op_key) = '$mul'      THEN RETURN '(' || array_to_string(v_parts, ' * ')  || ')'; END IF;
        IF lower(v_op_key) = '$div'      THEN RETURN '(' || v_parts[1] || ' / ' || v_parts[2] || ')'; END IF;
        IF lower(v_op_key) = '$mod'      THEN RETURN '(' || v_parts[1] || ' % ' || v_parts[2] || ')'; END IF;
        IF lower(v_op_key) = '$concat'   THEN RETURN '(' || array_to_string(v_parts, ' || ') || ')'; END IF;
        IF lower(v_op_key) = '$min'      THEN RETURN 'LEAST('    || array_to_string(v_parts, ', ') || ')'; END IF;
        IF lower(v_op_key) = '$max'      THEN RETURN 'GREATEST(' || array_to_string(v_parts, ', ') || ')'; END IF;
        IF lower(v_op_key) = '$coalesce' THEN RETURN 'COALESCE(' || array_to_string(v_parts, ', ') || ')'; END IF;
    END IF;

    -- Unary -----------------------------------------------------------
    IF lower(v_op_key) IN ('$neg', '$abs', '$floor', '$ceil', '$upper', '$lower', '$trim', '$length') THEN
        -- Accept both { $op: expr } and { $op: [expr] } (array-wrapped
        -- form, which is how the C# compilers emit single-arg calls).
        IF jsonb_typeof(v_args) = 'array' THEN
            IF jsonb_array_length(v_args) <> 1 THEN
                RAISE EXCEPTION 'pvt_build_scalar_expr: % expects exactly 1 operand (got %)',
                    v_op_key, jsonb_array_length(v_args);
            END IF;
            v_a := pvt_build_scalar_expr(v_args->0, p_fields, p_base_prefix);
        ELSE
            v_a := pvt_build_scalar_expr(v_args, p_fields, p_base_prefix);
        END IF;
        IF lower(v_op_key) = '$neg'    THEN RETURN '(-' || v_a || ')'; END IF;
        IF lower(v_op_key) = '$abs'    THEN RETURN 'ABS('    || v_a || ')'; END IF;
        IF lower(v_op_key) = '$floor'  THEN RETURN 'FLOOR('  || v_a || ')'; END IF;
        IF lower(v_op_key) = '$ceil'   THEN RETURN 'CEIL('   || v_a || ')'; END IF;
        IF lower(v_op_key) = '$upper'  THEN RETURN 'UPPER('  || v_a || ')'; END IF;
        IF lower(v_op_key) = '$lower'  THEN RETURN 'LOWER('  || v_a || ')'; END IF;
        IF lower(v_op_key) = '$trim'   THEN RETURN 'TRIM('   || v_a || ')'; END IF;
        IF lower(v_op_key) = '$length' THEN
            -- Polymorphic: array → array_length, otherwise → text length.
            -- We can't always know the type statically, so prefer the
            -- string form; callers wanting array-length should use the
            -- `.$length` legacy modifier on a registered array field.
            RETURN 'LENGTH(' || v_a || ')';
        END IF;
    END IF;

    -- Round (2-arg) ---------------------------------------------------
    IF lower(v_op_key) = '$round' THEN
        IF jsonb_typeof(v_args) <> 'array' OR jsonb_array_length(v_args) NOT IN (1, 2) THEN
            RAISE EXCEPTION 'pvt_build_scalar_expr: $round expects [value] or [value, digits]';
        END IF;
        v_a := pvt_build_scalar_expr(v_args->0, p_fields, p_base_prefix);
        IF jsonb_array_length(v_args) = 1 THEN
            RETURN 'ROUND(' || v_a || ')';
        END IF;
        v_b := pvt_build_scalar_expr(v_args->1, p_fields, p_base_prefix);
        RETURN 'ROUND(' || v_a || ', ' || v_b || ')';
    END IF;

    -- $cast -----------------------------------------------------------
    IF lower(v_op_key) = '$cast' THEN
        IF jsonb_typeof(v_args) <> 'array' OR jsonb_array_length(v_args) <> 2
           OR jsonb_typeof(v_args->0) <> 'string' THEN
            RAISE EXCEPTION 'pvt_build_scalar_expr: $cast expects ["<sql-type>", expr]';
        END IF;
        v_type := v_args->>0;
        -- Whitelist SQL types to keep the surface area sane.
        IF v_type NOT IN (
            'text', 'varchar', 'bigint', 'integer', 'int', 'smallint',
            'numeric', 'double precision', 'real', 'boolean', 'uuid',
            'timestamptz', 'timestamp', 'date', 'bytea'
        ) THEN
            RAISE EXCEPTION 'pvt_build_scalar_expr: $cast unsupported type "%"', v_type;
        END IF;
        v_a := pvt_build_scalar_expr(v_args->1, p_fields, p_base_prefix);
        RETURN '(' || v_a || ')::' || v_type;
    END IF;

    -- ---------------- Extended unary (Pro-parity + free extras) -----
    -- Strings:  $trimStart -> LTRIM, $trimEnd -> RTRIM
    -- Math:     $sqrt, $sign, $exp, $ln (natural log)
    -- Dates:    $year/$month/$day/$hour/$minute/$second
    --           $dayOfWeek/$dayOfYear (free extras over Pro)
    IF lower(v_op_key) IN (
        '$trimstart', '$trimend',
        '$sqrt', '$sign', '$exp', '$ln',
        '$year', '$month', '$day', '$hour', '$minute', '$second',
        '$dayofweek', '$dayofyear'
    ) THEN
        IF jsonb_typeof(v_args) = 'array' THEN
            IF jsonb_array_length(v_args) <> 1 THEN
                RAISE EXCEPTION 'pvt_build_scalar_expr: % expects exactly 1 operand (got %)',
                    v_op_key, jsonb_array_length(v_args);
            END IF;
            v_a := pvt_build_scalar_expr(v_args->0, p_fields, p_base_prefix);
        ELSE
            v_a := pvt_build_scalar_expr(v_args, p_fields, p_base_prefix);
        END IF;
        IF lower(v_op_key) = '$trimstart'  THEN RETURN 'LTRIM(' || v_a || ')'; END IF;
        IF lower(v_op_key) = '$trimend'    THEN RETURN 'RTRIM(' || v_a || ')'; END IF;
        IF lower(v_op_key) = '$sqrt'       THEN RETURN 'SQRT('  || v_a || ')'; END IF;
        IF lower(v_op_key) = '$sign'       THEN RETURN 'SIGN('  || v_a || ')'; END IF;
        IF lower(v_op_key) = '$exp'        THEN RETURN 'EXP('   || v_a || ')'; END IF;
        IF lower(v_op_key) = '$ln'         THEN RETURN 'LN('    || v_a || ')'; END IF;
        IF lower(v_op_key) = '$year'       THEN RETURN 'EXTRACT(YEAR FROM '   || v_a || ')'; END IF;
        IF lower(v_op_key) = '$month'      THEN RETURN 'EXTRACT(MONTH FROM '  || v_a || ')'; END IF;
        IF lower(v_op_key) = '$day'        THEN RETURN 'EXTRACT(DAY FROM '    || v_a || ')'; END IF;
        IF lower(v_op_key) = '$hour'       THEN RETURN 'EXTRACT(HOUR FROM '   || v_a || ')'; END IF;
        IF lower(v_op_key) = '$minute'     THEN RETURN 'EXTRACT(MINUTE FROM ' || v_a || ')'; END IF;
        IF lower(v_op_key) = '$second'     THEN RETURN 'EXTRACT(SECOND FROM ' || v_a || ')'; END IF;
        IF lower(v_op_key) = '$dayofweek'  THEN RETURN 'EXTRACT(DOW FROM '    || v_a || ')'; END IF;
        IF lower(v_op_key) = '$dayofyear'  THEN RETURN 'EXTRACT(DOY FROM '    || v_a || ')'; END IF;
    END IF;

    -- ---------------- $power (2-arg) -------------------------------
    IF lower(v_op_key) = '$power' THEN
        IF jsonb_typeof(v_args) <> 'array' OR jsonb_array_length(v_args) <> 2 THEN
            RAISE EXCEPTION 'pvt_build_scalar_expr: $power expects [base, exponent]';
        END IF;
        v_a := pvt_build_scalar_expr(v_args->0, p_fields, p_base_prefix);
        v_b := pvt_build_scalar_expr(v_args->1, p_fields, p_base_prefix);
        RETURN 'POWER(' || v_a || ', ' || v_b || ')';
    END IF;

    -- ---------------- $log (2-arg: base, value) --------------------
    IF lower(v_op_key) = '$log' THEN
        IF jsonb_typeof(v_args) <> 'array' OR jsonb_array_length(v_args) <> 2 THEN
            RAISE EXCEPTION 'pvt_build_scalar_expr: $log expects [base, value]';
        END IF;
        v_a := pvt_build_scalar_expr(v_args->0, p_fields, p_base_prefix);
        v_b := pvt_build_scalar_expr(v_args->1, p_fields, p_base_prefix);
        RETURN 'LOG(' || v_a || ', ' || v_b || ')';
    END IF;

    -- ---------------- $substring (2 or 3 args) ---------------------
    -- $substring: [str, start]  -> SUBSTRING(str FROM start)
    -- $substring: [str, start, length] -> SUBSTRING(str FROM start FOR length)
    -- Note: PostgreSQL SUBSTRING is 1-based; callers translating from
    -- C#'s 0-based String.Substring must add 1 in the AST.
    IF lower(v_op_key) = '$substring' THEN
        IF jsonb_typeof(v_args) <> 'array' OR jsonb_array_length(v_args) NOT IN (2, 3) THEN
            RAISE EXCEPTION 'pvt_build_scalar_expr: $substring expects [str, start] or [str, start, length]';
        END IF;
        v_a := pvt_build_scalar_expr(v_args->0, p_fields, p_base_prefix);
        v_b := pvt_build_scalar_expr(v_args->1, p_fields, p_base_prefix);
        IF jsonb_array_length(v_args) = 2 THEN
            RETURN 'SUBSTRING(' || v_a || ' FROM ' || v_b || ')';
        END IF;
        DECLARE
            v_c text := pvt_build_scalar_expr(v_args->2, p_fields, p_base_prefix);
        BEGIN
            RETURN 'SUBSTRING(' || v_a || ' FROM ' || v_b || ' FOR ' || v_c || ')';
        END;
    END IF;

    -- ---------------- $replace (3-arg) -----------------------------
    IF lower(v_op_key) = '$replace' THEN
        IF jsonb_typeof(v_args) <> 'array' OR jsonb_array_length(v_args) <> 3 THEN
            RAISE EXCEPTION 'pvt_build_scalar_expr: $replace expects [str, find, replaceWith]';
        END IF;
        v_a := pvt_build_scalar_expr(v_args->0, p_fields, p_base_prefix);
        DECLARE
            v_find    text := pvt_build_scalar_expr(v_args->1, p_fields, p_base_prefix);
            v_replace text := pvt_build_scalar_expr(v_args->2, p_fields, p_base_prefix);
        BEGIN
            RETURN 'REPLACE(' || v_a || ', ' || v_find || ', ' || v_replace || ')';
        END;
    END IF;

    -- ---------------- $indexOf (2-arg) -----------------------------
    -- Returns 1-based position; 0 when not found (Postgres POSITION).
    -- For C#'s 0-based IndexOf, subtract 1 in the caller AST.
    IF lower(v_op_key) = '$indexof' THEN
        IF jsonb_typeof(v_args) <> 'array' OR jsonb_array_length(v_args) <> 2 THEN
            RAISE EXCEPTION 'pvt_build_scalar_expr: $indexOf expects [str, needle]';
        END IF;
        v_a := pvt_build_scalar_expr(v_args->0, p_fields, p_base_prefix);
        v_b := pvt_build_scalar_expr(v_args->1, p_fields, p_base_prefix);
        RETURN 'POSITION(' || v_b || ' IN ' || v_a || ')';
    END IF;

    -- ---------------- $padLeft / $padRight (2 or 3-arg) ------------
    IF lower(v_op_key) IN ('$padleft', '$padright') THEN
        IF jsonb_typeof(v_args) <> 'array' OR jsonb_array_length(v_args) NOT IN (2, 3) THEN
            RAISE EXCEPTION 'pvt_build_scalar_expr: % expects [str, length] or [str, length, padChar]', v_op_key;
        END IF;
        v_a := pvt_build_scalar_expr(v_args->0, p_fields, p_base_prefix);
        v_b := pvt_build_scalar_expr(v_args->1, p_fields, p_base_prefix);
        DECLARE
            v_kw  text := CASE WHEN lower(v_op_key) = '$padleft' THEN 'LPAD' ELSE 'RPAD' END;
            -- PG LPAD/RPAD truncate when target length < input length, while
            -- C# PadLeft/PadRight are no-op in that case. Use GREATEST() so we
            -- never request a width smaller than the input string.
            v_len text := 'GREATEST(length(' || v_a || '), ' || v_b || ')';
        BEGIN
            IF jsonb_array_length(v_args) = 2 THEN
                RETURN v_kw || '(' || v_a || ', ' || v_len || ')';
            END IF;
            RETURN v_kw || '(' || v_a || ', ' || v_len || ', '
                       || pvt_build_scalar_expr(v_args->2, p_fields, p_base_prefix) || ')';
        END;
    END IF;

    -- ---------------- $dateAdd / $dateSub (3-arg) ------------------
    -- $dateAdd: ["day"|"month"|..., <date_expr>, <int_expr>]
    -- Emits:    <date_expr> + (<int_expr> * INTERVAL '1 day')
    IF lower(v_op_key) IN ('$dateadd', '$datesub') THEN
        IF jsonb_typeof(v_args) <> 'array' OR jsonb_array_length(v_args) <> 3
           OR jsonb_typeof(v_args->0) <> 'string' THEN
            RAISE EXCEPTION 'pvt_build_scalar_expr: % expects ["<unit>", date_expr, int_expr]', v_op_key;
        END IF;
        DECLARE
            v_unit text := lower(v_args->>0);
            v_op   text := CASE WHEN lower(v_op_key) = '$dateadd' THEN ' + ' ELSE ' - ' END;
        BEGIN
            IF v_unit NOT IN ('year','month','week','day','hour','minute','second','millisecond') THEN
                RAISE EXCEPTION 'pvt_build_scalar_expr: % unsupported unit "%"', v_op_key, v_unit;
            END IF;
            v_a := pvt_build_scalar_expr(v_args->1, p_fields, p_base_prefix);
            v_b := pvt_build_scalar_expr(v_args->2, p_fields, p_base_prefix);
            RETURN '(' || v_a || v_op || '(' || v_b || ' * INTERVAL ''1 ' || v_unit || '''))';
        END;
    END IF;

    -- ---------------- $dateDiff (3-arg) ----------------------------
    -- $dateDiff: ["<unit>", a, b]  -> integer count of <unit> from b to a.
    -- Implemented via EXTRACT(EPOCH ...) for sub-day units and AGE for
    -- year/month so calendar arithmetic is preserved.
    IF lower(v_op_key) = '$datediff' THEN
        IF jsonb_typeof(v_args) <> 'array' OR jsonb_array_length(v_args) <> 3
           OR jsonb_typeof(v_args->0) <> 'string' THEN
            RAISE EXCEPTION 'pvt_build_scalar_expr: $dateDiff expects ["<unit>", a, b]';
        END IF;
        DECLARE
            v_unit text := lower(v_args->>0);
        BEGIN
            v_a := pvt_build_scalar_expr(v_args->1, p_fields, p_base_prefix);
            v_b := pvt_build_scalar_expr(v_args->2, p_fields, p_base_prefix);
            IF v_unit IN ('second','millisecond','minute','hour','day','week') THEN
                DECLARE
                    v_divisor text := CASE v_unit
                        WHEN 'millisecond' THEN '0.001'
                        WHEN 'second'      THEN '1'
                        WHEN 'minute'      THEN '60'
                        WHEN 'hour'        THEN '3600'
                        WHEN 'day'         THEN '86400'
                        WHEN 'week'        THEN '604800'
                    END;
                BEGIN
                    RETURN '(EXTRACT(EPOCH FROM (' || v_a || ' - ' || v_b || ')) / ' || v_divisor || ')::bigint';
                END;
            ELSIF v_unit = 'month' THEN
                RETURN '((EXTRACT(YEAR FROM AGE(' || v_a || ', ' || v_b || ')) * 12'
                    || ' + EXTRACT(MONTH FROM AGE(' || v_a || ', ' || v_b || '))))';
            ELSIF v_unit = 'year' THEN
                RETURN 'EXTRACT(YEAR FROM AGE(' || v_a || ', ' || v_b || '))';
            END IF;
            RAISE EXCEPTION 'pvt_build_scalar_expr: $dateDiff unsupported unit "%"', v_unit;
        END;
    END IF;

    -- ---------------- $if (3-arg: cond, then, else) ----------------
    -- cond is a boolean expression node compiled via pvt_build_bool_expr.
    IF lower(v_op_key) = '$if' THEN
        IF jsonb_typeof(v_args) <> 'array' OR jsonb_array_length(v_args) <> 3 THEN
            RAISE EXCEPTION 'pvt_build_scalar_expr: $if expects [cond, then, else]';
        END IF;
        DECLARE
            v_cond text := pvt_build_bool_expr(v_args->0, p_fields, p_base_prefix);
            v_then text := pvt_build_scalar_expr(v_args->1, p_fields, p_base_prefix);
            v_else text := pvt_build_scalar_expr(v_args->2, p_fields, p_base_prefix);
        BEGIN
            RETURN '(CASE WHEN ' || v_cond || ' THEN ' || v_then || ' ELSE ' || v_else || ' END)';
        END;
    END IF;

    -- ---------------- $case ----------------------------------------
    -- $case: [ {"when": <bool>, "then": <expr>}, ..., {"else": <expr>}? ]
    -- The trailing else entry is optional; when absent NULL is used.
    IF lower(v_op_key) = '$case' THEN
        IF jsonb_typeof(v_args) <> 'array' OR jsonb_array_length(v_args) = 0 THEN
            RAISE EXCEPTION 'pvt_build_scalar_expr: $case expects a non-empty array of {when,then} / {else} entries';
        END IF;
        DECLARE
            v_sb     text := '(CASE';
            v_entry  jsonb;
            v_else   text := 'NULL';
            v_seen_else boolean := false;
        BEGIN
            FOR v_entry IN SELECT value FROM jsonb_array_elements(v_args) LOOP
                IF v_entry ? 'else' THEN
                    IF v_seen_else THEN
                        RAISE EXCEPTION 'pvt_build_scalar_expr: $case has multiple else entries';
                    END IF;
                    v_else := pvt_build_scalar_expr(v_entry->'else', p_fields, p_base_prefix);
                    v_seen_else := true;
                ELSIF v_entry ? 'when' AND v_entry ? 'then' THEN
                    v_sb := v_sb
                        || ' WHEN ' || pvt_build_bool_expr(v_entry->'when', p_fields, p_base_prefix)
                        || ' THEN ' || pvt_build_scalar_expr(v_entry->'then', p_fields, p_base_prefix);
                ELSE
                    RAISE EXCEPTION 'pvt_build_scalar_expr: $case entry must be {when,then} or {else} (got %)', v_entry::text;
                END IF;
            END LOOP;
            RETURN v_sb || ' ELSE ' || v_else || ' END)';
        END;
    END IF;

    -- ---------------- Trigonometry / log10 (unary) -----------------
    IF lower(v_op_key) IN ('$sin', '$cos', '$tan', '$asin', '$acos', '$atan', '$log10') THEN
        IF jsonb_typeof(v_args) = 'array' THEN
            IF jsonb_array_length(v_args) <> 1 THEN
                RAISE EXCEPTION 'pvt_build_scalar_expr: % expects exactly 1 operand (got %)',
                    v_op_key, jsonb_array_length(v_args);
            END IF;
            v_a := pvt_build_scalar_expr(v_args->0, p_fields, p_base_prefix);
        ELSE
            v_a := pvt_build_scalar_expr(v_args, p_fields, p_base_prefix);
        END IF;
        IF lower(v_op_key) = '$sin'   THEN RETURN 'SIN('   || v_a || ')'; END IF;
        IF lower(v_op_key) = '$cos'   THEN RETURN 'COS('   || v_a || ')'; END IF;
        IF lower(v_op_key) = '$tan'   THEN RETURN 'TAN('   || v_a || ')'; END IF;
        IF lower(v_op_key) = '$asin'  THEN RETURN 'ASIN('  || v_a || ')'; END IF;
        IF lower(v_op_key) = '$acos'  THEN RETURN 'ACOS('  || v_a || ')'; END IF;
        IF lower(v_op_key) = '$atan'  THEN RETURN 'ATAN('  || v_a || ')'; END IF;
        IF lower(v_op_key) = '$log10' THEN RETURN 'LOG('   || v_a || ')'; END IF;
    END IF;

    -- ---------------- $now / $today (zero-arg) ---------------------
    -- Shape: {"$now": null}, {"$now": []}, {"$today": []}.
    IF lower(v_op_key) IN ('$now', '$today', '$utcnow') THEN
        IF lower(v_op_key) = '$now'    THEN RETURN 'NOW()'; END IF;
        IF lower(v_op_key) = '$utcnow' THEN RETURN '(NOW() AT TIME ZONE ''UTC'')'; END IF;
        IF lower(v_op_key) = '$today'  THEN RETURN 'CURRENT_DATE'; END IF;
    END IF;

    -- ---------------- $dateTrunc(["<unit>", expr]) -----------------
    -- Emits DATE_TRUNC('<unit>', expr).
    IF lower(v_op_key) = '$datetrunc' THEN
        IF jsonb_typeof(v_args) <> 'array' OR jsonb_array_length(v_args) <> 2
           OR jsonb_typeof(v_args->0) <> 'string' THEN
            RAISE EXCEPTION 'pvt_build_scalar_expr: $dateTrunc expects ["<unit>", date_expr]';
        END IF;
        DECLARE
            v_unit text := lower(v_args->>0);
        BEGIN
            IF v_unit NOT IN (
                'microseconds','milliseconds','second','minute','hour',
                'day','week','month','quarter','year','decade','century','millennium'
            ) THEN
                RAISE EXCEPTION 'pvt_build_scalar_expr: $dateTrunc unsupported unit "%"', v_unit;
            END IF;
            v_a := pvt_build_scalar_expr(v_args->1, p_fields, p_base_prefix);
            RETURN 'DATE_TRUNC(' || quote_literal(v_unit) || ', ' || v_a || ')';
        END;
    END IF;

    -- ---------------- $regexReplace([str, pat, repl, flags?]) ------
    IF lower(v_op_key) = '$regexreplace' THEN
        IF jsonb_typeof(v_args) <> 'array' OR jsonb_array_length(v_args) NOT IN (3, 4) THEN
            RAISE EXCEPTION 'pvt_build_scalar_expr: $regexReplace expects [str, pattern, replacement] or [str, pattern, replacement, flags]';
        END IF;
        v_a := pvt_build_scalar_expr(v_args->0, p_fields, p_base_prefix);
        DECLARE
            v_pat  text := pvt_build_scalar_expr(v_args->1, p_fields, p_base_prefix);
            v_repl text := pvt_build_scalar_expr(v_args->2, p_fields, p_base_prefix);
        BEGIN
            IF jsonb_array_length(v_args) = 3 THEN
                RETURN 'REGEXP_REPLACE(' || v_a || ', ' || v_pat || ', ' || v_repl || ')';
            END IF;
            RETURN 'REGEXP_REPLACE(' || v_a || ', ' || v_pat || ', ' || v_repl || ', '
                || pvt_build_scalar_expr(v_args->3, p_fields, p_base_prefix) || ')';
        END;
    END IF;

    RAISE EXCEPTION 'pvt_build_scalar_expr: unsupported operator "%"', v_op_key;
END;
$BODY$;

COMMENT ON FUNCTION pvt_build_scalar_expr(jsonb, jsonb, text) IS
    'Compiles a JSON scalar-expression AST into a SQL fragment. Supports field/const, full arithmetic (+ - * / %), math (abs/round/floor/ceil/min/max), string (upper/lower/trim/length/concat), coalesce and cast. Recursive: every binary node wraps in parens so precedence is preserved.';


-- ---------- pvt_build_expr_predicate ----------------------------------
-- Compiles an expression-form predicate (key = $op, value = arguments)
-- into a SQL boolean fragment.
CREATE OR REPLACE FUNCTION pvt_build_expr_predicate(
    p_op          text,
    p_args        jsonb,
    p_fields      jsonb,
    p_base_prefix text DEFAULT ''
)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_op    text := lower(p_op);
    v_n     integer;
    v_l     text;
    v_r     text;
    v_lo    text;
    v_hi    text;
    v_parts text[];
    v_elem  jsonb;
    v_pat   text;
BEGIN
    -- Unary: $null / $notNull / $isNull / $exists
    IF v_op IN ('$null', '$notnull', '$isnull', '$exists') THEN
        -- Value may be the expression itself, or a boolean toggle
        -- ({"$null": true} is only legal in shorthand form, not here).
        v_l := pvt_build_scalar_expr(p_args, p_fields, p_base_prefix);
        IF v_op IN ('$null', '$isnull') THEN RETURN v_l || ' IS NULL';     END IF;
        IF v_op = '$notnull'             THEN RETURN v_l || ' IS NOT NULL'; END IF;
        IF v_op = '$exists'              THEN RETURN v_l || ' IS NOT NULL'; END IF;
    END IF;

    -- $between [L, lo, hi]
    IF v_op = '$between' THEN
        IF jsonb_typeof(p_args) <> 'array' OR jsonb_array_length(p_args) <> 3 THEN
            RAISE EXCEPTION 'pvt_build_expr_predicate: $between expects [expr, low, high]';
        END IF;
        v_l  := pvt_build_scalar_expr(p_args->0, p_fields, p_base_prefix);
        v_lo := pvt_build_scalar_expr(p_args->1, p_fields, p_base_prefix);
        v_hi := pvt_build_scalar_expr(p_args->2, p_fields, p_base_prefix);
        RETURN '(' || v_l || ' BETWEEN ' || v_lo || ' AND ' || v_hi || ')';
    END IF;

    -- $in / $nin [L, [v1, v2, ...]]
    IF v_op IN ('$in', '$nin') THEN
        IF jsonb_typeof(p_args) <> 'array' OR jsonb_array_length(p_args) <> 2 THEN
            RAISE EXCEPTION 'pvt_build_expr_predicate: % expects [expr, [v1, v2, ...]]', p_op;
        END IF;
        v_l := pvt_build_scalar_expr(p_args->0, p_fields, p_base_prefix);
        IF jsonb_typeof(p_args->1) <> 'array' THEN
            RAISE EXCEPTION 'pvt_build_expr_predicate: % RHS must be a JSON array of literals', p_op;
        END IF;
        IF jsonb_array_length(p_args->1) = 0 THEN
            RETURN CASE WHEN v_op = '$in' THEN 'FALSE' ELSE 'TRUE' END;
        END IF;
        v_parts := ARRAY[]::text[];
        FOR v_elem IN SELECT value FROM jsonb_array_elements(p_args->1) LOOP
            v_parts := v_parts || pvt_format_const(v_elem);
        END LOOP;
        IF v_op = '$in'  THEN RETURN '(' || v_l || ' IN ('     || array_to_string(v_parts, ', ') || '))'; END IF;
        IF v_op = '$nin' THEN RETURN '(' || v_l || ' NOT IN (' || array_to_string(v_parts, ', ') || '))'; END IF;
    END IF;

    -- ---------------- $fts (full-text search) ----------------------
    -- Shape A: {"$fts": [<query-expr>, <fieldExpr1>, <fieldExpr2>, ...]}
    --          (lang defaults to 'simple')
    -- Shape B: {"$fts": {"query": <expr>, "fields": [<fieldExpr>, ...],
    --                    "language": "english"|"simple"|"russian"|...}}
    -- Emits: ((to_tsvector(L, F1) || to_tsvector(L, F2) || ...)
    --         @@ websearch_to_tsquery(L, Q))
    IF v_op = '$fts' THEN
        DECLARE
            v_lang   text := 'simple';
            v_query  text;
            v_fields jsonb;
            v_vec    text;
        BEGIN
            IF jsonb_typeof(p_args) = 'array' THEN
                IF jsonb_array_length(p_args) < 2 THEN
                    RAISE EXCEPTION 'pvt_build_expr_predicate: $fts array form expects [query, field, ...] (>=2 elements)';
                END IF;
                v_query  := pvt_build_scalar_expr(p_args->0, p_fields, p_base_prefix);
                v_fields := jsonb_path_query_array(p_args, '$[1 to last]');
            ELSIF jsonb_typeof(p_args) = 'object' THEN
                IF NOT (p_args ? 'query') OR NOT (p_args ? 'fields') THEN
                    RAISE EXCEPTION 'pvt_build_expr_predicate: $fts object form expects {"query":..., "fields":[...], "language"?}';
                END IF;
                v_query := pvt_build_scalar_expr(p_args->'query', p_fields, p_base_prefix);
                IF jsonb_typeof(p_args->'fields') <> 'array' OR jsonb_array_length(p_args->'fields') = 0 THEN
                    RAISE EXCEPTION 'pvt_build_expr_predicate: $fts "fields" must be a non-empty array';
                END IF;
                v_fields := p_args->'fields';
                IF p_args ? 'language' THEN
                    IF jsonb_typeof(p_args->'language') <> 'string' THEN
                        RAISE EXCEPTION 'pvt_build_expr_predicate: $fts "language" must be a string';
                    END IF;
                    v_lang := lower(p_args->>'language');
                END IF;
            ELSE
                RAISE EXCEPTION 'pvt_build_expr_predicate: $fts expects array or object form';
            END IF;

            v_parts := ARRAY[]::text[];
            FOR v_elem IN SELECT value FROM jsonb_array_elements(v_fields) LOOP
                v_parts := v_parts ||
                    ('to_tsvector(' || quote_literal(v_lang) || ', COALESCE('
                        || pvt_build_scalar_expr(v_elem, p_fields, p_base_prefix)
                        || '::text, ''''))');
            END LOOP;
            IF array_length(v_parts, 1) = 1 THEN
                v_vec := v_parts[1];
            ELSE
                v_vec := '(' || array_to_string(v_parts, ' || ') || ')';
            END IF;
            RETURN '(' || v_vec || ' @@ websearch_to_tsquery('
                || quote_literal(v_lang) || ', ' || v_query || '))';
        END;
    END IF;

    -- Binary infix / LIKE family -------------------------------------
    IF jsonb_typeof(p_args) <> 'array' OR jsonb_array_length(p_args) <> 2 THEN
        RAISE EXCEPTION 'pvt_build_expr_predicate: % expects [exprL, exprR]', p_op;
    END IF;
    v_l := pvt_build_scalar_expr(p_args->0, p_fields, p_base_prefix);
    v_r := pvt_build_scalar_expr(p_args->1, p_fields, p_base_prefix);

    IF v_op = '$eq'  THEN RETURN '(' || v_l || ' = '  || v_r || ')'; END IF;
    IF v_op = '$ne'  THEN RETURN '(' || v_l || ' <> ' || v_r || ')'; END IF;
    IF v_op = '$lt'  THEN RETURN '(' || v_l || ' < '  || v_r || ')'; END IF;
    IF v_op = '$lte' THEN RETURN '(' || v_l || ' <= ' || v_r || ')'; END IF;
    IF v_op = '$gt'  THEN RETURN '(' || v_l || ' > '  || v_r || ')'; END IF;
    IF v_op = '$gte' THEN RETURN '(' || v_l || ' >= ' || v_r || ')'; END IF;

    IF v_op IN ('$like', '$ilike') THEN
        RETURN '(' || v_l || CASE WHEN v_op = '$like' THEN ' LIKE ' ELSE ' ILIKE ' END || v_r || ')';
    END IF;

    -- Sugar over LIKE/ILIKE: RHS must be a string literal so we can
    -- splice the wildcards. Accept $const-wrapped or bare-string form.
    IF v_op IN ('$contains', '$startswith', '$endswith',
                '$containsignorecase', '$startswithignorecase', '$endswithignorecase') THEN
        IF jsonb_typeof(p_args->1) = 'object' AND (p_args->1) ? '$const' THEN
            v_pat := (p_args->1)->>'$const';
        ELSIF jsonb_typeof(p_args->1) = 'string' THEN
            v_pat := p_args->1 #>> '{}';
        ELSE
            RAISE EXCEPTION
                'pvt_build_expr_predicate: % RHS must be a string literal (got %)',
                p_op, jsonb_typeof(p_args->1);
        END IF;
        DECLARE
            v_lit text;
            v_kw  text;
        BEGIN
            v_kw  := CASE WHEN right(v_op, length('ignorecase')) = 'ignorecase' THEN ' ILIKE ' ELSE ' LIKE ' END;
            v_lit := CASE
                WHEN v_op IN ('$contains', '$containsignorecase')   THEN quote_literal('%' || v_pat || '%')
                WHEN v_op IN ('$startswith', '$startswithignorecase') THEN quote_literal(v_pat || '%')
                WHEN v_op IN ('$endswith', '$endswithignorecase')     THEN quote_literal('%' || v_pat)
            END;
            RETURN '(' || v_l || v_kw || v_lit || ')';
        END;
    END IF;

    -- ---------------- $regex / $iregex / $notregex / $inotregex ----
    -- Shape: {"$regex": [strExpr, patternStringOrConst]} (PG POSIX regex).
    IF v_op IN ('$regex', '$iregex', '$notregex', '$inotregex') THEN
        IF jsonb_typeof(p_args) <> 'array' OR jsonb_array_length(p_args) <> 2 THEN
            RAISE EXCEPTION 'pvt_build_expr_predicate: % expects [expr, pattern]', p_op;
        END IF;
        v_l := pvt_build_scalar_expr(p_args->0, p_fields, p_base_prefix);
        v_r := pvt_build_scalar_expr(p_args->1, p_fields, p_base_prefix);
        DECLARE
            v_op_sql text := CASE v_op
                WHEN '$regex'     THEN ' ~ '
                WHEN '$iregex'    THEN ' ~* '
                WHEN '$notregex'  THEN ' !~ '
                WHEN '$inotregex' THEN ' !~* '
            END;
        BEGIN
            RETURN '(' || v_l || v_op_sql || v_r || ')';
        END;
    END IF;

    RAISE EXCEPTION 'pvt_build_expr_predicate: unsupported predicate "%"', p_op;
END;
$BODY$;

COMMENT ON FUNCTION pvt_build_expr_predicate(text, jsonb, jsonb, text) IS
    'Compiles an expression-form predicate (key = $eq/$ne/$lt/$gt/$like/$in/$between/...) into a SQL boolean fragment. LHS and RHS are full pvt_build_scalar_expr expressions; LIKE-family operators expect a string literal on RHS so wildcards can be spliced.';


-- ---------- pvt_build_bool_expr ---------------------------------------
-- Boolean-expression compiler used by $if / $case scalar nodes (and by
-- HAVING/window-FILTER builders). The node is either:
--   * { "$and": [<bool>, ...] } / { "$or": [<bool>, ...] }
--   * { "$not": <bool> }
--   * { "<predicate-op>": <args> }   (delegates to pvt_build_expr_predicate)
-- Bare booleans `true`/`false` map to SQL TRUE/FALSE literals.
CREATE OR REPLACE FUNCTION pvt_build_bool_expr(
    p_node        jsonb,
    p_fields      jsonb,
    p_base_prefix text DEFAULT ''
)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_op    text;
    v_args  jsonb;
    v_elem  jsonb;
    v_parts text[];
BEGIN
    IF p_node IS NULL OR jsonb_typeof(p_node) = 'null' THEN
        RETURN 'TRUE';
    END IF;
    IF jsonb_typeof(p_node) = 'boolean' THEN
        RETURN CASE WHEN (p_node)::text = 'true' THEN 'TRUE' ELSE 'FALSE' END;
    END IF;
    IF jsonb_typeof(p_node) <> 'object' THEN
        RAISE EXCEPTION 'pvt_build_bool_expr: bool node must be object/boolean (got %)', jsonb_typeof(p_node);
    END IF;

    SELECT key INTO v_op
      FROM jsonb_object_keys(p_node) AS t(key)
     WHERE left(key, 1) = '$'
     LIMIT 1;
    IF v_op IS NULL THEN
        RAISE EXCEPTION 'pvt_build_bool_expr: bool node has no operator key: %', p_node::text;
    END IF;
    v_args := p_node -> v_op;

    IF lower(v_op) = '$and' THEN
        IF jsonb_typeof(v_args) <> 'array' OR jsonb_array_length(v_args) = 0 THEN
            RAISE EXCEPTION '$and expects a non-empty array';
        END IF;
        v_parts := ARRAY[]::text[];
        FOR v_elem IN SELECT value FROM jsonb_array_elements(v_args) LOOP
            v_parts := v_parts || pvt_build_bool_expr(v_elem, p_fields, p_base_prefix);
        END LOOP;
        RETURN '(' || array_to_string(v_parts, ' AND ') || ')';
    END IF;
    IF lower(v_op) = '$or' THEN
        IF jsonb_typeof(v_args) <> 'array' OR jsonb_array_length(v_args) = 0 THEN
            RAISE EXCEPTION '$or expects a non-empty array';
        END IF;
        v_parts := ARRAY[]::text[];
        FOR v_elem IN SELECT value FROM jsonb_array_elements(v_args) LOOP
            v_parts := v_parts || pvt_build_bool_expr(v_elem, p_fields, p_base_prefix);
        END LOOP;
        RETURN '(' || array_to_string(v_parts, ' OR ') || ')';
    END IF;
    IF lower(v_op) = '$not' THEN
        RETURN 'NOT (' || pvt_build_bool_expr(v_args, p_fields, p_base_prefix) || ')';
    END IF;

    RETURN pvt_build_expr_predicate(v_op, v_args, p_fields, p_base_prefix);
END;
$BODY$;

COMMENT ON FUNCTION pvt_build_bool_expr(jsonb, jsonb, text) IS
    'Compiles a boolean expression node (with $and/$or/$not support) into a SQL boolean fragment. Leaf operators delegate to pvt_build_expr_predicate. Used by $if/$case scalar forms and by HAVING/window-FILTER builders.';
