-- =====================================================================
-- 13_pvt_condition.sql
-- ---------------------------------------------------------------------
-- Build a SQL predicate fragment for a single leaf node in the filter
-- JSON, referencing a CTE column (NOT the raw _values row). All
-- supported operators in v0.1.0 are emitted here; unsupported ones
-- raise with a clear message.
--
-- Function:
--   pvt_build_field_condition(p_field_name, p_field_meta, p_op_json) RETURNS text
--
-- Input shapes:
--   p_op_json = scalar  -> shorthand for {"$eq": <scalar>}
--   p_op_json = {"$op": <value>, ...}
--
-- Supported operators in v0.1.0:
--   $eq, $ne, $gt, $gte, $lt, $lte,
--   $in, $nin,
--   $like, $ilike,
--   $startsWith, $endsWith, $contains            (case-sensitive, LIKE),
--   $startsWithIgnoreCase, $endsWithIgnoreCase,
--   $containsIgnoreCase                          (case-insensitive, ILIKE),
--   $null, $exists, $isNull, $notNull
-- =====================================================================

-- Signature evolved in v0.4.0 (added p_base_prefix). CREATE OR REPLACE
-- cannot change a function's argument list, so drop the legacy form
-- first if present.
DROP FUNCTION IF EXISTS pvt_build_field_condition(text, jsonb, jsonb);

CREATE OR REPLACE FUNCTION pvt_build_field_condition(
    p_field_name   text,
    p_field_meta   jsonb,
    p_op_json      jsonb,
    p_base_prefix  text DEFAULT ''   -- '' → reference CTE column; 'o.' → reference _objects directly (pushdown)
)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_col      text;
    v_kind     text;
    v_db_type  text;
    v_cast     text;
    v_elem_cast text;
    v_li_prop  text;
    v_is_array boolean;
    v_length_mod boolean;
    v_arr_col  text;
    v_op_key   text;
    v_op_val   jsonb;
    v_parts    text[] := ARRAY[]::text[];
    v_op_count integer := 0;
    v_op_norm  text;
    v_lit      text;
    v_arr_lit  text;
BEGIN
    IF p_field_meta IS NULL THEN
        RAISE EXCEPTION 'pvt_build_field_condition: p_field_meta is NULL for field "%"', p_field_name;
    END IF;

    v_kind     := p_field_meta->>'kind';
    v_li_prop  := p_field_meta->>'list_item_prop';
    v_is_array := COALESCE((p_field_meta->>'is_array')::boolean, false);
    v_db_type  := p_field_meta->>'db_type';
    v_length_mod := COALESCE((p_field_meta->>'length_modifier')::boolean, false);
    v_arr_col  := quote_ident(COALESCE(p_field_meta->>'base_name', p_field_name));

    -- ---- Element-type cast (used by $arrayXxx scalar operands) ------
    v_elem_cast := CASE pvt_db_type_to_value_column(v_db_type)
        WHEN '_String'         THEN '::text'
        WHEN '_Long'           THEN '::bigint'
        WHEN '_Double'         THEN '::double precision'
        WHEN '_Numeric'        THEN '::numeric'
        WHEN '_Boolean'        THEN '::boolean'
        WHEN '_Guid'           THEN '::uuid'
        WHEN '_DateTimeOffset' THEN '::timestamptz'
        WHEN '_ByteArray'      THEN '::bytea'
        WHEN '_ListItem'       THEN '::bigint'
        WHEN '_Object'         THEN '::bigint'
        ELSE ''
    END;

    -- ---- LHS column reference ---------------------------------------
    -- length/count modifier rewrites the LHS to a scalar-int expression:
    --   * is_array=true  -> array_length(<arr>, 1)
    --   * is_array=false -> LENGTH(<text>) (string length, e.g. p.Name.Length)
    -- Base fields project from _objects under their system column name
    -- (e.g. `_id`, `_name`) and never under the user-facing path (e.g.
    -- `0$:Id`); ordinary cases reference the CTE alias which matches the
    -- original field path.
    DECLARE
        v_length_target text;
    BEGIN
        v_length_target := CASE
            WHEN v_kind = 'base'
                THEN p_base_prefix || quote_ident(p_field_meta->>'column')
            ELSE v_arr_col
        END;
        v_col := CASE
            WHEN v_length_mod AND v_is_array
                THEN 'COALESCE(array_length(' || v_length_target || ', 1), 0)'
            WHEN v_length_mod
                THEN 'COALESCE(LENGTH(' || v_length_target || '), 0)'
            WHEN v_kind = 'base'
                THEN p_base_prefix || quote_ident(p_field_meta->>'column')
            ELSE quote_ident(p_field_name)
        END;
        -- Scalar ListItem.Value / .Alias: pivot column holds the raw
        -- _ListItem id (parity with Pro). WHERE compares by id; the caller
        -- (LINQ translator) is responsible for resolving a string like
        -- 'Active' to its _list_items._id before passing it down. Only
        -- ORDER BY dereferences to _value / _alias for textual sort.
        -- Array path is unchanged: pivot emits text[] of values.
    END;

    -- ---- Pick a SQL cast suitable for the resolved CTE column type --
    -- For base fields the cast must reflect the underlying _objects
    -- column type, otherwise comparisons such as `o._id IN (text...)`
    -- raise `bigint = text`.
    v_cast := CASE
        WHEN v_length_mod                  THEN ''
        WHEN v_kind = 'base'               THEN
            CASE p_field_meta->>'column'
                WHEN '_id'              THEN '::bigint'
                WHEN '_id_parent'       THEN '::bigint'
                WHEN '_id_scheme'       THEN '::bigint'
                WHEN '_id_owner'        THEN '::bigint'
                WHEN '_id_who_change'   THEN '::bigint'
                WHEN '_value_long'      THEN '::bigint'
                WHEN '_key'             THEN '::bigint'
                WHEN '_value_double'    THEN '::double precision'
                WHEN '_value_numeric'   THEN '::numeric'
                WHEN '_value_bool'      THEN '::boolean'
                WHEN '_value_guid'      THEN '::uuid'
                WHEN '_value_datetime'  THEN '::timestamptz'
                WHEN '_date_create'     THEN '::timestamptz'
                WHEN '_date_modify'     THEN '::timestamptz'
                WHEN '_date_begin'      THEN '::timestamptz'
                WHEN '_date_complete'   THEN '::timestamptz'
                ELSE '' -- _name / _note / _value_string / _hash / _value_bytes -> text/bytea, no cast needed
            END
        -- Scalar ListItem.Value/.Alias pivot column holds the resolved
        -- _list_items._value / _alias string (pvt_build_cte_sql adds a
        -- LEFT JOIN _list_items li when needed; pvt_build_column_expr
        -- aggregates li._value / li._alias). Free LINQ passes string
        -- literals; comparison is text. Array path also dereferences
        -- to text[]. ListItem.Id keeps bigint.
        WHEN v_li_prop IN ('Value', 'Alias')                    THEN '::text'
        WHEN v_li_prop = 'Id'              THEN '::bigint'
        ELSE v_elem_cast
    END;

    -- ---- Shorthand: scalar literal == {"$eq": <literal>} ----
    IF jsonb_typeof(p_op_json) <> 'object' THEN
        IF jsonb_typeof(p_op_json) = 'null' THEN
            RETURN format('%s IS NULL', v_col);
        END IF;
        RETURN format('%s = %L%s', v_col, pvt_jsonb_to_sql_literal(p_op_json), v_cast);
    END IF;

    -- ---- Iterate operator object (AND of every op present) ----
    FOR v_op_key, v_op_val IN SELECT key, value FROM jsonb_each(p_op_json) LOOP
        v_op_count := v_op_count + 1;
        v_op_norm  := lower(v_op_key);

        IF v_op_norm IN ('$eq', '$ne', '$gt', '$gte', '$lt', '$lte') THEN
            IF jsonb_typeof(v_op_val) = 'null' THEN
                v_parts := v_parts || (CASE v_op_norm
                    WHEN '$eq' THEN format('%s IS NULL', v_col)
                    WHEN '$ne' THEN format('%s IS NOT NULL', v_col)
                    ELSE format('%s %s NULL', v_col,
                        CASE v_op_norm WHEN '$gt' THEN '>' WHEN '$gte' THEN '>='
                                       WHEN '$lt' THEN '<' WHEN '$lte' THEN '<=' END)
                END);
            ELSIF v_is_array AND NOT v_length_mod AND v_op_norm IN ('$eq', '$ne') THEN
                -- Array LHS: scalar = ANY / NOT (= ANY) semantics, e.g.
                -- Roles[].Any(r => r.Value == "X") -> 'X' = ANY(<text[]>)
                v_parts := v_parts || (CASE v_op_norm
                    WHEN '$eq'
                        THEN format('(%L%s = ANY(%s))',
                                pvt_jsonb_to_sql_literal(v_op_val), v_cast, v_col)
                    WHEN '$ne'
                        THEN format('(%s IS NULL OR NOT (%L%s = ANY(%s)))',
                                v_col, pvt_jsonb_to_sql_literal(v_op_val), v_cast, v_col)
                END);
            ELSE
                v_parts := v_parts || format('%s %s %L%s',
                    v_col,
                    CASE v_op_norm WHEN '$eq' THEN '=' WHEN '$ne' THEN '<>'
                                   WHEN '$gt' THEN '>' WHEN '$gte' THEN '>='
                                   WHEN '$lt' THEN '<' WHEN '$lte' THEN '<=' END,
                    pvt_jsonb_to_sql_literal(v_op_val),
                    v_cast);
            END IF;

        ELSIF v_op_norm IN ('$in', '$nin') THEN
            IF jsonb_typeof(v_op_val) <> 'array' THEN
                RAISE EXCEPTION 'pvt_build_field_condition: % expects array (field "%")', v_op_key, p_field_name;
            END IF;
            IF v_is_array AND NOT v_length_mod THEN
                -- Array LHS: use overlap (&&) instead of IN, since
                -- `text[] IN (text,...)` is not a valid SQL expression.
                IF v_op_norm = '$in' THEN
                    v_parts := v_parts || format(
                        '%s && (SELECT array_agg(x%s) FROM jsonb_array_elements_text(%L::jsonb) AS x)',
                        v_col, v_cast, v_op_val::text);
                ELSE
                    v_parts := v_parts || format(
                        '(%s IS NULL OR NOT (%s && (SELECT array_agg(x%s) FROM jsonb_array_elements_text(%L::jsonb) AS x)))',
                        v_col, v_col, v_cast, v_op_val::text);
                END IF;
            ELSE
                v_parts := v_parts || format('%s %s (SELECT (jsonb_array_elements_text(%L::jsonb))%s)',
                    v_col,
                    CASE v_op_norm WHEN '$in' THEN 'IN' ELSE 'NOT IN' END,
                    v_op_val::text,
                    v_cast);
            END IF;

        ELSIF v_op_norm IN ('$like', '$ilike') THEN
            v_parts := v_parts || format('%s %s %L',
                v_col,
                CASE v_op_norm WHEN '$like' THEN 'LIKE' ELSE 'ILIKE' END,
                v_op_val #>> '{}');

        ELSIF v_op_norm = '$startswith' THEN
            v_parts := v_parts || format('%s LIKE %L', v_col, (v_op_val #>> '{}') || '%');
        ELSIF v_op_norm = '$endswith' THEN
            v_parts := v_parts || format('%s LIKE %L', v_col, '%' || (v_op_val #>> '{}'));
        ELSIF v_op_norm = '$contains' THEN
            v_parts := v_parts || format('%s LIKE %L', v_col, '%' || (v_op_val #>> '{}') || '%');

        ELSIF v_op_norm = '$startswithignorecase' THEN
            v_parts := v_parts || format('%s ILIKE %L', v_col, (v_op_val #>> '{}') || '%');
        ELSIF v_op_norm = '$endswithignorecase' THEN
            v_parts := v_parts || format('%s ILIKE %L', v_col, '%' || (v_op_val #>> '{}'));
        ELSIF v_op_norm = '$containsignorecase' THEN
            v_parts := v_parts || format('%s ILIKE %L', v_col, '%' || (v_op_val #>> '{}') || '%');

        ELSIF v_op_norm IN ('$null', '$isnull') THEN
            v_parts := v_parts || (CASE
                WHEN (v_op_val::text)::boolean
                    THEN format('%s IS NULL', v_col)
                    ELSE format('%s IS NOT NULL', v_col)
                END);
        ELSIF v_op_norm IN ('$notnull', '$exists') THEN
            v_parts := v_parts || (CASE
                WHEN (v_op_val::text)::boolean
                    THEN format('%s IS NOT NULL', v_col)
                    ELSE format('%s IS NULL', v_col)
                END);

        -- ============ POSIX regex shorthand =========================
        --   {"Field": {"$regex": "pattern"}}      -> Field ~  'pattern'
        --   {"Field": {"$iregex": "pattern"}}     -> Field ~* 'pattern'
        --   {"Field": {"$notregex": "pattern"}}   -> Field !~  'pattern'
        --   {"Field": {"$inotregex": "pattern"}}  -> Field !~* 'pattern'
        ELSIF v_op_norm IN ('$regex', '$iregex', '$notregex', '$inotregex') THEN
            v_parts := v_parts || format('%s %s %L',
                v_col,
                CASE v_op_norm
                    WHEN '$regex'     THEN '~'
                    WHEN '$iregex'    THEN '~*'
                    WHEN '$notregex'  THEN '!~'
                    WHEN '$inotregex' THEN '!~*'
                END,
                v_op_val #>> '{}');

        -- ============ Full-text-search shorthand ====================
        --   {"Field": {"$fts": "query"}}                -- lang='simple'
        --   {"Field": {"$fts": {"query":"...",
        --                       "language":"english"}}} -- explicit lang
        ELSIF v_op_norm = '$fts' THEN
            DECLARE
                v_fts_lang  text := 'simple';
                v_fts_query text;
            BEGIN
                IF jsonb_typeof(v_op_val) = 'string' THEN
                    v_fts_query := v_op_val #>> '{}';
                ELSIF jsonb_typeof(v_op_val) = 'object' AND v_op_val ? 'query' THEN
                    v_fts_query := v_op_val ->> 'query';
                    IF v_op_val ? 'language' THEN
                        v_fts_lang := lower(v_op_val ->> 'language');
                    END IF;
                ELSE
                    RAISE EXCEPTION 'pvt_build_field_condition: $fts expects string or {"query":..., "language"?}';
                END IF;
                v_parts := v_parts || format(
                    '(to_tsvector(%L, COALESCE(%s::text, '''')) @@ websearch_to_tsquery(%L, %L))',
                    v_fts_lang, v_col, v_fts_lang, v_fts_query);
            END;

        -- ============ Array operators (Pro parity, v0.3.0) ===========
        -- All $arrayXxx require an array-shaped CTE column (typed array
        -- or ListItem-array projection: bigint[] for .Id / text[] for
        -- .Value / .Alias). The length/count modifier is incompatible
        -- because it collapses the array to a scalar.
        ELSIF v_op_norm LIKE '$array%' THEN
            IF v_length_mod THEN
                RAISE EXCEPTION
                    'pvt_build_field_condition: % cannot be combined with .$length / .$count (field "%")',
                    v_op_key, p_field_name;
            END IF;
            IF NOT v_is_array THEN
                RAISE EXCEPTION
                    'pvt_build_field_condition: % requires an array field (field "%")',
                    v_op_key, p_field_name;
            END IF;

            IF v_op_norm = '$arraycontains' THEN
                v_parts := v_parts || format('%L%s = ANY(%s)',
                    pvt_jsonb_to_sql_literal(v_op_val), v_elem_cast, v_col);
            ELSIF v_op_norm = '$arrayany' THEN
                -- Boolean-shorthand: true => not empty, false => empty.
                v_parts := v_parts || (CASE
                    WHEN (v_op_val::text)::boolean
                        THEN format('COALESCE(array_length(%s, 1), 0) > 0', v_col)
                        ELSE format('COALESCE(array_length(%s, 1), 0) = 0', v_col)
                    END);
            ELSIF v_op_norm = '$arrayempty' THEN
                v_parts := v_parts || (CASE
                    WHEN (v_op_val::text)::boolean
                        THEN format('COALESCE(array_length(%s, 1), 0) = 0', v_col)
                        ELSE format('COALESCE(array_length(%s, 1), 0) > 0', v_col)
                    END);
            ELSIF v_op_norm IN ('$arraycount', '$arraycountgt', '$arraycountgte',
                                '$arraycountlt', '$arraycountlte') THEN
                v_parts := v_parts || format('COALESCE(array_length(%s, 1), 0) %s %L::bigint',
                    v_col,
                    CASE v_op_norm
                        WHEN '$arraycount'    THEN '='
                        WHEN '$arraycountgt'  THEN '>'
                        WHEN '$arraycountgte' THEN '>='
                        WHEN '$arraycountlt'  THEN '<'
                        WHEN '$arraycountlte' THEN '<='
                    END,
                    pvt_jsonb_to_sql_literal(v_op_val));
            ELSIF v_op_norm = '$arrayat' THEN
                -- Operand: {"index": N, "value": V} (PG arrays are 1-based,
                -- but our convention is 0-based at the API; add 1).
                IF jsonb_typeof(v_op_val) <> 'object'
                   OR NOT (v_op_val ? 'index') OR NOT (v_op_val ? 'value') THEN
                    RAISE EXCEPTION 'pvt_build_field_condition: $arrayAt expects {"index":N,"value":V} (field "%")', p_field_name;
                END IF;
                v_parts := v_parts || format('%s[(%L::int) + 1] = %L%s',
                    v_col,
                    (v_op_val->>'index'),
                    pvt_jsonb_to_sql_literal(v_op_val->'value'),
                    v_elem_cast);
            ELSIF v_op_norm = '$arrayfirst' THEN
                v_parts := v_parts || format('%s[1] = %L%s',
                    v_col, pvt_jsonb_to_sql_literal(v_op_val), v_elem_cast);
            ELSIF v_op_norm = '$arraylast' THEN
                v_parts := v_parts || format('%s[array_length(%s, 1)] = %L%s',
                    v_col, v_col, pvt_jsonb_to_sql_literal(v_op_val), v_elem_cast);
            ELSIF v_op_norm = '$arraystartswith' THEN
                v_parts := v_parts || format('%s[1] LIKE %L', v_col, (v_op_val #>> '{}') || '%');
            ELSIF v_op_norm = '$arrayendswith' THEN
                v_parts := v_parts || format('%s[array_length(%s, 1)] LIKE %L',
                    v_col, v_col, '%' || (v_op_val #>> '{}'));
            ELSIF v_op_norm = '$arraymatches' THEN
                v_parts := v_parts || format(
                    'EXISTS (SELECT 1 FROM unnest(%s) AS _x WHERE _x LIKE %L)',
                    v_col, v_op_val #>> '{}');
            ELSIF v_op_norm IN ('$arraysum', '$arrayavg', '$arraymin', '$arraymax') THEN
                v_parts := v_parts || format(
                    '(SELECT %s(_x) FROM unnest(%s) AS _x) = %L%s',
                    CASE v_op_norm
                        WHEN '$arraysum' THEN 'SUM'
                        WHEN '$arrayavg' THEN 'AVG'
                        WHEN '$arraymin' THEN 'MIN'
                        WHEN '$arraymax' THEN 'MAX'
                    END,
                    v_col,
                    pvt_jsonb_to_sql_literal(v_op_val),
                    v_elem_cast);
            ELSE
                RAISE EXCEPTION
                    'pvt_build_field_condition: array operator "%" is not supported in v0.3.0 (field "%")',
                    v_op_key, p_field_name;
            END IF;

        ELSE
            RAISE EXCEPTION
                'pvt_build_field_condition: operator "%" is not supported in v0.3.0 (field "%")',
                v_op_key, p_field_name;
        END IF;
    END LOOP;

    IF v_op_count = 0 THEN
        RAISE EXCEPTION 'pvt_build_field_condition: empty operator object for field "%"', p_field_name;
    END IF;
    IF v_op_count = 1 THEN
        RETURN v_parts[1];
    END IF;
    RETURN '(' || array_to_string(v_parts, ' AND ') || ')';
END;
$BODY$;

COMMENT ON FUNCTION pvt_build_field_condition(text, jsonb, jsonb, text) IS
    'Builds an AND-joined predicate fragment for one leaf field. v0.3.0 supports $eq/$ne/$gt/$gte/$lt/$lte/$in/$nin/$like/$ilike/$startsWith/$endsWith/$contains/$startsWithIgnoreCase/$endsWithIgnoreCase/$containsIgnoreCase/$null/$exists/$isNull/$notNull plus array ops $arrayContains/$arrayAny/$arrayEmpty/$arrayCount/$arrayCountGt|Gte|Lt|Lte/$arrayAt/$arrayFirst/$arrayLast/$arrayStartsWith|EndsWith|Matches/$arraySum|Avg|Min|Max and .$length/.$count modifiers. References CTE alias columns, not raw _values.';


-- ---------- pvt_jsonb_to_sql_literal -----------------------------------
-- Convert a JSON scalar into a string suitable for the %L format spec
-- (which adds the surrounding single quotes). Objects/arrays are not
-- valid here and cause an error.
CREATE OR REPLACE FUNCTION pvt_jsonb_to_sql_literal(p_val jsonb)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
AS $BODY$
BEGIN
    IF p_val IS NULL OR jsonb_typeof(p_val) = 'null' THEN
        RETURN NULL;
    END IF;
    IF jsonb_typeof(p_val) IN ('object', 'array') THEN
        RAISE EXCEPTION 'pvt_jsonb_to_sql_literal: cannot coerce % to scalar literal', jsonb_typeof(p_val);
    END IF;
    -- jsonb_typeof in ('string','number','boolean'): #>> '{}' strips
    -- the JSON quoting for strings and gives a plain text rendering
    -- for numbers/booleans that PostgreSQL accepts in casts.
    RETURN p_val #>> '{}';
END;
$BODY$;

COMMENT ON FUNCTION pvt_jsonb_to_sql_literal(jsonb) IS
    'Unwraps a JSON scalar to the text form expected by format() %L. Throws on objects/arrays.';
