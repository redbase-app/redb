-- =====================================================================
-- 10_pvt_field_collection.sql
-- ---------------------------------------------------------------------
-- Field-name collection from filter/order JSON and metadata resolution
-- via _scheme_metadata_cache. Produces the `fields` jsonb consumed by
-- pvt_build_column_expr / pvt_build_cte_sql / pvt_build_*_condition.
--
-- Functions:
--   pvt_extract_field_names(p_filter jsonb) RETURNS SETOF text
--   pvt_collect_fields(p_scheme_id, p_filter, p_order, p_include_all) RETURNS jsonb
--   pvt_has_null_check(p_filter jsonb) RETURNS boolean
--
-- The result of pvt_collect_fields is a jsonb object:
--   {
--     "<field_name>": {
--       "semantic": "base"|"scalar"|"listitem"|"collection"|"nested",
--       "column":   "_id_parent"           -- only when semantic = "base"
--       "sid":      42,                    -- _structure_id
--       "db_type":  "Long",                -- db_type from cache
--       "type_semantic": "TimeSpan"|null,  -- type_semantic from cache
--       "collection_type": null|<int>,
--       "list_id":  null|<int>,
--       "parent_sid": null|<int>
--     },
--     ...
--   }
-- =====================================================================

-- ---------- pvt_extract_field_names ------------------------------------
-- Walk a filter JSON recursively and return every field-name reference
-- (a key whose name does NOT start with '$'). Logical operators ($and,
-- $or, $not, ...) are recursed into but never emitted themselves.
CREATE OR REPLACE FUNCTION pvt_extract_field_names(p_filter jsonb)
RETURNS SETOF text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    k text;
    v jsonb;
    elem jsonb;
BEGIN
    IF p_filter IS NULL OR jsonb_typeof(p_filter) <> 'object' THEN
        RETURN;
    END IF;

    FOR k, v IN SELECT key, value FROM jsonb_each(p_filter) LOOP
        IF left(k, 1) = '$' THEN
            -- $case has shape: [ {when:<bool>, then:<expr>}, ..., {else:<expr>}? ].
            -- Iterate VALUES of each branch object, not its keys, so that
            -- "when" / "then" / "else" never get harvested as field paths.
            IF lower(k) = '$case' AND jsonb_typeof(v) = 'array' THEN
                FOR elem IN SELECT value FROM jsonb_array_elements(v) LOOP
                    IF jsonb_typeof(elem) = 'object' THEN
                        IF elem ? 'when' THEN
                            RETURN QUERY SELECT pvt_extract_field_names(elem->'when');
                        END IF;
                        IF elem ? 'then' THEN
                            RETURN QUERY SELECT pvt_extract_field_names(elem->'then');
                        END IF;
                        IF elem ? 'else' THEN
                            RETURN QUERY SELECT pvt_extract_field_names(elem->'else');
                        END IF;
                    END IF;
                END LOOP;
                CONTINUE;
            END IF;
            -- $fts object form: { "query": <expr>, "fields": [<expr>...], "language": "..." }.
            -- Only `query` and `fields` carry field references; `language` is a literal.
            IF lower(k) = '$fts' AND jsonb_typeof(v) = 'object' THEN
                IF v ? 'query'  THEN RETURN QUERY SELECT pvt_extract_field_names(v->'query'); END IF;
                IF v ? 'fields' AND jsonb_typeof(v->'fields') = 'array' THEN
                    FOR elem IN SELECT value FROM jsonb_array_elements(v->'fields') LOOP
                        RETURN QUERY SELECT pvt_extract_field_names(elem);
                    END LOOP;
                END IF;
                CONTINUE;
            END IF;
            -- $cast(["<sql-type>", expr]) -- type literal must not be walked.
            IF lower(k) = '$cast' AND jsonb_typeof(v) = 'array' AND jsonb_array_length(v) >= 2 THEN
                RETURN QUERY SELECT pvt_extract_field_names(v->1);
                CONTINUE;
            END IF;
            -- $dateAdd/$dateSub/$dateDiff/$dateTrunc: first element is a unit string literal.
            IF lower(k) IN ('$dateadd','$datesub','$datediff','$datetrunc')
               AND jsonb_typeof(v) = 'array' AND jsonb_array_length(v) >= 1 THEN
                FOR elem IN SELECT value FROM jsonb_array_elements(v) WITH ORDINALITY AS t(value, ord)
                              WHERE ord >= 2 LOOP
                    RETURN QUERY SELECT pvt_extract_field_names(elem);
                END LOOP;
                CONTINUE;
            END IF;
            -- Logical operator: recurse into children.
            IF jsonb_typeof(v) = 'array' THEN
                FOR elem IN SELECT value FROM jsonb_array_elements(v) LOOP
                    RETURN QUERY SELECT pvt_extract_field_names(elem);
                END LOOP;
            ELSIF jsonb_typeof(v) = 'object' THEN
                RETURN QUERY SELECT pvt_extract_field_names(v);
            END IF;
        ELSE
            -- Field reference.
            RETURN NEXT k;
        END IF;
    END LOOP;
END;
$BODY$;

COMMENT ON FUNCTION pvt_extract_field_names(jsonb) IS
    'Recursively extracts every field-name reference from a filter JSON. Logical operators ($and/$or/$not/...) are descended into but never returned.';


-- ---------- pvt_extract_field_pairs -----------------------------------
-- Walk a filter JSON recursively and emit (path, op_value) pairs.
-- `op_value` is the underlying string operand when path ends with
-- `.ContainsKey` (Pro-style dictionary normalization); NULL otherwise.
CREATE OR REPLACE FUNCTION pvt_extract_field_pairs(p_filter jsonb)
RETURNS TABLE (path text, op_value text)
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    k text;
    v jsonb;
    elem jsonb;
BEGIN
    IF p_filter IS NULL OR jsonb_typeof(p_filter) <> 'object' THEN
        RETURN;
    END IF;

    -- Expression node shortcuts: only `$field` yields a path; `$const`
    -- carries a literal and contributes no field references.
    IF p_filter ? '$field' THEN
        RETURN QUERY SELECT (p_filter->>'$field')::text, NULL::text;
        RETURN;
    END IF;
    IF p_filter ? '$const' THEN
        RETURN;
    END IF;

    FOR k, v IN SELECT key, value FROM jsonb_each(p_filter) LOOP
        IF left(k, 1) = '$' THEN
            -- $case has shape: [ {when:<bool>, then:<expr>}, ..., {else:<expr>}? ].
            -- Descend into branch values only, never treat "when"/"then"/"else" as paths.
            IF lower(k) = '$case' AND jsonb_typeof(v) = 'array' THEN
                FOR elem IN SELECT value FROM jsonb_array_elements(v) LOOP
                    IF jsonb_typeof(elem) = 'object' THEN
                        IF elem ? 'when' THEN
                            RETURN QUERY SELECT pp.path, pp.op_value FROM pvt_extract_field_pairs(elem->'when') pp;
                        END IF;
                        IF elem ? 'then' THEN
                            RETURN QUERY SELECT pp.path, pp.op_value FROM pvt_extract_field_pairs(elem->'then') pp;
                        END IF;
                        IF elem ? 'else' THEN
                            RETURN QUERY SELECT pp.path, pp.op_value FROM pvt_extract_field_pairs(elem->'else') pp;
                        END IF;
                    END IF;
                END LOOP;
                CONTINUE;
            END IF;
            -- $fts object form: descend only into `query` and `fields` array.
            IF lower(k) = '$fts' AND jsonb_typeof(v) = 'object' THEN
                IF v ? 'query'  THEN
                    RETURN QUERY SELECT pp.path, pp.op_value FROM pvt_extract_field_pairs(v->'query') pp;
                END IF;
                IF v ? 'fields' AND jsonb_typeof(v->'fields') = 'array' THEN
                    FOR elem IN SELECT value FROM jsonb_array_elements(v->'fields') LOOP
                        RETURN QUERY SELECT pp.path, pp.op_value FROM pvt_extract_field_pairs(elem) pp;
                    END LOOP;
                END IF;
                CONTINUE;
            END IF;
            -- $cast(["<sql-type>", expr]): skip the type literal.
            IF lower(k) = '$cast' AND jsonb_typeof(v) = 'array' AND jsonb_array_length(v) >= 2 THEN
                RETURN QUERY SELECT pp.path, pp.op_value FROM pvt_extract_field_pairs(v->1) pp;
                CONTINUE;
            END IF;
            -- $dateAdd/$dateSub/$dateDiff/$dateTrunc: skip the unit literal.
            IF lower(k) IN ('$dateadd','$datesub','$datediff','$datetrunc')
               AND jsonb_typeof(v) = 'array' AND jsonb_array_length(v) >= 1 THEN
                FOR elem IN SELECT value FROM jsonb_array_elements(v) WITH ORDINALITY AS t(value, ord)
                              WHERE ord >= 2 LOOP
                    RETURN QUERY SELECT pp.path, pp.op_value FROM pvt_extract_field_pairs(elem) pp;
                END LOOP;
                CONTINUE;
            END IF;
            IF jsonb_typeof(v) = 'array' THEN
                FOR elem IN SELECT value FROM jsonb_array_elements(v) LOOP
                    RETURN QUERY SELECT pp.path, pp.op_value FROM pvt_extract_field_pairs(elem) pp;
                END LOOP;
            ELSIF jsonb_typeof(v) = 'object' THEN
                RETURN QUERY SELECT pp.path, pp.op_value FROM pvt_extract_field_pairs(v) pp;
            END IF;
        ELSE
            IF right(k, length('.ContainsKey')) = '.ContainsKey' THEN
                RETURN QUERY SELECT k, pvt_peek_contains_key_value(v);
            ELSE
                RETURN QUERY SELECT k, NULL::text;
            END IF;
        END IF;
    END LOOP;
END;
$BODY$;

COMMENT ON FUNCTION pvt_extract_field_pairs(jsonb) IS
    'Recursive walker: yields (path, op_value) pairs. op_value carries the operand text for `*.ContainsKey` predicates so the caller can normalize them to `<base>[<key>]`.';


-- ---------- pvt_collect_fields ----------------------------------------
-- Resolve metadata for every field mentioned in filter/order. When
-- p_include_all is true, ALL scheme fields are collected (used by the
-- full-result entry point that may need to project arbitrary fields).
CREATE OR REPLACE FUNCTION pvt_collect_fields(
    p_scheme_id   bigint,
    p_filter      jsonb,
    p_order       jsonb,
    p_include_all boolean DEFAULT false
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_pair       RECORD;
    v_normalized text;
    v_meta       jsonb;
    v_order_path text;
    v_all_name   text;
    v_result     jsonb := '{}'::jsonb;
BEGIN
    IF p_scheme_id IS NULL THEN
        RAISE EXCEPTION 'pvt_collect_fields: p_scheme_id is required';
    END IF;

    -- ---------- p_include_all: project every top-level field ----------
    IF p_include_all THEN
        FOR v_all_name IN
            SELECT DISTINCT _name
              FROM _scheme_metadata_cache
             WHERE _scheme_id = p_scheme_id
               AND _parent_structure_id IS NULL
        LOOP
            v_meta := pvt_resolve_field_path(p_scheme_id, v_all_name);
            v_result := v_result || jsonb_build_object(v_all_name, v_meta);
        END LOOP;
        RETURN v_result;
    END IF;

    -- ---------- Collect from filter (ContainsKey-aware) ---------------
    FOR v_pair IN
        SELECT DISTINCT path, op_value
          FROM pvt_extract_field_pairs(p_filter)
         WHERE path IS NOT NULL
    LOOP
        -- Detect .$length / .$count suffix: targets count of the base
        -- array. We register BOTH the base field (so the CTE pivots it)
        -- AND a modifier entry that the condition builder rewrites to
        -- COALESCE(array_length(<base>, 1), 0).
        DECLARE
            v_mod_kind text := NULL;
            v_base     text := NULL;
            v_base_meta jsonb;
        BEGIN
            IF right(v_pair.path, length('.$length')) = '.$length' THEN
                v_mod_kind := 'length';
                v_base := left(v_pair.path, length(v_pair.path) - length('.$length'));
            ELSIF right(v_pair.path, length('.$count')) = '.$count' THEN
                v_mod_kind := 'count';
                v_base := left(v_pair.path, length(v_pair.path) - length('.$count'));
            END IF;

            IF v_mod_kind IS NOT NULL THEN
                IF NOT (v_result ? v_base) THEN
                    v_base_meta := pvt_resolve_field_path(p_scheme_id, v_base);
                    v_result := v_result || jsonb_build_object(v_base, v_base_meta);
                ELSE
                    v_base_meta := v_result -> v_base;
                END IF;
                IF NOT (v_result ? v_pair.path) THEN
                    v_result := v_result || jsonb_build_object(
                        v_pair.path,
                        v_base_meta
                            || jsonb_build_object(
                                'length_modifier', true,
                                'modifier_kind', v_mod_kind,
                                'base_name', v_base));
                END IF;
                CONTINUE;
            END IF;
        END;

        v_normalized := pvt_normalize_field_name(v_pair.path, v_pair.op_value);
        IF v_result ? v_normalized THEN
            CONTINUE;
        END IF;
        v_meta := pvt_resolve_field_path(p_scheme_id, v_normalized);
        IF v_normalized <> v_pair.path THEN
            -- Tag ContainsKey rewrites: WHERE builder emits IS NOT NULL.
            v_meta := v_meta || jsonb_build_object('was_contains_key', true);
        END IF;
        v_result := v_result || jsonb_build_object(v_normalized, v_meta);
    END LOOP;

    -- ---------- Collect from order paths -----------------------------
    -- Each order entry may carry either:
    --   * plain path: { "field": "X" } or legacy { "field_path": "X" }
    --   * expression node: { "$expr": <scalar-expr-node>, "dir": "..." }
    -- For expressions we walk every `$field` inside via
    -- pvt_expr_field_names (defined in 17_pvt_expr.sql) so its metadata
    -- gets resolved and the field is pivoted into the CTE just like
    -- WHERE-referenced fields.
    IF p_order IS NOT NULL THEN
        FOR v_order_path IN
            SELECT path FROM (
                SELECT COALESCE(e->>'field', e->>'field_path') AS path
                  FROM jsonb_array_elements(p_order) AS e
                 WHERE e ? 'field' OR e ? 'field_path'
                UNION ALL
                SELECT pvt_expr_field_names(e->'$expr') AS path
                  FROM jsonb_array_elements(p_order) AS e
                 WHERE e ? '$expr'
            ) s
            WHERE s.path IS NOT NULL
        LOOP
            IF v_order_path IS NULL OR v_result ? v_order_path THEN
                CONTINUE;
            END IF;
            v_meta := pvt_resolve_field_path(p_scheme_id, v_order_path);
            v_result := v_result || jsonb_build_object(v_order_path, v_meta);
        END LOOP;
    END IF;

    RETURN v_result;
END;
$BODY$;

COMMENT ON FUNCTION pvt_collect_fields(bigint, jsonb, jsonb, boolean) IS
    'Walks filter/order JSON, normalizes Dict.ContainsKey paths, resolves each via pvt_resolve_field_path, and returns a jsonb keyed by the (possibly rewritten) field name. ContainsKey-rewritten entries carry was_contains_key=true so the WHERE builder collapses the predicate to IS NOT NULL.';


-- ---------- pvt_has_null_check ----------------------------------------
-- True iff the filter contains an explicit null-aware predicate. Used
-- by the CTE builder to decide LEFT JOIN (need NULL surface) vs INNER
-- JOIN (faster, suffices when all predicates require a present value).
CREATE OR REPLACE FUNCTION pvt_has_null_check(p_filter jsonb)
RETURNS boolean
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    k text; v jsonb; sk text; sv jsonb; elem jsonb;
BEGIN
    IF p_filter IS NULL OR jsonb_typeof(p_filter) <> 'object' THEN
        RETURN false;
    END IF;

    FOR k, v IN SELECT key, value FROM jsonb_each(p_filter) LOOP
        IF left(k, 1) = '$' THEN
            IF k IN ('$null', '$exists', '$isNull', '$notNull') THEN
                RETURN true;
            END IF;
            IF jsonb_typeof(v) = 'array' THEN
                FOR elem IN SELECT value FROM jsonb_array_elements(v) LOOP
                    IF pvt_has_null_check(elem) THEN RETURN true; END IF;
                END LOOP;
            ELSIF jsonb_typeof(v) = 'object' THEN
                IF pvt_has_null_check(v) THEN RETURN true; END IF;
            END IF;
        ELSE
            -- Field leaf: inspect its operator object.
            IF jsonb_typeof(v) = 'object' THEN
                FOR sk, sv IN SELECT key, value FROM jsonb_each(v) LOOP
                    IF sk IN ('$null', '$exists', '$isNull', '$notNull') THEN
                        RETURN true;
                    END IF;
                    IF sk IN ('$eq', '$ne') AND jsonb_typeof(sv) = 'null' THEN
                        RETURN true;
                    END IF;
                END LOOP;
            ELSIF jsonb_typeof(v) = 'null' THEN
                RETURN true;
            END IF;
        END IF;
    END LOOP;

    RETURN false;
END;
$BODY$;

COMMENT ON FUNCTION pvt_has_null_check(jsonb) IS
    'Returns true if the filter contains any explicit null-aware predicate ($null/$exists/$isNull/$notNull or $eq/$ne with null). The CTE builder uses this to pick LEFT vs INNER JOIN on _values.';


-- ---------- pvt_has_absence_check -------------------------------------
-- Strict subset of pvt_has_null_check: returns true only for predicates
-- that REQUIRE detecting absent _values rows (i.e. an object without
-- any value for the field must still appear in the outer result set).
-- These predicates demand the legacy wide+LEFT JOIN shape:
--
--   * `$null` / `$isNull`                  : field must be NULL.
--   * `{$eq: null}`                        : same as $null.
--   * `$exists` (any value, incl. false)   : tests presence; safest
--                                            kept in the "absence"
--                                            bucket.
--
-- Predicates that can be satisfied by an INNER JOIN over _values are
-- intentionally NOT flagged here:
--
--   * `$notNull`         : "must be present" -- INNER drops absent rows
--                          automatically (the desired outcome).
--   * `{$ne: null}`      : equivalent to "must be present".
--
-- The narrow Pro-shape pivot CTE inherently CANNOT represent absent
-- objects (its `FROM _values v` GROUP BY v._id_object only yields
-- object ids that have at least one matching row); hence the narrow
-- branch is gated on `NOT pvt_has_absence_check(...)` rather than the
-- broader pvt_has_null_check.
CREATE OR REPLACE FUNCTION pvt_has_absence_check(p_filter jsonb)
RETURNS boolean
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    k text; v jsonb; sk text; sv jsonb; elem jsonb;
BEGIN
    IF p_filter IS NULL OR jsonb_typeof(p_filter) <> 'object' THEN
        RETURN false;
    END IF;

    FOR k, v IN SELECT key, value FROM jsonb_each(p_filter) LOOP
        IF left(k, 1) = '$' THEN
            IF k IN ('$null', '$isNull', '$exists') THEN
                RETURN true;
            END IF;
            IF jsonb_typeof(v) = 'array' THEN
                FOR elem IN SELECT value FROM jsonb_array_elements(v) LOOP
                    IF pvt_has_absence_check(elem) THEN RETURN true; END IF;
                END LOOP;
            ELSIF jsonb_typeof(v) = 'object' THEN
                IF pvt_has_absence_check(v) THEN RETURN true; END IF;
            END IF;
        ELSE
            IF jsonb_typeof(v) = 'object' THEN
                FOR sk, sv IN SELECT key, value FROM jsonb_each(v) LOOP
                    IF sk IN ('$null', '$isNull', '$exists') THEN
                        RETURN true;
                    END IF;
                    IF sk = '$eq' AND jsonb_typeof(sv) = 'null' THEN
                        RETURN true;
                    END IF;
                END LOOP;
            ELSIF jsonb_typeof(v) = 'null' THEN
                RETURN true;
            END IF;
        END IF;
    END LOOP;

    RETURN false;
END;
$BODY$;

COMMENT ON FUNCTION pvt_has_absence_check(jsonb) IS
    'Strict subset of pvt_has_null_check: true only for predicates that require detecting absent _values rows ($null/$isNull/$exists/{$eq:null}). $notNull and {$ne:null} are NOT flagged because INNER JOIN already drops absent rows. Used to gate the narrow Pro-shape pivot, which cannot represent missing objects.';


-- =====================================================================
-- pvt_resolve_field_path: port of redb.Core.Pro.Schema.SchemeFieldResolver
-- ---------------------------------------------------------------------
-- Resolves a logical field path (e.g. "Name", "Dict[key]", "Status.Value",
-- "Auction.Costs", "Roles[].Value") to a rich FieldInfo jsonb consumed
-- by pvt_build_column_expr / pvt_build_cte_sql / pvt_build_field_condition.
--
-- Returned shape:
--   { "kind":"base", "column":"_id_parent", "name":"<raw>",
--     "is_array":false, "list_item_prop":null, "dict_key":null,
--     "parent_sid":null, "sid":null, "db_type":null, "db_column":null }
-- OR
--   { "kind":"field", "sid":<bigint>, "db_type":"String|...",
--     "db_column":"_String|_Long|...", "name":"<raw>",
--     "is_array":bool, "list_item_prop":null|"Id"|"Value"|"Alias",
--     "dict_key":null|"<key>", "parent_sid":null|<bigint> }
-- =====================================================================
CREATE OR REPLACE FUNCTION pvt_resolve_field_path(
    p_scheme_id bigint,
    p_path      text
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_dict_match     text[];
    v_root_name      text;
    v_dict_key       text;
    v_rest           text;
    v_child_name     text;
    v_parts          text[];
    v_last           text;
    v_root_li        text;
    v_base_col       text;
    v_db_col         text;
    v_sid            bigint;
    v_db_type        text;
    v_is_array       boolean;
    v_parent_sid     bigint := NULL;
    v_list_item_prop text := NULL;
    v_cur_sid        bigint;
    v_base_name      text;
    v_force_array    boolean := false;
    i                int;
BEGIN
    IF p_scheme_id IS NULL OR p_path IS NULL OR p_path = '' THEN
        RAISE EXCEPTION 'pvt_resolve_field_path: scheme_id/path are required (got %, %)', p_scheme_id, p_path;
    END IF;

    -- 0. Base field?
    v_base_col := pvt_normalize_base_field_name(p_path);
    IF v_base_col IS NOT NULL THEN
        RETURN jsonb_build_object(
            'kind','base','column',v_base_col,'name',p_path,
            'is_array',false,'list_item_prop',NULL,'dict_key',NULL,
            'parent_sid',NULL,'sid',NULL,'db_type',NULL,'db_column',NULL);
    END IF;

    -- 1. Dictionary path: Name[key] or Name[key].Child
    v_dict_match := regexp_match(p_path, '^([A-Za-z_][A-Za-z0-9_]*)\[([^\]]+)\](.*)$');
    IF v_dict_match IS NOT NULL THEN
        v_root_name := v_dict_match[1];
        v_dict_key  := v_dict_match[2];
        v_rest      := v_dict_match[3];

        SELECT _structure_id, db_type
          INTO v_sid, v_db_type
          FROM _scheme_metadata_cache
         WHERE _scheme_id = p_scheme_id AND _name = v_root_name AND _parent_structure_id IS NULL
         LIMIT 1;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'pvt_resolve_field_path: dict root field "%" not found in scheme %', v_root_name, p_scheme_id;
        END IF;

        IF v_rest IS NOT NULL AND v_rest LIKE '.%' THEN
            v_parent_sid := v_sid;
            v_child_name := substring(v_rest from 2);
            SELECT _structure_id, db_type, (_collection_type IS NOT NULL)
              INTO v_sid, v_db_type, v_is_array
              FROM _scheme_metadata_cache
             WHERE _scheme_id = p_scheme_id AND _name = v_child_name AND _parent_structure_id = v_parent_sid
             LIMIT 1;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'pvt_resolve_field_path: nested-dict child "%" not found under "%"', v_child_name, v_root_name;
            END IF;
        ELSE
            v_is_array := false;
        END IF;

        v_db_col := pvt_db_type_to_value_column(v_db_type);
        RETURN jsonb_build_object(
            'kind','field','sid',v_sid,'db_type',v_db_type,'db_column',v_db_col,
            'name',p_path,'is_array',v_is_array,
            'list_item_prop',NULL,'dict_key',v_dict_key,'parent_sid',v_parent_sid);
    END IF;

    -- 2. Dotted paths.
    IF position('.' in p_path) > 0 THEN
        v_parts := string_to_array(p_path, '.');
        v_last  := v_parts[array_length(v_parts,1)];

        -- 2a. Roles[].Value / .Alias / .Id  (ListItem array accessors)
        IF array_length(v_parts,1) = 2
           AND v_last IN ('Id','Value','Alias')
           AND v_parts[1] LIKE '%[]' THEN
            v_root_li := left(v_parts[1], length(v_parts[1]) - 2);
            SELECT _structure_id, db_type, (_collection_type IS NOT NULL)
              INTO v_sid, v_db_type, v_is_array
              FROM _scheme_metadata_cache
             WHERE _scheme_id = p_scheme_id AND _name = v_root_li AND _parent_structure_id IS NULL
             LIMIT 1;
            IF FOUND AND v_db_type = 'ListItem' AND v_is_array THEN
                RETURN jsonb_build_object(
                    'kind','field','sid',v_sid,'db_type','ListItem','db_column','_ListItem',
                    'name',p_path,'is_array',true,
                    'list_item_prop',v_last,'dict_key',NULL,'parent_sid',NULL);
            END IF;
        END IF;

        -- 2b. Status.Value / .Alias / .Id  (ListItem scalar accessors)
        IF array_length(v_parts,1) = 2 AND v_last IN ('Id','Value','Alias') THEN
            SELECT _structure_id, db_type, (_collection_type IS NOT NULL)
              INTO v_sid, v_db_type, v_is_array
              FROM _scheme_metadata_cache
             WHERE _scheme_id = p_scheme_id AND _name = v_parts[1] AND _parent_structure_id IS NULL
             LIMIT 1;
            IF FOUND AND v_db_type = 'ListItem' THEN
                RETURN jsonb_build_object(
                    'kind','field','sid',v_sid,'db_type','ListItem','db_column','_ListItem',
                    'name',p_path,'is_array',v_is_array,
                    'list_item_prop',v_last,'dict_key',NULL,'parent_sid',NULL);
            END IF;
        END IF;

        -- 2c. Generic nested: walk parent chain.
        SELECT _structure_id INTO v_cur_sid
          FROM _scheme_metadata_cache
         WHERE _scheme_id = p_scheme_id AND _name = v_parts[1] AND _parent_structure_id IS NULL
         LIMIT 1;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'pvt_resolve_field_path: root "%" of nested path "%" not found in scheme %', v_parts[1], p_path, p_scheme_id;
        END IF;

        FOR i IN 2..array_length(v_parts,1) LOOP
            SELECT _structure_id INTO v_cur_sid
              FROM _scheme_metadata_cache
             WHERE _scheme_id = p_scheme_id AND _name = v_parts[i] AND _parent_structure_id = v_cur_sid
             LIMIT 1;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'pvt_resolve_field_path: nested segment "%" of path "%" not found', v_parts[i], p_path;
            END IF;
        END LOOP;

        SELECT db_type, (_collection_type IS NOT NULL)
          INTO v_db_type, v_is_array
          FROM _scheme_metadata_cache
         WHERE _structure_id = v_cur_sid
         LIMIT 1;
        v_db_col := pvt_db_type_to_value_column(v_db_type);
        RETURN jsonb_build_object(
            'kind','field','sid',v_cur_sid,'db_type',v_db_type,'db_column',v_db_col,
            'name',p_path,'is_array',v_is_array,
            'list_item_prop',NULL,'dict_key',NULL,'parent_sid',NULL);
    END IF;

    -- 3. Bare root field (possibly `Foo[]` for arrays).
    v_base_name := p_path;
    IF p_path LIKE '%[]' THEN
        v_base_name   := left(p_path, length(p_path) - 2);
        v_force_array := true;
    END IF;
    SELECT _structure_id, db_type, (_collection_type IS NOT NULL)
      INTO v_sid, v_db_type, v_is_array
      FROM _scheme_metadata_cache
     WHERE _scheme_id = p_scheme_id AND _name = v_base_name AND _parent_structure_id IS NULL
     LIMIT 1;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'pvt_resolve_field_path: field "%" not found in scheme %', p_path, p_scheme_id;
    END IF;
    IF v_force_array THEN
        v_is_array := true;
    END IF;

    IF v_db_type = 'ListItem' THEN
        v_db_col := '_ListItem';
        v_list_item_prop := 'Id';  -- bare `Status` defaults to `.Id` (bigint FK)
    ELSE
        v_db_col := pvt_db_type_to_value_column(v_db_type);
    END IF;

    RETURN jsonb_build_object(
        'kind','field','sid',v_sid,'db_type',v_db_type,'db_column',v_db_col,
        'name',p_path,'is_array',v_is_array,
        'list_item_prop',v_list_item_prop,'dict_key',NULL,'parent_sid',NULL);
END;
$BODY$;

COMMENT ON FUNCTION pvt_resolve_field_path(bigint, text) IS
    'Resolves a logical field path to a rich FieldInfo jsonb (kind/sid/db_type/db_column/is_array/list_item_prop/dict_key/parent_sid). Mirrors redb.Core.Pro.Schema.SchemeFieldResolver. Throws when the path cannot be matched against the scheme metadata cache.';
