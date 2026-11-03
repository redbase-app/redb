-- =====================================================================
-- 26_pvt_array_groupby.sql
-- ---------------------------------------------------------------------
-- GROUP BY over the elements of an array (GroupByArray of the free provider).
--
--   pvt_build_array_groupby_sql(
--       p_scheme_id     bigint,
--       p_array_path    text,     -- path to the array through classes and arrays:
--                                 -- 'Contacts', 'Contacts.Metadata', 'HomeAddress.Building.Amenities'
--       p_filter        jsonb,    -- optional condition on the owning objects (PVT shape)
--       p_group_by      jsonb,    -- non-empty array of {field, alias}
--       p_aggregations  jsonb,    -- optional array of {field, func, alias, filter?}
--       p_having        jsonb,    -- optional boolean expression ($and/$or/$not, comparisons,
--                                 -- $isnull/$notnull, $count/$sum/$avg/$min/$max, $field, $const)
--       p_order         jsonb,    -- optional [{field, asc?}] over outer aliases
--       p_limit         integer,
--       p_offset        integer
--   ) RETURNS text
--
-- A field is a path inside the element ('Type', 'Address.City') or '$element', the element itself
-- of an array of simple values. A func is COUNT, SUM, AVG, MIN or MAX; COUNT of '*' with a filter is
-- the count of the elements that satisfy it.
--
-- Shape (the same in the SQL Server module, the SQLite extension and the Pro builder;
-- docs/array-groupby/PLAN.md):
--   SELECT <keys>, <aggregates> FROM (
--     SELECT <element columns, aliased by path>
--     FROM (SELECT DISTINCT _id FROM (<filter>) _filt) f            -- with a condition
--     JOIN _objects o ON o._id = f._id AND o._id_scheme = <s>
--     JOIN _values arr ON arr._id_object = f._id AND arr._id_structure = <array>
--                     AND arr._array_index IS NOT NULL
--     LEFT JOIN _values eN ON eN._array_parent_id = <parent>._id AND eN._id_structure = <field>
--                         AND eN._array_parent_id IS NOT NULL
--   ) elements GROUP BY <keys> [HAVING ...]
-- Without a condition the elements are read off the structure of the array. The elements of the
-- last array of the path are found by its structure alone: intermediate levels are not joined.
--
-- Everything it cannot answer raises: a path that is not an array, a dictionary in the path, a field
-- the elements do not have, '$element' on an array of classes, a field of an array of simple values,
-- a collection or a class named as a value, an unknown '$' token, an unknown func.
-- =====================================================================

DROP FUNCTION IF EXISTS pvt_build_array_groupby_sql(bigint, text, jsonb, jsonb, jsonb, jsonb, jsonb, integer, integer);

-- The column of one element path, with the joins it needs. p_joins maps "<parent alias>|<structure>"
-- to {alias, parent, sid}; the function returns {col, db_type, joins}.
CREATE OR REPLACE FUNCTION pvt_array_element_ref(
    p_scheme_id   bigint,
    p_array_path  text,
    p_arr_sid     bigint,
    p_arr_db_type text,
    p_is_class    boolean,
    p_path        text,
    p_joins       jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_segments  text[];
    v_i         integer;
    v_parent    bigint := p_arr_sid;
    v_alias     text := 'arr';
    v_key       text;
    v_row       record;
    v_col       text;
    v_joins     jsonb := COALESCE(p_joins, '{}'::jsonb);
BEGIN
    IF p_path = '$element' THEN
        IF p_is_class THEN
            RAISE EXCEPTION 'pvt_build_array_groupby_sql: "$element" names the element itself, but the elements of "%" are classes; name a field of the element', p_array_path;
        END IF;
        v_col := pvt_db_type_to_value_column(p_arr_db_type);
        IF v_col IS NULL THEN
            RAISE EXCEPTION 'pvt_build_array_groupby_sql: the elements of "%" are stored as %, which cannot be grouped or aggregated', p_array_path, p_arr_db_type;
        END IF;
        RETURN jsonb_build_object('col', 'arr.' || v_col, 'db_type', p_arr_db_type, 'joins', v_joins);
    END IF;
    IF left(p_path, 1) = '$' THEN
        RAISE EXCEPTION 'pvt_build_array_groupby_sql: "%" is not a field of the elements of "%"', p_path, p_array_path;
    END IF;
    IF NOT p_is_class THEN
        RAISE EXCEPTION 'pvt_build_array_groupby_sql: the elements of "%" are simple values and have no field "%"; use the element itself', p_array_path, p_path;
    END IF;

    v_segments := string_to_array(p_path, '.');
    FOR v_i IN 1 .. array_length(v_segments, 1) LOOP
        SELECT c._structure_id, c.db_type, c.type_name, c.collection_type_name INTO v_row
        FROM _scheme_metadata_cache c
        WHERE c._scheme_id = p_scheme_id AND c._name = v_segments[v_i] AND c._parent_structure_id = v_parent;
        IF v_row._structure_id IS NULL THEN
            RAISE EXCEPTION 'pvt_build_array_groupby_sql: "%" is not a field of the elements of "%"', array_to_string(v_segments[1:v_i], '.'), p_array_path;
        END IF;
        IF v_row.collection_type_name IS NOT NULL THEN
            RAISE EXCEPTION 'pvt_build_array_groupby_sql: "%" is a collection inside the element of "%"; group the inner collection instead', array_to_string(v_segments[1:v_i], '.'), p_array_path;
        END IF;
        IF v_i < array_length(v_segments, 1) AND v_row.type_name <> 'Class' THEN
            RAISE EXCEPTION 'pvt_build_array_groupby_sql: "%" is a value, not a class with fields', array_to_string(v_segments[1:v_i], '.');
        END IF;
        IF v_i = array_length(v_segments, 1) AND v_row.type_name = 'Class' THEN
            RAISE EXCEPTION 'pvt_build_array_groupby_sql: "%" is a class; a key or aggregate reads a stored value', p_path;
        END IF;

        v_key := v_alias || '|' || v_row._structure_id::text;
        IF v_joins ? v_key THEN
            v_alias := v_joins -> v_key ->> 'alias';
        ELSE
            v_joins := v_joins || jsonb_build_object(v_key, jsonb_build_object(
                'alias', 'e' || ((SELECT count(*) FROM jsonb_object_keys(v_joins)) + 1)::text,
                'parent', v_alias,
                'sid', v_row._structure_id));
            v_alias := v_joins -> v_key ->> 'alias';
        END IF;
        v_parent := v_row._structure_id;
    END LOOP;

    v_col := pvt_db_type_to_value_column(v_row.db_type);
    IF v_col IS NULL THEN
        RAISE EXCEPTION 'pvt_build_array_groupby_sql: "%" is stored as %, which cannot be grouped or aggregated', p_path, v_row.db_type;
    END IF;
    RETURN jsonb_build_object('col', v_alias || '.' || v_col, 'db_type', v_row.db_type, 'joins', v_joins);
END;
$BODY$;

CREATE OR REPLACE FUNCTION pvt_build_array_groupby_sql(
    p_scheme_id    bigint,
    p_array_path   text,
    p_filter       jsonb    DEFAULT NULL,
    p_group_by     jsonb    DEFAULT NULL,
    p_aggregations jsonb    DEFAULT NULL,
    p_having       jsonb    DEFAULT NULL,
    p_order        jsonb    DEFAULT NULL,
    p_limit        integer  DEFAULT NULL,
    p_offset       integer  DEFAULT 0
)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_segment       text;
    v_row           record;
    v_arr           record;
    v_parent        bigint := NULL;
    v_is_class      boolean;
    v_paths         text[] := ARRAY[]::text[];
    v_path          text;
    v_ref           jsonb;
    v_joins         jsonb := '{}'::jsonb;
    v_fields_map    jsonb := '{}'::jsonb;
    v_inner_select  text[] := ARRAY[]::text[];
    v_join_sql      text := '';
    v_entry         jsonb;
    v_alias         text;
    v_func          text;
    v_field         text;
    v_select_parts  text[] := ARRAY[]::text[];
    v_group_parts   text[] := ARRAY[]::text[];
    v_alias_seen    text[] := ARRAY[]::text[];
    v_ord_parts     text[] := ARRAY[]::text[];
    v_having_sql    text := '';
    v_paging        text := '';
    v_from          text;
    v_filter_sql    text;
BEGIN
    IF p_scheme_id IS NULL THEN
        RAISE EXCEPTION 'pvt_build_array_groupby_sql: p_scheme_id is required';
    END IF;
    IF p_array_path IS NULL OR p_array_path = '' THEN
        RAISE EXCEPTION 'pvt_build_array_groupby_sql: p_array_path is required';
    END IF;
    IF p_group_by IS NULL OR jsonb_typeof(p_group_by) <> 'array' OR jsonb_array_length(p_group_by) = 0 THEN
        RAISE EXCEPTION 'pvt_build_array_groupby_sql: p_group_by must be a non-empty JSON array';
    END IF;

    -- ---- the array: every segment exists, none is a dictionary, the last one is an array
    FOREACH v_segment IN ARRAY string_to_array(p_array_path, '.') LOOP
        SELECT c._structure_id, c.db_type, c.type_name, c.collection_type_name INTO v_row
        FROM _scheme_metadata_cache c
        WHERE c._scheme_id = p_scheme_id AND c._name = v_segment
          AND ((v_parent IS NULL AND c._parent_structure_id IS NULL) OR c._parent_structure_id = v_parent);
        IF v_row._structure_id IS NULL THEN
            RAISE EXCEPTION 'pvt_build_array_groupby_sql: "%" of the array path "%" is not a field of scheme %', v_segment, p_array_path, p_scheme_id;
        END IF;
        IF v_row.collection_type_name = 'Dictionary' THEN
            RAISE EXCEPTION 'pvt_build_array_groupby_sql: "%" in the array path "%" is a dictionary; dictionaries are not supported in the array path', v_segment, p_array_path;
        END IF;
        v_parent := v_row._structure_id;
        v_arr := v_row;
    END LOOP;
    IF v_arr.collection_type_name IS DISTINCT FROM 'Array' THEN
        RAISE EXCEPTION 'pvt_build_array_groupby_sql: "%" is not an array', p_array_path;
    END IF;
    v_is_class := v_arr.type_name = 'Class';

    -- ---- every element path the statement reads: keys, aggregate operands and conditions, HAVING
    FOR v_entry IN SELECT value FROM jsonb_array_elements(p_group_by) LOOP
        v_field := v_entry->>'field';
        IF v_field IS NULL OR v_field = '' THEN
            RAISE EXCEPTION 'pvt_build_array_groupby_sql: group_by entry missing "field"';
        END IF;
        v_paths := v_paths || v_field;
    END LOOP;
    IF p_aggregations IS NOT NULL AND jsonb_typeof(p_aggregations) = 'array' THEN
        FOR v_entry IN SELECT value FROM jsonb_array_elements(p_aggregations) LOOP
            v_field := COALESCE(v_entry->>'field', '*');
            IF v_field <> '*' THEN
                v_paths := v_paths || v_field;
            END IF;
            IF v_entry ? 'filter' THEN
                v_paths := v_paths || ARRAY(SELECT n FROM pvt_expr_field_names(v_entry->'filter') n WHERE n IS NOT NULL);
            END IF;
        END LOOP;
    END IF;
    IF p_having IS NOT NULL AND jsonb_typeof(p_having) = 'object' AND p_having <> '{}'::jsonb THEN
        v_paths := v_paths || ARRAY(SELECT n FROM pvt_expr_field_names(p_having) n WHERE n IS NOT NULL);
    END IF;

    FOR v_path IN SELECT DISTINCT unnest(v_paths) LOOP
        v_ref := pvt_array_element_ref(p_scheme_id, p_array_path, v_arr._structure_id, v_arr.db_type, v_is_class, v_path, v_joins);
        v_joins := v_ref -> 'joins';
        v_inner_select := v_inner_select || format('%s AS %I', v_ref->>'col', v_path);
        v_fields_map := v_fields_map || jsonb_build_object(v_path, jsonb_build_object('kind', 'props', 'db_type', v_ref->>'db_type'));
    END LOOP;

    SELECT string_agg(format(
               E'\nLEFT JOIN _values %s ON %s._array_parent_id = %s._id AND %s._id_structure = %s AND %s._array_parent_id IS NOT NULL',
               j.value->>'alias', j.value->>'alias', j.value->>'parent', j.value->>'alias', j.value->>'sid', j.value->>'alias'),
               '' ORDER BY substr(j.value->>'alias', 2)::integer)
      INTO v_join_sql
      FROM jsonb_each(v_joins) j;
    v_join_sql := COALESCE(v_join_sql, '');

    -- ---- keys
    FOR v_entry IN SELECT value FROM jsonb_array_elements(p_group_by) LOOP
        v_field := v_entry->>'field';
        v_alias := COALESCE(v_entry->>'alias', v_field);
        IF v_alias = ANY(v_alias_seen) THEN
            RAISE EXCEPTION 'pvt_build_array_groupby_sql: duplicate alias "%"', v_alias;
        END IF;
        v_select_parts := v_select_parts || (quote_ident(v_field) || ' AS ' || quote_ident(v_alias));
        v_group_parts  := v_group_parts  || quote_ident(v_field);
        v_alias_seen   := v_alias_seen   || v_alias;
    END LOOP;

    -- ---- aggregates
    IF p_aggregations IS NOT NULL AND jsonb_typeof(p_aggregations) = 'array' THEN
        FOR v_entry IN SELECT value FROM jsonb_array_elements(p_aggregations) LOOP
            v_field := COALESCE(v_entry->>'field', '*');
            v_func  := upper(COALESCE(v_entry->>'func', ''));
            v_alias := COALESCE(v_entry->>'alias', v_func);
            IF v_func NOT IN ('COUNT', 'SUM', 'AVG', 'MIN', 'MAX') THEN
                RAISE EXCEPTION 'pvt_build_array_groupby_sql: unknown aggregate function "%"', v_entry->>'func';
            END IF;
            IF v_alias = ANY(v_alias_seen) THEN
                RAISE EXCEPTION 'pvt_build_array_groupby_sql: duplicate alias "%"', v_alias;
            END IF;
            IF v_entry ? 'filter' THEN
                IF v_func <> 'COUNT' OR v_field <> '*' THEN
                    RAISE EXCEPTION 'pvt_build_array_groupby_sql: only COUNT(*) takes a filter ("%")', v_alias;
                END IF;
                v_select_parts := v_select_parts || (
                    'COUNT(CASE WHEN ' || pvt_build_bool_expr(v_entry->'filter', v_fields_map, '') || ' THEN 1 END) AS ' || quote_ident(v_alias));
            ELSIF v_field = '*' THEN
                IF v_func <> 'COUNT' THEN
                    RAISE EXCEPTION 'pvt_build_array_groupby_sql: % needs a field ("%")', v_func, v_alias;
                END IF;
                v_select_parts := v_select_parts || ('COUNT(*) AS ' || quote_ident(v_alias));
            ELSE
                v_select_parts := v_select_parts || (v_func || '(' || quote_ident(v_field) || ') AS ' || quote_ident(v_alias));
            END IF;
            v_alias_seen := v_alias_seen || v_alias;
        END LOOP;
    END IF;

    IF p_having IS NOT NULL AND jsonb_typeof(p_having) = 'object' AND p_having <> '{}'::jsonb THEN
        v_having_sql := E'\nHAVING ' || pvt_build_bool_expr(p_having, v_fields_map, '');
    END IF;

    -- ---- ORDER BY / paging over the outer aliases
    IF p_order IS NOT NULL AND jsonb_typeof(p_order) = 'array' AND jsonb_array_length(p_order) > 0 THEN
        FOR v_entry IN SELECT value FROM jsonb_array_elements(p_order) LOOP
            v_alias := v_entry->>'field';
            IF v_alias IS NULL OR v_alias = '' THEN
                RAISE EXCEPTION 'pvt_build_array_groupby_sql: order entry missing "field"';
            END IF;
            v_ord_parts := v_ord_parts || (quote_ident(v_alias) ||
                CASE WHEN COALESCE((v_entry->>'asc')::boolean, true) THEN '' ELSE ' DESC' END);
        END LOOP;
        v_paging := E'\nORDER BY ' || array_to_string(v_ord_parts, ', ');
    END IF;
    IF p_limit IS NOT NULL AND p_limit >= 0 THEN
        v_paging := v_paging || E'\nLIMIT ' || p_limit::text;
    END IF;
    IF COALESCE(p_offset, 0) > 0 THEN
        v_paging := v_paging || E'\nOFFSET ' || p_offset::text;
    END IF;

    -- ---- source of the elements
    IF p_filter IS NOT NULL AND jsonb_typeof(p_filter) = 'object' AND p_filter <> '{}'::jsonb THEN
        v_filter_sql := pvt_build_query_sql(
            p_scheme_id => p_scheme_id, p_filter => p_filter, p_limit => NULL, p_offset => 0,
            p_order => NULL, p_max_depth => NULL, p_distinct => false);
        IF v_filter_sql IS NULL OR v_filter_sql = '' THEN
            RAISE EXCEPTION 'pvt_build_array_groupby_sql: the condition on the objects did not compile';
        END IF;
        v_from := format(
            E'FROM (SELECT DISTINCT _id FROM (%s) _filt) f\nJOIN _objects o ON o._id = f._id AND o._id_scheme = %s\nJOIN _values arr ON arr._id_object = f._id AND arr._id_structure = %s AND arr._array_index IS NOT NULL%s',
            v_filter_sql, p_scheme_id, v_arr._structure_id, v_join_sql);
    ELSE
        v_from := format(
            E'FROM _values arr\nJOIN _objects o ON o._id = arr._id_object AND o._id_scheme = %s%s\nWHERE arr._id_structure = %s AND arr._array_index IS NOT NULL',
            p_scheme_id, v_join_sql, v_arr._structure_id);
    END IF;

    RETURN format(
        E'SELECT %s\nFROM (\nSELECT %s\n%s\n) elements\nGROUP BY %s%s%s',
        array_to_string(v_select_parts, ', '),
        array_to_string(v_inner_select, ', '),
        v_from,
        array_to_string(v_group_parts, ', '),
        v_having_sql,
        v_paging);
END;
$BODY$;

COMMENT ON FUNCTION pvt_build_array_groupby_sql(bigint, text, jsonb, jsonb, jsonb, jsonb, jsonb, integer, integer) IS
'Builds a GROUP BY over the elements of an array (path through classes and arrays). Element fields, nested classes of the element and $element of an array of simple values are joined by _array_parent_id; COUNT(*) takes a filter; HAVING through pvt_build_bool_expr. Raises on every form it cannot answer.';
