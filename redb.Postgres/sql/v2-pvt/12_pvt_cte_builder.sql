-- =====================================================================
-- 12_pvt_cte_builder.sql
-- ---------------------------------------------------------------------
-- Build the `WITH _pvt_cte AS (...)` clause that pivots _values into
-- one column per requested scheme field. All downstream predicates
-- (WHERE / ORDER BY) reference columns of this CTE.
--
-- Output shape (legacy WIDE, used for null-checks and tree mode):
--   WITH _pvt_cte AS (
--       SELECT
--           o._id, o._id_parent, ... <all 21 base cols>,
--           <one column per field via pvt_build_column_expr>
--       FROM _objects o
--       <LEFT|INNER> JOIN _values v ON v._id_object = o._id
--       WHERE o._id_scheme = <scheme_id>
--         [AND tree-restriction]
--         [AND <pushed base predicate>]
--       GROUP BY o._id, o._id_parent, ... (all _objects columns above)
--   )
--
-- Output shape (NARROW Pro-parity, used for flat + non-null queries):
--   WITH _pvt_cte AS (
--       SELECT
--           v._id_object,
--           <one column per field via pvt_build_column_expr>
--       FROM _values v
--       WHERE v._id_structure = ANY(ARRAY[<sid1>, <sid2>, ...]::bigint[])
--         AND v._id_object IN (
--             SELECT _id FROM _objects
--              WHERE _id_scheme = <scheme_id>
--                [AND <pushed base predicate>]
--                [AND o._id IN (SELECT _id FROM _pvt_tree)]
--         )
--       GROUP BY v._id_object
--   )
-- The narrow shape skips the wide GROUP BY (21 cols -> 1 col),
-- narrows the _values scan via the `_id_structure = ANY` index hint,
-- and lets the outer SELECT JOIN _objects so the planner can still
-- short-circuit by system-column indexes. Tree pushdown (Pro
-- CHANGELOG 2.0.1 parity): in tree mode the recursive `_pvt_tree`
-- CTE is emitted alongside `_pvt_cte` and its restriction is folded
-- INTO the narrow IN-subquery, so the recursive walk + base
-- pushdown both filter the object set BEFORE _values is touched.
--
-- Legacy WIDE always projects every _objects column the C# layer
-- needs to hydrate base fields. Narrow shape projects only
-- v._id_object; the outer query MUST `JOIN _objects o ON o._id =
-- _pvt_cte._id_object` to access base columns.
-- =====================================================================

-- Signature evolved in v0.6.0 (added p_include_seed + p_polymorphic and five
-- explicit tree modes). CREATE OR REPLACE cannot change argument lists, so
-- drop every legacy form here.
DROP FUNCTION IF EXISTS pvt_build_cte_sql(bigint, jsonb, text, bigint[], integer, boolean);
DROP FUNCTION IF EXISTS pvt_build_cte_sql(bigint, jsonb, text, bigint[], integer, boolean, text);
DROP FUNCTION IF EXISTS pvt_build_cte_sql(bigint, jsonb, text, bigint[], integer, boolean, text, boolean);
DROP FUNCTION IF EXISTS pvt_build_cte_sql(bigint, jsonb, text, bigint[], integer, boolean, text, boolean, boolean, boolean);

CREATE OR REPLACE FUNCTION pvt_build_cte_sql(
    p_scheme_id     bigint,
    p_fields        jsonb,
    p_source_mode   text     DEFAULT 'flat',
    p_tree_ids      bigint[] DEFAULT NULL,
    p_max_depth     integer  DEFAULT NULL,
    p_force_outer   boolean  DEFAULT true,
    p_extra_where   text     DEFAULT NULL,  -- Pro-style base-field pushdown: extra predicate AND-ed into the inner WHERE.
    p_narrow        boolean  DEFAULT false, -- emit Pro-shape narrow pivot (FROM _values + IN (...) + GROUP BY 1).
    p_include_seed  boolean  DEFAULT true,  -- include the seed object(s) themselves in tree_descendants / tree (false matches old free `WHERE depth>0`).
    p_polymorphic   boolean  DEFAULT true,  -- when false, AND `_id_scheme = p_scheme_id` into the recursive walk (Pro keeps walk polymorphic).
    p_residual_where text    DEFAULT NULL   -- Pro-parity WHERE pushdown: residual predicate (refs pivot cols only) wrapped INSIDE _pvt_cte so it filters BEFORE the outer JOIN _objects.
)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_pivot_cols text := '';
    v_join_kind  text;
    v_field_name text;
    v_field_meta jsonb;
    v_first       boolean := true;
    v_base_cols   constant text :=
        'o._id, o._id_parent, o._id_scheme, o._id_owner, o._id_who_change, '
        || 'o._name, o._date_create, o._date_modify, o._date_begin, o._date_complete, '
        || 'o._key, o._note, o._hash, '
        || 'o._value_long, o._value_string, o._value_guid, o._value_bool, '
        || 'o._value_double, o._value_numeric, o._value_datetime, o._value_bytes';
    v_where           text := format('o._id_scheme = %s', p_scheme_id::text);
    v_cte_parts       text[] := ARRAY[]::text[];
    v_has_recursive   boolean := false;
    v_nested_groups   jsonb := '{}'::jsonb;
    v_group_key       text;
    v_group_entry     jsonb;
    v_group_fields    jsonb;
    v_inner_field     jsonb;
    v_inner_meta      jsonb;
    v_inner_col       text;
    v_inner_sid       text;
    v_inner_alias     text;
    v_inner_is_array  boolean;
    v_inner_sub       text;
    v_inner_subs      text;
    v_group_idx       integer := 0;
    v_group_cte_name  text;
    v_nested_cols_sql text := '';
    v_nested_joins    text := '';
    v_inner_pivot     text;
    v_pvt_body        text;
    v_sids            bigint[] := ARRAY[]::bigint[];
    v_sid_text        text;
    v_objects_subq    text;
    v_tree_filter     text := '';
    -- Nested-only optimization (Pro-parity, see E062 in CHANGELOG):
    -- when v_pivot_cols is empty AND there's exactly one nested-dict
    -- group AND no force_outer AND narrow flag set, skip the heavy
    -- `pi` wrapper (SELECT 21 base cols FROM _objects ... GROUP BY 21)
    -- and emit `_pvt_cte AS (SELECT ndN._id_object, ndN.<cols> FROM
    -- nested_dict_N ndN)` directly. Object-set restriction (scheme +
    -- base pushdown + tree filter) is folded into the nested CTE's
    -- inner WHERE via v_nested_obj_filter.
    v_is_nested_only      boolean := false;
    v_nested_obj_filter   text    := '';
    v_flat_cols_sql       text    := '';
    v_pivot_cols_empty    boolean;
    -- Quoted aliases of every column projected by the pivot body
    -- (scalar + nested-dict). Used to emit Pro-shape `SELECT pvt._id_object,
    -- pvt."col1", pvt."col2" FROM (<inner>) pvt WHERE <residual>` when
    -- pvt_build_query_sql pushes a residual predicate inside _pvt_cte.
    v_pivot_aliases       text[]  := ARRAY[]::text[];
    -- True when any pivot field is ListItem.Value/.Alias (scalar or
    -- array). Triggers `LEFT JOIN _list_items li ON li._id = v._ListItem`
    -- on the pivot source so column exprs can reference `li._value`
    -- / `li._alias` instead of correlated subselects (Pro parity).
    v_has_listitem_join   boolean := false;
BEGIN
    IF p_scheme_id IS NULL THEN
        RAISE EXCEPTION 'pvt_build_cte_sql: p_scheme_id is required';
    END IF;
    IF p_source_mode NOT IN ('flat', 'tree', 'tree_descendants', 'tree_children', 'tree_roots', 'tree_leaves', 'tree_ancestors') THEN
        RAISE EXCEPTION 'pvt_build_cte_sql: invalid p_source_mode "%", expected flat|tree|tree_descendants|tree_children|tree_roots|tree_leaves|tree_ancestors', p_source_mode;
    END IF;

    -- ------- Partition requested fields: base / nested-dict / pivot --
    IF p_fields IS NOT NULL AND p_fields <> '{}'::jsonb THEN
        FOR v_field_name, v_field_meta IN
            SELECT key, value FROM jsonb_each(p_fields)
        LOOP
            -- Base fields are already in v_base_cols; skip to avoid dupes.
            IF v_field_meta->>'kind' = 'base' THEN
                CONTINUE;
            END IF;
            -- Modifier-only entries (.$length / .$count) project no
            -- column of their own; their base entry was inserted by
            -- pvt_collect_fields and will be emitted normally below.
            IF COALESCE((v_field_meta->>'length_modifier')::boolean, false) THEN
                CONTINUE;
            END IF;
            -- Collect structure id for the narrow Pro-shape
            -- `_id_structure = ANY(...)` filter. Skip nested-dict child
            -- fields (parent_sid set: they project from a side CTE
            -- scoped to the parent dict row, not the main _values scan).
            -- Simple-dict fields (dict_key set, parent_sid NULL) keep
            -- their sid here because pvt_build_column_expr's FILTER
            -- (_id_structure = X AND _array_index = 'key') reads
            -- straight from the main scan.
            IF (v_field_meta->>'sid') IS NOT NULL
               AND (v_field_meta->>'parent_sid') IS NULL THEN
                v_sids := v_sids || ((v_field_meta->>'sid')::bigint);
            END IF;
            -- Nested-dict: accumulate per (parent_sid, dict_key) group.
            IF (v_field_meta->>'dict_key') IS NOT NULL
               AND (v_field_meta->>'parent_sid') IS NOT NULL THEN
                v_group_key := (v_field_meta->>'parent_sid')
                               || '|' || (v_field_meta->>'dict_key');
                v_group_entry := COALESCE(
                    v_nested_groups -> v_group_key,
                    jsonb_build_object(
                        'parent_sid', v_field_meta->>'parent_sid',
                        'dict_key',   v_field_meta->>'dict_key',
                        'fields',     '[]'::jsonb));
                v_group_entry := jsonb_set(
                    v_group_entry,
                    '{fields}',
                    (v_group_entry->'fields')
                        || jsonb_build_array(
                            jsonb_build_object(
                                'name', v_field_name,
                                'meta', v_field_meta)));
                v_nested_groups := jsonb_set(
                    v_nested_groups,
                    ARRAY[v_group_key],
                    v_group_entry,
                    true);
                CONTINUE;
            END IF;
            -- Regular pivot column.
            IF NOT v_first THEN
                v_pivot_cols := v_pivot_cols || ',' || E'\n            ';
            ELSE
                v_pivot_cols := E',\n            ';
                v_first := false;
            END IF;
            v_pivot_cols := v_pivot_cols
                || pvt_build_column_expr(v_field_name, v_field_meta);
            v_pivot_aliases := v_pivot_aliases || quote_ident(v_field_name);
            IF (v_field_meta->>'list_item_prop') IN ('Value', 'Alias') THEN
                v_has_listitem_join := true;
            END IF;
        END LOOP;
    END IF;

    -- ------- JOIN kind: LEFT keeps objects with no _values rows -------
    v_join_kind := CASE WHEN p_force_outer THEN 'LEFT JOIN' ELSE 'INNER JOIN' END;

    -- ------- Tree-mode object-set restriction -------------------------
    -- Emit the `_pvt_tree(_id, depth)` CTE matching the requested
    -- tree mode (descendants / children / roots / leaves / ancestors),
    -- then capture the restriction `o._id IN (SELECT _id FROM
    -- _pvt_tree [WHERE depth>0])` in v_tree_filter so it can be
    -- AND-ed into either the wide WHERE or the narrow IN-subquery.
    --
    --   tree / tree_descendants  recursive walk DOWN from p_tree_ids.
    --                            depth=0 at seed; descendants depth>0.
    --                            p_max_depth caps walk; p_include_seed
    --                            controls whether seed itself stays.
    --   tree_children            single-level non-recursive: direct
    --                            children of p_tree_ids.
    --   tree_roots               non-recursive: scheme roots
    --                            (`_id_parent IS NULL`). p_tree_ids
    --                            optional and restricts the root set.
    --   tree_leaves              non-recursive: scheme objects with
    --                            no children. p_tree_ids optional.
    --   tree_ancestors           recursive walk UP from p_tree_ids.
    --                            seed depth=1 = direct parent; the
    --                            objects themselves are NOT in the
    --                            walk (seed=parent), so p_include_seed
    --                            does not apply here.
    --
    -- Polymorphic flag: when p_polymorphic = false AND scheme filter
    -- is folded into the recursive parts (Pro keeps walks polymorphic
    -- so cross-scheme parents can stitch hierarchies together).
    DECLARE
        v_seed_arr    text;
        v_scheme_pred text := '';
    BEGIN
        IF p_source_mode <> 'flat' THEN
            v_seed_arr := CASE
                WHEN p_tree_ids IS NULL OR array_length(p_tree_ids, 1) IS NULL
                THEN NULL
                ELSE quote_literal(p_tree_ids::text) || '::bigint[]'
            END;
            IF NOT p_polymorphic THEN
                v_scheme_pred := ' AND o._id_scheme = ' || p_scheme_id::text;
            END IF;
        END IF;

        IF p_source_mode IN ('tree', 'tree_descendants') THEN
            IF v_seed_arr IS NULL THEN
                RAISE EXCEPTION 'pvt_build_cte_sql: p_tree_ids is required when p_source_mode = ''%''', p_source_mode;
            END IF;
            v_cte_parts := v_cte_parts || (
                '_pvt_tree(_id, depth) AS ('
                || E'\n    SELECT _id, 0 FROM _objects WHERE _id = ANY(' || v_seed_arr || ')'
                || E'\n    UNION ALL'
                || E'\n    SELECT o._id, t.depth + 1 FROM _objects o '
                || 'JOIN _pvt_tree t ON o._id_parent = t._id'
                || CASE WHEN p_max_depth IS NOT NULL OR NOT p_polymorphic
                        THEN ' WHERE '
                             || CASE WHEN p_max_depth IS NOT NULL
                                     THEN 't.depth < ' || p_max_depth::text
                                     ELSE 'TRUE' END
                             || CASE WHEN NOT p_polymorphic
                                     THEN v_scheme_pred
                                     ELSE '' END
                        ELSE '' END
                || E'\n)');
            v_has_recursive := true;
            v_tree_filter := CASE WHEN p_include_seed
                                  THEN 'o._id IN (SELECT _id FROM _pvt_tree)'
                                  ELSE 'o._id IN (SELECT _id FROM _pvt_tree WHERE depth > 0)' END;

        ELSIF p_source_mode = 'tree_children' THEN
            IF v_seed_arr IS NULL THEN
                RAISE EXCEPTION 'pvt_build_cte_sql: p_tree_ids is required when p_source_mode = ''tree_children''';
            END IF;
            v_cte_parts := v_cte_parts || (
                '_pvt_tree(_id, depth) AS ('
                || E'\n    SELECT o._id, 1 FROM _objects o '
                || 'WHERE o._id_parent = ANY(' || v_seed_arr || ')'
                || CASE WHEN NOT p_polymorphic THEN v_scheme_pred ELSE '' END
                || E'\n)');
            v_tree_filter := 'o._id IN (SELECT _id FROM _pvt_tree)';

        ELSIF p_source_mode = 'tree_roots' THEN
            v_cte_parts := v_cte_parts || (
                '_pvt_tree(_id, depth) AS ('
                || E'\n    SELECT o._id, 0 FROM _objects o '
                || 'WHERE o._id_parent IS NULL AND o._id_scheme = ' || p_scheme_id::text
                || CASE WHEN v_seed_arr IS NOT NULL
                        THEN ' AND o._id = ANY(' || v_seed_arr || ')'
                        ELSE '' END
                || E'\n)');
            v_tree_filter := 'o._id IN (SELECT _id FROM _pvt_tree)';

        ELSIF p_source_mode = 'tree_leaves' THEN
            v_cte_parts := v_cte_parts || (
                '_pvt_tree(_id, depth) AS ('
                || E'\n    SELECT o._id, 0 FROM _objects o '
                || 'WHERE o._id_scheme = ' || p_scheme_id::text
                || ' AND NOT EXISTS (SELECT 1 FROM _objects c WHERE c._id_parent = o._id)'
                || CASE WHEN v_seed_arr IS NOT NULL
                        THEN ' AND o._id = ANY(' || v_seed_arr || ')'
                        ELSE '' END
                || E'\n)');
            v_tree_filter := 'o._id IN (SELECT _id FROM _pvt_tree)';

        ELSIF p_source_mode = 'tree_ancestors' THEN
            IF v_seed_arr IS NULL THEN
                RAISE EXCEPTION 'pvt_build_cte_sql: p_tree_ids is required when p_source_mode = ''tree_ancestors''';
            END IF;
            -- Walk UP via _id_parent. Seed: immediate parents of
            -- p_tree_ids (depth=1). Recursive step: grandparents and
            -- so on until _id_parent IS NULL or depth cap reached.
            v_cte_parts := v_cte_parts || (
                '_pvt_tree(_id, depth) AS ('
                || E'\n    SELECT seed._id_parent, 1 FROM _objects seed '
                || 'WHERE seed._id = ANY(' || v_seed_arr || ') AND seed._id_parent IS NOT NULL'
                || E'\n    UNION ALL'
                || E'\n    SELECT o._id_parent, t.depth + 1 FROM _objects o '
                || 'JOIN _pvt_tree t ON o._id = t._id'
                || ' WHERE o._id_parent IS NOT NULL'
                || CASE WHEN p_max_depth IS NOT NULL
                        THEN ' AND t.depth < ' || p_max_depth::text
                        ELSE '' END
                || CASE WHEN NOT p_polymorphic THEN v_scheme_pred ELSE '' END
                || E'\n)');
            v_has_recursive := true;
            v_tree_filter := 'o._id IN (SELECT _id FROM _pvt_tree)';
        END IF;

        IF v_tree_filter <> '' THEN
            v_where := v_where || ' AND ' || v_tree_filter;
        END IF;
    END;

    -- ------- Base-field pushdown (Pro parity) -------------------------
    -- pvt_split_filter feeds us a ready-baked predicate fragment that
    -- references `o.*` directly. We AND it into the inner WHERE so the
    -- planner can apply system-column indexes BEFORE the JOIN with
    -- _values and BEFORE the GROUP BY. Cuts work from O(scheme_size)
    -- down to O(filtered_objects).
    IF p_extra_where IS NOT NULL AND p_extra_where <> '' THEN
        v_where := v_where || ' AND ' || p_extra_where;
    END IF;

    -- ------- Nested-dict side CTEs (Pro pattern) ----------------------
    -- For each group (parent_sid, dict_key) we emit ONE CTE that scans
    -- the parent dict-row from _values and projects each child field as
    -- a correlated subselect over child _values rows. Outer _pvt_cte
    -- then LEFT JOINs all groups by _id_object.
    --
    -- Nested-only detection: when v_pivot_cols is empty AND exactly
    -- one nested-dict group is present AND p_narrow / NOT p_force_outer,
    -- the outer pi wrapper is wasteful. Fold the object-set restriction
    -- (scheme + base pushdown + tree filter) into the nested CTE's WHERE
    -- and emit a flat _pvt_cte body below.
    v_pivot_cols_empty := (COALESCE(v_pivot_cols, '') = '');
    v_is_nested_only := p_narrow
                       AND NOT p_force_outer
                       AND v_pivot_cols_empty
                       AND (v_nested_groups <> '{}'::jsonb);
    -- Pro-parity: always fold object-set restriction (scheme + base
    -- pushdown + tree filter) into every nested_dict_N CTE so the
    -- planner can prune dp rows by _id_scheme BEFORE the LEFT JOIN nv
    -- expansion. Previously we set this only on the nested-only path;
    -- mixed scalar+nested queries were left scanning the full _values
    -- partition for the parent sid. Mirrors PG PRO 12.
    v_nested_obj_filter := format(
        ' AND dp._id_object IN (SELECT o._id FROM _objects o WHERE o._id_scheme = %s%s%s)',
        p_scheme_id::text,
        CASE WHEN p_extra_where IS NOT NULL AND p_extra_where <> ''
             THEN ' AND ' || p_extra_where
             ELSE '' END,
        CASE WHEN v_tree_filter <> ''
             THEN ' AND ' || v_tree_filter
             ELSE '' END);

    FOR v_group_key, v_group_entry IN
        SELECT key, value FROM jsonb_each(v_nested_groups)
    LOOP
        v_group_idx := v_group_idx + 1;
        v_group_cte_name := 'nested_dict_' || v_group_idx::text;
        v_group_fields   := v_group_entry->'fields';
        v_inner_subs := '';
        DECLARE
            v_inner_sids text := '';   -- comma-sep sid list for the LEFT JOIN predicate
            v_seen       jsonb := '{}'::jsonb;
        BEGIN
        FOR v_inner_field IN SELECT * FROM jsonb_array_elements(v_group_fields) LOOP
            v_inner_meta     := v_inner_field->'meta';
            v_inner_alias    := v_inner_field->>'name';
            v_inner_sid      := v_inner_meta->>'sid';
            v_inner_col      := v_inner_meta->>'db_column';
            v_inner_is_array := COALESCE((v_inner_meta->>'is_array')::boolean, false);
            IF (v_inner_meta->>'list_item_prop') IS NOT NULL THEN
                RAISE EXCEPTION
                    'pvt_build_cte_sql: nested-dict ListItem accessor not supported in v0.3.0 (field "%")',
                    v_inner_alias;
            END IF;
            -- Pro-parity aggregate: one LEFT JOIN over all sids in the
            -- group + per-field array_agg(...) FILTER (WHERE _id_structure
            -- = SID AND _array_index <kind>). Scalar takes [1]; array
            -- takes the array_agg as-is.
            IF v_inner_is_array THEN
                v_inner_sub := format(
                    'array_agg(nv.%s) FILTER (WHERE nv._id_structure = %s AND nv._array_index IS NOT NULL) AS %I',
                    v_inner_col, v_inner_sid, v_inner_alias);
            ELSE
                v_inner_sub := format(
                    '(array_agg(nv.%s) FILTER (WHERE nv._id_structure = %s AND nv._array_index IS NULL))[1] AS %I',
                    v_inner_col, v_inner_sid, v_inner_alias);
            END IF;
            IF v_inner_subs <> '' THEN
                v_inner_subs := v_inner_subs || ',' || E'\n            ';
            END IF;
            v_inner_subs := v_inner_subs || v_inner_sub;
            -- Collect distinct sids for the JOIN predicate.
            IF (v_seen ? v_inner_sid) = false THEN
                v_seen := v_seen || jsonb_build_object(v_inner_sid, true);
                IF v_inner_sids <> '' THEN
                    v_inner_sids := v_inner_sids || ', ';
                END IF;
                v_inner_sids := v_inner_sids || v_inner_sid;
            END IF;
            -- Track alias for the explicit-cols residual wrapper.
            v_pivot_aliases := v_pivot_aliases || quote_ident(v_inner_alias);
            -- Outer projection + JOIN parts
            v_nested_cols_sql := v_nested_cols_sql
                || ', ' || quote_ident(v_group_cte_name) || '.' || quote_ident(v_inner_alias)
                || ' AS ' || quote_ident(v_inner_alias);
            -- Flat projection (nested-only path): no `pi.` prefix; the
            -- outer SELECT JOINs _objects on _pvt_cte._id_object.
            IF v_is_nested_only THEN
                IF v_flat_cols_sql <> '' THEN
                    v_flat_cols_sql := v_flat_cols_sql || ', ';
                END IF;
                v_flat_cols_sql := v_flat_cols_sql
                    || quote_ident(v_group_cte_name) || '.' || quote_ident(v_inner_alias)
                    || ' AS ' || quote_ident(v_inner_alias);
            END IF;
            -- (joins built once per group below)
        END LOOP;
        v_cte_parts := v_cte_parts || format(
            '%I AS (' || E'\n        SELECT dp._id_object,\n            %s\n        FROM _values dp\n        LEFT JOIN _values nv ON nv._array_parent_id = dp._id AND nv._id_structure %s\n        WHERE dp._id_structure = %s AND dp._array_index = %L%s\n        GROUP BY dp._id_object\n    )',
            v_group_cte_name,
            v_inner_subs,
            CASE WHEN position(',' in v_inner_sids) > 0
                 THEN 'IN (' || v_inner_sids || ')'
                 ELSE '= ' || v_inner_sids END,
            v_group_entry->>'parent_sid',
            v_group_entry->>'dict_key',
            v_nested_obj_filter);
        v_nested_joins := v_nested_joins
            || E'\n        LEFT JOIN ' || quote_ident(v_group_cte_name)
            || ' ON ' || quote_ident(v_group_cte_name) || '._id_object = pi._id';
        END;
    END LOOP;

    -- ------- Inner pivot SELECT ---------------------------------------
    -- Narrow Pro-shape: scan _values directly, restrict by
    -- _id_structure ANY(<requested sids>), restrict object set via an
    -- IN-subquery over _objects (carrying scheme + base pushdown +
    -- tree restriction), and group by a single column (_id_object).
    -- The outer SELECT must JOIN _objects to expose base columns.
    -- Eligible iff: narrow flag set, no LEFT JOIN required (would
    -- lose missing _values rows), no nested-dict groups (they require
    -- pi.* projection over the wide source), and at least one pivot
    -- sid was collected. Tree mode IS supported: v_tree_filter is
    -- folded into the IN-subquery so the recursive `_pvt_tree` walk
    -- and base pushdown both run before the _values scan.
    IF p_narrow
       AND NOT p_force_outer
       AND v_group_idx = 0
       AND array_length(v_sids, 1) IS NOT NULL THEN

        SELECT string_agg(DISTINCT s::text, ', ')
          INTO v_sid_text
          FROM unnest(v_sids) AS s;

        v_objects_subq := format(
            '(SELECT o._id FROM _objects o WHERE o._id_scheme = %s%s%s)',
            p_scheme_id::text,
            CASE WHEN p_extra_where IS NOT NULL AND p_extra_where <> ''
                 THEN ' AND ' || p_extra_where
                 ELSE '' END,
            CASE WHEN v_tree_filter <> ''
                 THEN ' AND ' || v_tree_filter
                 ELSE '' END);

        v_inner_pivot :=
            E'SELECT\n            v._id_object' || v_pivot_cols ||
            E'\n        FROM _values v' ||
            CASE WHEN v_has_listitem_join
                 THEN E'\n        LEFT JOIN _list_items li ON li._id = v._ListItem'
                 ELSE '' END ||
            E'\n        WHERE v._id_structure = ANY(ARRAY[' || v_sid_text || ']::bigint[])' ||
            E'\n          AND v._id_object IN ' || v_objects_subq ||
            E'\n        GROUP BY v._id_object';
    ELSE
        v_inner_pivot :=
            E'SELECT\n            ' || v_base_cols || v_pivot_cols ||
            E'\n        FROM _objects o\n        ' || v_join_kind ||
            E' _values v ON v._id_object = o._id' ||
            CASE WHEN v_has_listitem_join
                 THEN E'\n        LEFT JOIN _list_items li ON li._id = v._ListItem'
                 ELSE '' END ||
            E'\n        WHERE ' || v_where ||
            E'\n        GROUP BY ' || v_base_cols;
    END IF;

    -- ------- Final _pvt_cte body --------------------------------------
    IF v_is_nested_only THEN
        -- Nested-only: flatten across all nested_dict_N groups joined
        -- on _id_object. First group is the seed, additional groups
        -- LEFT JOIN onto it (objects missing entries in later groups
        -- keep NULL for those cols, matching wide-path LEFT JOIN).
        DECLARE
            v_nested_from text := '';
            v_idx         integer;
        BEGIN
            FOR v_idx IN 1..v_group_idx LOOP
                IF v_idx = 1 THEN
                    v_nested_from := 'nested_dict_1';
                ELSE
                    v_nested_from := v_nested_from
                        || E'\n        LEFT JOIN ' || 'nested_dict_' || v_idx::text
                        || ' ON ' || 'nested_dict_' || v_idx::text
                        || '._id_object = nested_dict_1._id_object';
                END IF;
            END LOOP;
            v_pvt_body :=
                'SELECT nested_dict_1._id_object, ' || v_flat_cols_sql
                || E'\n        FROM ' || v_nested_from;
        END;
    ELSIF v_group_idx = 0 THEN
        v_pvt_body := v_inner_pivot;
    ELSE
        v_pvt_body :=
            E'SELECT pi.*' || v_nested_cols_sql ||
            E'\n        FROM (' || v_inner_pivot || E') pi'
            || v_nested_joins;
    END IF;

    -- ------- Residual WHERE pushdown (Pro parity) ---------------------
    -- When pvt_build_query_sql determines the outer filter touches only
    -- pivoted columns, it passes the pre-rendered SQL here. Wrapping it
    -- inside _pvt_cte lets the planner prune rows BEFORE the outer
    -- JOIN _objects, mirroring Pro's `WHERE pvt.<col> = $N` shape.
    -- Explicit column list (not SELECT *): matches Pro 1:1 and keeps
    -- the projection contract stable for downstream consumers.
    IF p_residual_where IS NOT NULL
       AND p_residual_where <> ''
       AND p_residual_where <> 'TRUE' THEN
        DECLARE
            v_wrap_cols text;
        BEGIN
            v_wrap_cols := 'pvt._id_object';
            IF array_length(v_pivot_aliases, 1) IS NOT NULL THEN
                v_wrap_cols := v_wrap_cols
                            || ', pvt.'
                            || array_to_string(v_pivot_aliases, ', pvt.');
            END IF;
            v_pvt_body := 'SELECT ' || v_wrap_cols
                       || E'\n        FROM ('
                       || E'\n        ' || v_pvt_body
                       || E'\n        ) pvt WHERE ' || p_residual_where;
        END;
    END IF;

    v_cte_parts := v_cte_parts ||
        ('_pvt_cte AS (' || E'\n        ' || v_pvt_body || E'\n    )');

    -- ------- Assemble final CTE SQL -----------------------------------
    RETURN (CASE WHEN v_has_recursive THEN 'WITH RECURSIVE ' ELSE 'WITH ' END)
           || array_to_string(v_cte_parts, E',\n');
END;
$BODY$;

COMMENT ON FUNCTION pvt_build_cte_sql(bigint, jsonb, text, bigint[], integer, boolean, text, boolean, boolean, boolean, text) IS
    'Builds the WITH _pvt_cte AS (...) clause. Two pivot shapes: wide legacy (`FROM _objects o JOIN _values v` GROUP BY 21 base cols) used for null-checks, tree mode, and nested-dict groups; narrow Pro-shape (`FROM _values v WHERE _id_structure=ANY(...) AND _id_object IN (SELECT _id FROM _objects WHERE ...)` GROUP BY 1) selected via p_narrow=true. In narrow mode the outer SELECT must JOIN _objects to expose base columns. Source modes: flat | tree (alias for tree_descendants) | tree_descendants | tree_children (single level, non-recursive) | tree_roots (`_id_parent IS NULL`) | tree_leaves (no children, non-recursive) | tree_ancestors (recursive walk UP via _id_parent). p_include_seed=false strips the seed from tree_descendants results; p_polymorphic=false ANDs scheme_id into recursive walk.';
