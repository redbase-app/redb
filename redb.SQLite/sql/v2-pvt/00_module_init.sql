-- =====================================================================
-- v2-pvt module init
-- =====================================================================
-- Purpose: PVT-based search engine for REDB free (PostgreSQL).
-- Owner  : redb core team. Forked helpers in 01..07 mirror legacy
--          redb_facets_search.sql / redb_lazy_loading_search.sql.
-- Version: see pvt_module_version() at the bottom of this file.
--
-- This file must be applied FIRST. It performs three things:
--   1. Verifies that system infrastructure of REDB is in place
--      (core tables and two system functions).
--   2. Drops every function this module owns (CASCADE) so the module
--      can be redeployed cleanly.
--   3. Creates pvt_module_version() — used by the C# client to verify
--      compatibility on InitializeAsync(). No runtime fallback.
-- =====================================================================

-- ---------- 1. System infrastructure check ------------------------------
DO $$
BEGIN
    -- Required system function: scheme metadata reader. Source lives in
    -- redbPostgre.sql; ships in the generated bundle redb_init.sql.
    IF NOT EXISTS (
        SELECT 1
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE p.proname = 'get_scheme_definition'
          AND n.nspname = 'public'
    ) THEN
        RAISE EXCEPTION
            'v2-pvt: required system function public.get_scheme_definition(bigint) is missing. Deploy the REDB core schema first (redbPostgre.sql / generated redb_init.sql).';
    END IF;

    -- Required system function: object JSON materializer.
    IF NOT EXISTS (
        SELECT 1
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE p.proname = 'get_object_json'
          AND n.nspname = 'public'
    ) THEN
        RAISE EXCEPTION
            'v2-pvt: required system function public.get_object_json(bigint, integer) is missing. Deploy the REDB core schema first (redbPostgre.sql / generated redb_init.sql).';
    END IF;

    -- Required core tables.
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables
                   WHERE table_schema = 'public' AND table_name = '_objects') THEN
        RAISE EXCEPTION 'v2-pvt: required table public._objects is missing.';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables
                   WHERE table_schema = 'public' AND table_name = '_values') THEN
        RAISE EXCEPTION 'v2-pvt: required table public._values is missing.';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables
                   WHERE table_schema = 'public' AND table_name = '_structures') THEN
        RAISE EXCEPTION 'v2-pvt: required table public._structures is missing.';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables
                   WHERE table_schema = 'public' AND table_name = '_list_items') THEN
        RAISE EXCEPTION 'v2-pvt: required table public._list_items is missing.';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables
                   WHERE table_schema = 'public' AND table_name = '_scheme_metadata_cache') THEN
        RAISE EXCEPTION
            'v2-pvt: required cache table public._scheme_metadata_cache is missing. Deploy redb_metadata_cache.sql first.';
    END IF;
END $$;

-- ---------- 2. DROP every pvt_* function this module owns ---------------
-- Universal drop: enumerate all functions in the public schema whose name
-- starts with `pvt_` and drop them with their actual signatures. This
-- protects the module against signature drift between releases.
DO $$
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT p.oid::regprocedure::text AS sig
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'public'
          AND p.proname LIKE 'pvt\_%' ESCAPE '\'
    LOOP
        EXECUTE 'DROP FUNCTION IF EXISTS ' || r.sig || ' CASCADE';
    END LOOP;
END $$;

-- ---------- 3. Module version function ---------------------------------
-- semver: bump MAJOR on breaking changes to entry-point signatures or
-- result shape; bump MINOR on additive features; bump PATCH on bug fixes.
CREATE OR REPLACE FUNCTION pvt_module_version()
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
AS $BODY$
BEGIN
    -- 0.6.2 — Nested-dict object-set pushdown (mixed scalar+nested):
    --   * 12_pvt_cte_builder.sql now folds the object-set restriction
    --     (scheme + base pushdown + tree filter) into every
    --     nested_dict_N CTE's WHERE, not just the nested-only path.
    --     Mixed scalar+nested queries were previously scanning the
    --     full parent_sid partition of _values and gating via the
    --     outer JOIN; with this change PG prunes dp rows by
    --     _id_scheme BEFORE the LEFT JOIN nv expansion.
    -- 0.6.1 — ListItem.Value/.Alias Pro-parity perf:
    --   * pvt_build_cte_sql and the inline GROUP BY path in
    --     pvt_build_groupby_sql now emit a single
    --     `LEFT JOIN _list_items li ON li._id = v._ListItem` on the
    --     pivot source whenever any field projects `list_item_prop`
    --     in (Value, Alias). Per-column expressions reference bare
    --     `li._value` / `li._alias` and aggregate via
    --     `array_agg(li.<col>) FILTER (...)`.
    --   * Replaces N per-column correlated subselects
    --     `(SELECT li._value FROM _list_items li WHERE li._id = v._ListItem)`
    --     with one JOIN per pivot — matches Pro PivotSqlGenerator.
    --   * Scalar Value/Alias pivot column still holds resolved text
    --     (Free LINQ passes string literals; comparison is `= '...'::text`).
    -- 0.6.0 — Pro-parity perf rewrite (large-scale ops):
    --   * #1 Filter pushdown: pvt_split_filter detects narrow filter sets
    --     that contain no base refs and inlines the residual WHERE
    --     inside _pvt_cte with an explicit `SELECT pvt._id_object, pvt."col", ...`
    --     wrapper (pvt_filter_has_base_refs gate + explicit-cols
    --     projection in pvt_build_cte_sql). Outer WHERE collapses to TRUE.
    --   * #2 GROUP BY inline subquery: pvt_build_groupby_sql now skips
    --     the CTE for pure-scalar narrow shapes and emits
    --     `SELECT pvt.<grp>, agg(...) FROM (<inline pivot>) pvt`.
    --     `v._array_index IS NULL` is lifted from per-column FILTER into
    --     the inline subquery's outer WHERE — index-friendly at 100M+ rows.
    --     pvt_build_column_expr gained p_array_index_in_outer for this.
    --   * #3 Nested-dict side CTE: a single LEFT JOIN _values + per-field
    --     `array_agg(...) FILTER (...)` replaces N correlated subselects.
    --     SID list collapses to `IN (...)` (or `= sid`) with dedup.
    -- 0.5.0 — Expression engine (Pro parity, capability):
    --   * 17_pvt_expr.sql introduces pvt_build_scalar_expr (recursive
    --     compiler for $field/$const/arithmetic/Math/String/Concat/
    --     Coalesce/Cast) and pvt_build_expr_predicate (full predicate
    --     family $eq..$gte / $like / $ilike / $in / $nin / $between /
    --     $null / $notNull / $contains[IgnoreCase] / $startsWith / $endsWith).
    --   * pvt_build_where_from_json and pvt_split_filter route
    --     filter-level expression-form predicates through the new engine.
    --   * pvt_extract_field_pairs harvests $field references from
    --     expression subtrees so pvt_collect_fields resolves them.
    --   * Pushdown: expression predicates are pushed iff every $field
    --     reference inside resolves to kind=base (pvt_expr_is_base_only).
    -- 0.4.0 — Base-field pushdown (Pro parity, perf):
    --   * pvt_split_filter walks the filter and peels off base/hierarchical
    --     predicates into a SQL fragment over `_objects o.*`.
    --   * pvt_build_cte_sql accepts p_extra_where and ANDs it into the
    --     inner WHERE so PG can use system-column indexes BEFORE the
    --     JOIN with _values and the GROUP BY agg.
    --   * pvt_build_field_condition gained p_base_prefix; passed as 'o.'
    --     in pushdown context, '' (default) in the outer CTE WHERE.
    --   * $or/$not are pushed only when every leaf inside is base —
    --     mixed branches keep the original semantics.
    -- 0.3.0 — Pro parity rewrite:
    --   * `(array_agg(v.<col>) FILTER (...))[1]` idiom (works for bool/uuid/etc).
    --   * `_array_index IS NULL` filter for scalars (NOT `_array_parent_id IS NULL`).
    --   * `0$:` base-field prefix stripping in pvt_normalize_base_field_name.
    --   * full collection / nested / dictionary / ListItem.Value/Alias / array-op support.
    RETURN '0.6.2';
END;
$BODY$;

COMMENT ON FUNCTION pvt_module_version() IS
    'Returns the semver of the v2-pvt module. Used by the C# client on InitializeAsync to enforce compatibility (major must match, deployed minor >= required).';

-- ---------- 4. Shared legacy helpers used by pvt_* code ----------------
-- Forked verbatim from sql/deprecated/redb_facets_search.sql. They are
-- referenced by pvt_build_inner_condition / pvt_build_single_facet_condition
-- and were left in the legacy file before the PG free path was rewritten on
-- top of v2-pvt. Kept here (not under deprecated/) so the module is fully
-- self-contained — the bundled redb_init.sql no longer ships the legacy
-- facets_search file. Names keep the underscore prefix to avoid touching
-- every call site inside the pvt_* functions.

DROP TYPE IF EXISTS structure_info_type CASCADE;
CREATE TYPE structure_info_type AS (
    root_structure_id bigint,
    nested_structure_id bigint,
    root_type_info jsonb,
    nested_type_info jsonb
);

CREATE OR REPLACE FUNCTION _format_json_array_for_in(
    array_data jsonb
) RETURNS text
LANGUAGE 'plpgsql'
IMMUTABLE
AS $BODY$
DECLARE
    in_values text := '';
    json_element jsonb;
    first_item boolean := true;
    element_text text;
BEGIN
    IF jsonb_typeof(array_data) != 'array' THEN
        RAISE EXCEPTION 'JSON array expected, got: %', jsonb_typeof(array_data);
    END IF;

    FOR json_element IN SELECT value FROM jsonb_array_elements(array_data) LOOP
        IF NOT first_item THEN
            in_values := in_values || ', ';
        END IF;
        first_item := false;

        CASE jsonb_typeof(json_element)
            WHEN 'string' THEN
                element_text := quote_literal(json_element #>> '{}');
            WHEN 'number' THEN
                element_text := json_element::text;
            WHEN 'boolean' THEN
                element_text := CASE WHEN (json_element)::boolean THEN 'true' ELSE 'false' END;
            ELSE
                element_text := quote_literal(json_element #>> '{}');
        END CASE;

        in_values := in_values || element_text;
    END LOOP;

    RETURN in_values;
END;
$BODY$;

COMMENT ON FUNCTION _format_json_array_for_in(jsonb) IS
    'Converts JSONB array to string of values for SQL IN clause. Forked from redb_facets_search.sql into the v2-pvt module bundle (00_module_init.sql).';

-- pvt_resolve_field_path_table: TABLE-returning resolver used by
-- 26_pvt_array_groupby.sql. Forked verbatim from
-- sql/deprecated/redb_aggregation.sql (resolve_field_path). The PVT module
-- also ships pvt_resolve_field_path(bigint, text) RETURNS jsonb (see
-- 01_pvt_field_path.sql) — that one mirrors C# SchemeFieldResolver and is
-- used by the rest of pvt_*. Keep both: the table form is what the
-- array_groupby builder consumes (structure_id / db_type / is_array /
-- array_index / dict_key / is_dictionary).
CREATE OR REPLACE FUNCTION pvt_resolve_field_path_table(
    p_scheme_id bigint,
    p_field_path text
)
RETURNS TABLE(structure_id bigint, db_type text, is_array boolean, array_index int, dict_key text, is_dictionary boolean)
LANGUAGE plpgsql
AS $BODY$
DECLARE
    v_segments text[];
    v_segment text;
    v_clean_segment text;
    v_current_parent_id bigint := NULL;
    v_structure_id bigint;
    v_db_type text;
    v_is_collection boolean := false;
    v_is_dictionary boolean := false;
    v_found_collection_type bigint;
    v_array_index int := NULL;
    v_dict_key text := NULL;
    v_index_match text[];
    v_key_match text[];
    v_collection_type_name text;
BEGIN
    v_index_match := regexp_match(p_field_path, '\[(\d+)\]');
    IF v_index_match IS NOT NULL THEN
        v_array_index := v_index_match[1]::int;
    END IF;

    v_key_match := regexp_match(p_field_path, '\[([A-Za-z_][A-Za-z0-9_-]*)\]');
    IF v_key_match IS NOT NULL THEN
        v_dict_key := v_key_match[1];
    END IF;

    v_segments := string_to_array(regexp_replace(p_field_path, '\[[^\]]*\]', '', 'g'), '.');

    FOREACH v_segment IN ARRAY v_segments
    LOOP
        v_clean_segment := trim(v_segment);
        IF v_clean_segment = '' THEN
            CONTINUE;
        END IF;

        SELECT c._structure_id, c.db_type, c._collection_type
        INTO v_structure_id, v_db_type, v_found_collection_type
        FROM _scheme_metadata_cache c
        WHERE c._scheme_id = p_scheme_id
          AND c._name = v_clean_segment
          AND (
              (v_current_parent_id IS NULL AND c._parent_structure_id IS NULL)
              OR c._parent_structure_id = v_current_parent_id
          )
        LIMIT 1;

        IF v_structure_id IS NULL THEN
            RAISE EXCEPTION 'Field segment "%" not found in path "%" (scheme=%). Check cache: SELECT * FROM warmup_all_metadata_caches();',
                v_clean_segment, p_field_path, p_scheme_id;
        END IF;

        IF v_found_collection_type IS NOT NULL THEN
            v_is_collection := true;
            SELECT t._name INTO v_collection_type_name
            FROM _types t WHERE t._id = v_found_collection_type;
            IF v_collection_type_name = 'Dictionary' THEN
                v_is_dictionary := true;
            END IF;
        END IF;

        v_current_parent_id := v_structure_id;
    END LOOP;

    structure_id := v_structure_id;
    db_type := v_db_type;
    is_array := v_is_collection OR (p_field_path ~ '\[[^\]]*\]');
    array_index := v_array_index;
    dict_key := v_dict_key;
    is_dictionary := v_is_dictionary;
    RETURN NEXT;
END;
$BODY$;

COMMENT ON FUNCTION pvt_resolve_field_path_table(bigint, text) IS
    'TABLE-returning field-path resolver forked from redb_aggregation.sql. Consumed by pvt_build_array_groupby_sql (26_pvt_array_groupby.sql).';

DO $$
BEGIN
    RAISE NOTICE 'v2-pvt module init OK, version: %', pvt_module_version();
END $$;
