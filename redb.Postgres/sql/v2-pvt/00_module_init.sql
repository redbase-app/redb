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

    -- NOTE: get_object_json() is now OWNED by this module (defined in
    -- 08_core_object_json.sql), so it is no longer guarded as an external
    -- prerequisite — it is (re)created later in the same bundle. This lets
    -- its bug fixes ride the versioned auto-redeploy.

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
    -- 0.6.6 — migrate_structure_type joins the module, and its text conversions
    --   are guarded:
    --   * 27_migrate_structure_type.sql moved in from sql/. It used to ship only
    --     in redb_init.sql, which is applied to fresh databases only, so a fix to
    --     it never reached an existing one. Living in the module means the
    --     version check redeploys it like everything else here.
    --   * String -> Boolean destroyed unrecognised values: the CASE fell through
    --     to NULL while the same statement cleared _String, and the row counted
    --     as a success. Now predicated on the accepted token list, so anything
    --     else stays put and lands in error_count.
    --   * String -> DateTimeOffset and String -> Guid were bare casts that raised
    --     on the first bad row and aborted the whole migration. Now guarded, as
    --     the numeric branches always were and as MSSQL's TRY_CAST already did.
    -- 0.6.5 — Unicode-aware case folding for the Free query path:
    --   * new pvt_fold_case(text): wraps an expression in COLLATE when the
    --     redb.string_collation GUC is set, and returns it untouched otherwise.
    --     Case folding is driven by the database ctype, so on a database created
    --     with LC_CTYPE=C the ILIKE/LOWER/UPPER family folds ASCII only and
    --     'Привет' ILIKE '%привет%' is false. The GUC is set by the C# provider
    --     per connection, which keeps every existing function signature intact.
    --   * applied in 13_pvt_condition.sql and 17_pvt_expr.sql at every ILIKE,
    --     and at $lower/$upper, so the three operations cannot disagree.
    -- 0.6.4 — Scoped WhereLeaves()/WhereRoots() cross-tree leak fix:
    --   * 12_pvt_cte_builder.sql tree_leaves/tree_roots now honour the seed as a
    --     SUBTREE ROOT (descend, then apply the leaf/root predicate) instead of an
    --     exact-id membership, so TreeQuery(rootObj).WhereLeaves() returns the leaves
    --     of that subtree — not every leaf in the scheme.
    -- 0.6.3 — Soft-delete read-path fix + object-json materializer ownership:
    --   * The whole object->JSON materializer (get_object_json, get_objects_json,
    --     build_hierarchical_properties_optimized, build_listitem_jsonb) moved
    --     from core (redb_json_objects.sql, now deleted) into the module
    --     (08_core_object_json.sql) so its fixes auto-redeploy to existing
    --     databases via the version check (full redb_init.sql is not re-run
    --     once _schemes exists).
    --   * get_object_json() now treats soft-deleted objects
    --     (_id_scheme = -10, @@__deleted) as non-existent: a nested
    --     _Object reference to a trashed object resolves to NULL instead
    --     of materializing the tombstone. The _values pointer stays
    --     intact, so soft-delete remains reversible.
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
    RETURN '0.6.6';
END;
$BODY$;

-- ---------- 3a. Unicode-aware case folding -----------------------------
-- Case folding in PostgreSQL is driven by the collation's ctype. A database
-- created with LC_CTYPE=C folds ASCII and nothing else, so on such a database
--     'Привет' ILIKE '%привет%'   -> false
--     lower('Привет')             -> 'Привет'
-- while the same statements are correct on a database created with a real
-- locale. The fix is to attach an explicit collation to the operand.
--
-- The collation name arrives through a GUC rather than a function parameter.
-- That is deliberate: every entry point of this module (pvt_build_query_sql and
-- friends) would otherwise need an extra argument threaded through five layers,
-- which is a signature change on each. The GUC costs nothing at the call sites
-- and the C# provider sets it once per connection.
--
-- Unset GUC means unchanged behaviour, byte for byte, which is what makes this
-- safe to deploy to a database whose owner never asked for it.
--
-- STABLE, not IMMUTABLE: the result depends on a run-time setting.
CREATE OR REPLACE FUNCTION pvt_fold_case(p_expr text)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_collation text;
BEGIN
    -- The second argument makes a missing setting return NULL instead of raising.
    v_collation := btrim(coalesce(current_setting('redb.string_collation', true), ''));

    IF v_collation = '' THEN
        RETURN p_expr;
    END IF;

    -- quote_ident is the escaping, not decoration: the name is an identifier and
    -- therefore cannot be a bound parameter, so it is concatenated into SQL text.
    -- It doubles embedded quotes, which neutralises an injected name.
    RETURN '(' || p_expr || ' COLLATE ' || quote_ident(v_collation) || ')';
END;
$BODY$;

COMMENT ON FUNCTION pvt_fold_case(text) IS
    'Wraps a text expression in COLLATE when redb.string_collation is set, so ILIKE/LOWER/UPPER fold every script and not just ASCII. Returns the expression untouched when the GUC is unset.';

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
