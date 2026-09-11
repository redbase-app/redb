-- =====================================================================
-- v2-pvt module init
-- =====================================================================
-- Purpose: PVT-based search engine for REDB free (PostgreSQL).
-- Owner  : redb core team. Forked helpers in 01..07 mirror legacy
--          redb_facets_search.sql / redb_lazy_loading_search.sql.
-- Version: see pvt_module_version() in 99_module_version.sql (applied LAST).
--
-- This file must be applied FIRST. It performs two things:
--   1. Verifies that system infrastructure of REDB is in place
--      (core tables and two system functions).
--   2. Drops every function this module owns (CASCADE) so the module
--      can be redeployed cleanly — pvt_module_version() included, which is
--      why the version is recreated LAST, in 99_module_version.sql: a
--      failure anywhere in between leaves no version, and the next start
--      applies the bundle again.
-- =====================================================================

-- ---------- 0. Schema upgrades (idempotent, run on every redeploy) ----------
-- DDL for columns added after a database was created. The base DDL in
-- redbPostgre.sql stays the source of truth for fresh databases; this block
-- is the delivery to existing ones (SCHEMA_DELIVERY plan, V4). Every
-- statement is a no-op once applied.

-- V4 (UNIQUE stage 2): [RedbUnique] key metadata and the key hash column.
ALTER TABLE _structures ADD COLUMN IF NOT EXISTS _unique boolean NULL;
ALTER TABLE _structures ADD COLUMN IF NOT EXISTS _unique_version bigint NULL;
ALTER TABLE _values ADD COLUMN IF NOT EXISTS _unique uuid NULL;
-- S2 (subtree-unique plan, 0.7.7): the key index covers keyed rows at ANY position now -
-- nested scalar keys carry _array_parent_id, and the old positional filter kept them OUT of
-- the index, so their "uniqueness" silently never fired. Recreate on the old predicate.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public'
               AND indexname = 'UIX__values__structure_unique'
               AND indexdef LIKE '%_array_parent_id%') THEN
        DROP INDEX "UIX__values__structure_unique";
    END IF;
END $$;
CREATE UNIQUE INDEX IF NOT EXISTS "UIX__values__structure_unique"
    ON _values (_id_structure, _unique) INCLUDE (_id_object)
    WHERE _unique IS NOT NULL;
ALTER TABLE _scheme_metadata_cache ADD COLUMN IF NOT EXISTS _unique boolean NULL;
ALTER TABLE _scheme_metadata_cache ADD COLUMN IF NOT EXISTS _unique_version bigint NULL;
-- V4 (LAZY Л2): the lazy-reference marker, read from `virtual` at synchronisation.
ALTER TABLE _structures ADD COLUMN IF NOT EXISTS _lazy boolean NULL;
ALTER TABLE _scheme_metadata_cache ADD COLUMN IF NOT EXISTS _lazy boolean NULL;
UPDATE _scheme_metadata_cache c SET _lazy = s._lazy FROM _structures s
    WHERE s._id = c._structure_id AND c._lazy IS DISTINCT FROM s._lazy;

-- V4 (UNIQUE stage 1): the object key column and its unique index.
ALTER TABLE _objects ADD COLUMN IF NOT EXISTS _value_unique varchar(440) NULL;
CREATE UNIQUE INDEX IF NOT EXISTS "UIX__objects__scheme_unique"
    ON _objects (_id_scheme, _value_unique) INCLUDE (_id)
    WHERE _value_unique IS NOT NULL;

-- S3 (subtree-unique plan, 0.7.8): element-key scope of collection keys, and the free-form
-- _tags marker on schemes and structures (future / custom extensions; sync never wipes it).
ALTER TABLE _structures ADD COLUMN IF NOT EXISTS _unique_scope bigint NULL;
ALTER TABLE _structures ADD COLUMN IF NOT EXISTS _tags varchar(450) NULL;
ALTER TABLE _schemes ADD COLUMN IF NOT EXISTS _tags varchar(450) NULL;
CREATE INDEX IF NOT EXISTS "IX__structures__tags" ON _structures (_tags) WHERE _tags IS NOT NULL;
ALTER TABLE _scheme_metadata_cache ADD COLUMN IF NOT EXISTS _unique_scope bigint NULL;
ALTER TABLE _scheme_metadata_cache ADD COLUMN IF NOT EXISTS _tags varchar(450) NULL;
UPDATE _scheme_metadata_cache c SET _unique_scope = s._unique_scope, _tags = s._tags
    FROM _structures s
    WHERE s._id = c._structure_id
      AND (c._unique_scope IS DISTINCT FROM s._unique_scope OR c._tags IS DISTINCT FROM s._tags);

-- FK-column indexes of _values (perf, 2026-09-10): _Object/_ListItem carry foreign keys, and
-- without a leading index every DELETE of a referenced object or list item seq-scans the whole
-- table for the FK check. Partial form keeps them nearly empty.
CREATE INDEX IF NOT EXISTS "IX__values__ListItem_not_null" ON _values (_ListItem) WHERE _ListItem IS NOT NULL;
CREATE INDEX IF NOT EXISTS "IX__values__Object_not_null"   ON _values (_Object)   WHERE _Object   IS NOT NULL;

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


-- ---------- pvt_like_escape: make a sugar operand LITERAL under LIKE/ILIKE ----------
-- BR-7 (2026-09-02): $startsWith/$endsWith/$contains (+IgnoreCase; field, class-field, dict,
-- array and expression sugar) splice their operand into a LIKE pattern. The operand is a
-- LITERAL by contract: '%'/'_' must not act as wildcards and a backslash must not act as
-- LIKE's own escape character (PostgreSQL's default escape). The raw $like/$ilike/$matches
-- operators do NOT pass through here - their pattern belongs to the caller.
CREATE OR REPLACE FUNCTION pvt_like_escape(p_v text)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
RETURNS NULL ON NULL INPUT
AS $$
    SELECT replace(replace(replace(p_v, '\', '\\'), '%', '\%'), '_', '\_')
$$;

COMMENT ON FUNCTION pvt_like_escape(text) IS
    'Escapes LIKE metacharacters (backslash, %, _) so a sugar operand matches literally. BR-7 2026-09-02.';


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

