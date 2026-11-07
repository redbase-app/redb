-- ============================================================
-- METADATA CACHE SYNC — module-owned since V4
-- ============================================================
-- sync_metadata_cache_for_scheme and warmup_all_metadata_caches carry the
-- column list of _scheme_metadata_cache. A new _structures column (V4:
-- _unique, _unique_version) must appear in that list on EXISTING databases,
-- and sql/redb_metadata_cache.sql lands only in redb_init.sql — fresh
-- databases only. So the two functions ride the versioned bundle, exactly
-- like get_object_json (08_core_object_json.sql): their fixes and column
-- lists redeploy whenever pvt_module_version() disagrees with the build.
--
-- The cache TABLE, its indexes and the triggers stay in
-- sql/redb_metadata_cache.sql: the triggers call these functions by name at
-- run time, so recreating the functions here is enough. Column additions to
-- the table itself go through the "0. Schema upgrades" block of
-- 00_module_init.sql.
-- ============================================================

DROP FUNCTION IF EXISTS sync_metadata_cache_for_scheme(bigint);
DROP FUNCTION IF EXISTS warmup_all_metadata_caches();

CREATE OR REPLACE FUNCTION sync_metadata_cache_for_scheme(target_scheme_id bigint)
RETURNS void AS $$
BEGIN
    -- Remove old scheme data
    DELETE FROM _scheme_metadata_cache
    WHERE _scheme_id = target_scheme_id;

    -- Insert current data (with support for collection types and scheme type)
    INSERT INTO _scheme_metadata_cache (
        _scheme_id, _structure_id, _parent_structure_id, _id_override,
        _name, _alias,
        _type_id, _list_id, type_name, db_type, type_semantic,
        _scheme_type, scheme_type_name,
        _order, _collection_type, collection_type_name, _key_type, key_type_name,
        _readonly, _allow_not_null, _is_compress, _store_null,
        _unique, _unique_version, _unique_scope, _lazy, _tags,
        _default_value, _default_editor
    )
    SELECT
        s._id_scheme,
        s._id,
        s._id_parent,
        s._id_override,
        s._name,
        s._alias,
        t._id,
        s._id_list,
        t._name,
        t._db_type,
        t._type,
        sch._type,                    -- Scheme type
        scht._name,                   -- Scheme type name
        s._order,
        s._collection_type,           -- Collection type (Array/Dictionary/NULL)
        ct._name,                     -- Collection type name
        s._key_type,                  -- Key type for Dictionary
        kt._name,                     -- Key type name
        s._readonly,
        s._allow_not_null,
        s._is_compress,
        s._store_null,
        s._unique,                    -- V4: unique key flag ([RedbUnique])
        s._unique_version,            -- V4: encoder version of the stored keys
        s._unique_scope,              -- S3: element-key scope of a collection key
        s._lazy,                      -- V4 (LAZY Л2): lazy reference marker
        s._tags,                      -- V4: free-form marker
        s._default_value,
        s._default_editor
    FROM _structures s
    JOIN _types t ON t._id = s._id_type
    JOIN _schemes sch ON sch._id = s._id_scheme
    LEFT JOIN _types scht ON scht._id = sch._type         -- Scheme type
    LEFT JOIN _types ct ON ct._id = s._collection_type    -- Collection type
    LEFT JOIN _types kt ON kt._id = s._key_type           -- Key type
    WHERE s._id_scheme = target_scheme_id;

    -- NOTICE removed to avoid spam during mass warmup
    -- Use warmup_all_metadata_caches() to get statistics
END;
$$ LANGUAGE plpgsql;

-- Warmup function (for application startup or after crash)
CREATE OR REPLACE FUNCTION warmup_all_metadata_caches()
RETURNS TABLE(scheme_id bigint, structures_count bigint, scheme_name text, structure_hash uuid) AS $$
BEGIN
    TRUNCATE _scheme_metadata_cache;

    -- Rebuild cache for ALL schemes (removed filter for _structure_hash)
    PERFORM sync_metadata_cache_for_scheme(s._id)
    FROM _schemes s;

    -- Return statistics for ALL schemes
    RETURN QUERY
    SELECT
        s._id as scheme_id,
        COUNT(c._structure_id) as structures_count,
        s._name::text as scheme_name,
        s._structure_hash as structure_hash
    FROM _schemes s
    LEFT JOIN _scheme_metadata_cache c ON c._scheme_id = s._id
    GROUP BY s._id, s._name, s._structure_hash
    ORDER BY s._id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION warmup_all_metadata_caches() IS
'Warms up metadata cache for ALL schemes (including schemes without _structure_hash).
Recommended to call:
  1. On application startup
  2. After PostgreSQL crash (UNLOGGED TABLE is cleared)
  3. After schema migrations

Returns statistics: scheme_id → number of structures for all schemes.

Usage:
  SELECT * FROM warmup_all_metadata_caches();

UPDATED: Now warms up ALL schemes, not only those with _structure_hash IS NOT NULL.
This eliminates auto-filling of cache on every query to v_objects_json.
';

-- =====================================================
-- Repair pass moved here from 28_migrate_dateonly_db_type.sql (BR-5): the cache copies _db_type,
-- so every scheme with a DateOnly field is resynced after the retype in 28. It must run AFTER the
-- function above exists - in 28 a fresh database planned the call before this file had created it
-- and the whole init died with 42883; here it holds for any history, dump-restored databases
-- included. Idempotent and cheap: only schemes that actually have a DateOnly structure.
-- =====================================================
SELECT sync_metadata_cache_for_scheme(s._id)
  FROM _schemes s
 WHERE EXISTS (SELECT 1 FROM _structures st
                WHERE st._id_scheme = s._id
                  AND st._id_type = -9223372036854775686);
