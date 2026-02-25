-- ============================================================
-- METADATA CACHE: Solution for repeated JOIN problem
-- ============================================================
-- Goal: Avoid repeated JOIN _structures ← _types in each query
-- Approach: UNLOGGED TABLE + automatic synchronization via triggers on _structure_hash
-- Advantages:
--   ✅ No recursion problems (indexes created once)
--   ✅ Works with connection pooling (global table)
--   ✅ Automatic invalidation (triggers on _schemes._structure_hash)
--   ✅ No changes in C# code required
--   ✅ Minimal cache rebuilds (only on actual schema changes)
-- ============================================================

-- 1️⃣ Create metadata cache (ALL fields from _structures + fields from _types)
DROP TABLE IF EXISTS _scheme_metadata_cache CASCADE;

CREATE TABLE _scheme_metadata_cache ( --<-- UNLOGGED TABLE IF NEEDED
    -- Identifiers
    _scheme_id bigint NOT NULL,
    _structure_id bigint NOT NULL,
    _parent_structure_id bigint,
    _id_override bigint,
    
    -- Names and aliases
    _name text NOT NULL,
    _alias text,
    
    -- Structure type
    _type_id bigint NOT NULL,
    _list_id bigint,
    type_name text NOT NULL,
    db_type text NOT NULL,
    type_semantic text NOT NULL,
    
    -- Scheme type (Class/Array/Dictionary/JsonDocument/XDocument)
    _scheme_type bigint,
    scheme_type_name text,
    
    -- Structure attributes
    _order bigint,
    _collection_type bigint,      -- NULL = not a collection, otherwise collection type ID (Array/Dictionary)
    collection_type_name text,    -- Collection type name
    _key_type bigint,             -- Key type for Dictionary
    key_type_name text,           -- Key type name
    _readonly boolean,
    _allow_not_null boolean,
    _is_compress boolean,
    _store_null boolean,
    
    -- Default values
    _default_value bytea,
    _default_editor text
);

-- 2️⃣ Indexes for fast search
CREATE INDEX idx_metadata_cache_lookup 
    ON _scheme_metadata_cache(_scheme_id, _parent_structure_id, _order);

CREATE INDEX idx_metadata_cache_structure 
    ON _scheme_metadata_cache(_structure_id);

CREATE INDEX idx_metadata_cache_scheme
    ON _scheme_metadata_cache(_scheme_id);

CREATE INDEX idx_metadata_cache_name
    ON _scheme_metadata_cache(_scheme_id, _name);

CREATE INDEX idx_metadata_cache_collection
    ON _scheme_metadata_cache(_scheme_id, _collection_type)
    WHERE _collection_type IS NOT NULL;

CREATE INDEX idx_metadata_cache_scheme_type
    ON _scheme_metadata_cache(_scheme_id, _scheme_type);

CREATE INDEX idx_metadata_cache_key_type
    ON _scheme_metadata_cache(_scheme_id, _key_type)
    WHERE _key_type IS NOT NULL;

-- 3️⃣ Cache synchronization function for a scheme
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

-- 4️⃣ Trigger on _structure_hash change in _schemes
-- 🔥 KEY IDEA: Track ONLY hash changes, not every INSERT/UPDATE in _structures!
CREATE OR REPLACE FUNCTION sync_metadata_cache_on_hash_change()
RETURNS TRIGGER AS $$
BEGIN
    -- Check if _structure_hash changed
    IF (TG_OP = 'UPDATE' AND OLD._structure_hash IS DISTINCT FROM NEW._structure_hash) THEN
        -- Hash changed → rebuild cache for this scheme
        PERFORM sync_metadata_cache_for_scheme(NEW._id);
        RAISE NOTICE 'Metadata cache rebuilt for scheme_id=% due to structure_hash change (old=%, new=%)', 
            NEW._id, OLD._structure_hash, NEW._structure_hash;
        
    ELSIF (TG_OP = 'INSERT' AND NEW._structure_hash IS NOT NULL) THEN
        -- New scheme with hash → create cache
        PERFORM sync_metadata_cache_for_scheme(NEW._id);
        RAISE NOTICE 'Metadata cache created for new scheme_id=% (hash=%)', 
            NEW._id, NEW._structure_hash;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_sync_metadata_cache_on_hash_change ON _schemes;
CREATE TRIGGER trg_sync_metadata_cache_on_hash_change
AFTER INSERT OR UPDATE ON _schemes
FOR EACH ROW EXECUTE FUNCTION sync_metadata_cache_on_hash_change();

-- 5️⃣ Trigger on DELETE of scheme
CREATE OR REPLACE FUNCTION cleanup_metadata_cache_on_scheme_delete()
RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM _scheme_metadata_cache WHERE _scheme_id = OLD._id;
    RAISE NOTICE 'Metadata cache cleared for deleted scheme_id=%', OLD._id;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_cleanup_metadata_cache_on_scheme_delete ON _schemes;
CREATE TRIGGER trg_cleanup_metadata_cache_on_scheme_delete
AFTER DELETE ON _schemes
FOR EACH ROW EXECUTE FUNCTION cleanup_metadata_cache_on_scheme_delete();

-- 6️⃣ Trigger on type changes (invalidates entire cache)
-- 🔥 When _types change → all schemes must rebuild
CREATE OR REPLACE FUNCTION invalidate_all_metadata_caches()
RETURNS TRIGGER AS $$
BEGIN
    TRUNCATE _scheme_metadata_cache;
    RAISE NOTICE 'All metadata caches invalidated due to _types change';
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_invalidate_all_caches ON _types;
CREATE TRIGGER trg_invalidate_all_caches
AFTER UPDATE OR DELETE ON _types
FOR EACH STATEMENT EXECUTE FUNCTION invalidate_all_metadata_caches();

-- 7️⃣ Warmup function (for application startup or after crash)
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

-- 8️⃣ Cache consistency check function
CREATE OR REPLACE FUNCTION check_metadata_cache_consistency()
RETURNS TABLE(
    scheme_id bigint, 
    scheme_name text,
    cached_count bigint, 
    actual_count bigint, 
    is_consistent boolean
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        s._id as scheme_id,
        s._name::text as scheme_name,
        COALESCE(cache_stats.cached_count, 0) as cached_count,
        COALESCE(actual_stats.actual_count, 0) as actual_count,
        (COALESCE(cache_stats.cached_count, 0) = COALESCE(actual_stats.actual_count, 0)) as is_consistent
    FROM _schemes s
    LEFT JOIN (
        SELECT _scheme_id, COUNT(*) as cached_count
        FROM _scheme_metadata_cache
        GROUP BY _scheme_id
    ) cache_stats ON cache_stats._scheme_id = s._id
    LEFT JOIN (
        SELECT _id_scheme, COUNT(*) as actual_count
        FROM _structures
        GROUP BY _id_scheme
    ) actual_stats ON actual_stats._id_scheme = s._id
    ORDER BY s._id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION check_metadata_cache_consistency() IS 
'Checks metadata cache consistency.
Returns discrepancies between cache and actual data.

Usage:
  SELECT * FROM check_metadata_cache_consistency() WHERE NOT is_consistent;
';

COMMENT ON TABLE _scheme_metadata_cache IS 
'Metadata cache for schemes to optimize queries.
Automatically synchronized via triggers on _schemes._structure_hash.
Note: Use UNLOGGED TABLE for better performance if replication is not needed (cleared after crash).

Invalidation strategy:
  ✅ _schemes._structure_hash changed → rebuild cache for scheme (trigger)
  ✅ _schemes deleted → delete scheme cache (trigger)
  ✅ _types changed → clear entire cache (TRUNCATE trigger)
  
Cache warmup:
  ✅ warmup_all_metadata_caches() warms up ALL schemes (including those without _structure_hash)
  ✅ Auto-filling in functions (get_object_json, get_facets) as fallback
';

-- ============================================================
-- READY! Now use in queries:
-- 
-- INSTEAD OF:
--   FROM _structures s 
--   JOIN _types t ON t._id = s._id_type
--   WHERE s._id_scheme = object_scheme_id
--
-- USE:
--   FROM _scheme_metadata_cache c
--   WHERE c._scheme_id = object_scheme_id
-- 
-- AVAILABLE FIELDS:
--   _scheme_id, _structure_id, _parent_structure_id, _id_override
--   _name, _alias
--   _type_id, _list_id, type_name, db_type, type_semantic
--   _scheme_type, scheme_type_name (scheme type: Class/Array/Dictionary/JsonDocument/XDocument)
--   _order, _collection_type, collection_type_name (collection type: Array/Dictionary/NULL)
--   _key_type, key_type_name (key type for Dictionary)
--   _readonly, _allow_not_null, _is_compress, _store_null
--   _default_value, _default_editor
--
-- COLLECTION CHECK (instead of _is_array):
--   _collection_type IS NOT NULL = this is a collection (array or dictionary)
--   _collection_type IS NULL = not a collection
-- ============================================================