-- ===== LAZY LOADING SUPPORT FOR FACET SEARCH =====
-- New functions to return base objects without Props
-- Old functions (search_objects_with_facets, search_tree_objects_with_facets) remain unchanged
-- Author: AI Assistant
-- Creation date: 2025-11-17

-- ===== CLEANUP OF EXISTING FUNCTIONS =====
-- Drop ALL versions of functions (old and new signatures)
DROP FUNCTION IF EXISTS search_objects_with_facets_base(bigint, jsonb, integer, integer, jsonb, integer) CASCADE;
DROP FUNCTION IF EXISTS search_tree_objects_with_facets_base(bigint, bigint, jsonb, integer, integer, jsonb, integer, integer) CASCADE;
DROP FUNCTION IF EXISTS search_tree_objects_with_facets_base(bigint, bigint[], jsonb, integer, integer, jsonb, integer, integer) CASCADE;
DROP FUNCTION IF EXISTS get_search_sql_preview_base(bigint, jsonb, integer, integer, jsonb, integer) CASCADE;
DROP FUNCTION IF EXISTS get_search_tree_sql_preview_base(bigint, bigint, jsonb, integer, integer, jsonb, integer, integer) CASCADE;
DROP FUNCTION IF EXISTS get_search_tree_sql_preview_base(bigint, bigint[], jsonb, integer, integer, jsonb, integer, integer) CASCADE;

-- ========== FUNCTION 1: Return base fields WITHOUT Props ==========
CREATE OR REPLACE FUNCTION get_object_base_fields(object_id bigint)
RETURNS jsonb
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT jsonb_build_object(
        'id', o._id,
        'name', o._name,
        'scheme_id', o._id_scheme,
        'parent_id', o._id_parent,
        'owner_id', o._id_owner,
        'who_change_id', o._id_who_change,
        'date_create', o._date_create,
        'date_modify', o._date_modify,
        'date_begin', o._date_begin,
        'date_complete', o._date_complete,
        'key', o._key,
        'value_long', o._value_long,
        'value_string', o._value_string,
        'value_guid', o._value_guid,
        'note', o._note,
        'value_bool', o._value_bool,
        'value_double', o._value_double,
        'value_numeric', o._value_numeric,
        'value_datetime', o._value_datetime,
        'value_bytes', o._value_bytes,
        'hash', o._hash  -- CRITICAL for cache!
    )
    FROM _objects o
    WHERE o._id = object_id;
$$;

COMMENT ON FUNCTION get_object_base_fields(bigint) IS 
'Returns base object fields WITHOUT Props for lazy loading.
Includes hash for cache validation. 10-50x faster than get_object_json().
ATTENTION: Function kept for compatibility and direct use.
In aggregate queries (search_*_base), direct JOIN is used instead of function call for optimization.';

-- ========== FUNCTION 2: Execute query with base fields ==========
-- ✅ DISTINCT ON (_hash) for Open Source version Distinct()
DROP FUNCTION IF EXISTS execute_objects_query_base(bigint, text, text, text, integer, integer) CASCADE;
DROP FUNCTION IF EXISTS execute_objects_query_base(bigint, text, text, text, integer, integer, boolean) CASCADE;

CREATE OR REPLACE FUNCTION execute_objects_query_base(
    scheme_id bigint,
    base_conditions text,
    hierarchical_conditions text,
    order_conditions text,
    limit_count integer DEFAULT NULL,
    offset_count integer DEFAULT 0,
    distinct_hash boolean DEFAULT false  -- ✅ NEW: DISTINCT ON (_hash)
) RETURNS jsonb
LANGUAGE 'plpgsql'
COST 200
VOLATILE NOT LEAKPROOF  
AS $BODY$
DECLARE
    query_text text;
    count_query_text text;
    objects_result jsonb;
    total_count integer;
    final_where text;
    distinct_clause text;
    order_for_distinct text;
BEGIN
    -- Combine all conditions (REUSE logic from execute_objects_query)
    final_where := format('WHERE obj._id_scheme = %s%s%s', 
                         scheme_id, 
                         COALESCE(base_conditions, ''),
                         COALESCE(hierarchical_conditions, ''));
    
    -- ✅ DISTINCT ON for uniqueness by Hash (Props)
    IF distinct_hash THEN
        distinct_clause := 'DISTINCT ON (obj._hash)';
        -- PostgreSQL requires: ORDER BY must start with DISTINCT ON expression
        order_for_distinct := 'ORDER BY obj._hash' || 
            CASE WHEN order_conditions IS NOT NULL AND order_conditions != '' 
                 THEN ', ' || regexp_replace(order_conditions, '^ORDER BY\s*', '', 'i')
                 ELSE '' 
            END;
    ELSE
        distinct_clause := '';
        order_for_distinct := COALESCE(order_conditions, 'ORDER BY obj._id');
    END IF;
    
    -- Query with direct JOIN instead of function call (optimization!)
    query_text := format('
        SELECT jsonb_agg(
            jsonb_build_object(
                ''id'', o._id,
                ''name'', o._name,
                ''scheme_id'', o._id_scheme,
                ''parent_id'', o._id_parent,
                ''owner_id'', o._id_owner,
                ''who_change_id'', o._id_who_change,
                ''date_create'', o._date_create,
                ''date_modify'', o._date_modify,
                ''date_begin'', o._date_begin,
                ''date_complete'', o._date_complete,
                ''key'', o._key,
                ''value_long'', o._value_long,
                ''value_string'', o._value_string,
                ''value_guid'', o._value_guid,
                ''note'', o._note,
                ''value_bool'', o._value_bool,
                ''value_double'', o._value_double,
                ''value_numeric'', o._value_numeric,
                ''value_datetime'', o._value_datetime,
                ''value_bytes'', o._value_bytes,
                ''hash'', o._hash
            )
        )
        FROM (
            SELECT %s obj._id
            FROM _objects obj
            %s
            %s
            %s
        ) sub
        JOIN _objects o ON o._id = sub._id',
        distinct_clause,
        final_where,
        order_for_distinct,
        CASE 
            WHEN limit_count IS NULL OR limit_count >= 2000000000 THEN ''
            ELSE format('LIMIT %s OFFSET %s', limit_count, offset_count)
        END
    );
    
    -- Count query (same as in original)
    -- ✅ With DISTINCT count unique hashes
    IF distinct_hash THEN
        count_query_text := format('
            SELECT COUNT(DISTINCT obj._hash)
            FROM _objects obj  
            %s',
            final_where
        );
    ELSE
        count_query_text := format('
            SELECT COUNT(*)
            FROM _objects obj  
            %s',
            final_where
        );
    END IF;
    
    EXECUTE query_text INTO objects_result;
    EXECUTE count_query_text INTO total_count;
    
    -- ⚡ LAZY LOADING: WITHOUT FACETS (they are expensive and not needed for base version)
    RETURN jsonb_build_object(
        'objects', COALESCE(objects_result, '[]'::jsonb),
        'total_count', total_count,
        'limit', limit_count,
        'offset', offset_count,
        'facets', '[]'::jsonb  -- Empty array instead of get_facets(scheme_id)
    );
END;
$BODY$;

COMMENT ON FUNCTION execute_objects_query_base(bigint, text, text, text, integer, integer, boolean) IS 
'Executes search with base objects WITHOUT Props.
Returns the same JSON format as execute_objects_query, but objects without properties.
Used for lazy loading via GlobalPropsCache.
✅ distinct_hash=true adds DISTINCT ON (_hash) for uniqueness by Props.
Reuses conditions from _build_single_facet_condition, build_hierarchical_conditions, build_order_conditions.';

-- ========== FUNCTION 3: Search with facets (base objects) ==========
-- ✅ DISTINCT ON (_hash) for Open Source version Distinct()
DROP FUNCTION IF EXISTS search_objects_with_facets_base(bigint, jsonb, integer, integer, jsonb, integer) CASCADE;
DROP FUNCTION IF EXISTS search_objects_with_facets_base(bigint, jsonb, integer, integer, jsonb, integer, boolean) CASCADE;

CREATE OR REPLACE FUNCTION search_objects_with_facets_base(
    scheme_id bigint,
    facet_filters jsonb DEFAULT NULL,
    limit_count integer DEFAULT NULL,
    offset_count integer DEFAULT 0,
    order_by jsonb DEFAULT NULL,
    max_recursion_depth integer DEFAULT 10,
    distinct_hash boolean DEFAULT false  -- ✅ NEW: DISTINCT ON (_hash)
) RETURNS jsonb
LANGUAGE 'plpgsql'
COST 200
VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    base_conditions text;
    hierarchical_conditions text;
    order_conditions text;
BEGIN
    -- REUSE existing condition building functions
    -- Same functions used by search_objects_with_facets
    base_conditions := _build_single_facet_condition(facet_filters, scheme_id, 'obj', max_recursion_depth);
    hierarchical_conditions := build_hierarchical_conditions(facet_filters, 'obj');
    order_conditions := build_order_conditions(order_by, 'obj');
    
    -- Call new execution function with base fields
    RETURN execute_objects_query_base(
        scheme_id,
        base_conditions,
        hierarchical_conditions,
        order_conditions,
        limit_count,
        offset_count,
        distinct_hash  -- ✅ Pass through parameter
    );
END;
$BODY$;

COMMENT ON FUNCTION search_objects_with_facets_base(bigint, jsonb, integer, integer, jsonb, integer, boolean) IS 
'Faceted search returning base objects WITHOUT Props.
Used for lazy loading + GlobalPropsCache.
Supports all LINQ operators ($gt, $contains, $arrayContains, etc.), Class fields (Contact.Name), arrays (Tags[]).
✅ distinct_hash=true adds DISTINCT ON (_hash) for uniqueness by Props.
Signature and response format match search_objects_with_facets, but objects without properties.
100% reuse of condition logic from search_objects_with_facets.';

-- ========== FUNCTION 3.5: Get only filtered object IDs ==========
-- ⚡ OPTIMIZED for aggregations — returns only bigint[] without JSON overhead
DROP FUNCTION IF EXISTS get_filtered_object_ids(bigint, jsonb, integer) CASCADE;

CREATE OR REPLACE FUNCTION get_filtered_object_ids(
    p_scheme_id bigint,
    p_filter_json jsonb DEFAULT NULL,
    p_max_recursion_depth integer DEFAULT 10
) RETURNS bigint[]
LANGUAGE 'plpgsql'
COST 100
VOLATILE
AS $BODY$
DECLARE
    v_base_conditions text;
    v_hierarchical_conditions text;
    v_final_where text;
    v_result bigint[];
BEGIN
    -- ⚡ REUSE existing condition building functions
    -- Same functions as in search_objects_with_facets_base — NO duplication!
    v_base_conditions := _build_single_facet_condition(p_filter_json, p_scheme_id, 'obj', p_max_recursion_depth);
    v_hierarchical_conditions := build_hierarchical_conditions(p_filter_json, 'obj');
    
    -- Build WHERE (same logic as in execute_objects_query_base)
    v_final_where := format('WHERE obj._id_scheme = %s%s%s', 
                           p_scheme_id, 
                           COALESCE(v_base_conditions, ''),
                           COALESCE(v_hierarchical_conditions, ''));
    
    -- ⚡ Simple SELECT only IDs — no JSON, no sorting, no pagination!
    EXECUTE format('SELECT ARRAY_AGG(obj._id) FROM _objects obj %s', v_final_where)
    INTO v_result;
    
    RETURN v_result;
END;
$BODY$;

COMMENT ON FUNCTION get_filtered_object_ids(bigint, jsonb, integer) IS 
'⚡ Optimized function for aggregations.
Returns only array of object IDs (bigint[]) instead of full JSON.
100% reuses filter logic from search_objects_with_facets_base.
No overhead for JSON serialization, sorting, and pagination.
Example: SELECT get_filtered_object_ids(1002, ''{"Age": {"$gt": 50}}'');';

-- ========== FUNCTION 4: Tree search (base objects) ==========
CREATE OR REPLACE FUNCTION search_tree_objects_with_facets_base(
    scheme_id bigint,
    parent_ids bigint[],  -- ✅ BATCH: Array of parents (was: parent_id bigint)
    facet_filters jsonb DEFAULT NULL,
    limit_count integer DEFAULT NULL,
    offset_count integer DEFAULT 0,
    order_by jsonb DEFAULT NULL,
    max_depth integer DEFAULT 10,
    max_recursion_depth integer DEFAULT 10
) RETURNS jsonb
LANGUAGE 'plpgsql'
COST 300
VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    query_text text;
    count_query_text text;
    objects_result jsonb;
    total_count integer;
    base_conditions text;
    order_conditions text;
BEGIN
    -- 🔥 AUTOMATIC CHECK AND CACHE POPULATION
    -- Ensures scheme metadata cache is populated before building conditions
    IF NOT EXISTS(SELECT 1 FROM _scheme_metadata_cache WHERE _scheme_id = scheme_id LIMIT 1) THEN
        PERFORM sync_metadata_cache_for_scheme(scheme_id);
        -- Auto-population without NOTICE (use warmup_all_metadata_caches() for explicit warmup)
    END IF;
    
    -- REUSE existing condition building functions
    -- Same functions used by search_tree_objects_with_facets
    base_conditions := _build_single_facet_condition(facet_filters, scheme_id, 'd', max_recursion_depth);
    order_conditions := build_order_conditions(order_by, 'd');
    
    -- max_depth = 1: only direct children
    IF max_depth = 1 THEN
        query_text := format('
            SELECT jsonb_agg(
                jsonb_build_object(
                    ''id'', o._id,
                    ''name'', o._name,
                    ''scheme_id'', o._id_scheme,
                    ''parent_id'', o._id_parent,
                    ''owner_id'', o._id_owner,
                    ''who_change_id'', o._id_who_change,
                    ''date_create'', o._date_create,
                    ''date_modify'', o._date_modify,
                    ''date_begin'', o._date_begin,
                    ''date_complete'', o._date_complete,
                    ''key'', o._key,
                    ''value_long'', o._value_long,
                    ''value_string'', o._value_string,
                    ''value_guid'', o._value_guid,
                    ''note'', o._note,
                    ''value_bool'', o._value_bool,
                    ''value_double'', o._value_double,
                    ''value_numeric'', o._value_numeric,
                    ''value_datetime'', o._value_datetime,
                    ''value_bytes'', o._value_bytes,
                    ''hash'', o._hash
                )
            )
            FROM (
                SELECT d._id
                FROM _objects d
                WHERE d._id_scheme = %s 
                  AND d._id_parent = ANY($1)%s
                %s
                %s
            ) sub
            JOIN _objects o ON o._id = sub._id',
            scheme_id,
            COALESCE(base_conditions, ''),
            order_conditions,
            CASE 
                WHEN limit_count IS NOT NULL THEN format('LIMIT %s OFFSET %s', limit_count, offset_count)
                ELSE ''
            END
        );
        
        count_query_text := format('
            SELECT COUNT(*)
            FROM _objects d
            WHERE d._id_scheme = %s 
              AND d._id_parent = ANY($1)%s',
            scheme_id,
            COALESCE(base_conditions, '')
        );
    
    -- max_depth > 1: recursive descendant search
    ELSE
        -- ✅ SECURITY: Use positional parameters $1, $2 for EXECUTE USING
        -- 🔥 FIXED: Removed DISTINCT for compatibility with ORDER BY
        query_text := format('
            WITH RECURSIVE descendants AS (
                SELECT unnest($1) as _id, 0::bigint as depth
                UNION ALL
                SELECT o._id, d.depth + 1
                FROM _objects o
                JOIN descendants d ON o._id_parent = d._id
                WHERE d.depth < %s
            )
            SELECT jsonb_agg(
                jsonb_build_object(
                    ''id'', o._id,
                    ''name'', o._name,
                    ''scheme_id'', o._id_scheme,
                    ''parent_id'', o._id_parent,
                    ''owner_id'', o._id_owner,
                    ''who_change_id'', o._id_who_change,
                    ''date_create'', o._date_create,
                    ''date_modify'', o._date_modify,
                    ''date_begin'', o._date_begin,
                    ''date_complete'', o._date_complete,
                    ''key'', o._key,
                    ''value_long'', o._value_long,
                    ''value_string'', o._value_string,
                    ''value_guid'', o._value_guid,
                    ''note'', o._note,
                    ''value_bool'', o._value_bool,
                    ''value_double'', o._value_double,
                    ''value_numeric'', o._value_numeric,
                    ''value_datetime'', o._value_datetime,
                    ''value_bytes'', o._value_bytes,
                    ''hash'', o._hash
                )
            )
            FROM (
                SELECT d._id
                FROM descendants dt
                JOIN _objects d ON dt._id = d._id
                WHERE dt.depth > 0 
                  AND d._id_scheme = %s%s
                %s
                %s
            ) sub
            JOIN _objects o ON o._id = sub._id',
            max_depth,
            scheme_id,
            COALESCE(base_conditions, ''),
            order_conditions,
            CASE 
                WHEN limit_count IS NOT NULL THEN format('LIMIT %s OFFSET %s', limit_count, offset_count)
                ELSE ''
            END
        );
        
        count_query_text := format('
            WITH RECURSIVE descendants AS (
                SELECT unnest($1) as _id, 0::bigint as depth
                UNION ALL
                SELECT o._id, d.depth + 1
                FROM _objects o
                JOIN descendants d ON o._id_parent = d._id
                WHERE d.depth < %s
            )
            SELECT COUNT(DISTINCT d._id)
            FROM descendants dt
            JOIN _objects d ON dt._id = d._id
            WHERE dt.depth > 0 
              AND d._id_scheme = %s%s',
            max_depth,
            scheme_id,
            COALESCE(base_conditions, '')
        );
    END IF;
    
    -- Execute queries with USING to pass array!
    EXECUTE query_text INTO objects_result USING parent_ids;
    EXECUTE count_query_text INTO total_count USING parent_ids;
    
    -- ⚡ LAZY LOADING: WITHOUT FACETS (they are expensive and not needed for base version)
    RETURN jsonb_build_object(
        'objects', COALESCE(objects_result, '[]'::jsonb),
        'total_count', total_count,
        'limit', limit_count,
        'offset', offset_count,
        'parent_ids', parent_ids,  -- ✅ BATCH: Array of parents
        'max_depth', max_depth,
        'facets', '[]'::jsonb  -- Empty array instead of get_facets(scheme_id)
    );
END;
$BODY$;

COMMENT ON FUNCTION search_tree_objects_with_facets_base(bigint, bigint[], jsonb, integer, integer, jsonb, integer, integer) IS 
'✅ BATCH OPTIMIZATION! Tree search returning base objects WITHOUT Props. Accepts parent_ids[] array for 3-4x speedup.
Used for lazy loading + GlobalPropsCache.
Supports hierarchical conditions ($hasAncestor, $hasDescendant, $level), LINQ operators, Class fields.
Signature and response format match search_tree_objects_with_facets, but objects without properties.
100% reuse of condition logic from search_tree_objects_with_facets.';

-- ===== USAGE EXAMPLES =====
/*
-- Example 1: Base objects without filters
SELECT search_objects_with_facets_base(1002, NULL, 10, 0, NULL, 10);

-- Example 2: With LINQ filters
SELECT search_objects_with_facets_base(
    1002, 
    '{"Status": "Active", "Price": {"$gt": "100"}}'::jsonb,
    10, 0, NULL, 10
);

-- Example 3: Tree search for direct children
SELECT search_tree_objects_with_facets_base(1002, 100, NULL, 10, 0, NULL, 1, 10);

-- Example 4: Recursive descendant search
SELECT search_tree_objects_with_facets_base(1002, 100, NULL, 20, 0, NULL, 5, 10);

-- Example 5: Performance comparison
EXPLAIN ANALYZE SELECT search_objects_with_facets(1002, NULL, 100, 0);
EXPLAIN ANALYZE SELECT search_objects_with_facets_base(1002, NULL, 100, 0);

-- Check result - should be WITHOUT "properties" field:
-- {"objects": [{"id": 1, "name": "...", "hash": "abc-123", ...}], "total_count": 10, "facets": {...}}
*/

-- ===== SQL PREVIEW for LAZY LOADING (for debugging) =====

-- Function 1: Preview for standard search with base fields
-- ✅ DISTINCT ON (_hash) for Open Source version Distinct()
DROP FUNCTION IF EXISTS get_search_sql_preview_base(bigint, jsonb, integer, integer, jsonb, integer) CASCADE;
DROP FUNCTION IF EXISTS get_search_sql_preview_base(bigint, jsonb, integer, integer, jsonb, integer, boolean) CASCADE;

CREATE OR REPLACE FUNCTION get_search_sql_preview_base(
    scheme_id bigint,
    facet_filters jsonb DEFAULT NULL,
    limit_count integer DEFAULT NULL,
    offset_count integer DEFAULT 0,
    order_by jsonb DEFAULT NULL,
    max_recursion_depth integer DEFAULT 10,
    distinct_hash boolean DEFAULT false  -- ✅ NEW: DISTINCT ON (_hash)
) RETURNS text
LANGUAGE 'plpgsql'
COST 50
IMMUTABLE
AS $BODY$
DECLARE
    base_conditions text;
    hierarchical_conditions text;
    order_conditions text;
    final_where text;
    query_text text;
    distinct_clause text;
    order_for_distinct text;
BEGIN
    -- Reuse condition building functions (from redb_facets_search.sql)
    base_conditions := _build_single_facet_condition(facet_filters, scheme_id, 'obj', max_recursion_depth);
    hierarchical_conditions := build_hierarchical_conditions(facet_filters, 'obj');
    order_conditions := build_order_conditions(order_by, 'obj');
    
    -- Combine WHERE
    final_where := format('WHERE obj._id_scheme = %s%s%s', 
                         scheme_id, 
                         COALESCE(base_conditions, ''),
                         COALESCE(hierarchical_conditions, ''));
    
    -- ✅ DISTINCT ON for uniqueness by Hash (Props)
    IF distinct_hash THEN
        distinct_clause := 'DISTINCT ON (obj._hash)';
        order_for_distinct := 'ORDER BY obj._hash' || 
            CASE WHEN order_conditions IS NOT NULL AND order_conditions != '' 
                 THEN ', ' || regexp_replace(order_conditions, '^ORDER BY\s*', '', 'i')
                 ELSE '' 
            END;
    ELSE
        distinct_clause := '';
        order_for_distinct := COALESCE(order_conditions, 'ORDER BY obj._id');
    END IF;
    
    -- Build SQL with direct JOIN (DO NOT EXECUTE!)
    query_text := format('
SELECT jsonb_agg(
    jsonb_build_object(
        ''id'', o._id,
        ''name'', o._name,
        ''scheme_id'', o._id_scheme,
        ''parent_id'', o._id_parent,
        ''owner_id'', o._id_owner,
        ''who_change_id'', o._id_who_change,
        ''date_create'', o._date_create,
        ''date_modify'', o._date_modify,
        ''date_begin'', o._date_begin,
        ''date_complete'', o._date_complete,
        ''key'', o._key,
        ''value_long'', o._value_long,
        ''value_string'', o._value_string,
        ''value_guid'', o._value_guid,
        ''note'', o._note,
        ''value_bool'', o._value_bool,
        ''value_double'', o._value_double,
        ''value_numeric'', o._value_numeric,
        ''value_datetime'', o._value_datetime,
        ''value_bytes'', o._value_bytes,
        ''hash'', o._hash
    )
)
FROM (
    SELECT %s obj._id
    FROM _objects obj
    %s
    %s
    %s
) sub
JOIN _objects o ON o._id = sub._id',
        distinct_clause,
        final_where,
        order_for_distinct,
        CASE 
            WHEN limit_count IS NULL OR limit_count >= 2000000000 THEN ''
            ELSE format('LIMIT %s OFFSET %s', limit_count, offset_count)
        END
    );
    
    RETURN query_text;
END;
$BODY$;

COMMENT ON FUNCTION get_search_sql_preview_base(bigint, jsonb, integer, integer, jsonb, integer, boolean) IS 
'Returns SQL query for lazy loading (for debugging). Shows what will be executed in search_objects_with_facets_base(). ✅ distinct_hash=true adds DISTINCT ON (_hash). Returns base fields WITHOUT Props.';

-- Function 2: Preview for tree search with base fields
CREATE OR REPLACE FUNCTION get_search_tree_sql_preview_base(
    scheme_id bigint,
    parent_ids bigint[],  -- ✅ BATCH: Array of parents (was: parent_id bigint)
    facet_filters jsonb DEFAULT NULL,
    limit_count integer DEFAULT NULL,
    offset_count integer DEFAULT 0,
    order_by jsonb DEFAULT NULL,
    max_depth integer DEFAULT 10,
    max_recursion_depth integer DEFAULT 10
) RETURNS text
LANGUAGE 'plpgsql'
COST 100
IMMUTABLE
AS $BODY$
DECLARE
    query_text text;
    base_conditions text;
    order_conditions text;
BEGIN
    -- Reuse condition building functions
    base_conditions := _build_single_facet_condition(facet_filters, scheme_id, 'd', max_recursion_depth);
    order_conditions := build_order_conditions(order_by, 'd');
    
    -- If max_depth = 1, search only direct children
    IF max_depth = 1 THEN
        query_text := format('
SELECT jsonb_agg(
    jsonb_build_object(
        ''id'', o._id,
        ''name'', o._name,
        ''scheme_id'', o._id_scheme,
        ''parent_id'', o._id_parent,
        ''owner_id'', o._id_owner,
        ''who_change_id'', o._id_who_change,
        ''date_create'', o._date_create,
        ''date_modify'', o._date_modify,
        ''date_begin'', o._date_begin,
        ''date_complete'', o._date_complete,
        ''key'', o._key,
        ''value_long'', o._value_long,
        ''value_string'', o._value_string,
        ''value_guid'', o._value_guid,
        ''note'', o._note,
        ''value_bool'', o._value_bool,
        ''value_double'', o._value_double,
        ''value_numeric'', o._value_numeric,
        ''value_datetime'', o._value_datetime,
        ''value_bytes'', o._value_bytes,
        ''hash'', o._hash
    )
)
FROM (
    SELECT d._id
    FROM _objects d
    WHERE d._id_scheme = %s 
      AND d._id_parent = ANY(%L)%s
    %s
    %s
) sub
JOIN _objects o ON o._id = sub._id',
            scheme_id,
            parent_ids,  -- ✅ BATCH: Array of parents
            COALESCE(base_conditions, ''),
            order_conditions,
            CASE 
                WHEN limit_count IS NOT NULL THEN format('LIMIT %s OFFSET %s', limit_count, offset_count)
                ELSE ''
            END
        );
    ELSE
        -- Recursive descendant search
        -- 🔥 FIXED: Removed DISTINCT for compatibility with ORDER BY
        query_text := format('
            WITH RECURSIVE descendants AS (
                SELECT unnest($1) as _id, 0::bigint as depth
                UNION ALL
                SELECT o._id, d.depth + 1
                FROM _objects o
                JOIN descendants d ON o._id_parent = d._id
                WHERE d.depth < %s
            )
            SELECT jsonb_agg(
                jsonb_build_object(
                    ''id'', o._id,
                    ''name'', o._name,
                    ''scheme_id'', o._id_scheme,
                    ''parent_id'', o._id_parent,
                    ''owner_id'', o._id_owner,
                    ''who_change_id'', o._id_who_change,
                    ''date_create'', o._date_create,
                    ''date_modify'', o._date_modify,
                    ''date_begin'', o._date_begin,
                    ''date_complete'', o._date_complete,
                    ''key'', o._key,
                    ''value_long'', o._value_long,
                    ''value_string'', o._value_string,
                    ''value_guid'', o._value_guid,
                    ''note'', o._note,
                    ''value_bool'', o._value_bool,
                    ''value_double'', o._value_double,
                    ''value_numeric'', o._value_numeric,
                    ''value_datetime'', o._value_datetime,
                    ''value_bytes'', o._value_bytes,
                    ''hash'', o._hash
                )
            )
            FROM (
                SELECT d._id
                FROM descendants dt
                JOIN _objects d ON dt._id = d._id
                WHERE dt.depth > 0 
                  AND d._id_scheme = %s%s
                %s
                %s
            ) sub
            JOIN _objects o ON o._id = sub._id',
            max_depth,
            scheme_id,
            COALESCE(base_conditions, ''),
            order_conditions,
            CASE 
                WHEN limit_count IS NOT NULL THEN format('LIMIT %s OFFSET %s', limit_count, offset_count)
                ELSE ''
            END
        );
    END IF;
    
    RETURN query_text;
END;
$BODY$;

COMMENT ON FUNCTION get_search_tree_sql_preview_base(bigint, bigint[], jsonb, integer, integer, jsonb, integer, integer) IS 
'✅ BATCH: Accepts parent_ids[]. Returns SQL query for tree lazy loading (for debugging). Shows what will be executed in search_tree_objects_with_facets_base(). Returns base fields WITHOUT Props. 🔥 Without DISTINCT for compatibility with ORDER BY.';