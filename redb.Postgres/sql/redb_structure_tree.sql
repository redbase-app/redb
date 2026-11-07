-- ====================================================================================================
-- FUNCTIONS FOR WORKING WITH SCHEME STRUCTURE TREE
-- ====================================================================================================
-- Supports hierarchical navigation through structures: parent → children → descendants
-- Solves flat structure search problems in SaveAsync
-- ====================================================================================================

-- MAIN FUNCTION: Build scheme structure tree (SIMPLE APPROACH)
-- SIMPLE AND CLEAR LOGIC: get current layer → for each structure get children recursively
CREATE OR REPLACE FUNCTION get_scheme_structure_tree(
    scheme_id bigint,
    parent_id bigint DEFAULT NULL,
    max_depth integer DEFAULT 10
) RETURNS jsonb
LANGUAGE 'plpgsql'
COST 100
VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    result jsonb := '[]'::jsonb;
    structure_record RECORD;
    children_json jsonb;
BEGIN
    -- Protection from infinite recursion
    IF max_depth <= 0 THEN
        RETURN jsonb_build_array(jsonb_build_object('error', 'Max recursion depth reached'));
    END IF;
    
    -- Check scheme existence
    IF NOT EXISTS(SELECT 1 FROM _schemes WHERE _id = scheme_id) THEN
        RETURN jsonb_build_array(jsonb_build_object('error', 'Scheme not found'));
    END IF;
    
    -- AUTOMATIC CACHE CHECK AND FILL
    IF NOT EXISTS(SELECT 1 FROM _scheme_metadata_cache WHERE _scheme_id = scheme_id LIMIT 1) THEN
        PERFORM sync_metadata_cache_for_scheme(scheme_id);
        -- Auto-fill without NOTICE (use warmup_all_metadata_caches() for explicit warmup)
    END IF;
    
    -- SIMPLE LOGIC: Get structures of CURRENT LEVEL
    -- OPTIMIZATION: Use _scheme_metadata_cache instead of JOIN _structures ← _types
    FOR structure_record IN
        SELECT 
            c._structure_id as _id,
            c._name,
            c._order,
            c._collection_type IS NOT NULL as _is_array,  -- _collection_type != NULL = array/dict
            c._collection_type,
            c._store_null,
            c._allow_not_null,
            c.type_name,
            c.db_type,
            c.type_semantic
        FROM _scheme_metadata_cache c
        WHERE c._scheme_id = scheme_id
          AND ((parent_id IS NULL AND c._parent_structure_id IS NULL) 
               OR (parent_id IS NOT NULL AND c._parent_structure_id = parent_id))
        ORDER BY c._order, c._structure_id
    LOOP
        -- CHECK IF STRUCTURE HAS CHILDREN
        IF EXISTS(SELECT 1 FROM _structures 
                 WHERE _id_scheme = scheme_id 
                   AND _id_parent = structure_record._id) THEN
            -- RECURSIVELY get children (simple function call!)
            children_json := get_scheme_structure_tree(scheme_id, structure_record._id, max_depth - 1);
        ELSE
            -- No children - empty array
            children_json := '[]'::jsonb;
        END IF;
        
        -- ADD STRUCTURE TO RESULT (simple construction)
        result := result || jsonb_build_array(
            jsonb_build_object(
                'structure_id', structure_record._id,
                'name', structure_record._name,
                'order', structure_record._order,
                'is_array', structure_record._is_array,  -- For compatibility
                'collection_type', structure_record._collection_type,  -- New collection type
                'store_null', structure_record._store_null,
                'allow_not_null', structure_record._allow_not_null,
                'type_name', structure_record.type_name,
                'db_type', structure_record.db_type,
                'type_semantic', structure_record.type_semantic,
                'children', children_json  -- Recursively obtained children
            )
        );
    END LOOP;
    
    RETURN result;
END;
$BODY$;

-- HELPER FUNCTION: Get only direct child structures  
CREATE OR REPLACE FUNCTION get_structure_children(
    scheme_id bigint,
    parent_id bigint
) RETURNS jsonb
LANGUAGE 'plpgsql'
COST 50
VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
    -- AUTOMATIC CACHE CHECK AND FILL
    IF NOT EXISTS(SELECT 1 FROM _scheme_metadata_cache WHERE _scheme_id = scheme_id LIMIT 1) THEN
        PERFORM sync_metadata_cache_for_scheme(scheme_id);
        -- Auto-fill without NOTICE (use warmup_all_metadata_caches() for explicit warmup)
    END IF;
    
    -- OPTIMIZATION: Use _scheme_metadata_cache instead of JOIN _structures ← _types
    RETURN (
        SELECT COALESCE(jsonb_agg(
            jsonb_build_object(
                'structure_id', c._structure_id,
                'name', c._name,
                'order', c._order,
                'is_array', c._collection_type IS NOT NULL,  -- For compatibility
                'collection_type', c._collection_type,       -- New collection type
                'type_name', c.type_name,
                'db_type', c.db_type,
                'type_semantic', c.type_semantic
            ) ORDER BY c._order, c._structure_id
        ), '[]'::jsonb)
        FROM _scheme_metadata_cache c
        WHERE c._scheme_id = scheme_id
          AND c._parent_structure_id = parent_id
    );
END;
$BODY$;

-- DIAGNOSTIC FUNCTION: Validate structure tree for redundancy
CREATE OR REPLACE FUNCTION validate_structure_tree(
    scheme_id bigint
) RETURNS jsonb
LANGUAGE 'plpgsql'
COST 100
VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    validation_result jsonb;
    excessive_structures jsonb;
    orphaned_structures jsonb;
    circular_references jsonb;
BEGIN
    -- 1. Find excessive structures (structures without values connections)
    SELECT jsonb_agg(
        jsonb_build_object(
            'structure_id', s._id,
            'name', s._name,
            'parent_name', parent_s._name,
            'issue', 'No values found - possibly excessive structure'
        )
    ) INTO excessive_structures
    FROM _structures s
    LEFT JOIN _structures parent_s ON parent_s._id = s._id_parent
    LEFT JOIN _values v ON v._id_structure = s._id
    WHERE s._id_scheme = scheme_id
      AND v._id IS NULL  -- No values for this structure
      AND s._id_parent IS NOT NULL; -- Only child structures
    
    -- 2. Find orphaned structures (parent doesn't exist)
    SELECT jsonb_agg(
        jsonb_build_object(
            'structure_id', s._id,
            'name', s._name,
            'parent_id', s._id_parent,
            'issue', 'Parent structure does not exist'
        )
    ) INTO orphaned_structures
    FROM _structures s
    WHERE s._id_scheme = scheme_id
      AND s._id_parent IS NOT NULL
      AND NOT EXISTS(SELECT 1 FROM _structures parent_s WHERE parent_s._id = s._id_parent);
    
    -- 3. Simple check for circular references (structure references itself via chain)
    WITH RECURSIVE cycle_check AS (
        SELECT _id, _id_parent, ARRAY[_id] as path, false as has_cycle
        FROM _structures WHERE _id_scheme = scheme_id AND _id_parent IS NOT NULL
        
        UNION ALL
        
        SELECT s._id, s._id_parent, cc.path || s._id, s._id = ANY(cc.path)
        FROM _structures s
        JOIN cycle_check cc ON cc._id_parent = s._id
        WHERE NOT cc.has_cycle AND array_length(cc.path, 1) < 50
    )
    SELECT jsonb_agg(
        jsonb_build_object(
            'structure_id', _id,
            'path', path,
            'issue', 'Circular reference detected'
        )
    ) INTO circular_references
    FROM cycle_check 
    WHERE has_cycle;
    
    -- Form final report
    validation_result := jsonb_build_object(
        'scheme_id', scheme_id,
        'validation_date', NOW(),
        'excessive_structures', COALESCE(excessive_structures, '[]'::jsonb),
        'orphaned_structures', COALESCE(orphaned_structures, '[]'::jsonb), 
        'circular_references', COALESCE(circular_references, '[]'::jsonb),
        'total_structures', (SELECT COUNT(*) FROM _structures WHERE _id_scheme = scheme_id),
        'is_valid', (excessive_structures IS NULL AND orphaned_structures IS NULL AND circular_references IS NULL)
    );
    
    RETURN validation_result;
END;
$BODY$;

-- FUNCTION: Get all structure descendants (flat list)
CREATE OR REPLACE FUNCTION get_structure_descendants(
    scheme_id bigint,
    parent_id bigint
) RETURNS jsonb
LANGUAGE 'plpgsql'  
COST 100
VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
    RETURN (
        WITH RECURSIVE descendants AS (
            -- Direct child structures
            SELECT _id, _name, _id_parent, 0 as level
            FROM _structures 
            WHERE _id_scheme = scheme_id AND _id_parent = parent_id
            
            UNION ALL
            
            -- Recursively all descendants
            SELECT s._id, s._name, s._id_parent, d.level + 1
            FROM _structures s
            JOIN descendants d ON d._id = s._id_parent
            WHERE s._id_scheme = scheme_id AND d.level < 10
        )
        SELECT COALESCE(jsonb_agg(
            jsonb_build_object(
                'structure_id', _id,
                'name', _name, 
                'parent_id', _id_parent,
                'level', level
            ) ORDER BY level, _id
        ), '[]'::jsonb)
        FROM descendants
    );
END;
$BODY$;

-- Comments for structure tree functions
COMMENT ON FUNCTION get_scheme_structure_tree(bigint, bigint, integer) IS 'Build complete scheme structure tree with hierarchy. Supports recursion depth limit. Used by PostgresSchemeSyncProvider for correct structure traversal in SaveAsync.';

COMMENT ON FUNCTION get_structure_children(bigint, bigint) IS 'Get only direct child structures without recursion. Fast function for simple tree navigation cases.';

COMMENT ON FUNCTION validate_structure_tree(bigint) IS 'Structure tree diagnostics: find excessive structures, orphaned references, circular dependencies. Helps identify issues like with Address.Details.Tags1.';

COMMENT ON FUNCTION get_structure_descendants(bigint, bigint) IS 'Get all structure descendants in flat format with nesting level indication. Useful for analyzing deep hierarchies.';
