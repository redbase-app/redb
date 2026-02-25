-- =====================================================
-- SOFT DELETE FUNCTIONS FOR POSTGRESQL
-- Part of Background Deletion System
-- =====================================================

-- Drop existing functions if any
DROP FUNCTION IF EXISTS mark_for_deletion(bigint[], bigint);
DROP FUNCTION IF EXISTS mark_for_deletion(bigint[], bigint, bigint);
DROP FUNCTION IF EXISTS purge_trash(bigint, integer);

-- =====================================================
-- FUNCTION: mark_for_deletion
-- Marks objects for deletion by moving them under a trash container
-- Creates trash container, finds all descendants via CTE, updates parent and scheme
-- All operations in single transaction (atomic)
-- p_trash_parent_id: optional parent for trash container (NULL = root level)
-- =====================================================
CREATE OR REPLACE FUNCTION mark_for_deletion(
    p_object_ids bigint[],
    p_user_id bigint,
    p_trash_parent_id bigint DEFAULT NULL
) RETURNS TABLE(trash_id bigint, marked_count bigint) AS $$
DECLARE
    v_trash_id bigint;
    v_count bigint;
BEGIN
    -- 1. Create Trash container object with @@__deleted scheme
    -- Progress fields: _value_long=total, _key=deleted, _value_string=status
    INSERT INTO _objects (
        _id, _id_scheme, _id_parent, _id_owner, _id_who_change,
        _name, _date_create, _date_modify,
        _value_long, _key, _value_string
    ) VALUES (
        nextval('global_identity'), 
        -10,  -- @@__deleted scheme
        p_trash_parent_id,  -- user-specified parent or NULL
        p_user_id, 
        p_user_id,
        '__TRASH__' || p_user_id || '_' || extract(epoch from now())::bigint,
        NOW(), 
        NOW(),
        0,          -- _value_long = total (will be updated after count)
        0,          -- _key = deleted
        'pending'   -- _value_string = status
    ) RETURNING _id INTO v_trash_id;
    
    -- 2. CTE: find all objects and their descendants recursively
    -- 3. UPDATE: move all found objects under Trash container and change scheme
    WITH RECURSIVE all_descendants AS (
        -- Start with requested objects
        SELECT _id FROM _objects 
        WHERE _id = ANY(p_object_ids)
          AND _id_scheme != -10  -- skip already deleted
        
        UNION ALL
        
        -- Recursively find children
        SELECT o._id FROM _objects o
        INNER JOIN all_descendants d ON o._id_parent = d._id
        WHERE o._id_scheme != -10  -- skip already deleted
    )
    UPDATE _objects 
    SET _id_parent = v_trash_id,
        _id_scheme = -10,
        _date_modify = NOW()
    WHERE _id IN (SELECT _id FROM all_descendants);
    
    GET DIAGNOSTICS v_count = ROW_COUNT;
    
    -- 4. Update trash container with total count
    UPDATE _objects 
    SET _value_long = v_count
    WHERE _id = v_trash_id;
    
    -- 5. Return trash container ID and count of marked objects
    RETURN QUERY SELECT v_trash_id, v_count;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION mark_for_deletion(bigint[], bigint, bigint) IS 
'Marks objects for soft-deletion. Creates a trash container, moves all specified objects 
and their descendants under it with scheme=@@__deleted. Returns (trash_id, marked_count). 
p_trash_parent_id: optional parent ID for trash container (NULL = root level).
Atomic operation - all or nothing.';


-- =====================================================
-- FUNCTION: purge_trash
-- Physically deletes objects from a trash container in batches
-- ON DELETE CASCADE handles _values deletion automatically
-- Updates progress in trash container (_key=deleted, _value_string=status)
-- After all children deleted, removes the trash container itself
-- =====================================================
CREATE OR REPLACE FUNCTION purge_trash(
    p_trash_id bigint,
    p_batch_size integer DEFAULT 10
) RETURNS TABLE(deleted_count bigint, remaining_count bigint) AS $$
DECLARE
    v_deleted bigint;
    v_remaining bigint;
BEGIN
    -- Update status to 'running' if it was 'pending'
    UPDATE _objects 
    SET _value_string = 'running',
        _date_modify = NOW()
    WHERE _id = p_trash_id AND _value_string = 'pending';
    
    -- Delete a batch of objects (CASCADE handles _values)
    WITH to_delete AS (
        SELECT _id FROM _objects
        WHERE _id_parent = p_trash_id
        LIMIT p_batch_size
    )
    DELETE FROM _objects 
    WHERE _id IN (SELECT _id FROM to_delete);
    
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    
    -- Count remaining objects in this trash
    SELECT COUNT(*) INTO v_remaining
    FROM _objects 
    WHERE _id_parent = p_trash_id;
    
    -- Update progress in trash container
    UPDATE _objects 
    SET _key = _key + v_deleted,
        _value_string = CASE WHEN v_remaining = 0 THEN 'completed' ELSE 'running' END,
        _date_modify = NOW()
    WHERE _id = p_trash_id;
    
    -- If no more children, delete the trash container itself
    IF v_remaining = 0 THEN
        DELETE FROM _objects WHERE _id = p_trash_id;
    END IF;
    
    RETURN QUERY SELECT v_deleted, v_remaining;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION purge_trash(bigint, integer) IS 
'Physically deletes objects from a trash container in batches. 
p_trash_id: ID of the trash container created by mark_for_deletion.
p_batch_size: Number of objects to delete per call (default 10).
Returns (deleted_count, remaining_count). When remaining=0, trash container is also deleted.
Call repeatedly until remaining_count = 0.';

