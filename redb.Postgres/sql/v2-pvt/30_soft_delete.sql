-- =====================================================
-- SOFT DELETE FUNCTIONS FOR POSTGRESQL - module-owned since V4
-- =====================================================
-- Moved from sql/redb_soft_delete.sql: that location lands only in
-- redb_init.sql, i.e. fresh databases. mark_for_deletion now also releases
-- unique keys (_values._unique = NULL, UNIQUE plan decision 9), and that
-- change must reach existing databases - so the functions ride the
-- versioned bundle. Part of Background Deletion System.
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
        _value_unique = NULL, -- V4 (decision 9): the trash releases object keys too
        _hash = NULL, -- full-object hash: parent and key change here outside the save path; no cached copy may outlive the move
        _date_modify = NOW()
    WHERE _id IN (SELECT _id FROM all_descendants);
    
    GET DIAGNOSTICS v_count = ROW_COUNT;
    
    -- V4 (UNIQUE, decision 9): a soft-deleted object releases its keys, whole and at once.
    -- Without this the trash scheme (-10) acts as one more uniqueness namespace and a repeated
    -- or cross-scheme delete fails on the unique index. Same transaction as the move above:
    -- there is no window where the object is in the trash but its key is still taken.
    UPDATE _values SET _unique = NULL
    WHERE _unique IS NOT NULL
      AND _id_object IN (SELECT _id FROM _objects WHERE _id_parent = v_trash_id AND _id_scheme = -10);
    
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
--
-- References (_values._Object, a foreign key with no ON DELETE action):
-- * a reference held by an object that is itself in the trash dies with the purge - the
--   referencing value row is removed first, so the order containers are purged in never matters;
-- * an object referenced by a LIVE object is skipped, never deleted and never unlinked (nulling
--   a live reference would hide the caller's bug inside another object's data). When nothing
--   but such objects is left, the container is marked 'failed': the background worker stops
--   claiming it, and the caller learns which references hold it.
-- =====================================================
CREATE OR REPLACE FUNCTION purge_trash(
    p_trash_id bigint,
    p_batch_size integer DEFAULT 10
) RETURNS TABLE(deleted_count bigint, remaining_count bigint) AS $$
DECLARE
    v_batch bigint[];
    v_deleted bigint;
    v_remaining bigint;
BEGIN
    -- Update status to 'running' if it was 'pending'
    UPDATE _objects
    SET _value_string = 'running',
        _date_modify = NOW()
    WHERE _id = p_trash_id AND _value_string = 'pending';

    -- The batch: objects of this container that no live object references
    SELECT COALESCE(array_agg(c._id), ARRAY[]::bigint[]) INTO v_batch
    FROM (
        SELECT o._id FROM _objects o
        WHERE o._id_parent = p_trash_id
          AND NOT EXISTS (
              SELECT 1 FROM _values v
              INNER JOIN _objects r ON r._id = v._id_object
              WHERE v._Object = o._id AND r._id_scheme <> -10)
        LIMIT p_batch_size
    ) c;

    -- References to the batch held by trashed objects go first. Restricted to trashed holders on
    -- purpose: a live reference written after the batch was chosen makes the delete below fail
    -- loudly instead of being removed here.
    DELETE FROM _values v
    USING _objects r
    WHERE v._Object = ANY(v_batch)
      AND r._id = v._id_object
      AND r._id_scheme = -10;

    -- Delete the batch (CASCADE handles its own _values)
    DELETE FROM _objects
    WHERE _id = ANY(v_batch);

    GET DIAGNOSTICS v_deleted = ROW_COUNT;

    -- Count remaining objects in this trash
    SELECT COUNT(*) INTO v_remaining
    FROM _objects
    WHERE _id_parent = p_trash_id;

    -- Update progress in trash container. An empty batch while objects remain = every one of them
    -- is referenced by a live object: 'failed', no longer claimed by the background worker. A batch
    -- that deleted nothing is another purger's work - the rows went between choosing the batch and
    -- deleting it - and the container stays 'running': the next batch takes what is left.
    UPDATE _objects
    SET _key = _key + v_deleted,
        _value_string = CASE WHEN v_remaining = 0 THEN 'completed'
                             WHEN cardinality(v_batch) = 0 THEN 'failed'
                             ELSE 'running' END,
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
Objects referenced by live objects are skipped; deleted_count = 0 with remaining_count > 0
means only such objects are left, and the container is marked failed.
Call repeatedly until remaining_count = 0 or deleted_count = 0.';

