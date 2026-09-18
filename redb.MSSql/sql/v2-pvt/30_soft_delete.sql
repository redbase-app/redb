-- =====================================================
-- SOFT DELETE PROCEDURES FOR MSSQL - module-owned since V4
-- =====================================================
-- Moved from sql/redb_soft_delete.sql: that location lands only in
-- redb_init.sql, i.e. fresh databases. sp_mark_for_deletion now also
-- releases unique keys (_values._unique = NULL, UNIQUE plan decision 9),
-- and that change must reach existing databases - so the procedures ride
-- the versioned bundle. Part of Background Deletion System.
--
-- Transactions (trash review, 2026-09-14): both procedures run with
-- SET XACT_ABORT ON and open a transaction only when the caller has none.
-- Without XACT_ABORT a command timeout or client cancel arrives as an
-- attention that skips CATCH, and BEGIN TRANSACTION stayed open on the
-- session: every later statement of the scope ran inside it and was rolled
-- back with it when the pooled session was reset. With XACT_ABORT ON the
-- server rolls the transaction back on any error or attention. Inside a
-- caller's transaction the procedures neither commit nor roll back - the
-- owner of the transaction does.
-- =====================================================


-- Drop existing procedures if any
IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'sp_mark_for_deletion')
DROP PROCEDURE [dbo].[sp_mark_for_deletion]
GO

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'sp_purge_trash')
DROP PROCEDURE [dbo].[sp_purge_trash]
GO

-- =====================================================
-- PROCEDURE: sp_mark_for_deletion
-- Marks objects for deletion by moving them under a trash container
-- Creates trash container, finds all descendants via CTE, updates parent and scheme
-- All operations in single transaction (atomic)
-- @trash_parent_id: optional parent for trash container (NULL = root level)
-- =====================================================
CREATE PROCEDURE [dbo].[sp_mark_for_deletion]
    @object_ids NVARCHAR(MAX),  -- Comma-separated list of object IDs
    @user_id BIGINT,
    @trash_parent_id BIGINT = NULL,  -- Optional parent for trash container
    @trash_id BIGINT OUTPUT,
    @marked_count BIGINT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @own_transaction BIT = CASE WHEN @@TRANCOUNT = 0 THEN 1 ELSE 0 END;
    IF @own_transaction = 1
        BEGIN TRANSACTION;

    BEGIN TRY
        -- 1. Create Trash container object with @@__deleted scheme
        -- Progress fields: _value_long=total, _key=deleted, _value_string=status
        SET @trash_id = NEXT VALUE FOR [dbo].[global_identity];

        INSERT INTO [dbo].[_objects] (
            [_id], [_id_scheme], [_id_parent], [_id_owner], [_id_who_change],
            [_name], [_date_create], [_date_modify],
            [_value_long], [_key], [_value_string]
        ) VALUES (
            @trash_id,
            -10,  -- @@__deleted scheme
            @trash_parent_id,  -- user-specified parent or NULL
            @user_id,
            @user_id,
            '__TRASH__' + CAST(@user_id AS NVARCHAR(20)) + '_' + CAST(DATEDIFF(SECOND, '1970-01-01', GETUTCDATE()) AS NVARCHAR(20)),
            SYSDATETIMEOFFSET(),
            SYSDATETIMEOFFSET(),
            0,          -- _value_long = total (will be updated after count)
            0,          -- _key = deleted
            'pending'   -- _value_string = status
        );

        -- 2. Create temp table for objects to process
        CREATE TABLE #objects_to_delete (_id BIGINT);

        -- 3. CTE: find all objects and their descendants recursively
        ;WITH all_descendants AS (
            -- Start with requested objects
            SELECT o._id
            FROM [dbo].[_objects] o
            WHERE o._id IN (SELECT CAST(value AS BIGINT) FROM STRING_SPLIT(@object_ids, ','))
              AND o._id_scheme != -10  -- skip already deleted

            UNION ALL

            -- Recursively find children
            SELECT o._id
            FROM [dbo].[_objects] o
            INNER JOIN all_descendants d ON o._id_parent = d._id
            WHERE o._id_scheme != -10  -- skip already deleted
        )
        INSERT INTO #objects_to_delete
        SELECT _id FROM all_descendants;

        -- 4. UPDATE: move all found objects under Trash container and change scheme
        UPDATE [dbo].[_objects]
        SET [_id_parent] = @trash_id,
            [_id_scheme] = -10,
            [_value_unique] = NULL, -- V4 (decision 9): the trash releases object keys too
            [_hash] = NULL, -- full-object hash: parent and key change here outside the save path; no cached copy may outlive the move
            [_date_modify] = SYSDATETIMEOFFSET()
        WHERE [_id] IN (SELECT _id FROM #objects_to_delete);

        SET @marked_count = @@ROWCOUNT;

        -- V4 (UNIQUE, decision 9): a soft-deleted object releases its keys, whole and at once.
        -- Same transaction as the move above: no window where the object is in the trash but
        -- its key is still taken.
        UPDATE v SET v.[_unique] = NULL
        FROM [dbo].[_values] v
        INNER JOIN #objects_to_delete d ON v.[_id_object] = d.[_id]
        WHERE v.[_unique] IS NOT NULL;

        -- 5. Update trash container with total count
        UPDATE [dbo].[_objects]
        SET [_value_long] = @marked_count
        WHERE [_id] = @trash_id;

        DROP TABLE #objects_to_delete;

        IF @own_transaction = 1
            COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @own_transaction = 1 AND @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END
GO

-- =====================================================
-- PROCEDURE: sp_purge_trash
-- Physically deletes objects from a trash container in batches
-- TR__objects__cascade_values trigger handles _values deletion
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
CREATE PROCEDURE [dbo].[sp_purge_trash]
    @trash_id BIGINT,
    @batch_size INT = 10,
    @deleted_count BIGINT OUTPUT,
    @remaining_count BIGINT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @own_transaction BIT = CASE WHEN @@TRANCOUNT = 0 THEN 1 ELSE 0 END;
    DECLARE @batch TABLE ([_id] BIGINT PRIMARY KEY);

    IF @own_transaction = 1
        BEGIN TRANSACTION;

    BEGIN TRY
        -- Update status to 'running' if it was 'pending'
        UPDATE [dbo].[_objects]
        SET [_value_string] = 'running',
            [_date_modify] = SYSDATETIMEOFFSET()
        WHERE [_id] = @trash_id AND [_value_string] = 'pending';

        -- The batch: objects of this container that no live object references
        INSERT INTO @batch ([_id])
        SELECT TOP (@batch_size) o.[_id]
        FROM [dbo].[_objects] o
        WHERE o.[_id_parent] = @trash_id
          AND NOT EXISTS (
              SELECT 1 FROM [dbo].[_values] v
              INNER JOIN [dbo].[_objects] r ON r.[_id] = v.[_id_object]
              WHERE v.[_Object] = o.[_id] AND r.[_id_scheme] <> -10);

        SET @deleted_count = @@ROWCOUNT;

        -- References to the batch held by trashed objects go first. Restricted to trashed holders on
        -- purpose: a live reference written after the batch was chosen makes the delete below fail
        -- loudly instead of being removed here.
        DELETE v FROM [dbo].[_values] v
        INNER JOIN @batch b ON v.[_Object] = b.[_id]
        INNER JOIN [dbo].[_objects] r ON r.[_id] = v.[_id_object]
        WHERE r.[_id_scheme] = -10;

        -- Delete the batch (trigger handles its own _values). The count comes from the batch table:
        -- the INSTEAD OF trigger does the actual delete.
        DELETE o FROM [dbo].[_objects] o
        INNER JOIN @batch b ON o.[_id] = b.[_id];

        -- Count remaining objects in this trash
        SELECT @remaining_count = COUNT(*)
        FROM [dbo].[_objects]
        WHERE [_id_parent] = @trash_id;

        -- Update progress in trash container. Nothing deletable while objects remain = every one of
        -- them is referenced by a live object: 'failed', no longer claimed by the background worker.
        UPDATE [dbo].[_objects]
        SET [_key] = [_key] + @deleted_count,
            [_value_string] = CASE WHEN @remaining_count = 0 THEN 'completed'
                                   WHEN @deleted_count = 0 THEN 'failed'
                                   ELSE 'running' END,
            [_date_modify] = SYSDATETIMEOFFSET()
        WHERE [_id] = @trash_id;

        -- If no more children, delete the trash container itself
        IF @remaining_count = 0
        BEGIN
            DELETE FROM [dbo].[_objects] WHERE [_id] = @trash_id;
        END

        IF @own_transaction = 1
            COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @own_transaction = 1 AND @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END
GO

