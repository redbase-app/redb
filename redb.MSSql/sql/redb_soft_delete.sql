-- =====================================================
-- SOFT DELETE PROCEDURES FOR MSSQL
-- Part of Background Deletion System
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
            [_date_modify] = SYSDATETIMEOFFSET()
        WHERE [_id] IN (SELECT _id FROM #objects_to_delete);
        
        SET @marked_count = @@ROWCOUNT;
        
        -- 5. Update trash container with total count
        UPDATE [dbo].[_objects] 
        SET [_value_long] = @marked_count
        WHERE [_id] = @trash_id;
        
        DROP TABLE #objects_to_delete;
        
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
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
-- =====================================================
CREATE PROCEDURE [dbo].[sp_purge_trash]
    @trash_id BIGINT,
    @batch_size INT = 10,
    @deleted_count BIGINT OUTPUT,
    @remaining_count BIGINT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Update status to 'running' if it was 'pending'
    UPDATE [dbo].[_objects] 
    SET [_value_string] = 'running',
        [_date_modify] = SYSDATETIMEOFFSET()
    WHERE [_id] = @trash_id AND [_value_string] = 'pending';
    
    -- Delete a batch of objects (trigger handles _values cascade)
    DELETE TOP (@batch_size) FROM [dbo].[_objects]
    WHERE [_id_parent] = @trash_id;
    
    SET @deleted_count = @@ROWCOUNT;
    
    -- Count remaining objects in this trash
    SELECT @remaining_count = COUNT(*) 
    FROM [dbo].[_objects] 
    WHERE [_id_parent] = @trash_id;
    
    -- Update progress in trash container
    UPDATE [dbo].[_objects] 
    SET [_key] = [_key] + @deleted_count,
        [_value_string] = CASE WHEN @remaining_count = 0 THEN 'completed' ELSE 'running' END,
        [_date_modify] = SYSDATETIMEOFFSET()
    WHERE [_id] = @trash_id;
    
    -- If no more children, delete the trash container itself
    IF @remaining_count = 0
    BEGIN
        DELETE FROM [dbo].[_objects] WHERE [_id] = @trash_id;
    END
END
GO

