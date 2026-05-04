-- =============================================================================
-- REDB Permissions Functions for MS SQL Server
-- Compatible with SQL Server 2016+ (uses JSON functions, window functions, CTEs)
-- =============================================================================

-- Drop existing function if exists
IF OBJECT_ID('dbo.get_user_permissions_for_object', 'TF') IS NOT NULL
    DROP FUNCTION dbo.get_user_permissions_for_object;
GO

-- =============================================================================
-- Function: get_user_permissions_for_object
-- Purpose: Returns effective permissions for a specific object considering 
--          hierarchical inheritance and priorities (user > role).
--          If @user_id = NULL, returns the first found permission without 
--          filtering by user (for use in triggers).
--
-- Parameters:
--   @object_id BIGINT - Target object ID
--   @user_id BIGINT = NULL - User ID (optional for trigger usage)
--
-- Returns: Table with permission details
--
-- Usage:
--   SELECT * FROM dbo.get_user_permissions_for_object(12345, 100);
--   SELECT * FROM dbo.get_user_permissions_for_object(12345, NULL); -- for triggers
-- =============================================================================
CREATE FUNCTION dbo.get_user_permissions_for_object
(
    @object_id BIGINT,
    @user_id BIGINT = NULL
)
RETURNS @result TABLE
(
    object_id BIGINT,
    user_id BIGINT,
    permission_source_id BIGINT,
    permission_type NVARCHAR(50),
    _id_role BIGINT,
    _id_user BIGINT,
    can_select BIT,
    can_insert BIT,
    can_update BIT,
    can_delete BIT
)
AS
BEGIN
    -- System user (id=0) has full permissions on everything
    IF @user_id = 0
    BEGIN
        INSERT INTO @result
        SELECT 
            @object_id AS object_id,
            0 AS user_id,
            0 AS permission_source_id,
            N'system' AS permission_type,
            NULL AS _id_role,
            0 AS _id_user,
            1 AS can_select,
            1 AS can_insert,
            1 AS can_update,
            1 AS can_delete;
        RETURN;
    END;

    -- Use CTE to find permissions with hierarchical search
    ;WITH permission_search AS (
        -- Step 1: Start from target object
        SELECT 
            @object_id AS object_id,
            @object_id AS current_search_id,
            o._id_parent,
            0 AS level,
            CASE WHEN EXISTS(SELECT 1 FROM _permissions WHERE _id_ref = @object_id) 
                 THEN 1 ELSE 0 END AS has_permission
        FROM _objects o
        WHERE o._id = @object_id
        
        UNION ALL
        
        -- Step 2: If NO permission - go to parent
        SELECT 
            ps.object_id,
            o._id AS current_search_id,
            o._id_parent,
            ps.level + 1,
            CASE WHEN EXISTS(SELECT 1 FROM _permissions WHERE _id_ref = o._id) 
                 THEN 1 ELSE 0 END AS has_permission
        FROM _objects o
        INNER JOIN permission_search ps ON o._id = ps._id_parent
        WHERE ps.level < 50
          AND ps.has_permission = 0  -- continue only if NO permission
    ),
    -- Get first found permission for object using ROW_NUMBER instead of DISTINCT ON
    object_permission_ranked AS (
        SELECT 
            ps.object_id,
            p._id AS permission_id,
            p._id_user,
            p._id_role,
            p._select,
            p._insert,
            p._update,
            p._delete,
            ps.level,
            ps.current_search_id AS permission_source_id,
            ROW_NUMBER() OVER (PARTITION BY ps.object_id ORDER BY ps.level) AS rn
        FROM permission_search ps
        INNER JOIN _permissions p ON p._id_ref = ps.current_search_id
        WHERE ps.has_permission = 1
    ),
    object_permission AS (
        SELECT 
            object_id,
            permission_id,
            _id_user,
            _id_role,
            _select,
            _insert,
            _update,
            _delete,
            level,
            permission_source_id
        FROM object_permission_ranked
        WHERE rn = 1
    ),
    -- Add global permissions as fallback (_id_ref = 0)
    global_permission AS (
        SELECT 
            @object_id AS object_id,
            p._id AS permission_id,
            p._id_user,
            p._id_role,
            p._select,
            p._insert,
            p._update,
            p._delete,
            999 AS level,  -- low priority
            CAST(0 AS BIGINT) AS permission_source_id
        FROM _permissions p
        WHERE p._id_ref = 0
    ),
    -- Combine specific and global permissions
    all_permissions AS (
        SELECT * FROM object_permission
        UNION ALL
        SELECT * FROM global_permission
    ),
    -- Get first by priority (specific > global) using ROW_NUMBER
    final_permission_ranked AS (
        SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY object_id ORDER BY level) AS rn
        FROM all_permissions
    ),
    final_permission AS (
        SELECT 
            object_id,
            permission_id,
            _id_user,
            _id_role,
            _select,
            _insert,
            _update,
            _delete,
            level,
            permission_source_id
        FROM final_permission_ranked
        WHERE rn = 1
    )
    -- Result: for user permissions - direct, for role - through users_roles
    INSERT INTO @result
    SELECT 
        fp.object_id,
        CASE 
            WHEN @user_id IS NULL THEN NULL  -- if user_id not passed for trigger
            WHEN fp._id_user IS NOT NULL THEN fp._id_user  -- direct user permission
            ELSE ur._id_user  -- through role
        END AS user_id,
        fp.permission_source_id,
        CASE 
            WHEN fp._id_user IS NOT NULL THEN N'user'
            ELSE N'role'
        END AS permission_type,
        fp._id_role,
        fp._id_user,
        fp._select AS can_select,
        fp._insert AS can_insert,
        fp._update AS can_update,
        fp._delete AS can_delete
    FROM final_permission fp
    LEFT JOIN _users_roles ur ON ur._id_role = fp._id_role  -- only for role permissions
    WHERE @user_id IS NULL 
       OR (fp._id_user = @user_id OR ur._id_user = @user_id);  -- if user_id NULL - all permissions, else filter

    RETURN;
END;
GO

-- =============================================================================
-- Trigger: auto_create_node_permissions
-- Purpose: Automatically creates permissions when creating node objects.
--          If parent has no direct permission, finds inherited permission
--          and creates copy for the new object.
--
-- Note: This trigger is for reference. Adjust based on your actual requirements.
-- =============================================================================

-- Drop existing trigger if exists
IF OBJECT_ID('dbo.tr_auto_create_node_permissions', 'TR') IS NOT NULL
    DROP TRIGGER dbo.tr_auto_create_node_permissions;
GO

CREATE TRIGGER dbo.tr_auto_create_node_permissions
ON _objects
AFTER INSERT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @new_id BIGINT;
    DECLARE @parent_id BIGINT;
    DECLARE @source_permission_id BIGINT;
    DECLARE @source_user_id BIGINT;
    DECLARE @source_role_id BIGINT;
    DECLARE @source_select BIT;
    DECLARE @source_insert BIT;
    DECLARE @source_update BIT;
    DECLARE @source_delete BIT;
    DECLARE @next_id BIGINT;
    
    -- Process each inserted object with parent
    DECLARE insert_cursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT i._id, i._id_parent
        FROM inserted i
        WHERE i._id_parent IS NOT NULL;
    
    OPEN insert_cursor;
    FETCH NEXT FROM insert_cursor INTO @new_id, @parent_id;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Check if parent already has direct permission
        IF NOT EXISTS(SELECT 1 FROM _permissions WHERE _id_ref = @parent_id)
        BEGIN
            -- Use function without user_id to find source permission
            SELECT TOP 1
                @source_permission_id = permission_source_id,
                @source_user_id = _id_user,
                @source_role_id = _id_role,
                @source_select = can_select,
                @source_insert = can_insert,
                @source_update = can_update,
                @source_delete = can_delete
            FROM dbo.get_user_permissions_for_object(@parent_id, NULL);
            
            -- If source permission found, create copy for parent
            IF @source_permission_id IS NOT NULL
            BEGIN
                -- Get next ID from sequence
                SELECT @next_id = NEXT VALUE FOR global_identity;
                
                INSERT INTO _permissions (
                    _id, _id_ref, _id_user, _id_role,
                    _select, _insert, _update, _delete
                )
                VALUES (
                    @next_id, @parent_id, @source_user_id, @source_role_id,
                    @source_select, @source_insert, @source_update, @source_delete
                );
            END;
        END;
        
        FETCH NEXT FROM insert_cursor INTO @new_id, @parent_id;
    END;
    
    CLOSE insert_cursor;
    DEALLOCATE insert_cursor;
END;
GO

-- =============================================================================
-- Function: check_user_permission
-- Purpose: Quick check if user has specific permission on object.
--          Returns 1 if has permission, 0 otherwise.
--
-- Parameters:
--   @object_id BIGINT - Target object ID
--   @user_id BIGINT - User ID
--   @permission_type NVARCHAR(10) - 'select', 'insert', 'update', 'delete'
--
-- Usage:
--   IF dbo.check_user_permission(12345, 100, 'update') = 1 
--     PRINT 'User can update';
-- =============================================================================
IF OBJECT_ID('dbo.check_user_permission', 'FN') IS NOT NULL
    DROP FUNCTION dbo.check_user_permission;
GO

CREATE FUNCTION dbo.check_user_permission
(
    @object_id BIGINT,
    @user_id BIGINT,
    @permission_type NVARCHAR(10)
)
RETURNS BIT
AS
BEGIN
    DECLARE @result BIT = 0;
    
    -- System user always has permission
    IF @user_id = 0
        RETURN 1;
    
    SELECT @result = 
        CASE @permission_type
            WHEN N'select' THEN can_select
            WHEN N'insert' THEN can_insert
            WHEN N'update' THEN can_update
            WHEN N'delete' THEN can_delete
            ELSE 0
        END
    FROM dbo.get_user_permissions_for_object(@object_id, @user_id);
    
    RETURN ISNULL(@result, 0);
END;
GO

-- =============================================================================
-- Function: get_user_accessible_objects
-- Purpose: Returns list of object IDs that user can access with given permission.
--          Uses hierarchical permission inheritance.
--
-- Parameters:
--   @user_id BIGINT - User ID
--   @permission_type NVARCHAR(10) - 'select', 'insert', 'update', 'delete'
--   @scheme_id BIGINT = NULL - Optional filter by scheme
--
-- Usage:
--   SELECT * FROM dbo.get_user_accessible_objects(100, 'select', NULL);
-- =============================================================================
IF OBJECT_ID('dbo.get_user_accessible_objects', 'TF') IS NOT NULL
    DROP FUNCTION dbo.get_user_accessible_objects;
GO

CREATE FUNCTION dbo.get_user_accessible_objects
(
    @user_id BIGINT,
    @permission_type NVARCHAR(10),
    @scheme_id BIGINT = NULL
)
RETURNS @result TABLE
(
    object_id BIGINT PRIMARY KEY
)
AS
BEGIN
    -- System user sees everything
    IF @user_id = 0
    BEGIN
        INSERT INTO @result
        SELECT _id FROM _objects
        WHERE @scheme_id IS NULL OR _id_scheme = @scheme_id;
        RETURN;
    END;
    
    -- Get user's roles
    DECLARE @user_roles TABLE (role_id BIGINT PRIMARY KEY);
    INSERT INTO @user_roles
    SELECT _id_role FROM _users_roles WHERE _id_user = @user_id;
    
    -- Find all permissions applicable to user (direct or via role)
    ;WITH applicable_permissions AS (
        SELECT 
            p._id_ref AS object_id,
            CASE @permission_type
                WHEN N'select' THEN p._select
                WHEN N'insert' THEN p._insert
                WHEN N'update' THEN p._update
                WHEN N'delete' THEN p._delete
                ELSE 0
            END AS has_permission
        FROM _permissions p
        WHERE p._id_user = @user_id
           OR p._id_role IN (SELECT role_id FROM @user_roles)
    ),
    -- Objects with direct permissions
    permitted_roots AS (
        SELECT object_id
        FROM applicable_permissions
        WHERE has_permission = 1
    ),
    -- Recursively find all descendants of permitted objects
    all_accessible AS (
        SELECT o._id AS object_id
        FROM _objects o
        WHERE o._id IN (SELECT object_id FROM permitted_roots)
        
        UNION ALL
        
        SELECT o._id
        FROM _objects o
        INNER JOIN all_accessible a ON o._id_parent = a.object_id
    )
    INSERT INTO @result
    SELECT DISTINCT aa.object_id
    FROM all_accessible aa
    INNER JOIN _objects o ON o._id = aa.object_id
    WHERE @scheme_id IS NULL OR o._id_scheme = @scheme_id;
    
    RETURN;
END;
GO

-- =============================================================================
-- Inline Function: fn_can_user_edit_object
-- Purpose: Optimized scalar check for edit (update) permission.
--          Returns 1 if user can edit, 0 otherwise.
--          Inline version for better query plan optimization.
-- =============================================================================
IF OBJECT_ID('dbo.fn_can_user_edit_object', 'IF') IS NOT NULL
    DROP FUNCTION dbo.fn_can_user_edit_object;
GO

CREATE FUNCTION dbo.fn_can_user_edit_object
(
    @object_id BIGINT,
    @user_id BIGINT
)
RETURNS TABLE
AS
RETURN
(
    SELECT 
        CASE 
            WHEN @user_id = 0 THEN 1  -- system user
            WHEN EXISTS(
                SELECT 1 FROM dbo.get_user_permissions_for_object(@object_id, @user_id)
                WHERE can_update = 1
            ) THEN 1
            ELSE 0
        END AS can_edit
);
GO

-- =============================================================================
-- Inline Function: fn_can_user_select_object
-- Purpose: Optimized scalar check for select permission.
-- =============================================================================
IF OBJECT_ID('dbo.fn_can_user_select_object', 'IF') IS NOT NULL
    DROP FUNCTION dbo.fn_can_user_select_object;
GO

CREATE FUNCTION dbo.fn_can_user_select_object
(
    @object_id BIGINT,
    @user_id BIGINT
)
RETURNS TABLE
AS
RETURN
(
    SELECT 
        CASE 
            WHEN @user_id = 0 THEN 1  -- system user
            WHEN EXISTS(
                SELECT 1 FROM dbo.get_user_permissions_for_object(@object_id, @user_id)
                WHERE can_select = 1
            ) THEN 1
            ELSE 0
        END AS can_select
);
GO

-- =============================================================================
-- Inline Function: fn_can_user_delete_object
-- Purpose: Optimized scalar check for delete permission.
-- =============================================================================
IF OBJECT_ID('dbo.fn_can_user_delete_object', 'IF') IS NOT NULL
    DROP FUNCTION dbo.fn_can_user_delete_object;
GO

CREATE FUNCTION dbo.fn_can_user_delete_object
(
    @object_id BIGINT,
    @user_id BIGINT
)
RETURNS TABLE
AS
RETURN
(
    SELECT 
        CASE 
            WHEN @user_id = 0 THEN 1  -- system user
            WHEN EXISTS(
                SELECT 1 FROM dbo.get_user_permissions_for_object(@object_id, @user_id)
                WHERE can_delete = 1
            ) THEN 1
            ELSE 0
        END AS can_delete
);
GO

-- =============================================================================
-- Inline Function: fn_can_user_insert_scheme
-- Purpose: Check if user can insert objects of specific scheme.
--          Looks for global permission (on _id_ref = 0 or scheme itself).
-- =============================================================================
IF OBJECT_ID('dbo.fn_can_user_insert_scheme', 'IF') IS NOT NULL
    DROP FUNCTION dbo.fn_can_user_insert_scheme;
GO

CREATE FUNCTION dbo.fn_can_user_insert_scheme
(
    @scheme_id BIGINT,
    @user_id BIGINT
)
RETURNS TABLE
AS
RETURN
(
    WITH user_roles AS (
        SELECT _id_role AS role_id FROM _users_roles WHERE _id_user = @user_id
    )
    SELECT 
        CASE 
            WHEN @user_id = 0 THEN 1  -- system user
            WHEN EXISTS(
                SELECT 1 FROM _permissions p
                WHERE (p._id_ref = 0 OR p._id_ref = @scheme_id)
                  AND p._insert = 1
                  AND (p._id_user = @user_id OR p._id_role IN (SELECT role_id FROM user_roles))
            ) THEN 1
            ELSE 0
        END AS can_insert
);
GO

PRINT 'REDB Permissions functions created successfully';
GO

