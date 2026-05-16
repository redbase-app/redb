using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Core.Models.Enums;
using redb.Core.Models.Permissions;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace redb.Core.Providers
{
    /// <summary>
    /// Provider for access permission management.
    /// </summary>
    public interface IPermissionProvider
    {
        // ===== BASE METHODS (use _securityContext by default) =====
        
        /// <summary>
        /// Get IDs of objects readable by current user.
        /// </summary>
        IQueryable<long> GetReadableObjectIds();
        
        /// <summary>
        /// Check if current user can edit object.
        /// </summary>
        Task<bool> CanUserEditObject(IRedbObject obj);
        
        /// <summary>
        /// Check if current user can read object.
        /// </summary>
        Task<bool> CanUserSelectObject(IRedbObject obj);
        
        /// <summary>
        /// Check if current user can create objects in scheme.
        /// </summary>
        Task<bool> CanUserInsertScheme(IRedbScheme scheme);
        
        /// <summary>
        /// Check if current user can delete object.
        /// </summary>
        Task<bool> CanUserDeleteObject(IRedbObject obj);

        // ===== OVERLOADS WITH EXPLICIT USER =====
        
        /// <summary>
        /// Get IDs of objects readable by user.
        /// </summary>
        IQueryable<long> GetReadableObjectIds(IRedbUser user);
        
        /// <summary>
        /// Check if user can edit object.
        /// </summary>
        Task<bool> CanUserEditObject(IRedbObject obj, IRedbUser user);
        
        /// <summary>
        /// Check if user can read object.
        /// </summary>
        Task<bool> CanUserSelectObject(IRedbObject obj, IRedbUser user);
        
        /// <summary>
        /// Check if user can create objects in scheme.
        /// </summary>
        Task<bool> CanUserInsertScheme(IRedbScheme scheme, IRedbUser user);
        
        /// <summary>
        /// Check if user can delete object.
        /// </summary>
        Task<bool> CanUserDeleteObject(IRedbObject obj, IRedbUser user);

        // ===== METHODS WITH REDBOBJECT =====
        
        /// <summary>
        /// Check if current user can edit object.
        /// </summary>
        Task<bool> CanUserEditObject(RedbObject obj);
        
        /// <summary>
        /// Check if current user can read object.
        /// </summary>
        Task<bool> CanUserSelectObject(RedbObject obj);
        
        /// <summary>
        /// Check if current user can delete object.
        /// </summary>
        Task<bool> CanUserDeleteObject(RedbObject obj);
        
        /// <summary>
        /// Check if user can edit object.
        /// </summary>
        Task<bool> CanUserEditObject(RedbObject obj, IRedbUser user);
        
        /// <summary>
        /// Check if user can read object.
        /// </summary>
        Task<bool> CanUserSelectObject(RedbObject obj, IRedbUser user);
        
        /// <summary>
        /// Check if user can create objects in object's scheme.
        /// </summary>
        Task<bool> CanUserInsertScheme(RedbObject obj, IRedbUser user);
        
        /// <summary>
        /// Check if user can delete object.
        /// </summary>
        Task<bool> CanUserDeleteObject(RedbObject obj, IRedbUser user);

        // ===== CRUD METHODS FOR PERMISSIONS =====
        
        /// <summary>
        /// Create new permission.
        /// </summary>
        /// <param name="request">Permission data</param>
        /// <param name="currentUser">Current user (for audit)</param>
        /// <returns>Created permission</returns>
        Task<IRedbPermission> CreatePermissionAsync(PermissionRequest request, IRedbUser? currentUser = null);
        
        /// <summary>
        /// Update permission.
        /// </summary>
        /// <param name="permission">Permission to update</param>
        /// <param name="request">New permission data</param>
        /// <param name="currentUser">Current user (for audit)</param>
        /// <returns>Updated permission</returns>
        Task<IRedbPermission> UpdatePermissionAsync(IRedbPermission permission, PermissionRequest request, IRedbUser? currentUser = null);
        
        /// <summary>
        /// Delete permission.
        /// </summary>
        /// <param name="permission">Permission to delete</param>
        /// <param name="currentUser">Current user (for audit)</param>
        /// <returns>true if permission deleted</returns>
        Task<bool> DeletePermissionAsync(IRedbPermission permission, IRedbUser? currentUser = null);
        
        // ===== PERMISSION SEARCH =====
        
        /// <summary>
        /// Get user permissions.
        /// </summary>
        /// <param name="user">User</param>
        /// <returns>List of user permissions</returns>
        Task<List<IRedbPermission>> GetPermissionsByUserAsync(IRedbUser user);
        
        /// <summary>
        /// Get role permissions.
        /// </summary>
        /// <param name="role">Role</param>
        /// <returns>List of role permissions</returns>
        Task<List<IRedbPermission>> GetPermissionsByRoleAsync(IRedbRole role);
        
        /// <summary>
        /// Get permissions for object.
        /// </summary>
        /// <param name="obj">Object</param>
        /// <returns>List of permissions for object</returns>
        Task<List<IRedbPermission>> GetPermissionsByObjectAsync(IRedbObject obj);
        
        /// <summary>
        /// Get permission by ID.
        /// </summary>
        /// <param name="permissionId">Permission ID</param>
        /// <returns>Permission or null if not found</returns>
        Task<IRedbPermission?> GetPermissionByIdAsync(long permissionId);
        
        // ===== PERMISSION MANAGEMENT =====
        
        /// <summary>
        /// Grant permission to user.
        /// </summary>
        /// <param name="user">User</param>
        /// <param name="obj">Object</param>
        /// <param name="actions">Permission actions</param>
        /// <param name="currentUser">Current user (for audit)</param>
        /// <returns>true if permission granted</returns>
        Task<bool> GrantPermissionAsync(IRedbUser user, IRedbObject obj, PermissionAction actions, IRedbUser? currentUser = null);
        
        /// <summary>
        /// Grant permission to role.
        /// </summary>
        /// <param name="role">Role</param>
        /// <param name="obj">Object</param>
        /// <param name="actions">Permission actions</param>
        /// <param name="currentUser">Current user (for audit)</param>
        /// <returns>true if permission granted</returns>
        Task<bool> GrantPermissionAsync(IRedbRole role, IRedbObject obj, PermissionAction actions, IRedbUser? currentUser = null);
        
        /// <summary>
        /// Revoke permission from user.
        /// </summary>
        /// <param name="user">User</param>
        /// <param name="obj">Object</param>
        /// <param name="currentUser">Current user (for audit)</param>
        /// <returns>true if permission revoked</returns>
        Task<bool> RevokePermissionAsync(IRedbUser user, IRedbObject obj, IRedbUser? currentUser = null);
        
        /// <summary>
        /// Revoke permission from role.
        /// </summary>
        /// <param name="role">Role</param>
        /// <param name="obj">Object</param>
        /// <param name="currentUser">Current user (for audit)</param>
        /// <returns>true if permission revoked</returns>
        Task<bool> RevokePermissionAsync(IRedbRole role, IRedbObject obj, IRedbUser? currentUser = null);
        
        /// <summary>
        /// Revoke all user permissions.
        /// </summary>
        /// <param name="user">User</param>
        /// <param name="currentUser">Current user (for audit)</param>
        /// <returns>Number of revoked permissions</returns>
        Task<int> RevokeAllUserPermissionsAsync(IRedbUser user, IRedbUser? currentUser = null);
        
        /// <summary>
        /// Revoke all role permissions.
        /// </summary>
        /// <param name="role">Role</param>
        /// <param name="currentUser">Current user (for audit)</param>
        /// <returns>Number of revoked permissions</returns>
        Task<int> RevokeAllRolePermissionsAsync(IRedbRole role, IRedbUser? currentUser = null);
        
        // ===== EFFECTIVE PERMISSIONS =====
        
        /// <summary>
        /// Get effective user permissions for object (including inheritance and roles).
        /// </summary>
        /// <param name="user">User</param>
        /// <param name="obj">Object</param>
        /// <returns>Effective user permissions</returns>
        Task<EffectivePermissionResult> GetEffectivePermissionsAsync(IRedbUser user, IRedbObject obj);
        
        /// <summary>
        /// Get effective user permissions for multiple objects (batch).
        /// </summary>
        /// <param name="user">User</param>
        /// <param name="objects">Array of objects</param>
        /// <returns>Dictionary object -> effective permissions</returns>
        Task<Dictionary<IRedbObject, EffectivePermissionResult>> GetEffectivePermissionsBatchAsync(IRedbUser user, IRedbObject[] objects);
        
        /// <summary>
        /// Get all effective user permissions.
        /// </summary>
        /// <param name="user">User</param>
        /// <returns>List of all effective user permissions</returns>
        Task<List<EffectivePermissionResult>> GetAllEffectivePermissionsAsync(IRedbUser user);
        
        // ===== STATISTICS =====
        
        /// <summary>
        /// Get total permission count.
        /// </summary>
        /// <returns>Total number of permissions</returns>
        Task<int> GetPermissionCountAsync();
        
        /// <summary>
        /// Get user permission count.
        /// </summary>
        /// <param name="user">User</param>
        /// <returns>Number of user permissions</returns>
        Task<int> GetUserPermissionCountAsync(IRedbUser user);
        
        /// <summary>
        /// Get role permission count.
        /// </summary>
        /// <param name="role">Role</param>
        /// <returns>Number of role permissions</returns>
        Task<int> GetRolePermissionCountAsync(IRedbRole role);

        //=== Low-level access
        Task<bool> CanUserEditObject(long objectId, long userId);

        Task<bool> CanUserSelectObject(long objectId, long userId);

        Task<bool> CanUserInsertScheme(long schemeId, long userId);

        Task<bool> CanUserDeleteObject(long objectId, long userId);
    }
}
