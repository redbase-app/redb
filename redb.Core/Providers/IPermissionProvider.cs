using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Core.Models.Enums;
using redb.Core.Models.Permissions;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
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
        Task<bool> CanUserEditObject(IRedbObject obj,
        CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Check if current user can read object.
        /// </summary>
        Task<bool> CanUserSelectObject(IRedbObject obj,
        CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Check if current user can create objects in scheme.
        /// </summary>
        Task<bool> CanUserInsertScheme(IRedbScheme scheme,
        CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Check if current user can delete object.
        /// </summary>
        Task<bool> CanUserDeleteObject(IRedbObject obj,
        CancellationToken cancellationToken = default);

        // ===== OVERLOADS WITH EXPLICIT USER =====
        
        /// <summary>
        /// Get IDs of objects readable by user.
        /// </summary>
        IQueryable<long> GetReadableObjectIds(IRedbUser user);
        
        /// <summary>
        /// Check if user can edit object.
        /// </summary>
        Task<bool> CanUserEditObject(IRedbObject obj, IRedbUser user,
        CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Check if user can read object.
        /// </summary>
        Task<bool> CanUserSelectObject(IRedbObject obj, IRedbUser user,
        CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Check if user can create objects in scheme.
        /// </summary>
        Task<bool> CanUserInsertScheme(IRedbScheme scheme, IRedbUser user,
        CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Check if user can delete object.
        /// </summary>
        Task<bool> CanUserDeleteObject(IRedbObject obj, IRedbUser user,
        CancellationToken cancellationToken = default);

        // ===== METHODS WITH REDBOBJECT =====
        
        /// <summary>
        /// Check if current user can edit object.
        /// </summary>
        Task<bool> CanUserEditObject(RedbObject obj,
        CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Check if current user can read object.
        /// </summary>
        Task<bool> CanUserSelectObject(RedbObject obj,
        CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Check if current user can delete object.
        /// </summary>
        Task<bool> CanUserDeleteObject(RedbObject obj,
        CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Check if user can edit object.
        /// </summary>
        Task<bool> CanUserEditObject(RedbObject obj, IRedbUser user,
        CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Check if user can read object.
        /// </summary>
        Task<bool> CanUserSelectObject(RedbObject obj, IRedbUser user,
        CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Check if user can create objects in object's scheme.
        /// </summary>
        Task<bool> CanUserInsertScheme(RedbObject obj, IRedbUser user,
        CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Check if user can delete object.
        /// </summary>
        Task<bool> CanUserDeleteObject(RedbObject obj, IRedbUser user,
        CancellationToken cancellationToken = default);

        // ===== CRUD METHODS FOR PERMISSIONS =====
        
        /// <summary>
        /// Create new permission.
        /// </summary>
        /// <param name="request">Permission data</param>
        /// <param name="currentUser">Current user (for audit)</param>
        /// <returns>Created permission</returns>
        /// <param name="cancellationToken">Cancels the operation (OCE only; a rollback in flight always completes).</param>
        Task<IRedbPermission> CreatePermissionAsync(PermissionRequest request, IRedbUser? currentUser = null,
        CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Update permission.
        /// </summary>
        /// <param name="permission">Permission to update</param>
        /// <param name="request">New permission data</param>
        /// <param name="currentUser">Current user (for audit)</param>
        /// <returns>Updated permission</returns>
        /// <param name="cancellationToken">Cancels the operation (OCE only; a rollback in flight always completes).</param>
        Task<IRedbPermission> UpdatePermissionAsync(IRedbPermission permission, PermissionRequest request, IRedbUser? currentUser = null,
        CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Delete permission.
        /// </summary>
        /// <param name="permission">Permission to delete</param>
        /// <param name="currentUser">Current user (for audit)</param>
        /// <returns>true if permission deleted</returns>
        /// <param name="cancellationToken">Cancels the operation (OCE only; a rollback in flight always completes).</param>
        Task<bool> DeletePermissionAsync(IRedbPermission permission, IRedbUser? currentUser = null,
        CancellationToken cancellationToken = default);
        
        // ===== PERMISSION SEARCH =====
        
        /// <summary>
        /// Get user permissions.
        /// </summary>
        /// <param name="user">User</param>
        /// <returns>List of user permissions</returns>
        /// <param name="cancellationToken">Cancels the operation (OCE only; a rollback in flight always completes).</param>
        Task<List<IRedbPermission>> GetPermissionsByUserAsync(IRedbUser user,
        CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Get role permissions.
        /// </summary>
        /// <param name="role">Role</param>
        /// <returns>List of role permissions</returns>
        /// <param name="cancellationToken">Cancels the operation (OCE only; a rollback in flight always completes).</param>
        Task<List<IRedbPermission>> GetPermissionsByRoleAsync(IRedbRole role,
        CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Get permissions for object.
        /// </summary>
        /// <param name="obj">Object</param>
        /// <returns>List of permissions for object</returns>
        /// <param name="cancellationToken">Cancels the operation (OCE only; a rollback in flight always completes).</param>
        Task<List<IRedbPermission>> GetPermissionsByObjectAsync(IRedbObject obj,
        CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Get permission by ID.
        /// </summary>
        /// <param name="permissionId">Permission ID</param>
        /// <returns>Permission or null if not found</returns>
        /// <param name="cancellationToken">Cancels the operation (OCE only; a rollback in flight always completes).</param>
        Task<IRedbPermission?> GetPermissionByIdAsync(long permissionId,
        CancellationToken cancellationToken = default);
        
        // ===== PERMISSION MANAGEMENT =====
        
        /// <summary>
        /// Grant permission to user.
        /// </summary>
        /// <param name="user">User</param>
        /// <param name="obj">Object</param>
        /// <param name="actions">Permission actions</param>
        /// <param name="currentUser">Current user (for audit)</param>
        /// <returns>true if permission granted</returns>
        /// <param name="cancellationToken">Cancels the operation (OCE only; a rollback in flight always completes).</param>
        Task<bool> GrantPermissionAsync(IRedbUser user, IRedbObject obj, PermissionAction actions, IRedbUser? currentUser = null,
        CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Grant permission to role.
        /// </summary>
        /// <param name="role">Role</param>
        /// <param name="obj">Object</param>
        /// <param name="actions">Permission actions</param>
        /// <param name="currentUser">Current user (for audit)</param>
        /// <returns>true if permission granted</returns>
        /// <param name="cancellationToken">Cancels the operation (OCE only; a rollback in flight always completes).</param>
        Task<bool> GrantPermissionAsync(IRedbRole role, IRedbObject obj, PermissionAction actions, IRedbUser? currentUser = null,
        CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Revoke permission from user.
        /// </summary>
        /// <param name="user">User</param>
        /// <param name="obj">Object</param>
        /// <param name="currentUser">Current user (for audit)</param>
        /// <returns>true if permission revoked</returns>
        /// <param name="cancellationToken">Cancels the operation (OCE only; a rollback in flight always completes).</param>
        Task<bool> RevokePermissionAsync(IRedbUser user, IRedbObject obj, IRedbUser? currentUser = null,
        CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Revoke permission from role.
        /// </summary>
        /// <param name="role">Role</param>
        /// <param name="obj">Object</param>
        /// <param name="currentUser">Current user (for audit)</param>
        /// <returns>true if permission revoked</returns>
        /// <param name="cancellationToken">Cancels the operation (OCE only; a rollback in flight always completes).</param>
        Task<bool> RevokePermissionAsync(IRedbRole role, IRedbObject obj, IRedbUser? currentUser = null,
        CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Revoke all user permissions.
        /// </summary>
        /// <param name="user">User</param>
        /// <param name="currentUser">Current user (for audit)</param>
        /// <returns>Number of revoked permissions</returns>
        /// <param name="cancellationToken">Cancels the operation (OCE only; a rollback in flight always completes).</param>
        Task<int> RevokeAllUserPermissionsAsync(IRedbUser user, IRedbUser? currentUser = null,
        CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Revoke all role permissions.
        /// </summary>
        /// <param name="role">Role</param>
        /// <param name="currentUser">Current user (for audit)</param>
        /// <returns>Number of revoked permissions</returns>
        /// <param name="cancellationToken">Cancels the operation (OCE only; a rollback in flight always completes).</param>
        Task<int> RevokeAllRolePermissionsAsync(IRedbRole role, IRedbUser? currentUser = null,
        CancellationToken cancellationToken = default);
        
        // ===== EFFECTIVE PERMISSIONS =====
        
        /// <summary>
        /// Get effective user permissions for object (including inheritance and roles).
        /// </summary>
        /// <param name="user">User</param>
        /// <param name="obj">Object</param>
        /// <returns>Effective user permissions</returns>
        /// <param name="cancellationToken">Cancels the operation (OCE only; a rollback in flight always completes).</param>
        Task<EffectivePermissionResult> GetEffectivePermissionsAsync(IRedbUser user, IRedbObject obj,
        CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Get effective user permissions for multiple objects (batch).
        /// </summary>
        /// <param name="user">User</param>
        /// <param name="objects">Array of objects</param>
        /// <returns>Dictionary object -> effective permissions</returns>
        /// <param name="cancellationToken">Cancels the operation (OCE only; a rollback in flight always completes).</param>
        Task<Dictionary<IRedbObject, EffectivePermissionResult>> GetEffectivePermissionsBatchAsync(IRedbUser user, IRedbObject[] objects,
        CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Get all effective user permissions.
        /// </summary>
        /// <param name="user">User</param>
        /// <returns>List of all effective user permissions</returns>
        /// <param name="cancellationToken">Cancels the operation (OCE only; a rollback in flight always completes).</param>
        Task<List<EffectivePermissionResult>> GetAllEffectivePermissionsAsync(IRedbUser user,
        CancellationToken cancellationToken = default);
        
        // ===== STATISTICS =====
        
        /// <summary>
        /// Get total permission count.
        /// </summary>
        /// <returns>Total number of permissions</returns>
        Task<int> GetPermissionCountAsync(CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Get user permission count.
        /// </summary>
        /// <param name="user">User</param>
        /// <returns>Number of user permissions</returns>
        /// <param name="cancellationToken">Cancels the operation (OCE only; a rollback in flight always completes).</param>
        Task<int> GetUserPermissionCountAsync(IRedbUser user,
        CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Get role permission count.
        /// </summary>
        /// <param name="role">Role</param>
        /// <returns>Number of role permissions</returns>
        /// <param name="cancellationToken">Cancels the operation (OCE only; a rollback in flight always completes).</param>
        Task<int> GetRolePermissionCountAsync(IRedbRole role,
        CancellationToken cancellationToken = default);

        //=== Low-level access
        Task<bool> CanUserEditObject(long objectId, long userId,
        CancellationToken cancellationToken = default);

        Task<bool> CanUserSelectObject(long objectId, long userId,
        CancellationToken cancellationToken = default);

        /// <summary>
        /// Synchronous <see cref="CanUserSelectObject(long, long, CancellationToken)"/> for the
        /// thread-pool-free lazy path (the sync getter of RedbListItem.Object). The default falls
        /// back to blocking on the async form; the in-tree base provider overrides with a true
        /// sync SQL call on the calling thread.
        /// </summary>
        bool CanUserSelectObjectSync(long objectId, long userId)
            => CanUserSelectObject(objectId, userId).ConfigureAwait(false).GetAwaiter().GetResult();

        Task<bool> CanUserInsertScheme(long schemeId, long userId,
        CancellationToken cancellationToken = default);

        Task<bool> CanUserDeleteObject(long objectId, long userId,
        CancellationToken cancellationToken = default);
    }
}
