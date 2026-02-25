using System.Collections.Generic;
using System.Threading.Tasks;
using redb.Core.Models.Contracts;
using redb.Core.Models.Roles;

namespace redb.Core.Providers
{
    /// <summary>
    /// Provider for user role management.
    /// Provides CRUD operations for roles and user-role relationship management.
    /// </summary>
    public interface IRoleProvider
    {
        // === ROLE CRUD ===
        
        /// <summary>
        /// Create new role.
        /// </summary>
        /// <param name="request">Role creation data</param>
        /// <param name="currentUser">Current user (for audit)</param>
        /// <returns>Created role</returns>
        Task<IRedbRole> CreateRoleAsync(CreateRoleRequest request, IRedbUser? currentUser = null);
        
        /// <summary>
        /// Update role.
        /// </summary>
        /// <param name="role">Role to update</param>
        /// <param name="newName">New role name</param>
        /// <param name="currentUser">Current user (for audit)</param>
        /// <returns>Updated role</returns>
        Task<IRedbRole> UpdateRoleAsync(IRedbRole role, string newName, IRedbUser? currentUser = null);
        
        /// <summary>
        /// Delete role.
        /// All related permissions are also deleted (cascade).
        /// </summary>
        /// <param name="role">Role to delete</param>
        /// <param name="currentUser">Current user (for audit)</param>
        /// <returns>true if role deleted</returns>
        Task<bool> DeleteRoleAsync(IRedbRole role, IRedbUser? currentUser = null);
        
        // === ROLE SEARCH ===
        
        /// <summary>
        /// Get role by ID.
        /// </summary>
        /// <param name="roleId">Role ID</param>
        /// <returns>Role or null if not found</returns>
        Task<IRedbRole?> GetRoleByIdAsync(long roleId);
        
        /// <summary>
        /// Get role by name.
        /// </summary>
        /// <param name="roleName">Role name</param>
        /// <returns>Role or null if not found</returns>
        Task<IRedbRole?> GetRoleByNameAsync(string roleName);
        
        /// <summary>
        /// Load role by ID (throws exception if not found).
        /// </summary>
        /// <param name="roleId">Role ID</param>
        /// <returns>Role</returns>
        /// <exception cref="ArgumentException">If role not found</exception>
        Task<IRedbRole> LoadRoleAsync(long roleId);
        
        /// <summary>
        /// Load role by name (throws exception if not found).
        /// </summary>
        /// <param name="roleName">Role name</param>
        /// <returns>Role</returns>
        /// <exception cref="ArgumentException">If role not found</exception>
        Task<IRedbRole> LoadRoleAsync(string roleName);
        
        /// <summary>
        /// Get all roles.
        /// </summary>
        /// <returns>List of all roles</returns>
        Task<List<IRedbRole>> GetRolesAsync();
        
        // === USER-ROLE MANAGEMENT ===
        
        /// <summary>
        /// Assign role to user.
        /// </summary>
        /// <param name="user">User</param>
        /// <param name="role">Role</param>
        /// <param name="currentUser">Current user (for audit)</param>
        /// <returns>true if role assigned</returns>
        Task<bool> AssignUserToRoleAsync(IRedbUser user, IRedbRole role, IRedbUser? currentUser = null);
        
        /// <summary>
        /// Remove role from user.
        /// </summary>
        /// <param name="user">User</param>
        /// <param name="role">Role</param>
        /// <param name="currentUser">Current user (for audit)</param>
        /// <returns>true if role removed</returns>
        Task<bool> RemoveUserFromRoleAsync(IRedbUser user, IRedbRole role, IRedbUser? currentUser = null);
        
        /// <summary>
        /// Set user roles (replace all existing).
        /// </summary>
        /// <param name="user">User</param>
        /// <param name="roles">Array of roles</param>
        /// <param name="currentUser">Current user (for audit)</param>
        /// <returns>true if roles set</returns>
        Task<bool> SetUserRolesAsync(IRedbUser user, IRedbRole[] roles, IRedbUser? currentUser = null);
        
        /// <summary>
        /// Get user roles.
        /// </summary>
        /// <param name="user">User</param>
        /// <returns>List of user roles</returns>
        Task<List<IRedbRole>> GetUserRolesAsync(IRedbUser user);
        
        /// <summary>
        /// Get role users.
        /// </summary>
        /// <param name="role">Role</param>
        /// <returns>List of users with this role</returns>
        Task<List<IRedbUser>> GetRoleUsersAsync(IRedbRole role);
        
        /// <summary>
        /// Check if user has role.
        /// </summary>
        /// <param name="user">User</param>
        /// <param name="role">Role</param>
        /// <returns>true if user has role</returns>
        Task<bool> UserHasRoleAsync(IRedbUser user, IRedbRole role);
        
        // === VALIDATION ===
        
        /// <summary>
        /// Check role name availability.
        /// </summary>
        /// <param name="roleName">Role name to check</param>
        /// <param name="excludeRole">Role to exclude (for update)</param>
        /// <returns>true if role name available</returns>
        Task<bool> IsRoleNameAvailableAsync(string roleName, IRedbRole? excludeRole = null);
        
        // === STATISTICS ===
        
        /// <summary>
        /// Get role count.
        /// </summary>
        /// <returns>Number of roles</returns>
        Task<int> GetRoleCountAsync();
        
        /// <summary>
        /// Get user count in role.
        /// </summary>
        /// <param name="role">Role</param>
        /// <returns>Number of users in role</returns>
        Task<int> GetRoleUserCountAsync(IRedbRole role);
        
        /// <summary>
        /// Get role statistics (role -> user count).
        /// </summary>
        /// <returns>Dictionary role -> user count</returns>
        Task<Dictionary<IRedbRole, int>> GetRoleStatisticsAsync();
        
        // === CONFIGURATION MANAGEMENT ===
        
        /// <summary>
        /// Get role configuration ID.
        /// </summary>
        /// <param name="roleId">Role ID</param>
        /// <returns>Configuration ID or null if not set</returns>
        Task<long?> GetRoleConfigurationIdAsync(long roleId);
        
        /// <summary>
        /// Set role configuration.
        /// </summary>
        /// <param name="roleId">Role ID</param>
        /// <param name="configId">Configuration ID (RedbObject&lt;UserConfigurationProps&gt;) or null to reset</param>
        Task SetRoleConfigurationAsync(long roleId, long? configId);
    }
}
