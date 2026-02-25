using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using redb.Core.Models.Contracts;
using redb.Core.Models.Users;

namespace redb.Core.Providers
{
    /// <summary>
    /// Provider for user management.
    /// Provides CRUD operations and business logic for working with users.
    /// </summary>
    public interface IUserProvider
    {
        // === CRUD OPERATIONS ===
        
        /// <summary>
        /// Create new user.
        /// </summary>
        /// <param name="request">User creation data</param>
        /// <param name="currentUser">Current user (for audit)</param>
        /// <returns>Created user</returns>
        Task<IRedbUser> CreateUserAsync(CreateUserRequest request, IRedbUser? currentUser = null);
        
        /// <summary>
        /// Update user data.
        /// </summary>
        /// <param name="user">User to update</param>
        /// <param name="request">New user data</param>
        /// <param name="currentUser">Current user (for audit)</param>
        /// <returns>Updated user</returns>
        Task<IRedbUser> UpdateUserAsync(IRedbUser user, UpdateUserRequest request, IRedbUser? currentUser = null);
        
        /// <summary>
        /// Delete user (soft delete - deactivation).
        /// System users (ID 0, 1) cannot be deleted.
        /// </summary>
        /// <param name="user">User to delete</param>
        /// <param name="currentUser">Current user (for audit)</param>
        /// <returns>true if user deleted</returns>
        Task<bool> DeleteUserAsync(IRedbUser user, IRedbUser? currentUser = null);
        
        // === SEARCH AND RETRIEVAL ===
        
        /// <summary>
        /// Get user by ID.
        /// </summary>
        /// <param name="userId">User ID</param>
        /// <returns>User or null if not found</returns>
        Task<IRedbUser?> GetUserByIdAsync(long userId);
        
        /// <summary>
        /// Get user by login.
        /// </summary>
        /// <param name="login">User login</param>
        /// <returns>User or null if not found</returns>
        Task<IRedbUser?> GetUserByLoginAsync(string login);
        
        /// <summary>
        /// Load user by login (throws exception if not found).
        /// </summary>
        /// <param name="login">User login</param>
        /// <returns>User</returns>
        /// <exception cref="ArgumentException">If user not found</exception>
        Task<IRedbUser> LoadUserAsync(string login);
        
        /// <summary>
        /// Load user by ID (throws exception if not found).
        /// </summary>
        /// <param name="userId">User ID</param>
        /// <returns>User</returns>
        /// <exception cref="ArgumentException">If user not found</exception>
        Task<IRedbUser> LoadUserAsync(long userId);
        
        /// <summary>
        /// Get list of users with filtering.
        /// </summary>
        /// <param name="criteria">Search criteria (can be null)</param>
        /// <returns>List of users</returns>
        Task<List<IRedbUser>> GetUsersAsync(UserSearchCriteria? criteria = null);
        
        // === AUTHENTICATION ===
        
        /// <summary>
        /// Validate user login and password.
        /// </summary>
        /// <param name="login">Login</param>
        /// <param name="password">Password (plain text)</param>
        /// <returns>User if credentials valid, null if invalid</returns>
        Task<IRedbUser?> ValidateUserAsync(string login, string password);
        
        /// <summary>
        /// Change user password.
        /// </summary>
        /// <param name="user">User</param>
        /// <param name="currentPassword">Current password (for verification)</param>
        /// <param name="newPassword">New password</param>
        /// <param name="currentUser">Current user (for audit)</param>
        /// <returns>true if password changed</returns>
        Task<bool> ChangePasswordAsync(IRedbUser user, string currentPassword, string newPassword, IRedbUser? currentUser = null);
        
        /// <summary>
        /// Set new password for user (without checking old password).
        /// For administrators only.
        /// </summary>
        /// <param name="user">User</param>
        /// <param name="newPassword">New password</param>
        /// <param name="currentUser">Current user (for audit)</param>
        /// <returns>true if password set</returns>
        Task<bool> SetPasswordAsync(IRedbUser user, string newPassword, IRedbUser? currentUser = null);
        
        // === STATUS MANAGEMENT ===
        
        /// <summary>
        /// Activate user.
        /// </summary>
        /// <param name="user">User</param>
        /// <param name="currentUser">Current user (for audit)</param>
        /// <returns>true if user activated</returns>
        Task<bool> EnableUserAsync(IRedbUser user, IRedbUser? currentUser = null);
        
        /// <summary>
        /// Deactivate user.
        /// System users (ID 0, 1) cannot be deactivated.
        /// </summary>
        /// <param name="user">User</param>
        /// <param name="currentUser">Current user (for audit)</param>
        /// <returns>true if user deactivated</returns>
        Task<bool> DisableUserAsync(IRedbUser user, IRedbUser? currentUser = null);
        
        // === VALIDATION ===
        
        /// <summary>
        /// Validate user data correctness.
        /// </summary>
        /// <param name="request">Data to validate</param>
        /// <returns>Validation result</returns>
        Task<UserValidationResult> ValidateUserDataAsync(CreateUserRequest request);
        
        /// <summary>
        /// Check login availability.
        /// </summary>
        /// <param name="login">Login to check</param>
        /// <param name="excludeUserId">User ID to exclude (for update)</param>
        /// <returns>true if login available</returns>
        Task<bool> IsLoginAvailableAsync(string login, long? excludeUserId = null);
        
        // === STATISTICS ===
        
        /// <summary>
        /// Get user count.
        /// </summary>
        /// <param name="includeDisabled">Include deactivated users</param>
        /// <returns>Number of users</returns>
        Task<int> GetUserCountAsync(bool includeDisabled = false);
        
        /// <summary>
        /// Get active user count for period.
        /// </summary>
        /// <param name="fromDate">Start date</param>
        /// <param name="toDate">End date</param>
        /// <returns>Number of active users</returns>
        Task<int> GetActiveUserCountAsync(DateTimeOffset fromDate, DateTimeOffset toDate);
        
        // === CONFIGURATION MANAGEMENT ===
        
        /// <summary>
        /// Get user configuration ID.
        /// </summary>
        /// <param name="userId">User ID</param>
        /// <returns>Configuration ID or null if not set</returns>
        Task<long?> GetUserConfigurationIdAsync(long userId);
        
        /// <summary>
        /// Set user configuration.
        /// </summary>
        /// <param name="userId">User ID</param>
        /// <param name="configId">Configuration ID (RedbObject&lt;UserConfigurationProps&gt;) or null to reset</param>
        Task SetUserConfigurationAsync(long userId, long? configId);
        
        /// <summary>
        /// Get user roles.
        /// Used for building effective configuration.
        /// </summary>
        /// <param name="userId">User ID</param>
        /// <returns>List of user roles</returns>
        Task<List<IRedbRole>> GetUserRolesAsync(long userId);
    }
}
