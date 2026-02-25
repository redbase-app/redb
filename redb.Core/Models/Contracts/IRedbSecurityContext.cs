using System;

namespace redb.Core.Models.Contracts
{
    /// <summary>
    /// REDB security context
    /// Manages current user and operation mode
    /// </summary>
    public interface IRedbSecurityContext
    {
        /// <summary>
        /// Current user (can be null for system context)
        /// </summary>
        IRedbUser? CurrentUser { get; }
        
        /// <summary>
        /// System context (no permission checking)
        /// </summary>
        bool IsSystemContext { get; }
        
        /// <summary>
        /// User is authenticated (not system context and user is set)
        /// </summary>
        bool IsAuthenticated { get; }
        
        /// <summary>
        /// Get effective user ID with fallback logic
        /// Returns current user ID or sys ID (0)
        /// </summary>
        long GetEffectiveUserId();
        
        /// <summary>
        /// Get effective user
        /// Returns current user or system user
        /// </summary>
        IRedbUser GetEffectiveUser();
        
        /// <summary>
        /// Set current user
        /// </summary>
        void SetCurrentUser(IRedbUser? user);
        
        /// <summary>
        /// Create temporary system context
        /// </summary>
        IDisposable CreateSystemContext();
    }
}
