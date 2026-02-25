using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using System;

namespace redb.Core.Models.Security
{
    /// <summary>
    /// REDB security context implementation
    /// Manages current user and system context
    /// </summary>
    public class RedbSecurityContext : IRedbSecurityContext
    {
        internal IRedbUser? _currentUser;
        internal bool _isSystemContext;
        
        public IRedbUser? CurrentUser => _currentUser;
        public bool IsSystemContext => _isSystemContext;
        public bool IsAuthenticated => _currentUser != null && !_isSystemContext;
        
        public long GetEffectiveUserId()
        {
            var user = GetEffectiveUser();
            return user.Id;
        }
        
        /// <summary>
        /// Get effective user
        /// </summary>
        public IRedbUser GetEffectiveUser()
        {
            // If there is current user and not system context
            if (_currentUser != null && !_isSystemContext)
            {
                return _currentUser;
            }
            
            // Otherwise return system user
            return RedbUser.SystemUser;
        }
        
        public void SetCurrentUser(IRedbUser? user)
        {
            _currentUser = user;
            _isSystemContext = false; // Reset system mode when setting user
        }
        
        public IDisposable CreateSystemContext()
        {
            return new SystemContextScope(this);
        }
        
        /// <summary>
        /// Create context with specified user
        /// </summary>
        public static RedbSecurityContext WithUser(IRedbUser user)
        {
            var context = new RedbSecurityContext();
            context.SetCurrentUser(user);
            return context;
        }
        
        /// <summary>
        /// Create system context
        /// </summary>
        public static RedbSecurityContext System()
        {
            return new RedbSecurityContext { _isSystemContext = true };
        }
        
        /// <summary>
        /// Create context with admin user
        /// </summary>
        public static RedbSecurityContext WithAdmin()
        {
            var context = new RedbSecurityContext();
            context.SetCurrentUser(RedbUser.SystemUser);
            return context;
        }
    }
    
    /// <summary>
    /// Temporary system context (IDisposable)
    /// </summary>
    internal class SystemContextScope : IDisposable
    {
        private readonly RedbSecurityContext _context;
        private readonly IRedbUser? _previousUser;
        private readonly bool _previousSystemMode;
        
        public SystemContextScope(RedbSecurityContext context)
        {
            _context = context;
            _previousUser = context.CurrentUser;
            _previousSystemMode = context.IsSystemContext;
            
            // Set system mode
            _context._isSystemContext = true;
        }
        
        public void Dispose()
        {
            // Restore previous state
            _context._currentUser = _previousUser;
            _context._isSystemContext = _previousSystemMode;
        }
    }
}
