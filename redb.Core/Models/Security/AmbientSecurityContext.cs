using redb.Core.Models.Contracts;
using System;
using System.Threading;

namespace redb.Core.Models.Security
{
    /// <summary>
    /// Ambient security context (Thread-Local)
    /// Allows automatic access to current security context anywhere in code
    /// </summary>
    public static class AmbientSecurityContext
    {
        private static readonly AsyncLocal<IRedbSecurityContext?> _current = new();
        
        /// <summary>
        /// Current security context for this thread
        /// </summary>
        public static IRedbSecurityContext? Current
        {
            get => _current.Value;
            set => _current.Value = value;
        }
        
        /// <summary>
        /// Get current context or create default system context
        /// </summary>
        public static IRedbSecurityContext GetOrCreateDefault()
        {
            return Current ?? RedbSecurityContext.WithAdmin();
        }
        
        /// <summary>
        /// Set context for the duration of an action
        /// </summary>
        public static IDisposable SetContext(IRedbSecurityContext context)
        {
            return new AmbientContextScope(context);
        }
        
        /// <summary>
        /// Create temporary system context
        /// </summary>
        public static IDisposable CreateSystemContext()
        {
            return SetContext(RedbSecurityContext.System());
        }
        
        /// <summary>
        /// Create temporary context with user
        /// </summary>
        public static IDisposable CreateUserContext(IRedbUser user)
        {
            return SetContext(RedbSecurityContext.WithUser(user));
        }
        
        /// <summary>
        /// Create temporary admin context
        /// </summary>
        public static IDisposable CreateAdminContext()
        {
            return SetContext(RedbSecurityContext.WithAdmin());
        }
    }
    
    /// <summary>
    /// Scope for temporary ambient context change
    /// </summary>
    internal class AmbientContextScope : IDisposable
    {
        private readonly IRedbSecurityContext? _previousContext;
        
        public AmbientContextScope(IRedbSecurityContext newContext)
        {
            _previousContext = AmbientSecurityContext.Current;
            AmbientSecurityContext.Current = newContext;
        }
        
        public void Dispose()
        {
            AmbientSecurityContext.Current = _previousContext;
        }
    }
}
