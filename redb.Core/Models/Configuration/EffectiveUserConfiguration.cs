using System;
using System.Collections.Generic;

namespace redb.Core.Models.Configuration
{
    /// <summary>
    /// Effective user configuration (result of merging all configurations)
    /// All values are resolved (no null), obtained from chain:
    /// RedbServiceConfiguration -> Default User Config -> Role Config -> User Config
    /// </summary>
    public class EffectiveUserConfiguration
    {
        // === CACHE QUOTAS ===
        
        /// <summary>
        /// User object cache quota
        /// null = no limits (for sys user)
        /// </summary>
        public int? PropsCacheSize { get; set; }
        
        /// <summary>
        /// User list cache quota
        /// null = no limits (for sys user)
        /// </summary>
        public int? ListCacheSize { get; set; }
        
        /// <summary>
        /// Object cache TTL
        /// </summary>
        public TimeSpan PropsCacheTtl { get; set; }
        
        /// <summary>
        /// List cache TTL
        /// </summary>
        public TimeSpan ListCacheTtl { get; set; }
        
        // === LOADING LIMITS ===
        
        /// <summary>
        /// Maximum object loading depth
        /// </summary>
        public int MaxLoadDepth { get; set; }
        
        /// <summary>
        /// Maximum tree depth
        /// </summary>
        public int MaxTreeDepth { get; set; }
        
        // === PERFORMANCE ===
        
        /// <summary>
        /// Enable lazy loading for Props
        /// </summary>
        public bool EnableLazyLoadingForProps { get; set; }
        
        // === SECURITY ===
        
        /// <summary>
        /// Always check permissions on load
        /// </summary>
        public bool AlwaysCheckPermissionsOnLoad { get; set; }
        
        /// <summary>
        /// Always check permissions on save
        /// </summary>
        public bool AlwaysCheckPermissionsOnSave { get; set; }
        
        // === API QUOTAS ===
        
        /// <summary>
        /// Maximum number of requests per minute
        /// null = no limits
        /// </summary>
        public int? MaxRequestsPerMinute { get; set; }
        
        /// <summary>
        /// Maximum batch operation size
        /// </summary>
        public int MaxBatchSize { get; set; }
        
        // === STRATEGIES ===
        
        /// <summary>
        /// EAV properties save strategy
        /// </summary>
        public EavSaveStrategy EavSaveStrategy { get; set; }
        
        // === METADATA ===
        
        /// <summary>
        /// List of configuration parameter sources
        /// Shows where each value was taken from
        /// </summary>
        public List<ConfigurationSource> Sources { get; set; } = new();
        
        /// <summary>
        /// User ID for which the configuration was built
        /// </summary>
        public long UserId { get; set; }
        
        /// <summary>
        /// Effective configuration creation time (for caching)
        /// </summary>
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    }
    
    /// <summary>
    /// Configuration parameter source
    /// Used for debugging and understanding where value was taken from
    /// </summary>
    public class ConfigurationSource
    {
        /// <summary>
        /// Parameter name
        /// </summary>
        public string ParameterName { get; set; } = string.Empty;
        
        /// <summary>
        /// Value source
        /// Examples: "RedbServiceConfiguration", "Default User Config", "Role:Admin", "User"
        /// </summary>
        public string Source { get; set; } = string.Empty;
        
        /// <summary>
        /// Source priority (higher means more important)
        /// </summary>
        public int Priority { get; set; }
        
        /// <summary>
        /// Parameter value (as string for debugging)
        /// </summary>
        public string? Value { get; set; }
    }
}

