using System;
using redb.Core.Attributes;
using redb.Core.Models.Entities;

namespace redb.Core.Models.Configuration
{
    /// <summary>
    /// User configuration - personal settings and quotas
    /// All fields are nullable - null means "not overridden, take from higher configuration"
    /// Priority chain: RedbServiceConfiguration (sys) -> Default User Config -> Role Config -> User Config
    /// </summary>
    [RedbScheme("User configuration")]
    public class UserConfigurationProps
    {
        // === CACHE QUOTAS ===
        
        /// <summary>
        /// User object cache quota (number of objects)
        /// null = use value from higher configuration
        /// </summary>
        public int? PropsCacheSize { get; set; }
        
        /// <summary>
        /// User list cache quota
        /// null = use value from higher configuration
        /// </summary>
        public int? ListCacheSize { get; set; }
        
        /// <summary>
        /// Object cache TTL (in minutes)
        /// null = use value from higher configuration
        /// </summary>
        public int? PropsCacheTtlMinutes { get; set; }
        
        /// <summary>
        /// List cache TTL (in minutes)
        /// null = use value from higher configuration
        /// </summary>
        public int? ListCacheTtlMinutes { get; set; }
        
        // === LOADING LIMITS ===
        
        /// <summary>
        /// Maximum object loading depth
        /// null = use value from higher configuration
        /// </summary>
        public int? MaxLoadDepth { get; set; }
        
        /// <summary>
        /// Maximum tree depth
        /// null = use value from higher configuration
        /// </summary>
        public int? MaxTreeDepth { get; set; }
        
        // === PERFORMANCE ===
        
        /// <summary>
        /// Enable lazy loading for Props
        /// null = use value from higher configuration
        /// </summary>
        public bool? EnableLazyLoadingForProps { get; set; }
        
        // === SECURITY ===
        
        /// <summary>
        /// Always check permissions on load
        /// null = use value from higher configuration
        /// </summary>
        public bool? AlwaysCheckPermissionsOnLoad { get; set; }
        
        /// <summary>
        /// Always check permissions on save
        /// null = use value from higher configuration
        /// </summary>
        public bool? AlwaysCheckPermissionsOnSave { get; set; }
        
        // === API QUOTAS ===
        
        /// <summary>
        /// Maximum number of requests per minute
        /// null = no limits or use value from higher configuration
        /// </summary>
        public int? MaxRequestsPerMinute { get; set; }
        
        /// <summary>
        /// Maximum batch operation size
        /// null = use value from higher configuration
        /// </summary>
        public int? MaxBatchSize { get; set; }
        
        // === STRATEGIES ===
        
        /// <summary>
        /// EAV properties save strategy
        /// null = use value from higher configuration
        /// </summary>
        public EavSaveStrategy? EavSaveStrategy { get; set; }
        
        // === METADATA ===
        
        /// <summary>
        /// Configuration description
        /// </summary>
        public string? Description { get; set; }
        
        /// <summary>
        /// Configuration priority (for roles)
        /// Higher priority means more important configuration during merging
        /// User configurations are always applied last
        /// </summary>
        public int Priority { get; set; } = 0;
    }
}

