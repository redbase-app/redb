using System;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Serialization;
using redb.Core.Configuration;
using redb.Core.Caching;

namespace redb.Core.Models.Configuration
{
    /// <summary>
    /// RedbService behavior configuration.
    /// </summary>
    public class RedbServiceConfiguration
    {
        // === CONNECTION SETTINGS ===
        
        /// <summary>
        /// PostgreSQL connection string. Required for automatic IRedbContext registration.
        /// If not set, user must register IRedbContext manually.
        /// </summary>
        public string? ConnectionString { get; set; }
        
        /// <summary>
        /// Cache domain name for isolating scheme caches between different databases.
        /// If not set, computed from connection string hash.
        /// Use explicit name when connecting to same DB from multiple services.
        /// </summary>
        public string? CacheDomain { get; set; }
        
        // === OBJECT DELETION SETTINGS ===

        /// <summary>
        /// Strategy for handling ID after object deletion.
        /// </summary>
        [JsonConverter(typeof(ObjectIdResetStrategyJsonConverter))]
        public ObjectIdResetStrategy IdResetStrategy { get; set; } = ObjectIdResetStrategy.Manual;

        /// <summary>
        /// Strategy for handling non-existent objects on UPDATE.
        /// </summary>
        [JsonConverter(typeof(MissingObjectStrategyJsonConverter))]
        public MissingObjectStrategy MissingObjectStrategy { get; set; } = MissingObjectStrategy.AutoSwitchToInsert;

        // === DEFAULT SECURITY SETTINGS ===

        /// <summary>
        /// Check permissions by default when loading objects.
        /// </summary>
        public bool DefaultCheckPermissionsOnLoad { get; set; } = false;

        /// <summary>
        /// Check permissions by default when saving objects.
        /// </summary>
        public bool DefaultCheckPermissionsOnSave { get; set; } = false;

        /// <summary>
        /// Check permissions by default when deleting objects.
        /// </summary>
        public bool DefaultCheckPermissionsOnDelete { get; set; } = true;

        /// <summary>
        /// Check permissions by default when executing queries.
        /// </summary>
        public bool DefaultCheckPermissionsOnQuery { get; set; } = false;

        // === SCHEME SETTINGS ===

        /// <summary>
        /// Strictly delete extra fields when synchronizing schemes by default.
        /// </summary>
        public bool DefaultStrictDeleteExtra { get; set; } = true;

        /// <summary>
        /// Automatically synchronize schemes when saving objects.
        /// </summary>
        public bool AutoSyncSchemesOnSave { get; set; } = true;

        // === OBJECT LOADING SETTINGS ===

        /// <summary>
        /// Default depth for loading nested objects.
        /// </summary>
        private int _defaultLoadDepth = 10;
        public int DefaultLoadDepth 
        { 
            get => _defaultLoadDepth;
            set 
            {
                if (value < 1)
                    throw new ArgumentOutOfRangeException(nameof(DefaultLoadDepth), "Minimum 1");
                if (value > 100)
                    throw new ArgumentOutOfRangeException(nameof(DefaultLoadDepth), "Maximum 100");
                _defaultLoadDepth = value;
            }
        }

        /// <summary>
        /// Maximum depth for tree structures.
        /// </summary>
        private int _defaultMaxTreeDepth = 50;
        public int DefaultMaxTreeDepth 
        { 
            get => _defaultMaxTreeDepth;
            set 
            {
                if (value < 1)
                    throw new ArgumentOutOfRangeException(nameof(DefaultMaxTreeDepth), "Minimum 1");
                if (value > 1000)
                    throw new ArgumentOutOfRangeException(nameof(DefaultMaxTreeDepth), "Maximum 1000");
                _defaultMaxTreeDepth = value;
            }
        }

        /// <summary>
        /// Throw exception if object not found in LoadAsync.
        /// true (default) - throws InvalidOperationException
        /// false - returns null for single object, skips in batch load
        /// </summary>
        public bool ThrowOnObjectNotFound { get; set; } = false;

        // === PERFORMANCE SETTINGS ===

        /// <summary>
        /// Enable lazy loading for RedbObject Props.
        /// true = Props loaded on demand when accessing obj.Props
        /// false = Props in main JSON (everything at once)
        /// Default is false (backward compatibility)
        /// </summary>
        public bool EnableLazyLoadingForProps { get; set; } = false;

        /// <summary>
        /// Enable transparent Props object caching.
        /// Works only when EnableLazyLoadingForProps = true
        /// Default is false
        /// </summary>
        public bool EnablePropsCache { get; set; } = false;

        /// <summary>
        /// Maximum number of objects in Props cache.
        /// On overflow - simple eviction (remove first)
        /// </summary>
        private int _propsCacheMaxSize = 10000;
        public int PropsCacheMaxSize 
        { 
            get => _propsCacheMaxSize;
            set 
            {
                if (value <= 0)
                    throw new ArgumentOutOfRangeException(nameof(PropsCacheMaxSize), "Must be greater than 0");
                if (value > 10_000_000)
                    throw new ArgumentOutOfRangeException(nameof(PropsCacheMaxSize), "Maximum 10,000,000");
                _propsCacheMaxSize = value;
            }
        }

        /// <summary>
        /// Lifetime of Props cache entry.
        /// After expiration - entry considered stale
        /// </summary>
        private TimeSpan _propsCacheTtl = TimeSpan.FromMinutes(60);
        public TimeSpan PropsCacheTtl 
        { 
            get => _propsCacheTtl;
            set 
            {
                if (value <= TimeSpan.Zero)
                    throw new ArgumentOutOfRangeException(nameof(PropsCacheTtl), "TTL must be greater than 0");
                if (value > TimeSpan.FromDays(7))
                    throw new ArgumentOutOfRangeException(nameof(PropsCacheTtl), "Maximum 7 days");
                _propsCacheTtl = value;
            }
        }

        /// <summary>
        /// Skip hash validation in DB before cache access.
        /// true - for monolithic apps (faster, trust cache)
        /// false - for distributed systems (safer, check freshness)
        /// Default false (safe)
        /// </summary>
        public bool SkipHashValidationOnCacheCheck { get; set; } = false;

        // === LIST CACHE SETTINGS ===

        /// <summary>
        /// Enable caching of lists and their items.
        /// </summary>
        public bool EnableListCache { get; set; } = true;

        /// <summary>
        /// Lifetime of list cache entry (TTL).
        /// Eventual consistency: other clients see changes after TTL
        /// </summary>
        private TimeSpan _listCacheTtl = TimeSpan.FromMinutes(5);
        public TimeSpan ListCacheTtl 
        { 
            get => _listCacheTtl;
            set 
            {
                if (value <= TimeSpan.Zero)
                    throw new ArgumentOutOfRangeException(nameof(ListCacheTtl), "TTL must be greater than 0");
                if (value > TimeSpan.FromDays(7))
                    throw new ArgumentOutOfRangeException(nameof(ListCacheTtl), "Maximum 7 days");
                _listCacheTtl = value;
            }
        }

        /// <summary>
        /// Enable scheme metadata caching (OBSOLETE - use MetadataCache).
        /// </summary>
        //[Obsolete("Use MetadataCache.EnableMetadataCache instead")]
        public bool EnableMetadataCache { get; set; } = true;

        /// <summary>
        /// Metadata cache lifetime in minutes (OBSOLETE - use MetadataCache).
        /// </summary>
        //[Obsolete("Use MetadataCache.Schemes.LifetimeMinutes instead")]
        private int _metadataCacheLifetimeMinutes = 30;
        public int MetadataCacheLifetimeMinutes 
        { 
            get => _metadataCacheLifetimeMinutes;
            set 
            {
                if (value < 1)
                    throw new ArgumentOutOfRangeException(nameof(MetadataCacheLifetimeMinutes), "Minimum 1 minute");
                if (value > 10080)
                    throw new ArgumentOutOfRangeException(nameof(MetadataCacheLifetimeMinutes), "Maximum 10080 minutes (7 days)");
                _metadataCacheLifetimeMinutes = value;
            }
        }

        /// <summary>
        /// Warm up scheme metadata cache during RedbService initialization.
        /// Calls warmup_all_metadata_caches() in InitializeAsync()
        /// Recommended for production (predictable performance)
        /// Default: true
        /// </summary>
        public bool WarmupMetadataCacheOnInit { get; set; } = true;

        // === METADATA CACHING SETTINGS (NEW) ===

        /// <summary>
        /// Extended metadata caching configuration.
        /// </summary>
        //public MetadataCacheConfiguration MetadataCache { get; set; } = new();

        // === VALIDATION SETTINGS ===

        /// <summary>
        /// Enable scheme validation before synchronization.
        /// </summary>
        public bool EnableSchemaValidation { get; set; } = true;

        /// <summary>
        /// Enable data validation when saving.
        /// </summary>
        public bool EnableDataValidation { get; set; } = true;

        // === AUDIT SETTINGS ===

        /// <summary>
        /// Automatically set modification date when saving.
        /// </summary>
        public bool AutoSetModifyDate { get; set; } = true;

        /// <summary>
        /// Automatically recompute hash when saving.
        /// </summary>
        public bool AutoRecomputeHash { get; set; } = true;

        // === SECURITY CONTEXT SETTINGS ===

        // Security context priority removed - simple GetEffectiveUser() logic used

        /// <summary>
        /// System user ID for operations without permission checks.
        /// </summary>
        public long SystemUserId { get; set; } = 0;

        // === EAV SAVE SETTINGS ===

        /// <summary>
        /// EAV properties save strategy.
        /// </summary>
        [JsonConverter(typeof(EavSaveStrategyJsonConverter))]
        public EavSaveStrategy EavSaveStrategy { get; set; } = EavSaveStrategy.DeleteInsert;





        // === SERIALIZATION SETTINGS ===

        /// <summary>
        /// JSON serialization settings for arrays.
        /// </summary>
        public JsonSerializationOptions JsonOptions { get; set; } = new JsonSerializationOptions();

        // === METHODS ===

        /// <summary>
        /// Create configuration copy.
        /// </summary>
        public RedbServiceConfiguration Clone()
        {
            return new RedbServiceConfiguration
            {
                ConnectionString = ConnectionString,
                CacheDomain = CacheDomain,
                IdResetStrategy = IdResetStrategy,
                MissingObjectStrategy = MissingObjectStrategy,
                DefaultCheckPermissionsOnLoad = DefaultCheckPermissionsOnLoad,
                DefaultCheckPermissionsOnSave = DefaultCheckPermissionsOnSave,
                DefaultCheckPermissionsOnDelete = DefaultCheckPermissionsOnDelete,
                DefaultStrictDeleteExtra = DefaultStrictDeleteExtra,
                AutoSyncSchemesOnSave = AutoSyncSchemesOnSave,
                DefaultLoadDepth = DefaultLoadDepth,
                DefaultMaxTreeDepth = DefaultMaxTreeDepth,
                ThrowOnObjectNotFound = ThrowOnObjectNotFound,
                EnableLazyLoadingForProps = EnableLazyLoadingForProps,
                EnablePropsCache = EnablePropsCache,
                PropsCacheMaxSize = PropsCacheMaxSize,
                PropsCacheTtl = PropsCacheTtl,
                SkipHashValidationOnCacheCheck = SkipHashValidationOnCacheCheck,
                EnableListCache = EnableListCache,
                ListCacheTtl = ListCacheTtl,
                EnableMetadataCache = EnableMetadataCache,
                MetadataCacheLifetimeMinutes = MetadataCacheLifetimeMinutes,
                WarmupMetadataCacheOnInit = WarmupMetadataCacheOnInit,
                //MetadataCache = MetadataCache, // New caching settings
                EnableSchemaValidation = EnableSchemaValidation,
                EnableDataValidation = EnableDataValidation,
                AutoSetModifyDate = AutoSetModifyDate,
                AutoRecomputeHash = AutoRecomputeHash,
                // DefaultSecurityPriority removed,
                SystemUserId = SystemUserId,
                JsonOptions = new JsonSerializationOptions
                {
                    WriteIndented = JsonOptions.WriteIndented,
                    UseUnsafeRelaxedJsonEscaping = JsonOptions.UseUnsafeRelaxedJsonEscaping
                }
            };
        }

        /// <summary>
        /// Get configuration description.
        /// </summary>
        public string GetDescription()
        {
            return $"RedbService Configuration: " +
                   $"LoadDepth={DefaultLoadDepth}, " +
                   $"TreeDepth={DefaultMaxTreeDepth}, " +
                   $"Cache={EnableMetadataCache}, " +
                   // $"Security={DefaultSecurityPriority}, " +
                   $"IdReset={IdResetStrategy}, " +
                   $"MissingObj={MissingObjectStrategy}";
        }

        /// <summary>
        /// Check if configuration is safe for production
        /// </summary>
        public bool IsProductionSafe()
        {
            return DefaultCheckPermissionsOnLoad &&
                   DefaultCheckPermissionsOnSave &&
                   DefaultCheckPermissionsOnDelete &&
                   EnableSchemaValidation &&
                   EnableDataValidation &&
                   DefaultLoadDepth <= 10 &&
                   !JsonOptions.WriteIndented;
        }

        /// <summary>
        /// Check if configuration is optimized for performance
        /// </summary>
        public bool IsPerformanceOptimized()
        {
            return !DefaultCheckPermissionsOnLoad &&
                   !DefaultCheckPermissionsOnSave &&
                   !DefaultCheckPermissionsOnDelete &&
                   (EnableMetadataCache/* || MetadataCache.EnableMetadataCache*/) && // Support for new and old settings
                   DefaultLoadDepth <= 5 &&
                   DefaultMaxTreeDepth <= 10 &&
                   !JsonOptions.WriteIndented;
                   //&& MetadataCache.Warmup.EnableWarmup; // Additional performance check
        }
        
        /// <summary>
        /// Gets effective cache domain name.
        /// Returns explicit CacheDomain if set, otherwise computes from connection string.
        /// </summary>
        public string GetEffectiveCacheDomain()
        {
            if (!string.IsNullOrEmpty(CacheDomain))
                return CacheDomain;
                
            if (string.IsNullOrEmpty(ConnectionString))
                return "default";
                
            // Compute short hash from connection string (sanitized - without password)
            var sanitized = SanitizeConnectionString(ConnectionString);
            var hash = SHA256.HashData(Encoding.UTF8.GetBytes(sanitized));
            return Convert.ToHexString(hash)[..16].ToLowerInvariant();
        }
        
        /// <summary>
        /// Removes sensitive info (password) from connection string for hashing.
        /// </summary>
        private static string SanitizeConnectionString(string connectionString)
        {
            // Remove password= or pwd= from connection string
            var result = System.Text.RegularExpressions.Regex.Replace(
                connectionString, 
                @"(password|pwd)\s*=\s*[^;]*;?", 
                "", 
                System.Text.RegularExpressions.RegexOptions.IgnoreCase);
            return result.ToLowerInvariant().Trim();
        }
    }

    /// <summary>
    /// Object ID handling strategy after deletion
    /// </summary>
    public enum ObjectIdResetStrategy
    {
        /// <summary>
        /// Manual ID reset (current behavior)
        /// </summary>
        Manual,

        /// <summary>
        /// Automatic ID reset when deleting via DeleteAsync(RedbObject)
        /// </summary>
        AutoResetOnDelete,

        /// <summary>
        /// Automatic creation of new object when trying to save deleted object
        /// </summary>
        AutoCreateNewOnSave
    }

    /// <summary>
    /// Strategy for handling non-existent objects on UPDATE
    /// </summary>
    public enum MissingObjectStrategy
    {
        /// <summary>
        /// Throw exception (current behavior)
        /// </summary>
        ThrowException,

        /// <summary>
        /// Automatically switch to INSERT
        /// </summary>
        AutoSwitchToInsert,

        /// <summary>
        /// Return null/false without error
        /// </summary>
        ReturnNull
    }

    /// <summary>
    /// EAV properties save strategy
    /// </summary>
    public enum EavSaveStrategy
    {
        /// <summary>
        /// Simple strategy - always DELETE + INSERT all properties
        /// Reliable, but inefficient for large objects
        /// </summary>
        DeleteInsert,
        
        /// <summary>
        /// Efficient strategy - compare with DB and update only changed properties
        /// Recommended by default
        /// </summary>
        ChangeTracking
    }

    /// <summary>
    /// JSON serialization settings
    /// </summary>
    public class JsonSerializationOptions
    {
        /// <summary>
        /// Format JSON with indentation
        /// </summary>
        public bool WriteIndented { get; set; } = false;

        /// <summary>
        /// Use unsafe relaxed JavaScript encoding
        /// </summary>
        public bool UseUnsafeRelaxedJsonEscaping { get; set; } = true;
    }
}
