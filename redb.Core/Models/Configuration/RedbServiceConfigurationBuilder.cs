using System;

namespace redb.Core.Models.Configuration
{
    /// <summary>
    /// Builder for convenient RedbService configuration setup
    /// </summary>
    public class RedbServiceConfigurationBuilder
    {
        private RedbServiceConfiguration _configuration;

        public RedbServiceConfigurationBuilder()
        {
            _configuration = new RedbServiceConfiguration();
        }

        public RedbServiceConfigurationBuilder(RedbServiceConfiguration baseConfiguration)
        {
            _configuration = baseConfiguration ?? new RedbServiceConfiguration();
        }

        // === OBJECT DELETION SETTINGS ===

        /// <summary>
        /// Configure ID handling strategy after object deletion
        /// </summary>
        public RedbServiceConfigurationBuilder WithIdResetStrategy(ObjectIdResetStrategy strategy)
        {
            _configuration.IdResetStrategy = strategy;
            return this;
        }

        /// <summary>
        /// Configure strategy for handling non-existent objects on UPDATE
        /// </summary>
        public RedbServiceConfigurationBuilder WithMissingObjectStrategy(MissingObjectStrategy strategy)
        {
            _configuration.MissingObjectStrategy = strategy;
            return this;
        }

        // === SECURITY SETTINGS ===

        /// <summary>
        /// Configure default permission checks
        /// </summary>
        public RedbServiceConfigurationBuilder WithDefaultPermissions(
            bool checkOnLoad = false, 
            bool checkOnSave = false, 
            bool checkOnDelete = true)
        {
            _configuration.DefaultCheckPermissionsOnLoad = checkOnLoad;
            _configuration.DefaultCheckPermissionsOnSave = checkOnSave;
            _configuration.DefaultCheckPermissionsOnDelete = checkOnDelete;
            return this;
        }

        /// <summary>
        /// Enable strict security (permission checks everywhere)
        /// </summary>
        public RedbServiceConfigurationBuilder WithStrictSecurity()
        {
            return WithDefaultPermissions(checkOnLoad: true, checkOnSave: true, checkOnDelete: true);
        }

        /// <summary>
        /// Disable permission checks (for development/testing)
        /// </summary>
        public RedbServiceConfigurationBuilder WithoutPermissionChecks()
        {
            return WithDefaultPermissions(checkOnLoad: false, checkOnSave: false, checkOnDelete: false);
        }

        /// <summary>
        /// Configure system user ID
        /// </summary>
        public RedbServiceConfigurationBuilder WithSystemUser(long systemUserId)
        {
            _configuration.SystemUserId = systemUserId;
            return this;
        }

        /// <summary>
        /// Configure security context priority
        /// </summary>
        // public RedbServiceConfigurationBuilder WithSecurityPriority(SecurityContextPriority priority)
        // {
        //     _configuration.DefaultSecurityPriority = priority;
        //     return this;
        // }

        // === SCHEMA SETTINGS ===

        /// <summary>
        /// Configure schema synchronization behavior
        /// </summary>
        public RedbServiceConfigurationBuilder WithSchemaSync(
            bool strictDeleteExtra = true, 
            bool autoSyncOnSave = true)
        {
            _configuration.DefaultStrictDeleteExtra = strictDeleteExtra;
            _configuration.AutoSyncSchemesOnSave = autoSyncOnSave;
            return this;
        }

        // === LOADING SETTINGS ===

        /// <summary>
        /// Configure object loading depth
        /// </summary>
        public RedbServiceConfigurationBuilder WithLoadDepth(int defaultDepth = 10, int maxTreeDepth = 50)
        {
            _configuration.DefaultLoadDepth = defaultDepth;
            _configuration.DefaultMaxTreeDepth = maxTreeDepth;
            return this;
        }

        // === PERFORMANCE SETTINGS ===

        /// <summary>
        /// Configure metadata caching
        /// </summary>
        public RedbServiceConfigurationBuilder WithMetadataCache(
            bool enabled = true, 
            int lifetimeMinutes = 30)
        {
            _configuration.EnableMetadataCache = enabled;
            _configuration.MetadataCacheLifetimeMinutes = lifetimeMinutes;
            return this;
        }

        /// <summary>
        /// Disable caching (for debugging)
        /// </summary>
        public RedbServiceConfigurationBuilder WithoutCache()
        {
            return WithMetadataCache(enabled: false);
        }

        // === VALIDATION SETTINGS ===

        /// <summary>
        /// Configure validation
        /// </summary>
        public RedbServiceConfigurationBuilder WithValidation(
            bool schemaValidation = true, 
            bool dataValidation = true)
        {
            _configuration.EnableSchemaValidation = schemaValidation;
            _configuration.EnableDataValidation = dataValidation;
            return this;
        }

        /// <summary>
        /// Disable validation (for performance)
        /// </summary>
        public RedbServiceConfigurationBuilder WithoutValidation()
        {
            return WithValidation(schemaValidation: false, dataValidation: false);
        }

        // === AUDIT SETTINGS ===

        /// <summary>
        /// Configure automatic audit
        /// </summary>
        public RedbServiceConfigurationBuilder WithAudit(
            bool autoSetModifyDate = true, 
            bool autoRecomputeHash = true)
        {
            _configuration.AutoSetModifyDate = autoSetModifyDate;
            _configuration.AutoRecomputeHash = autoRecomputeHash;
            return this;
        }

        // === JSON SETTINGS ===

        /// <summary>
        /// Configure JSON serialization
        /// </summary>
        public RedbServiceConfigurationBuilder WithJsonOptions(Action<JsonSerializationOptions> configure)
        {
            configure(_configuration.JsonOptions);
            return this;
        }

        /// <summary>
        /// Enable pretty JSON formatting
        /// </summary>
        public RedbServiceConfigurationBuilder WithPrettyJson()
        {
            _configuration.JsonOptions.WriteIndented = true;
            return this;
        }

        /// <summary>
        /// Configure list cache
        /// </summary>
        public RedbServiceConfigurationBuilder WithListCache(bool enabled = true, int ttlMinutes = 5)
        {
            _configuration.EnableListCache = enabled;
            _configuration.ListCacheTtl = TimeSpan.FromMinutes(ttlMinutes);
            return this;
        }

        // === PREDEFINED CONFIGURATIONS ===

        /// <summary>
        /// Configuration for development/testing
        /// </summary>
        public RedbServiceConfigurationBuilder ForDevelopment()
        {
            return WithoutPermissionChecks()
                .WithIdResetStrategy(ObjectIdResetStrategy.AutoCreateNewOnSave)
                .WithMissingObjectStrategy(MissingObjectStrategy.AutoSwitchToInsert)
                .WithValidation(schemaValidation: true, dataValidation: true)
                .WithPrettyJson()
                .WithMetadataCache(enabled: false); // Disable cache for debugging
        }

        /// <summary>
        /// Configuration for production (high security)
        /// </summary>
        public RedbServiceConfigurationBuilder ForProduction()
        {
            return WithStrictSecurity()
                .WithIdResetStrategy(ObjectIdResetStrategy.Manual)
                .WithMissingObjectStrategy(MissingObjectStrategy.ThrowException)
                .WithValidation(schemaValidation: true, dataValidation: true)
                .WithLoadDepth(defaultDepth: 5, maxTreeDepth: 30) // Less for performance
                .WithMetadataCache(enabled: true, lifetimeMinutes: 60);
        }

        /// <summary>
        /// Configuration for bulk operations
        /// </summary>
        public RedbServiceConfigurationBuilder ForBulkOperations()
        {
            return WithoutPermissionChecks()
                .WithIdResetStrategy(ObjectIdResetStrategy.AutoCreateNewOnSave)
                .WithMissingObjectStrategy(MissingObjectStrategy.AutoSwitchToInsert)
                .WithoutValidation()
                .WithLoadDepth(defaultDepth: 1, maxTreeDepth: 1)
                .WithoutCache();
        }

        /// <summary>
        /// Configuration for high performance
        /// </summary>
        public RedbServiceConfigurationBuilder ForHighPerformance()
        {
            return WithoutPermissionChecks()
                .WithoutValidation()
                .WithLoadDepth(defaultDepth: 3, maxTreeDepth: 10)
                .WithMetadataCache(enabled: true, lifetimeMinutes: 120)
                .WithAudit(autoSetModifyDate: false, autoRecomputeHash: false);
        }

        /// <summary>
        /// Build configuration
        /// </summary>
        public RedbServiceConfiguration Build()
        {
            return _configuration;
        }

        /// <summary>
        /// Apply additional configuration
        /// </summary>
        public RedbServiceConfigurationBuilder Configure(Action<RedbServiceConfiguration> configure)
        {
            configure(_configuration);
            return this;
        }

        /// <summary>
        /// Configuration for integration testing
        /// </summary>
        public RedbServiceConfigurationBuilder ForIntegrationTesting()
        {
            _configuration = PredefinedConfigurations.IntegrationTesting.Clone();
            return this;
        }
    }
}
