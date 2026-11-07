using System;

namespace redb.Core.Models.Configuration
{
    /// <summary>
    /// Predefined configurations for various usage scenarios
    /// </summary>
    public static class PredefinedConfigurations
    {
        /// <summary>
        /// Default configuration (balanced)
        /// </summary>
        public static RedbServiceConfiguration Default => new RedbServiceConfiguration();

        /// <summary>
        /// Configuration for development and testing
        /// Priority: development convenience, detailed diagnostics
        /// </summary>
        public static RedbServiceConfiguration Development => new RedbServiceConfiguration
        {
            // Disable permission checks for convenience
            DefaultCheckPermissionsOnLoad = false,
            DefaultCheckPermissionsOnSave = false,
            DefaultCheckPermissionsOnDelete = false,

            // Automatic recovery after errors
            IdResetStrategy = ObjectIdResetStrategy.AutoCreateNewOnSave,
            MissingObjectStrategy = MissingObjectStrategy.AutoSwitchToInsert,

            // Enable all validations for early problem detection
            EnableSchemaValidation = true,
            EnableDataValidation = true,

            // Disable cache for data freshness during development
            EnableMetadataCache = false,

            // Detailed JSON logging
            JsonOptions = new JsonSerializationOptions
            {
                WriteIndented = true,
                UseUnsafeRelaxedJsonEscaping = true
            },

            // Standard depths
            DefaultLoadDepth = 10,
            DefaultMaxTreeDepth = 50,

            // Automatic audit
            AutoSetModifyDate = true,
            AutoRecomputeHash = true,

            // System user for tests
            SystemUserId = 0,
            // DefaultSecurityPriority removed
        };

        /// <summary>
        /// Configuration for production (high security)
        /// Priority: security, stability, control
        /// </summary>
        public static RedbServiceConfiguration Production => new RedbServiceConfiguration
        {
            // Strict permission checks everywhere
            DefaultCheckPermissionsOnLoad = true,
            DefaultCheckPermissionsOnSave = true,
            DefaultCheckPermissionsOnDelete = true,

            // Conservative error handling
            IdResetStrategy = ObjectIdResetStrategy.Manual,
            MissingObjectStrategy = MissingObjectStrategy.ThrowException,

            // Full validation
            EnableSchemaValidation = true,
            EnableDataValidation = true,

            // Aggressive caching for performance
            EnableMetadataCache = true,
            MetadataCacheLifetimeMinutes = 60,

            // Compact JSON
            JsonOptions = new JsonSerializationOptions
            {
                WriteIndented = false,
                UseUnsafeRelaxedJsonEscaping = true
            },

            // Limited depths for performance
            DefaultLoadDepth = 5,
            DefaultMaxTreeDepth = 30,

            // Full audit
            AutoSetModifyDate = true,
            AutoRecomputeHash = true,

            // Strict schema settings
            DefaultStrictDeleteExtra = true,
            AutoSyncSchemesOnSave = true,

            SystemUserId = 0,
            // DefaultSecurityPriority removed
        };

        /// <summary>
        /// Configuration for bulk operations
        /// Priority: maximum speed, minimum checks
        /// </summary>
        public static RedbServiceConfiguration BulkOperations => new RedbServiceConfiguration
        {
            // Disable all permission checks
            DefaultCheckPermissionsOnLoad = false,
            DefaultCheckPermissionsOnSave = false,
            DefaultCheckPermissionsOnDelete = false,

            // Automatic recovery to continue operations
            IdResetStrategy = ObjectIdResetStrategy.AutoCreateNewOnSave,
            MissingObjectStrategy = MissingObjectStrategy.AutoSwitchToInsert,

            // Disable validation for speed
            EnableSchemaValidation = false,
            EnableDataValidation = false,

            // Disable cache (may be stale during bulk changes)
            EnableMetadataCache = false,

            // Minimal JSON
            JsonOptions = new JsonSerializationOptions
            {
                WriteIndented = false,
                UseUnsafeRelaxedJsonEscaping = true
            },

            // Minimum depths
            DefaultLoadDepth = 1,
            DefaultMaxTreeDepth = 1,

            // Disable automatic audit for speed
            AutoSetModifyDate = false,
            AutoRecomputeHash = false,

            // Auto-syncing schemas can slow down bulk operations
            AutoSyncSchemesOnSave = false,
            DefaultStrictDeleteExtra = false,

            SystemUserId = 0,
            // DefaultSecurityPriority removed
        };

        /// <summary>
        /// Configuration for high performance
        /// Priority: speed, caching, optimization
        /// </summary>
        public static RedbServiceConfiguration HighPerformance => new RedbServiceConfiguration
        {
            // Minimum permission checks
            DefaultCheckPermissionsOnLoad = false,
            DefaultCheckPermissionsOnSave = false,
            DefaultCheckPermissionsOnDelete = true, // Keep for security

            // Moderate recovery
            IdResetStrategy = ObjectIdResetStrategy.AutoResetOnDelete,
            MissingObjectStrategy = MissingObjectStrategy.AutoSwitchToInsert,

            // Disable data validation, keep schemas
            EnableSchemaValidation = true,
            EnableDataValidation = false,

            // Aggressive caching
            EnableMetadataCache = true,
            MetadataCacheLifetimeMinutes = 120,

            // Compact JSON
            JsonOptions = new JsonSerializationOptions
            {
                WriteIndented = false,
                UseUnsafeRelaxedJsonEscaping = true
            },

            // Optimal depths
            DefaultLoadDepth = 3,
            DefaultMaxTreeDepth = 10,

            // Minimal audit
            AutoSetModifyDate = true,
            AutoRecomputeHash = false, // Disable for speed

            // Optimized synchronization
            AutoSyncSchemesOnSave = true,
            DefaultStrictDeleteExtra = false, // Don't delete extra fields for speed

            SystemUserId = 0,
            // DefaultSecurityPriority removed
        };

        /// <summary>
        /// Configuration for debugging
        /// Priority: maximum information, detailed diagnostics
        /// </summary>
        public static RedbServiceConfiguration Debug => new RedbServiceConfiguration
        {
            // Disable checks for debugging convenience
            DefaultCheckPermissionsOnLoad = false,
            DefaultCheckPermissionsOnSave = false,
            DefaultCheckPermissionsOnDelete = false,

            // Auto-recovery to continue debugging
            IdResetStrategy = ObjectIdResetStrategy.AutoCreateNewOnSave,
            MissingObjectStrategy = MissingObjectStrategy.AutoSwitchToInsert,

            // Maximum validation
            EnableSchemaValidation = true,
            EnableDataValidation = true,

            // Disable cache for freshness
            EnableMetadataCache = false,

            // Detailed JSON for analysis
            JsonOptions = new JsonSerializationOptions
            {
                WriteIndented = true,
                UseUnsafeRelaxedJsonEscaping = true
            },

            // Large depths for complete picture
            DefaultLoadDepth = 20,
            DefaultMaxTreeDepth = 100,

            // Full audit
            AutoSetModifyDate = true,
            AutoRecomputeHash = true,

            // Detailed synchronization
            AutoSyncSchemesOnSave = true,
            DefaultStrictDeleteExtra = true,

            SystemUserId = 0,
            // DefaultSecurityPriority removed
        };

        /// <summary>
        /// Configuration for integration tests
        /// Priority: predictability, isolation, reproducibility
        /// </summary>
        public static RedbServiceConfiguration IntegrationTesting => new RedbServiceConfiguration
        {
            // Enable permission checks for security testing
            DefaultCheckPermissionsOnLoad = true,
            DefaultCheckPermissionsOnSave = true,
            DefaultCheckPermissionsOnDelete = true,

            // Strict error handling to identify issues
            IdResetStrategy = ObjectIdResetStrategy.Manual,
            MissingObjectStrategy = MissingObjectStrategy.ThrowException,

            // Full validation
            EnableSchemaValidation = true,
            EnableDataValidation = true,

            // Disable cache for test isolation
            EnableMetadataCache = false,

            // Readable JSON for result analysis
            JsonOptions = new JsonSerializationOptions
            {
                WriteIndented = true,
                UseUnsafeRelaxedJsonEscaping = true
            },

            // Standard depths
            DefaultLoadDepth = 10,
            DefaultMaxTreeDepth = 50,

            // Full audit for verification
            AutoSetModifyDate = true,
            AutoRecomputeHash = true,

            // Strict synchronization
            AutoSyncSchemesOnSave = true,
            DefaultStrictDeleteExtra = true,

            SystemUserId = 0,
            // DefaultSecurityPriority removed
        };

        /// <summary>
        /// Configuration for data migration
        /// Priority: reliability, recovery, schema flexibility
        /// </summary>
        public static RedbServiceConfiguration DataMigration => new RedbServiceConfiguration
        {
            // Disable permission checks for system operations
            DefaultCheckPermissionsOnLoad = false,
            DefaultCheckPermissionsOnSave = false,
            DefaultCheckPermissionsOnDelete = false,

            // Maximum error tolerance
            IdResetStrategy = ObjectIdResetStrategy.AutoCreateNewOnSave,
            MissingObjectStrategy = MissingObjectStrategy.AutoSwitchToInsert,

            // Schema validation is important, data validation is not (may have incorrect data)
            EnableSchemaValidation = true,
            EnableDataValidation = false,

            // Disable cache (schemas may change frequently)
            EnableMetadataCache = false,

            // Compact JSON to save space
            JsonOptions = new JsonSerializationOptions
            {
                WriteIndented = false,
                UseUnsafeRelaxedJsonEscaping = true
            },

            // Medium depths
            DefaultLoadDepth = 5,
            DefaultMaxTreeDepth = 25,

            // Full audit for migration tracking
            AutoSetModifyDate = true,
            AutoRecomputeHash = true,

            // Flexible schema synchronization
            AutoSyncSchemesOnSave = true,
            DefaultStrictDeleteExtra = false, // Don't delete fields during migration

            SystemUserId = 0,
            // DefaultSecurityPriority removed
        };

        /// <summary>
        /// Get configuration by name
        /// </summary>
        public static RedbServiceConfiguration GetByName(string name)
        {
            return name.ToLowerInvariant() switch
            {
                "default" => Default,
                "development" => Development,
                "production" => Production,
                "bulk" or "bulkoperations" => BulkOperations,
                "performance" or "highperformance" => HighPerformance,
                "debug" => Debug,
                "test" or "integrationtesting" => IntegrationTesting,
                "migration" or "datamigration" => DataMigration,
                _ => throw new ArgumentException($"Unknown configuration name: {name}")
            };
        }

        /// <summary>
        /// Get all available configuration names
        /// </summary>
        public static string[] GetAvailableNames()
        {
            return new[]
            {
                "Default",
                "Development", 
                "Production",
                "BulkOperations",
                "HighPerformance",
                "Debug",
                "IntegrationTesting",
                "DataMigration"
            };
        }
    }
}
