using System;
using redb.Core.Models.Configuration;

namespace redb.Core.Extensions
{
    /// <summary>
    /// Extension methods for working with RedbService configuration
    /// </summary>
    public static class RedbServiceConfigurationExtensions
    {
        /// <summary>
        /// Create builder for configuration
        /// </summary>
        public static RedbServiceConfigurationBuilder CreateBuilder(this RedbServiceConfiguration configuration)
        {
            return new RedbServiceConfigurationBuilder(configuration);
        }

        /// <summary>
        /// Merge configurations (target is overwritten with source values)
        /// </summary>
        public static RedbServiceConfiguration MergeWith(this RedbServiceConfiguration target, RedbServiceConfiguration source)
        {
            var result = target.Clone();

            // Merge only non-default values
            if (source.IdResetStrategy != ObjectIdResetStrategy.Manual)
                result.IdResetStrategy = source.IdResetStrategy;

            if (source.MissingObjectStrategy != MissingObjectStrategy.ThrowException)
                result.MissingObjectStrategy = source.MissingObjectStrategy;

            // Security - always merge
            result.DefaultCheckPermissionsOnLoad = source.DefaultCheckPermissionsOnLoad;
            result.DefaultCheckPermissionsOnSave = source.DefaultCheckPermissionsOnSave;
            result.DefaultCheckPermissionsOnDelete = source.DefaultCheckPermissionsOnDelete;

            // Other settings
            result.DefaultStrictDeleteExtra = source.DefaultStrictDeleteExtra;
            result.AutoSyncSchemesOnSave = source.AutoSyncSchemesOnSave;
            result.DefaultLoadDepth = source.DefaultLoadDepth;
            result.DefaultMaxTreeDepth = source.DefaultMaxTreeDepth;
            result.EnableMetadataCache = source.EnableMetadataCache;
            result.MetadataCacheLifetimeMinutes = source.MetadataCacheLifetimeMinutes;
            result.EnableSchemaValidation = source.EnableSchemaValidation;
            result.EnableDataValidation = source.EnableDataValidation;
            result.AutoSetModifyDate = source.AutoSetModifyDate;
            result.AutoRecomputeHash = source.AutoRecomputeHash;
            // result.DefaultSecurityPriority = source.DefaultSecurityPriority; // Removed
            result.SystemUserId = source.SystemUserId;

            // JSON settings
            result.JsonOptions.WriteIndented = source.JsonOptions.WriteIndented;
            result.JsonOptions.UseUnsafeRelaxedJsonEscaping = source.JsonOptions.UseUnsafeRelaxedJsonEscaping;

            return result;
        }

        /// <summary>
        /// Check if configuration is safe for production
        /// </summary>
        public static bool IsProductionSafe(this RedbServiceConfiguration configuration)
        {
            return configuration.DefaultCheckPermissionsOnLoad &&
                   configuration.DefaultCheckPermissionsOnSave &&
                   configuration.DefaultCheckPermissionsOnDelete &&
                   configuration.EnableSchemaValidation &&
                   configuration.EnableDataValidation &&
                   configuration.MissingObjectStrategy == MissingObjectStrategy.ThrowException;
        }

        /// <summary>
        /// Check if configuration is optimized for performance
        /// </summary>
        public static bool IsPerformanceOptimized(this RedbServiceConfiguration configuration)
        {
            return !configuration.DefaultCheckPermissionsOnLoad &&
                   !configuration.DefaultCheckPermissionsOnSave &&
                   !configuration.EnableSchemaValidation &&
                   !configuration.EnableDataValidation &&
                   configuration.DefaultLoadDepth <= 3 &&
                   configuration.EnableMetadataCache;
        }

        /// <summary>
        /// Get configuration description
        /// </summary>
        public static string GetDescription(this RedbServiceConfiguration configuration)
        {
            var features = new System.Collections.Generic.List<string>();

            // Security
            if (configuration.DefaultCheckPermissionsOnLoad || 
                configuration.DefaultCheckPermissionsOnSave || 
                configuration.DefaultCheckPermissionsOnDelete)
            {
                features.Add("Security enabled");
            }

            // Strategies
            if (configuration.IdResetStrategy != ObjectIdResetStrategy.Manual)
                features.Add($"ID Reset: {configuration.IdResetStrategy}");

            if (configuration.MissingObjectStrategy != MissingObjectStrategy.ThrowException)
                features.Add($"Missing Objects: {configuration.MissingObjectStrategy}");

            // Performance
            if (configuration.EnableMetadataCache)
                features.Add($"Cache: {configuration.MetadataCacheLifetimeMinutes}min");

            if (configuration.DefaultLoadDepth != 10)
                features.Add($"Load Depth: {configuration.DefaultLoadDepth}");

            // Validation
            if (!configuration.EnableSchemaValidation || !configuration.EnableDataValidation)
                features.Add("Validation disabled");

            return string.Join(", ", features);
        }

        /// <summary>
        /// Apply temporary configuration with automatic restore
        /// </summary>
        public static IDisposable ApplyTemporary(this IRedbService service, RedbServiceConfiguration temporaryConfig)
        {
            // CFG-1: every behaviour setting, in place (providers hold the same configuration instance); the
            // connection identity stays. It used to be a hand-written list of fourteen settings out of thirty-six.
            var originalConfig = service.Configuration.Clone();
            service.UpdateConfiguration(config => config.CopyBehaviourFrom(temporaryConfig));
            return new TemporaryConfigurationScope(service, originalConfig);
        }

        /// <summary>
        /// Apply temporary configuration via builder
        /// </summary>
        public static IDisposable ApplyTemporary(this IRedbService service, Func<RedbServiceConfigurationBuilder, RedbServiceConfigurationBuilder> configure)
        {
            // CFG-1: the builder works on a copy. On the live configuration the change was applied before the
            // snapshot meant to undo it was taken, and the scope "restored" the changed values.
            var builder = new RedbServiceConfigurationBuilder(service.Configuration.Clone());
            var temporaryConfig = configure(builder).Build();
            
            return service.ApplyTemporary(temporaryConfig);
        }
    }

    /// <summary>
    /// Scope for temporary configuration
    /// </summary>
    internal class TemporaryConfigurationScope : IDisposable
    {
        private readonly IRedbService _service;
        private readonly RedbServiceConfiguration _originalConfiguration;
        private bool _disposed = false;

        public TemporaryConfigurationScope(IRedbService service, RedbServiceConfiguration originalConfiguration)
        {
            _service = service;
            _originalConfiguration = originalConfiguration;
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                _service.UpdateConfiguration(config => config.CopyBehaviourFrom(_originalConfiguration));
                _disposed = true;
            }
        }
    }
}
