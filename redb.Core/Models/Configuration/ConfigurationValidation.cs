using System;
using System.Collections.Generic;
using System.Linq;

namespace redb.Core.Models.Configuration
{
    /// <summary>
    /// Configuration validation result
    /// </summary>
    public class ConfigurationValidationResult
    {
        public bool IsValid { get; set; }
        public List<ConfigurationValidationError> Errors { get; set; } = new();
        public List<ConfigurationValidationWarning> Warnings { get; set; } = new();

        /// <summary>
        /// Are there critical errors
        /// </summary>
        public bool HasCriticalErrors => Errors.Any(e => e.Severity == ConfigurationValidationSeverity.Critical);

        /// <summary>
        /// Are there warnings
        /// </summary>
        public bool HasWarnings => Warnings.Any();

        /// <summary>
        /// Get all messages
        /// </summary>
        public IEnumerable<string> GetAllMessages()
        {
            foreach (var error in Errors)
                yield return $"ERROR: {error.Message}";
            
            foreach (var warning in Warnings)
                yield return $"WARNING: {warning.Message}";
        }
    }

    /// <summary>
    /// Configuration validation error
    /// </summary>
    public class ConfigurationValidationError
    {
        public string PropertyName { get; set; } = string.Empty;
        public string Message { get; set; } = string.Empty;
        public ConfigurationValidationSeverity Severity { get; set; } = ConfigurationValidationSeverity.Error;
        public object? CurrentValue { get; set; }
        public string? SuggestedFix { get; set; }
    }

    /// <summary>
    /// Configuration validation warning
    /// </summary>
    public class ConfigurationValidationWarning
    {
        public string PropertyName { get; set; } = string.Empty;
        public string Message { get; set; } = string.Empty;
        public string? Recommendation { get; set; }
    }

    /// <summary>
    /// Configuration validation error severity level
    /// </summary>
    public enum ConfigurationValidationSeverity
    {
        Warning,
        Error,
        Critical
    }

    /// <summary>
    /// RedbService configuration validator
    /// </summary>
    public static class ConfigurationValidator
    {
        /// <summary>
        /// Validate configuration
        /// </summary>
        public static ConfigurationValidationResult Validate(RedbServiceConfiguration configuration)
        {
            var result = new ConfigurationValidationResult { IsValid = true };

            // Load depth validation
            ValidateLoadDepth(configuration, result);

            // Caching validation
            ValidateCaching(configuration, result);

            // Security validation
            ValidateSecurity(configuration, result);

            // Strategies validation
            ValidateStrategies(configuration, result);

            // JSON options validation
            ValidateJsonOptions(configuration, result);

            // Settings compatibility check
            ValidateCompatibility(configuration, result);

            result.IsValid = !result.HasCriticalErrors;
            return result;
        }

        private static void ValidateLoadDepth(RedbServiceConfiguration config, ConfigurationValidationResult result)
        {
            if (config.DefaultLoadDepth < 1)
            {
                result.Errors.Add(new ConfigurationValidationError
                {
                    PropertyName = nameof(config.DefaultLoadDepth),
                    Message = "DefaultLoadDepth must be greater than 0",
                    Severity = ConfigurationValidationSeverity.Critical,
                    CurrentValue = config.DefaultLoadDepth,
                    SuggestedFix = "Set value >= 1"
                });
            }

            if (config.DefaultLoadDepth > 50)
            {
                result.Warnings.Add(new ConfigurationValidationWarning
                {
                    PropertyName = nameof(config.DefaultLoadDepth),
                    Message = "Large load depth can reduce performance",
                    Recommendation = "Consider using value <= 10 for better performance"
                });
            }

            if (config.DefaultMaxTreeDepth < 1)
            {
                result.Errors.Add(new ConfigurationValidationError
                {
                    PropertyName = nameof(config.DefaultMaxTreeDepth),
                    Message = "DefaultMaxTreeDepth must be greater than 0",
                    Severity = ConfigurationValidationSeverity.Critical,
                    CurrentValue = config.DefaultMaxTreeDepth,
                    SuggestedFix = "Set value >= 1"
                });
            }

            if (config.DefaultMaxTreeDepth > 100)
            {
                result.Warnings.Add(new ConfigurationValidationWarning
                {
                    PropertyName = nameof(config.DefaultMaxTreeDepth),
                    Message = "Very large tree depth can cause performance issues",
                    Recommendation = "Consider using value <= 50"
                });
            }
        }

        private static void ValidateCaching(RedbServiceConfiguration config, ConfigurationValidationResult result)
        {
            if (config.EnableMetadataCache && config.MetadataCacheLifetimeMinutes < 1)
            {
                result.Errors.Add(new ConfigurationValidationError
                {
                    PropertyName = nameof(config.MetadataCacheLifetimeMinutes),
                    Message = "Cache lifetime must be greater than 0 minutes when caching is enabled",
                    Severity = ConfigurationValidationSeverity.Error,
                    CurrentValue = config.MetadataCacheLifetimeMinutes,
                    SuggestedFix = "Set value >= 1 or disable caching"
                });
            }

            if (config.MetadataCacheLifetimeMinutes > 1440) // 24 hours
            {
                result.Warnings.Add(new ConfigurationValidationWarning
                {
                    PropertyName = nameof(config.MetadataCacheLifetimeMinutes),
                    Message = "Very long cache lifetime can lead to stale data",
                    Recommendation = "Consider using value <= 120 minutes"
                });
            }
        }

        private static void ValidateSecurity(RedbServiceConfiguration config, ConfigurationValidationResult result)
        {
            if (config.SystemUserId < 0)
            {
                result.Errors.Add(new ConfigurationValidationError
                {
                    PropertyName = nameof(config.SystemUserId),
                    Message = "SystemUserId cannot be negative",
                    Severity = ConfigurationValidationSeverity.Error,
                    CurrentValue = config.SystemUserId,
                    SuggestedFix = "Set value >= 0"
                });
            }

            // Warning about unsafe configuration
            if (!config.DefaultCheckPermissionsOnLoad && 
                !config.DefaultCheckPermissionsOnSave && 
                !config.DefaultCheckPermissionsOnDelete)
            {
                result.Warnings.Add(new ConfigurationValidationWarning
                {
                    PropertyName = "Security",
                    Message = "All permission checks are disabled - this may be unsafe",
                    Recommendation = "Enable permission checks for production"
                });
            }
        }

        private static void ValidateStrategies(RedbServiceConfiguration config, ConfigurationValidationResult result)
        {
            // Strategy compatibility check
            if (config.IdResetStrategy == ObjectIdResetStrategy.AutoCreateNewOnSave &&
                config.MissingObjectStrategy == MissingObjectStrategy.ThrowException)
            {
                result.Warnings.Add(new ConfigurationValidationWarning
                {
                    PropertyName = "Strategies",
                    Message = "Strategy conflict: AutoCreateNewOnSave + ThrowException may lead to unexpected behavior",
                    Recommendation = "Use MissingObjectStrategy.AutoSwitchToInsert with AutoCreateNewOnSave"
                });
            }
        }

        private static void ValidateJsonOptions(RedbServiceConfiguration config, ConfigurationValidationResult result)
        {
            if (config.JsonOptions.WriteIndented)
            {
                result.Warnings.Add(new ConfigurationValidationWarning
                {
                    PropertyName = nameof(config.JsonOptions.WriteIndented),
                    Message = "Formatted JSON increases data size",
                    Recommendation = "Disable WriteIndented for production"
                });
            }
        }

        private static void ValidateCompatibility(RedbServiceConfiguration config, ConfigurationValidationResult result)
        {
            // High performance configuration check
            if (!config.EnableSchemaValidation && !config.EnableDataValidation && config.EnableMetadataCache)
            {
                // This is normal for high-performance scenarios
                result.Warnings.Add(new ConfigurationValidationWarning
                {
                    PropertyName = "Performance",
                    Message = "Configuration is optimized for performance (validation disabled)",
                    Recommendation = "Ensure data is valid at application level"
                });
            }

            // Development configuration check
            if (config.IdResetStrategy == ObjectIdResetStrategy.AutoCreateNewOnSave &&
                config.MissingObjectStrategy == MissingObjectStrategy.AutoSwitchToInsert &&
                !config.DefaultCheckPermissionsOnLoad)
            {
                result.Warnings.Add(new ConfigurationValidationWarning
                {
                    PropertyName = "Development",
                    Message = "Configuration looks like development settings",
                    Recommendation = "Do not use this configuration in production"
                });
            }
        }

        /// <summary>
        /// Quick check for critical errors
        /// </summary>
        public static bool HasCriticalErrors(RedbServiceConfiguration configuration)
        {
            return configuration.DefaultLoadDepth < 1 ||
                   configuration.DefaultMaxTreeDepth < 1 ||
                   configuration.SystemUserId < 0 ||
                   configuration.EnableMetadataCache && configuration.MetadataCacheLifetimeMinutes < 1;
        }

        /// <summary>
        /// Fix critical errors automatically
        /// </summary>
        public static RedbServiceConfiguration FixCriticalErrors(RedbServiceConfiguration configuration)
        {
            var fixedConfig = configuration.Clone();

            if (fixedConfig.DefaultLoadDepth < 1)
                fixedConfig.DefaultLoadDepth = 10;

            if (fixedConfig.DefaultMaxTreeDepth < 1)
                fixedConfig.DefaultMaxTreeDepth = 50;

            if (fixedConfig.SystemUserId < 0)
                fixedConfig.SystemUserId = 0;

            if (fixedConfig.EnableMetadataCache && fixedConfig.MetadataCacheLifetimeMinutes < 1)
                fixedConfig.MetadataCacheLifetimeMinutes = 30;

            return fixedConfig;
        }
    }
}
