using System;
using System.Text.Json;
using Microsoft.Extensions.Configuration;
using redb.Core.Models.Configuration;

namespace redb.Core.Configuration
{
    /// <summary>
    /// Extension methods for integrating RedbServiceConfiguration with IConfiguration
    /// </summary>
    public static class ConfigurationExtensions
    {
        /// <summary>
        /// Get RedbService configuration from IConfiguration
        /// </summary>
        /// <param name="configuration">Application configuration</param>
        /// <param name="sectionName">Configuration section name (default "RedbService")</param>
        /// <returns>Configured RedbService configuration</returns>
        public static RedbServiceConfiguration GetRedbServiceConfiguration(
            this IConfiguration configuration, 
            string sectionName = "RedbService")
        {
            var section = configuration.GetSection(sectionName);
            
            if (!section.Exists())
            {
                // If section doesn't exist, return default configuration
                return new RedbServiceConfiguration();
            }

            // Profile support
            var profileName = section["Profile"];
            var baseConfig = GetBaseConfigurationFromProfile(profileName);
            
            // Apply main settings from configuration
            ApplyConfigurationSettings(section, baseConfig);
            
            // Apply overrides
            var overridesSection = section.GetSection("Overrides");
            if (overridesSection.Exists())
            {
                ApplyConfigurationSettings(overridesSection, baseConfig);
            }
            
            return baseConfig;
        }

        /// <summary>
        /// Get RedbService configuration with validation
        /// </summary>
        /// <param name="configuration">Application configuration</param>
        /// <param name="sectionName">Configuration section name</param>
        /// <param name="throwOnValidationError">Throw exception on validation errors</param>
        /// <returns>Validated RedbService configuration</returns>
        public static RedbServiceConfiguration GetValidatedRedbServiceConfiguration(
            this IConfiguration configuration,
            string sectionName = "RedbService",
            bool throwOnValidationError = true)
        {
            var config = configuration.GetRedbServiceConfiguration(sectionName);
            
            var validationResult = ConfigurationValidator.Validate(config);
            
            if (!validationResult.IsValid)
            {
                if (throwOnValidationError)
                {
                    var errors = string.Join(Environment.NewLine, validationResult.GetAllMessages());
                    throw new InvalidOperationException($"Invalid RedbService configuration:{Environment.NewLine}{errors}");
                }
                
                // Automatic fixing of critical errors
                config = ConfigurationValidator.FixCriticalErrors(config);
            }
            
            return config;
        }

        /// <summary>
        /// Check if RedbService configuration section exists
        /// </summary>
        /// <param name="configuration">Application configuration</param>
        /// <param name="sectionName">Configuration section name</param>
        /// <returns>true if section exists</returns>
        public static bool HasRedbServiceConfiguration(
            this IConfiguration configuration,
            string sectionName = "RedbService")
        {
            return configuration.GetSection(sectionName).Exists();
        }

        /// <summary>
        /// Get configuration description from IConfiguration
        /// </summary>
        /// <param name="configuration">Application configuration</param>
        /// <param name="sectionName">Configuration section name</param>
        /// <returns>Configuration description</returns>
        public static string GetRedbServiceConfigurationDescription(
            this IConfiguration configuration,
            string sectionName = "RedbService")
        {
            var config = configuration.GetRedbServiceConfiguration(sectionName);
            return config.GetDescription();
        }

        /// <summary>
        /// Create builder based on configuration from IConfiguration
        /// </summary>
        /// <param name="configuration">Application configuration</param>
        /// <param name="sectionName">Configuration section name</param>
        /// <returns>Builder for further configuration</returns>
        public static RedbServiceConfigurationBuilder CreateRedbServiceBuilder(
            this IConfiguration configuration,
            string sectionName = "RedbService")
        {
            var baseConfig = configuration.GetRedbServiceConfiguration(sectionName);
            return new RedbServiceConfigurationBuilder(baseConfig);
        }

        // === PRIVATE METHODS ===

        /// <summary>
        /// Get base configuration from profile
        /// </summary>
        private static RedbServiceConfiguration GetBaseConfigurationFromProfile(string? profileName)
        {
            if (string.IsNullOrEmpty(profileName))
            {
                return new RedbServiceConfiguration();
            }

            try
            {
                return PredefinedConfigurations.GetByName(profileName);
            }
            catch (ArgumentException)
            {
                // If profile not found, use default configuration
                return new RedbServiceConfiguration();
            }
        }

        /// <summary>
        /// Apply settings from configuration section
        /// </summary>
        private static void ApplyConfigurationSettings(IConfigurationSection section, RedbServiceConfiguration config)
        {
            // Object deletion settings
            if (section["IdResetStrategy"] != null)
            {
                if (Enum.TryParse<ObjectIdResetStrategy>(section["IdResetStrategy"], true, out var idResetStrategy))
                {
                    config.IdResetStrategy = idResetStrategy;
                }
            }

            if (section["MissingObjectStrategy"] != null)
            {
                if (Enum.TryParse<MissingObjectStrategy>(section["MissingObjectStrategy"], true, out var missingObjectStrategy))
                {
                    config.MissingObjectStrategy = missingObjectStrategy;
                }
            }

            // Security settings
            if (section["DefaultCheckPermissionsOnLoad"] != null)
            {
                config.DefaultCheckPermissionsOnLoad = section.GetValue<bool>("DefaultCheckPermissionsOnLoad");
            }

            if (section["DefaultCheckPermissionsOnSave"] != null)
            {
                config.DefaultCheckPermissionsOnSave = section.GetValue<bool>("DefaultCheckPermissionsOnSave");
            }

            if (section["DefaultCheckPermissionsOnDelete"] != null)
            {
                config.DefaultCheckPermissionsOnDelete = section.GetValue<bool>("DefaultCheckPermissionsOnDelete");
            }

            // Schema settings
            if (section["DefaultStrictDeleteExtra"] != null)
            {
                config.DefaultStrictDeleteExtra = section.GetValue<bool>("DefaultStrictDeleteExtra");
            }

            if (section["AutoSyncSchemesOnSave"] != null)
            {
                config.AutoSyncSchemesOnSave = section.GetValue<bool>("AutoSyncSchemesOnSave");
            }

            // Loading settings
            if (section["DefaultLoadDepth"] != null)
            {
                config.DefaultLoadDepth = section.GetValue<int>("DefaultLoadDepth");
            }

            if (section["DefaultMaxTreeDepth"] != null)
            {
                config.DefaultMaxTreeDepth = section.GetValue<int>("DefaultMaxTreeDepth");
            }

            // Performance settings
            if (section["EnableMetadataCache"] != null)
            {
                config.EnableMetadataCache = section.GetValue<bool>("EnableMetadataCache");
            }

            if (section["MetadataCacheLifetimeMinutes"] != null)
            {
                config.MetadataCacheLifetimeMinutes = section.GetValue<int>("MetadataCacheLifetimeMinutes");
            }

            // Validation settings
            if (section["EnableSchemaValidation"] != null)
            {
                config.EnableSchemaValidation = section.GetValue<bool>("EnableSchemaValidation");
            }

            if (section["EnableDataValidation"] != null)
            {
                config.EnableDataValidation = section.GetValue<bool>("EnableDataValidation");
            }

            // Audit settings
            if (section["AutoSetModifyDate"] != null)
            {
                config.AutoSetModifyDate = section.GetValue<bool>("AutoSetModifyDate");
            }

            if (section["AutoRecomputeHash"] != null)
            {
                config.AutoRecomputeHash = section.GetValue<bool>("AutoRecomputeHash");
            }

            // Security context settings
            if (section["DefaultSecurityPriority"] != null)
            {
                // DefaultSecurityPriority removed - simple GetEffectiveUser() logic is used
            }

            if (section["SystemUserId"] != null)
            {
                config.SystemUserId = section.GetValue<long>("SystemUserId");
            }

            // JSON settings
            var jsonSection = section.GetSection("JsonOptions");
            if (jsonSection.Exists())
            {
                if (jsonSection["WriteIndented"] != null)
                {
                    config.JsonOptions.WriteIndented = jsonSection.GetValue<bool>("WriteIndented");
                }

                if (jsonSection["UseUnsafeRelaxedJsonEscaping"] != null)
                {
                    config.JsonOptions.UseUnsafeRelaxedJsonEscaping = jsonSection.GetValue<bool>("UseUnsafeRelaxedJsonEscaping");
                }
            }
        }
    }
}
