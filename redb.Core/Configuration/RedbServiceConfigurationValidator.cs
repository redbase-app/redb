using System;
using System.Linq;
using Microsoft.Extensions.Options;
using redb.Core.Models.Configuration;

namespace redb.Core.Configuration
{
    /// <summary>
    /// RedbService configuration validator for integration with Options pattern
    /// </summary>
    public class RedbServiceConfigurationValidator : IValidateOptions<RedbServiceConfiguration>
    {
        /// <summary>
        /// Validate configuration
        /// </summary>
        /// <param name="name">Configuration name (usually null for default)</param>
        /// <param name="options">Configuration to validate</param>
        /// <returns>Validation result</returns>
        public ValidateOptionsResult Validate(string? name, RedbServiceConfiguration options)
        {
            if (options == null)
            {
                return ValidateOptionsResult.Fail("RedbServiceConfiguration cannot be null");
            }

            var validationResult = ConfigurationValidator.Validate(options);

            if (validationResult.IsValid)
            {
                // If there are only warnings, consider configuration valid
                return ValidateOptionsResult.Success;
            }

            // Collect all errors
            var errorMessages = validationResult.Errors
                .Select(e => $"{e.PropertyName}: {e.Message}")
                .ToList();

            // Add warnings as informational messages
            var warningMessages = validationResult.Warnings
                .Select(w => $"WARNING - {w.PropertyName}: {w.Message}")
                .ToList();

            var allMessages = errorMessages.Concat(warningMessages);

            return ValidateOptionsResult.Fail(allMessages);
        }
    }

    /// <summary>
    /// Extended validator with auto-fix support
    /// </summary>
    public class RedbServiceConfigurationValidatorWithAutoFix : IValidateOptions<RedbServiceConfiguration>
    {
        private readonly bool _autoFixCriticalErrors;

        /// <summary>
        /// Create validator with auto-fix capability
        /// </summary>
        /// <param name="autoFixCriticalErrors">Automatically fix critical errors</param>
        public RedbServiceConfigurationValidatorWithAutoFix(bool autoFixCriticalErrors = true)
        {
            _autoFixCriticalErrors = autoFixCriticalErrors;
        }

        public ValidateOptionsResult Validate(string? name, RedbServiceConfiguration options)
        {
            if (options == null)
            {
                return ValidateOptionsResult.Fail("RedbServiceConfiguration cannot be null");
            }

            var validationResult = ConfigurationValidator.Validate(options);

            // If there are critical errors and auto-fix is enabled
            if (validationResult.HasCriticalErrors && _autoFixCriticalErrors)
            {
                // Fix critical errors in-place
                var fixedConfig = ConfigurationValidator.FixCriticalErrors(options);
                CopyFixedValues(fixedConfig, options);

                // Re-validate the fixed configuration
                validationResult = ConfigurationValidator.Validate(options);
            }

            if (validationResult.IsValid)
            {
                return ValidateOptionsResult.Success;
            }

            // If errors remain after auto-fix
            var errorMessages = validationResult.Errors
                .Where(e => e.Severity != ConfigurationValidationSeverity.Warning)
                .Select(e => $"{e.PropertyName}: {e.Message} (Current: {e.CurrentValue})")
                .ToList();

            if (errorMessages.Any())
            {
                return ValidateOptionsResult.Fail(errorMessages);
            }

            // Only warnings - consider valid
            return ValidateOptionsResult.Success;
        }

        /// <summary>
        /// Copy fixed values
        /// </summary>
        private static void CopyFixedValues(RedbServiceConfiguration source, RedbServiceConfiguration target)
        {
            target.DefaultLoadDepth = source.DefaultLoadDepth;
            target.DefaultMaxTreeDepth = source.DefaultMaxTreeDepth;
            target.SystemUserId = source.SystemUserId;
            target.MetadataCacheLifetimeMinutes = source.MetadataCacheLifetimeMinutes;
        }
    }

    /// <summary>
    /// Validator for specific usage scenarios
    /// </summary>
    public class ScenarioBasedConfigurationValidator : IValidateOptions<RedbServiceConfiguration>
    {
        private readonly ConfigurationScenario _expectedScenario;

        /// <summary>
        /// Create validator for specific scenario
        /// </summary>
        /// <param name="expectedScenario">Expected usage scenario</param>
        public ScenarioBasedConfigurationValidator(ConfigurationScenario expectedScenario)
        {
            _expectedScenario = expectedScenario;
        }

        public ValidateOptionsResult Validate(string? name, RedbServiceConfiguration options)
        {
            if (options == null)
            {
                return ValidateOptionsResult.Fail("RedbServiceConfiguration cannot be null");
            }

            // Base validation
            var baseValidation = ConfigurationValidator.Validate(options);
            if (baseValidation.HasCriticalErrors)
            {
                var criticalErrors = baseValidation.Errors
                    .Where(e => e.Severity == ConfigurationValidationSeverity.Critical)
                    .Select(e => e.Message);
                return ValidateOptionsResult.Fail(criticalErrors);
            }

            // Validation for specific scenario
            var scenarioErrors = ValidateForScenario(options, _expectedScenario);
            if (scenarioErrors.Any())
            {
                return ValidateOptionsResult.Fail(scenarioErrors);
            }

            return ValidateOptionsResult.Success;
        }

        /// <summary>
        /// Validation for specific scenario
        /// </summary>
        private static string[] ValidateForScenario(RedbServiceConfiguration config, ConfigurationScenario scenario)
        {
            var errors = new System.Collections.Generic.List<string>();

            switch (scenario)
            {
                case ConfigurationScenario.Production:
                    if (!config.IsProductionSafe())
                    {
                        errors.Add("Configuration is not safe for production environment");
                    }
                    if (config.JsonOptions.WriteIndented)
                    {
                        errors.Add("WriteIndented should be false in production for performance");
                    }
                    break;

                case ConfigurationScenario.Development:
                    if (config.DefaultCheckPermissionsOnLoad ||
                        config.DefaultCheckPermissionsOnSave ||
                        config.DefaultCheckPermissionsOnDelete)
                    {
                        errors.Add("Permission checks should typically be disabled in development");
                    }
                    break;

                case ConfigurationScenario.HighPerformance:
                    if (!config.IsPerformanceOptimized())
                    {
                        errors.Add("Configuration is not optimized for high performance");
                    }
                    if (config.DefaultLoadDepth > 5)
                    {
                        errors.Add("DefaultLoadDepth should be <= 5 for high performance scenarios");
                    }
                    break;

                case ConfigurationScenario.BulkOperations:
                    if (config.EnableDataValidation)
                    {
                        errors.Add("Data validation should be disabled for bulk operations");
                    }
                    if (config.DefaultLoadDepth > 1)
                    {
                        errors.Add("DefaultLoadDepth should be 1 for bulk operations");
                    }
                    break;
            }

            return errors.ToArray();
        }
    }

    /// <summary>
    /// Configuration usage scenarios
    /// </summary>
    public enum ConfigurationScenario
    {
        /// <summary>
        /// Production environment
        /// </summary>
        Production,

        /// <summary>
        /// Development environment
        /// </summary>
        Development,

        /// <summary>
        /// High performance
        /// </summary>
        HighPerformance,

        /// <summary>
        /// Bulk operations
        /// </summary>
        BulkOperations,

        /// <summary>
        /// Integration testing
        /// </summary>
        IntegrationTesting,

        /// <summary>
        /// Debug
        /// </summary>
        Debug
    }
}
