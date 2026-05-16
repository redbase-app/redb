using System;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using redb.Core.Models.Configuration;

namespace redb.Core.Configuration
{
    /// <summary>
    /// Extension methods for registering RedbService in DI container.
    /// </summary>
    public static class ServiceCollectionExtensions
    {
        /// <summary>
        /// Add RedbService configuration from IConfiguration.
        /// </summary>
        /// <param name="services">Service collection</param>
        /// <param name="configuration">Application configuration</param>
        /// <param name="sectionName">Configuration section name</param>
        /// <returns>Service collection for chaining</returns>
        [Obsolete("Use services.AddRedb(configuration) directly")]
        public static IServiceCollection AddRedbServiceConfiguration(
            this IServiceCollection services,
            IConfiguration configuration,
            string sectionName = "RedbService")
        {
            // Register configuration through Options pattern
            services.Configure<RedbServiceConfiguration>(
                configuration.GetSection(sectionName));

            // Add configuration validation
            services.AddSingleton<IValidateOptions<RedbServiceConfiguration>, RedbServiceConfigurationValidator>();

            // Register direct access to configuration
            services.AddSingleton<RedbServiceConfiguration>(provider =>
            {
                var options = provider.GetRequiredService<IOptionsMonitor<RedbServiceConfiguration>>();
                return options.CurrentValue;
            });

            return services;
        }

        /// <summary>
        /// Add RedbService configuration with validation.
        /// </summary>
        /// <param name="services">Service collection</param>
        /// <param name="configuration">Application configuration</param>
        /// <param name="sectionName">Configuration section name</param>
        /// <param name="throwOnValidationError">Throw exception on validation errors</param>
        /// <returns>Service collection for chaining</returns>
        [Obsolete("Use services.AddRedb(configuration) - validation is built into properties")]
        public static IServiceCollection AddValidatedRedbServiceConfiguration(
            this IServiceCollection services,
            IConfiguration configuration,
            string sectionName = "RedbService",
            bool throwOnValidationError = true)
        {
            // Get and validate configuration during registration
            var config = configuration.GetValidatedRedbServiceConfiguration(sectionName, throwOnValidationError);

            // Register validated configuration
            services.AddSingleton(config);

            // Also register through Options pattern for compatibility
            services.Configure<RedbServiceConfiguration>(options =>
            {
                // Copy all properties from validated configuration
                CopyConfigurationProperties(config, options);
            });

            return services;
        }

        /// <summary>
        /// Add RedbService configuration via builder.
        /// </summary>
        /// <param name="services">Service collection</param>
        /// <param name="configureBuilder">Delegate for builder configuration</param>
        /// <returns>Service collection for chaining</returns>
        [Obsolete("Use services.AddRedb(options => { ... }) directly")]
        public static IServiceCollection AddRedbServiceConfiguration(
            this IServiceCollection services,
            Action<RedbServiceConfigurationBuilder> configureBuilder)
        {
            var builder = new RedbServiceConfigurationBuilder();
            configureBuilder(builder);
            var config = builder.Build();

            services.AddSingleton(config);

            return services;
        }

        /// <summary>
        /// Add RedbService configuration combining IConfiguration and builder.
        /// </summary>
        /// <param name="services">Service collection</param>
        /// <param name="configuration">Application configuration</param>
        /// <param name="configureBuilder">Delegate for additional configuration</param>
        /// <param name="sectionName">Configuration section name</param>
        /// <returns>Service collection for chaining</returns>
        [Obsolete("Use services.AddRedb(configuration) directly")]
        public static IServiceCollection AddRedbServiceConfiguration(
            this IServiceCollection services,
            IConfiguration configuration,
            Action<RedbServiceConfigurationBuilder> configureBuilder,
            string sectionName = "RedbService")
        {
            // Create builder based on configuration
            var builder = configuration.CreateRedbServiceBuilder(sectionName);
            
            // Apply additional settings
            configureBuilder(builder);
            
            var config = builder.Build();
            services.AddSingleton(config);

            return services;
        }

        /// <summary>
        /// Add predefined RedbService configuration.
        /// </summary>
        /// <param name="services">Service collection</param>
        /// <param name="predefinedConfig">Predefined configuration</param>
        /// <returns>Service collection for chaining</returns>
        [Obsolete("Use services.AddRedb(config) directly")]
        public static IServiceCollection AddRedbServiceConfiguration(
            this IServiceCollection services,
            RedbServiceConfiguration predefinedConfig)
        {
            services.AddSingleton(predefinedConfig);
            return services;
        }

        /// <summary>
        /// Add RedbService configuration by profile name.
        /// </summary>
        /// <param name="services">Service collection</param>
        /// <param name="profileName">Configuration profile name</param>
        /// <param name="configureBuilder">Optional additional configuration</param>
        /// <returns>Service collection for chaining</returns>
        [Obsolete("Use services.AddRedb(PredefinedConfigurations.GetByName(profile)) directly")]
        public static IServiceCollection AddRedbServiceConfiguration(
            this IServiceCollection services,
            string profileName,
            Action<RedbServiceConfigurationBuilder>? configureBuilder = null)
        {
            var config = PredefinedConfigurations.GetByName(profileName);
            
            if (configureBuilder != null)
            {
                var builder = new RedbServiceConfigurationBuilder(config);
                configureBuilder(builder);
                config = builder.Build();
            }

            services.AddSingleton(config);
            return services;
        }

        /// <summary>
        /// Add RedbService configuration change monitoring.
        /// </summary>
        /// <param name="services">Service collection</param>
        /// <param name="configuration">Application configuration</param>
        /// <param name="sectionName">Configuration section name</param>
        /// <returns>Service collection for chaining</returns>
        [Obsolete("Use services.AddRedb(configuration) - configuration changes via direct property modification")]
        public static IServiceCollection AddRedbServiceConfigurationMonitoring(
            this IServiceCollection services,
            IConfiguration configuration,
            string sectionName = "RedbService")
        {
            // Register change monitoring through IOptionsMonitor
            services.Configure<RedbServiceConfiguration>(
                configuration.GetSection(sectionName));

            // Add validation on changes
            services.AddSingleton<IValidateOptions<RedbServiceConfiguration>, RedbServiceConfigurationValidator>();

            // Register service for tracking changes
            services.AddSingleton<IRedbServiceConfigurationMonitor, RedbServiceConfigurationMonitor>();

            return services;
        }

        // === PRIVATE METHODS ===

        /// <summary>
        /// Copy configuration properties.
        /// </summary>
        private static void CopyConfigurationProperties(RedbServiceConfiguration source, RedbServiceConfiguration target)
        {
            target.IdResetStrategy = source.IdResetStrategy;
            target.MissingObjectStrategy = source.MissingObjectStrategy;
            target.DefaultCheckPermissionsOnLoad = source.DefaultCheckPermissionsOnLoad;
            target.DefaultCheckPermissionsOnSave = source.DefaultCheckPermissionsOnSave;
            target.DefaultCheckPermissionsOnDelete = source.DefaultCheckPermissionsOnDelete;
            target.DefaultStrictDeleteExtra = source.DefaultStrictDeleteExtra;
            target.AutoSyncSchemesOnSave = source.AutoSyncSchemesOnSave;
            target.DefaultLoadDepth = source.DefaultLoadDepth;
            target.DefaultMaxTreeDepth = source.DefaultMaxTreeDepth;
            target.EnableMetadataCache = source.EnableMetadataCache;
            target.MetadataCacheLifetimeMinutes = source.MetadataCacheLifetimeMinutes;
            target.EnableSchemaValidation = source.EnableSchemaValidation;
            target.EnableDataValidation = source.EnableDataValidation;
            target.AutoSetModifyDate = source.AutoSetModifyDate;
            target.AutoRecomputeHash = source.AutoRecomputeHash;
            // target.DefaultSecurityPriority = source.DefaultSecurityPriority; // Removed
            target.SystemUserId = source.SystemUserId;
            target.JsonOptions.WriteIndented = source.JsonOptions.WriteIndented;
            target.JsonOptions.UseUnsafeRelaxedJsonEscaping = source.JsonOptions.UseUnsafeRelaxedJsonEscaping;
        }
    }

    /// <summary>
    /// Interface for configuration change monitoring.
    /// </summary>
    public interface IRedbServiceConfigurationMonitor
    {
        /// <summary>
        /// Current configuration.
        /// </summary>
        RedbServiceConfiguration CurrentConfiguration { get; }

        /// <summary>
        /// Configuration changed event.
        /// </summary>
        event Action<RedbServiceConfiguration> ConfigurationChanged;
    }

    /// <summary>
    /// Configuration change monitoring implementation.
    /// </summary>
    internal class RedbServiceConfigurationMonitor : IRedbServiceConfigurationMonitor, IDisposable
    {
        private readonly IOptionsMonitor<RedbServiceConfiguration> _optionsMonitor;
        private readonly IDisposable? _changeSubscription;

        public RedbServiceConfigurationMonitor(IOptionsMonitor<RedbServiceConfiguration> optionsMonitor)
        {
            _optionsMonitor = optionsMonitor;
            _changeSubscription = _optionsMonitor.OnChange(OnConfigurationChanged);
        }

        public RedbServiceConfiguration CurrentConfiguration => _optionsMonitor.CurrentValue;

        public event Action<RedbServiceConfiguration>? ConfigurationChanged;

        private void OnConfigurationChanged(RedbServiceConfiguration configuration)
        {
            var validationResult = ConfigurationValidator.Validate(configuration);
            if (validationResult.HasCriticalErrors)
            {
                throw new InvalidOperationException(
                    $"Critical configuration errors: {string.Join(", ", validationResult.GetAllMessages())}");
            }

            ConfigurationChanged?.Invoke(configuration);
        }

        public void Dispose()
        {
            _changeSubscription?.Dispose();
        }
    }
}
