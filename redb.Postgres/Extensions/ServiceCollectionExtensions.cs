using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using redb.Core;
using redb.Core.Configuration;
using System;
using redb.Core.Models.Configuration;

namespace redb.Postgres.Extensions
{
    /// <summary>
    /// Extension methods for registering RedbService in DI container.
    /// </summary>
    public static class ServiceCollectionExtensions
    {
        /// <summary>
        /// Register RedbService with default configuration.
        /// </summary>
        [Obsolete("Use services.AddRedb() from redb.Postgres.Extensions.RedbServiceExtensions")]
        public static IServiceCollection AddRedbService(this IServiceCollection services)
        {
            services.AddScoped<RedbServiceConfiguration>();
            services.AddScoped<IRedbService, RedbService>();
            return services;
        }

        /// <summary>
        /// Register RedbService with configuration from IConfiguration.
        /// </summary>
        [Obsolete("Use services.AddRedb(configuration) from redb.Postgres.Extensions.RedbServiceExtensions")]
        public static IServiceCollection AddRedbService(
            this IServiceCollection services, 
            IConfiguration configuration, 
            string sectionName = "RedbService")
        {
            // Register configuration from appsettings.json
            var config = configuration.GetSection(sectionName).Get<RedbServiceConfiguration>() 
                        ?? new RedbServiceConfiguration();
            
            services.AddSingleton(config);
            services.AddScoped<IRedbService, RedbService>();

            return services;
        }

        /// <summary>
        /// Register RedbService with programmatic configuration via Action.
        /// </summary>
        [Obsolete("Use services.AddRedb(options => { ... }) from redb.Postgres.Extensions.RedbServiceExtensions")]
        public static IServiceCollection AddRedbService(
            this IServiceCollection services,
            Action<RedbServiceConfiguration> configureOptions)
        {
            var config = new RedbServiceConfiguration();
            configureOptions(config);
            
            services.AddSingleton(config);
            services.AddScoped<IRedbService, RedbService>();

            return services;
        }

        /// <summary>
        /// Register RedbService with programmatic configuration via Builder.
        /// </summary>
        [Obsolete("Use services.AddRedb(options => { ... }) from redb.Postgres.Extensions.RedbServiceExtensions")]
        public static IServiceCollection AddRedbService(
            this IServiceCollection services,
            Action<RedbServiceConfigurationBuilder> configureBuilder)
        {
            var builder = new RedbServiceConfigurationBuilder();
            configureBuilder(builder);
            var config = builder.Build();

            services.AddSingleton(config);
            services.AddScoped<IRedbService, RedbService>();

            return services;
        }

        /// <summary>
        /// Register RedbService with predefined profile.
        /// </summary>
        [Obsolete("Use services.AddRedb(PredefinedConfigurations.GetByName(profile)) from redb.Postgres.Extensions.RedbServiceExtensions")]
        public static IServiceCollection AddRedbService(
            this IServiceCollection services,
            string profileName)
        {
            var config = profileName.ToLowerInvariant() switch
            {
                "development" => PredefinedConfigurations.Development,
                "production" => PredefinedConfigurations.Production,
                "highperformance" => PredefinedConfigurations.HighPerformance,
                "bulkoperations" => PredefinedConfigurations.BulkOperations,
                "debug" => PredefinedConfigurations.Debug,
                "integrationtesting" => PredefinedConfigurations.IntegrationTesting,
                "datamigration" => PredefinedConfigurations.DataMigration,
                _ => throw new ArgumentException($"Unknown profile: {profileName}")
            };

            services.AddSingleton(config);
            services.AddScoped<IRedbService, RedbService>();

            return services;
        }

        /// <summary>
        /// Register RedbService with combined configuration (profile + additional settings).
        /// </summary>
        [Obsolete("Use services.AddRedb(options => { ... }) from redb.Postgres.Extensions.RedbServiceExtensions")]
        public static IServiceCollection AddRedbService(
            this IServiceCollection services,
            string profileName,
            Action<RedbServiceConfigurationBuilder> additionalConfiguration)
        {
            var baseConfig = profileName.ToLowerInvariant() switch
            {
                "development" => PredefinedConfigurations.Development,
                "production" => PredefinedConfigurations.Production,
                "highperformance" => PredefinedConfigurations.HighPerformance,
                "bulkoperations" => PredefinedConfigurations.BulkOperations,
                "debug" => PredefinedConfigurations.Debug,
                "integrationtesting" => PredefinedConfigurations.IntegrationTesting,
                "datamigration" => PredefinedConfigurations.DataMigration,
                _ => throw new ArgumentException($"Unknown profile: {profileName}")
            };

            var builder = new RedbServiceConfigurationBuilder(baseConfig);
            additionalConfiguration(builder);
            var finalConfig = builder.Build();

            services.AddSingleton(finalConfig);
            services.AddScoped<IRedbService, RedbService>();

            return services;
        }

        /// <summary>
        /// Register RedbService with configuration validation.
        /// </summary>
        [Obsolete("Use services.AddRedb(configuration) - validation now built into configuration properties")]
        public static IServiceCollection AddValidatedRedbService(
            this IServiceCollection services,
            IConfiguration configuration,
            bool throwOnValidationError = false,
            string sectionName = "RedbService")
        {
            var config = configuration.GetSection(sectionName).Get<RedbServiceConfiguration>() 
                        ?? new RedbServiceConfiguration();
            
            // Validate configuration
            var validator = new RedbServiceConfigurationValidator();
            var validationResult = validator.Validate(null, config);
            
            if (!validationResult.Succeeded && throwOnValidationError)
            {
                throw new InvalidOperationException($"Configuration validation failed: {validationResult.FailureMessage}");
            }

            services.AddSingleton(config);
            services.AddScoped<IRedbService, RedbService>();

            return services;
        }

        /// <summary>
        /// Register RedbService with configuration change monitoring (hot-reload).
        /// </summary>
        [Obsolete("Use services.AddRedb(configuration) from redb.Postgres.Extensions.RedbServiceExtensions")]
        public static IServiceCollection AddRedbServiceWithHotReload(
            this IServiceCollection services,
            IConfiguration configuration,
            string sectionName = "RedbService")
        {
            // For hot-reload use IOptionsMonitor
            services.Configure<RedbServiceConfiguration>(configuration.GetSection(sectionName));
            
            // Register change monitoring (if available)
            // Configuration monitoring is deferred
            // services.AddRedbServiceConfigurationMonitoring(configuration);

            // Register RedbService with hot-reload support
            services.AddScoped<IRedbService>(provider =>
            {
                try
                {
                    var configMonitor = provider.GetService<IOptionsMonitor<RedbServiceConfiguration>>();
                    if (configMonitor != null)
                    {
                        // TODO: Implement hot-reload support in RedbService
                        var config = configMonitor.CurrentValue;
                        return new RedbService(provider);
                    }
                }
                catch { }
                
                // Fallback to regular configuration
                return new RedbService(provider);
            });

            return services;
        }
    }
}
