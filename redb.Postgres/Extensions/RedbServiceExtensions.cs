using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using redb.Core;
using redb.Core.Models.Configuration;
using redb.Core.Models.Contracts;
using redb.Core.Models.Security;
using redb.Postgres.Providers;
using redb.Core.Providers;
using redb.Core.Serialization;

namespace redb.Postgres.Extensions
{
    /// <summary>
    /// Extension methods for unified REDB registration following EF Core AddDbContext pattern
    /// </summary>
    public static class RedbServiceExtensions
    {
        /// <summary>
        /// Registers REDB with configuration via lambda (main method).
        /// Automatically registers all necessary providers and services.
        /// </summary>
        /// <param name="services">Service collection</param>
        /// <param name="configure">Delegate for configuration setup</param>
        /// <returns>Service collection for method chaining</returns>
        /// <example>
        /// services.AddRedb(options => {
        ///     options.EavSaveStrategy = EavSaveStrategy.ChangeTracking;
        ///     options.EnablePropsCache = true;
        ///     options.PropsCacheMaxSize = 50000;
        /// });
        /// </example>
        [Obsolete("Use services.AddRedb(o => o.UsePostgres(connectionString)) from redb.Core.Extensions")]
        public static IServiceCollection AddRedbPostgres(
            this IServiceCollection services,
            Action<RedbServiceConfiguration>? configure = null)
        {
            var config = new RedbServiceConfiguration();
            configure?.Invoke(config); // Validation triggers in setters!
            
            services.AddSingleton(config);
            
            // Auto-register IRedbContext if ConnectionString is provided
            if (!string.IsNullOrEmpty(config.ConnectionString))
            {
                var dataSource = Npgsql.NpgsqlDataSource.Create(config.ConnectionString);
                services.AddSingleton(dataSource);
                services.AddScoped<Core.Data.IRedbContext>(sp => 
                    new Data.NpgsqlRedbContext(sp.GetRequiredService<Npgsql.NpgsqlDataSource>()));
            }
            
            // Register SecurityContext (required for providers)
            services.AddScoped<IRedbSecurityContext>(sp => 
                AmbientSecurityContext.GetOrCreateDefault());
            
            // Register ALL providers automatically
            services.AddScoped<ISchemeSyncProvider, PostgresSchemeSyncProvider>();
            services.AddScoped<IObjectStorageProvider, PostgresObjectStorageProvider>();
            services.AddScoped<IUserProvider, PostgresUserProvider>();
            services.AddScoped<IPermissionProvider, PostgresPermissionProvider>();
            services.AddScoped<IRedbObjectSerializer, SystemTextJsonRedbSerializer>();
            
            // Register user configuration service
            services.AddScoped<Core.Configuration.IUserConfigurationService, Postgres.Configuration.UserConfigurationService>();
            
            services.AddScoped<IRedbService, RedbService>();
            
            return services;
        }

        /// <summary>
        /// Registers REDB with configuration from appsettings.json
        /// </summary>
        /// <param name="services">Service collection</param>
        /// <param name="configuration">Application configuration</param>
        /// <param name="sectionName">Section name in appsettings.json (default "RedbService")</param>
        /// <returns>Service collection for method chaining</returns>
        /// <example>
        /// In appsettings.json:
        /// "RedbService": { "EnablePropsCache": true, "PropsCacheMaxSize": 50000 }
        /// services.AddRedb(Configuration);
        /// </example>
        [Obsolete("Use services.AddRedb(o => o.UsePostgres(connectionString)) from redb.Core.Extensions")]
        public static IServiceCollection AddRedbPostgresFromConfig(
            this IServiceCollection services,
            IConfiguration configuration,
            string sectionName = "RedbService")
        {
            var config = configuration.GetSection(sectionName).Get<RedbServiceConfiguration>() 
                        ?? new RedbServiceConfiguration();
            
            services.AddSingleton(config);
            
            // Auto-register IRedbContext if ConnectionString is provided
            if (!string.IsNullOrEmpty(config.ConnectionString))
            {
                var dataSource = Npgsql.NpgsqlDataSource.Create(config.ConnectionString);
                services.AddSingleton(dataSource);
                services.AddScoped<Core.Data.IRedbContext>(sp => 
                    new Data.NpgsqlRedbContext(sp.GetRequiredService<Npgsql.NpgsqlDataSource>()));
            }
            
            // Register SecurityContext (required for providers)
            services.AddScoped<IRedbSecurityContext>(sp => 
                AmbientSecurityContext.GetOrCreateDefault());
            
            // Register providers
            services.AddScoped<ISchemeSyncProvider, PostgresSchemeSyncProvider>();
            services.AddScoped<IObjectStorageProvider, PostgresObjectStorageProvider>();
            services.AddScoped<IUserProvider, PostgresUserProvider>();
            services.AddScoped<IPermissionProvider, PostgresPermissionProvider>();
            services.AddScoped<IRedbObjectSerializer, SystemTextJsonRedbSerializer>();
            
            // Register user configuration service
            services.AddScoped<Core.Configuration.IUserConfigurationService, Postgres.Configuration.UserConfigurationService>();
            
            services.AddScoped<IRedbService, RedbService>();
            
            return services;
        }

        /// <summary>
        /// Registers REDB with ready configuration instance
        /// </summary>
        /// <param name="services">Service collection</param>
        /// <param name="configuration">Ready configuration instance</param>
        /// <returns>Service collection for method chaining</returns>
        /// <example>
        /// var config = PredefinedConfigurations.Production;
        /// services.AddRedb(config);
        /// </example>
        public static IServiceCollection AddRedb(
            this IServiceCollection services,
            RedbServiceConfiguration configuration)
        {
            services.AddSingleton(configuration);
            
            // Auto-register IRedbContext if ConnectionString is provided
            if (!string.IsNullOrEmpty(configuration.ConnectionString))
            {
                var dataSource = Npgsql.NpgsqlDataSource.Create(configuration.ConnectionString);
                services.AddSingleton(dataSource);
                services.AddScoped<Core.Data.IRedbContext>(sp => 
                    new Data.NpgsqlRedbContext(sp.GetRequiredService<Npgsql.NpgsqlDataSource>()));
            }
            
            // Register SecurityContext (required for providers)
            services.AddScoped<IRedbSecurityContext>(sp => 
                AmbientSecurityContext.GetOrCreateDefault());
            
            services.AddScoped<ISchemeSyncProvider, PostgresSchemeSyncProvider>();
            services.AddScoped<IObjectStorageProvider, PostgresObjectStorageProvider>();
            services.AddScoped<IUserProvider, PostgresUserProvider>();
            services.AddScoped<IPermissionProvider, PostgresPermissionProvider>();
            services.AddScoped<IRedbObjectSerializer, SystemTextJsonRedbSerializer>();
            services.AddScoped<IRedbService, RedbService>();
            
            return services;
        }
    }
}

