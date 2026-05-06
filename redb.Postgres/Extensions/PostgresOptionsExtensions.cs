using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using redb.Core;
using redb.Core.Data;
using redb.Core.Extensions;
using redb.Core.Models.Configuration;
using redb.Core.Models.Contracts;
using redb.Core.Models.Security;
using redb.Core.Providers;
using redb.Core.Query;
using redb.Core.Serialization;
using redb.Core.Services;
using redb.Postgres.Data;
using redb.Postgres.Providers;
using redb.Postgres.Sql;

namespace redb.Postgres.Extensions;

/// <summary>
/// PostgreSQL provider extensions for RedbOptionsBuilder.
/// </summary>
public static class PostgresOptionsExtensions
{
    /// <summary>
    /// Configures REDB to use PostgreSQL as the database provider.
    /// For Pro version, use AddRedbPro() from redb.Postgres.Pro package.
    /// </summary>
    /// <param name="builder">The options builder.</param>
    /// <param name="connectionString">PostgreSQL connection string. If null, must be set in Configure().</param>
    /// <returns>The options builder for chaining.</returns>
    /// <example>
    /// services.AddRedb(options => options
    ///     .UsePostgres("Host=localhost;Database=mydb;Username=user;Password=pass")
    ///     .Configure(c => c.EnableLazyLoadingForProps = true));
    /// </example>
    public static RedbOptionsBuilder UsePostgres(
        this RedbOptionsBuilder builder,
        string? connectionString = null)
    {
        var infrastructure = (IRedbOptionsBuilderInfrastructure)builder;
        
        if (!string.IsNullOrEmpty(connectionString))
            infrastructure.Configuration.ConnectionString = connectionString;
        
        infrastructure.SetProviderRegistration((services, config, isPro) =>
        {
            if (isPro)
                throw new InvalidOperationException(
                    "Pro version requires redb.Postgres.Pro package. Use AddRedbPro() from redb.Postgres.Pro.Extensions.");
            
            RegisterOpenSourceServices(services, config);
        });
        
        return builder;
    }
    
    /// <summary>
    /// Registers OpenSource PostgreSQL services.
    /// Called by UsePostgres() and can be used by Pro version as fallback.
    /// </summary>
    public static void RegisterOpenSourceServices(IServiceCollection services, RedbServiceConfiguration config)
    {
        // DataSource and Context
        if (!string.IsNullOrEmpty(config.ConnectionString))
        {
            var dataSource = Npgsql.NpgsqlDataSource.Create(config.ConnectionString);
            services.AddSingleton(dataSource);
            services.AddScoped<IRedbContext>(sp => 
                new NpgsqlRedbContext(sp.GetRequiredService<Npgsql.NpgsqlDataSource>()));
        }
        
        // Security
        services.AddScoped<IRedbSecurityContext>(sp => 
            AmbientSecurityContext.GetOrCreateDefault());
        
        // SQL Dialect
        services.AddSingleton<ISqlDialect, PostgreSqlDialect>();
        
        // Core providers
        services.AddScoped<ISchemeSyncProvider, PostgresSchemeSyncProvider>();
        services.AddScoped<IObjectStorageProvider, PostgresObjectStorageProvider>();
        services.AddScoped<IUserProvider, PostgresUserProvider>();
        services.AddScoped<IPermissionProvider, PostgresPermissionProvider>();
        services.AddScoped<IRedbObjectSerializer, SystemTextJsonRedbSerializer>();
        services.AddScoped<Core.Configuration.IUserConfigurationService, Configuration.UserConfigurationService>();
        
        // Query providers
        services.AddScoped<ILazyPropsLoader, LazyPropsLoader>();
        services.AddScoped<IQueryableProvider, PostgresQueryableProvider>();
        services.AddScoped<ITreeProvider, PostgresTreeProvider>();
        services.AddScoped<IListProvider, PostgresListProvider>();
        services.AddScoped<IValidationProvider, PostgresValidationProvider>();
        
        // Background deletion service (singleton + hosted service)
        services.AddSingleton<BackgroundDeletionService>();
        services.AddSingleton<IBackgroundDeletionService>(sp => sp.GetRequiredService<BackgroundDeletionService>());
        services.AddHostedService(sp => sp.GetRequiredService<BackgroundDeletionService>());
        
        // Main service
        services.AddScoped<IRedbService, RedbService>();
    }
}

