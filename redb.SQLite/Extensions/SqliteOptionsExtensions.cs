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
using redb.SQLite.Data;
using redb.SQLite.Providers;
using redb.SQLite.Sql;

namespace redb.SQLite.Extensions;

/// <summary>
/// SQLite provider extensions for RedbOptionsBuilder.
/// </summary>
public static class SqliteOptionsExtensions
{
    /// <summary>
    /// Configures REDB to use SQLite as the database provider.
    /// For Pro version, use AddRedbPro() from redb.SQLite.Pro package.
    /// </summary>
    /// <param name="builder">The options builder.</param>
    /// <param name="connectionString">SQLite connection string. If null, must be set in Configure().</param>
    /// <returns>The options builder for chaining.</returns>
    /// <example>
    /// services.AddRedb(options => options
    ///     .UseSqlite("Host=localhost;Database=mydb;Username=user;Password=pass")
    ///     .Configure(c => c.EnableLazyLoadingForProps = true));
    /// </example>
    public static RedbOptionsBuilder UseSqlite(
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
                    "Pro version requires redb.SQLite.Pro package. Use AddRedbPro() from redb.SQLite.Pro.Extensions.");
            
            RegisterOpenSourceServices(services, config);
        });
        
        return builder;
    }
    
    /// <summary>
    /// Registers OpenSource SQLite services.
    /// Called by UseSqlite() and can be used by Pro version as fallback.
    /// </summary>
    public static void RegisterOpenSourceServices(IServiceCollection services, RedbServiceConfiguration config)
    {
        // Free tier loads the native SQLite extension that hosts the server-side SQL
        // functions (get_object_json, pvt_build_*_sql, ...). Honour an explicit override
        // (REDB_SQLITE_EXTENSION env / manual NativeExtensionPath) first; otherwise
        // auto-locate the binary shipped in this package next to the running app.
        // Pro never sets NativeExtensionPath (it materializes/queries in C#).
        Data.SqliteDataSource.NativeExtensionPath ??= Data.SqliteDataSource.LocatePackagedExtension();

        // DataSource and Context
        if (!string.IsNullOrEmpty(config.ConnectionString))
        {
            var dataSource = Data.SqliteDataSource.Create(config.ConnectionString);
            services.AddSingleton(dataSource);
            services.AddScoped<IRedbContext>(sp => 
                new SqliteRedbContext(sp.GetRequiredService<Data.SqliteDataSource>()));
        }
        
        // Security
        services.AddScoped<IRedbSecurityContext>(sp => 
            AmbientSecurityContext.GetOrCreateDefault());
        
        // SQL Dialect
        services.AddSingleton<ISqlDialect, SqliteDialect>();
        
        // Core providers
        services.AddScoped<ISchemeSyncProvider, SqliteSchemeSyncProvider>();
        services.AddScoped<IObjectStorageProvider, SqliteObjectStorageProvider>();
        services.AddScoped<IUserProvider, SqliteUserProvider>();
        services.AddScoped<IPermissionProvider, SqlitePermissionProvider>();
        services.AddScoped<IRedbObjectSerializer, SystemTextJsonRedbSerializer>();
        services.AddScoped<Core.Configuration.IUserConfigurationService, Configuration.UserConfigurationService>();
        
        // Query providers
        services.AddScoped<ILazyPropsLoader, LazyPropsLoader>();
        services.AddScoped<IQueryableProvider, SqliteQueryableProvider>();
        services.AddScoped<ITreeProvider, SqliteTreeProvider>();
        services.AddScoped<IListProvider, SqliteListProvider>();
        services.AddScoped<IValidationProvider, SqliteValidationProvider>();
        
        // Background deletion service (singleton + hosted service)
        services.AddSingleton<BackgroundDeletionService>();
        services.AddSingleton<IBackgroundDeletionService>(sp => sp.GetRequiredService<BackgroundDeletionService>());
        services.AddHostedService(sp => sp.GetRequiredService<BackgroundDeletionService>());
        
        // Main service
        services.AddScoped<IRedbService, RedbService>();
    }
}

