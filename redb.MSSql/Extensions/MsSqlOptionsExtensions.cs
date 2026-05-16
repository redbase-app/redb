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
using redb.Core.Configuration;
using redb.Core.Services;
using redb.MSSql.Data;
using redb.MSSql.Providers;
using redb.MSSql.Sql;

namespace redb.MSSql.Extensions;

/// <summary>
/// MSSQL provider extensions for RedbOptionsBuilder.
/// </summary>
public static class MsSqlOptionsExtensions
{
    /// <summary>
    /// Configures REDB to use Microsoft SQL Server as the database provider.
    /// </summary>
    /// <param name="builder">The options builder.</param>
    /// <param name="connectionString">MSSQL connection string. If null, must be set in Configure().</param>
    /// <returns>The options builder for chaining.</returns>
    /// <example>
    /// services.AddRedb(options => options
    ///     .UseMsSql("Server=localhost;Database=redb;Trusted_Connection=true")
    ///     .Configure(c => c.EnableLazyLoadingForProps = true));
    /// </example>
    public static RedbOptionsBuilder UseMsSql(
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
                    "Pro version requires redb.MSSql.Pro package. Use AddRedbPro() from redb.MSSql.Pro.Extensions.");
            
            RegisterOpenSourceServices(services, config);
        });
        
        return builder;
    }
    
    /// <summary>
    /// Registers OpenSource MSSQL services.
    /// Called by UseMsSql() and can be extended by Pro version.
    /// </summary>
    internal static void RegisterOpenSourceServices(IServiceCollection services, RedbServiceConfiguration config)
    {
        // Context
        if (!string.IsNullOrEmpty(config.ConnectionString))
        {
            services.AddScoped<IRedbContext>(_ => 
                new SqlRedbContext(config.ConnectionString));
        }
        
        // Security
        services.AddScoped<IRedbSecurityContext>(_ => 
            AmbientSecurityContext.GetOrCreateDefault());
        
        // SQL Dialect
        services.AddSingleton<ISqlDialect, MsSqlDialect>();
        
        // Core providers
        services.AddScoped<ISchemeSyncProvider, MssqlSchemeSyncProvider>();
        services.AddScoped<IObjectStorageProvider, MssqlObjectStorageProvider>();
        services.AddScoped<IUserProvider, MssqlUserProvider>();
        services.AddScoped<IRoleProvider, MssqlRoleProvider>();
        services.AddScoped<IPermissionProvider, MssqlPermissionProvider>();
        services.AddScoped<IRedbObjectSerializer, SystemTextJsonRedbSerializer>();
        services.AddScoped<IUserConfigurationService, UserConfigurationService>();
        
        // Query providers
        services.AddScoped<ILazyPropsLoader, LazyPropsLoader>();
        services.AddScoped<IQueryableProvider, MssqlQueryableProvider>();
        services.AddScoped<ITreeProvider, MssqlTreeProvider>();
        services.AddScoped<IListProvider, MssqlListProvider>();
        services.AddScoped<IValidationProvider, MssqlValidationProvider>();
        
        // Background deletion service (singleton + hosted service)
        services.AddSingleton<BackgroundDeletionService>();
        services.AddSingleton<IBackgroundDeletionService>(sp => sp.GetRequiredService<BackgroundDeletionService>());
        services.AddHostedService(sp => sp.GetRequiredService<BackgroundDeletionService>());
        
        // Main service
        services.AddScoped<IRedbService, RedbService>();
    }
}

