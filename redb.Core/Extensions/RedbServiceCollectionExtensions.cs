using Microsoft.Extensions.DependencyInjection;

namespace redb.Core.Extensions;

/// <summary>
/// Extension methods for registering REDB services.
/// </summary>
public static class RedbServiceCollectionExtensions
{
    /// <summary>
    /// Registers REDB OpenSource services with the specified database provider.
    /// </summary>
    /// <example>
    /// services.AddRedb(options => options
    ///     .UsePostgres(connectionString)
    ///     .Configure(c => c.EnableLazyLoadingForProps = true));
    /// </example>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Action to configure REDB options including database provider.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddRedb(
        this IServiceCollection services,
        Action<RedbOptionsBuilder> configure)
    {
        var builder = new RedbOptionsBuilder(services);
        var infrastructure = (IRedbOptionsBuilderInfrastructure)builder;
        infrastructure.IsPro = false;
        configure(builder);
        infrastructure.Build();
        return services;
    }
}

