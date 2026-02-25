using Microsoft.Extensions.DependencyInjection;
using redb.PropsEditor.Services;

namespace redb.PropsEditor.Extensions;

/// <summary>
/// Extension methods for registering redb.PropsEditor services.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds redb.PropsEditor services to the service collection.
    /// </summary>
    /// <example>
    /// builder.Services.AddRedbPropsEditor();
    /// </example>
    public static IServiceCollection AddRedbPropsEditor(this IServiceCollection services)
    {
        services.AddScoped<PropsMetadataService>();
        return services;
    }
}
