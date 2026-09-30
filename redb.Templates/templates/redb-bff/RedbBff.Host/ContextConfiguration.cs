using Microsoft.Extensions.Configuration;
using redb.Route.Abstractions;

namespace RedbBff.Host;

/// <summary>
/// The context configuration the way a Tsak worker builds it, in miniature. Each layer deep-merges over
/// the previous one, later layers win:
/// <list type="number">
///   <item>the module's <c>RedbBff.Backend.config.json</c>,</item>
///   <item><c>Tsak:Contexts:{context}:Override</c> of the host configuration.</item>
/// </list>
/// Every root key then becomes a property of the route context; a section becomes a dictionary. So the
/// same keys and the same environment variables work here and on a Tsak worker.
/// </summary>
public static class ContextConfiguration
{
    /// <summary>The context the module asks for, from its <c>ContextName</c>.</summary>
    public static string ContextNameOf(string moduleConfigPath) =>
        new ConfigurationBuilder().AddJsonFile(moduleConfigPath, optional: false).Build()["ContextName"]
        ?? throw new InvalidOperationException($"{moduleConfigPath} has no ContextName.");

    public static IDictionary<string, object?> Build(IConfiguration host, string contextName, string moduleConfigPath)
    {
        var config = Read(new ConfigurationBuilder().AddJsonFile(moduleConfigPath, optional: false).Build());
        Merge(config, Read(host.GetSection($"Tsak:Contexts:{contextName}:Override")));
        return config;
    }

    public static void ApplyTo(IRouteContext context, IDictionary<string, object?> config)
    {
        foreach (var (key, value) in config)
        {
            if (value is not null)
                context.SetProperty(key, value);
        }
    }

    private static Dictionary<string, object?> Read(IConfiguration section)
    {
        var result = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (var child in section.GetChildren())
            result[child.Key] = child.GetChildren().Any() ? Read(child) : child.Value;
        return result;
    }

    private static void Merge(IDictionary<string, object?> target, IDictionary<string, object?> layer)
    {
        foreach (var (key, value) in layer)
        {
            if (value is IDictionary<string, object?> nested
                && target.TryGetValue(key, out var existing) && existing is IDictionary<string, object?> current)
                Merge(current, nested);
            else
                target[key] = value;
        }
    }
}
