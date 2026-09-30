using System.Globalization;
using redb.Route.Abstractions;

namespace RedbBff.Backend;

/// <summary>
/// The module's settings, read from the properties of its route context. Both hosts fill them the same
/// way: RedbBff.Backend.config.json first, then the <c>Tsak:Contexts:{context}:Override</c> section of the
/// host configuration, which wins. The service key belongs only in the Override layer (RedbBff.Host
/// appsettings.json, or <c>Tsak__Contexts__redbbff__Override__Api__ServiceKey</c>), never in the module's
/// config file.
/// </summary>
public sealed record ModuleSettings(string ApiHost, int ApiPort, string ServiceKey)
{
    public static ModuleSettings FromContext(IRouteContext context)
    {
        var api = context.GetProperty<IDictionary<string, object?>>("Api") ?? throw Missing(context, "Api");

        var serviceKey = Required(context, api, "ServiceKey");
        if (serviceKey.Length < 32)
            throw new InvalidOperationException($"Api:ServiceKey of context '{context.ContextId}' must be at least 32 characters.");

        return new(
            // 127.0.0.1 by default: only the web server of the same machine calls the backend. A container
            // sets 0.0.0.0 and keeps the port inside the compose network (deploy/*.yml).
            ApiHost: Required(context, api, "Host"),
            ApiPort: int.Parse(Required(context, api, "Port"), CultureInfo.InvariantCulture),
            ServiceKey: serviceKey);
    }

    private static string Required(IRouteContext context, IDictionary<string, object?> section, string key) =>
        section.TryGetValue(key, out var value) && Convert.ToString(value, CultureInfo.InvariantCulture) is { Length: > 0 } text
            ? text
            : throw Missing(context, $"Api:{key}");

    private static InvalidOperationException Missing(IRouteContext context, string key) => new(
        $"{key} is not configured for context '{context.ContextId}': set it in RedbBff.Backend.config.json " +
        $"or in Tsak:Contexts:{context.ContextId}:Override.");
}
