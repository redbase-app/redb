using System.Globalization;
using redb.Route.Abstractions;

namespace RedbIdentityApp.Module;

/// <summary>
/// The module's settings, read from the properties of its route context. The Tsak worker fills them from
/// RedbIdentityApp.Module.config.json, then from the <c>Tsak:Contexts:{context}:Override</c> section of
/// its configuration, which wins. The three client secrets belong only in the Override layer
/// (environment variables, deploy/.env), never in the module's config file.
/// </summary>
public sealed record ModuleSettings(
    string ApiHost,
    int ApiPort,
    string ResourceClientId,
    string ResourceClientSecret,
    string ApiScope,
    string AdminScope,
    string ManagementClientId,
    string ManagementClientSecret,
    string? WebClientId,
    string? WebClientSecret,
    IReadOnlyList<string> WebRedirectUris,
    IReadOnlyList<string> WebPostLogoutRedirectUris,
    TimeSpan IntrospectionCacheTtl,
    TimeSpan InactiveCacheTtl,
    TimeSpan IntrospectionTimeout)
{
    /// <summary>The direct-vm address Identity's audit sends its events to; see README, "Revocation".</summary>
    public string EventsEndpoint(IRouteContext context) => $"direct-vm://{context.ContextId}-identity-events";

    public static ModuleSettings FromContext(IRouteContext context)
    {
        var api = Section(context, context.GetProperty<IDictionary<string, object?>>("Api"), "Api");
        var identity = Section(context, context.GetProperty<IDictionary<string, object?>>("Identity"), "Identity");
        var introspection = identity.TryGetValue("Introspection", out var raw) && raw is IDictionary<string, object?> section
            ? section
            : new Dictionary<string, object?>();

        var webClientId = Optional(identity, "WebClientId");

        return new(
            // 127.0.0.1 by default: the web server shares the worker's network namespace.
            ApiHost: Required(context, api, "Api", "Host"),
            ApiPort: int.Parse(Required(context, api, "Api", "Port"), CultureInfo.InvariantCulture),
            ResourceClientId: Required(context, identity, "Identity", "ResourceClientId"),
            ResourceClientSecret: Secret(context, identity, "ResourceClientSecret"),
            ApiScope: Required(context, identity, "Identity", "ApiScope"),
            AdminScope: Required(context, identity, "Identity", "AdminScope"),
            ManagementClientId: Required(context, identity, "Identity", "ManagementClientId"),
            ManagementClientSecret: Secret(context, identity, "ManagementClientSecret"),
            WebClientId: webClientId,
            WebClientSecret: webClientId is null ? null : Secret(context, identity, "WebClientSecret"),
            WebRedirectUris: List(identity, "WebRedirectUris"),
            WebPostLogoutRedirectUris: List(identity, "WebPostLogoutRedirectUris"),
            IntrospectionCacheTtl: Seconds(introspection, "CacheSeconds", 60),
            InactiveCacheTtl: Seconds(introspection, "InactiveCacheSeconds", 10),
            IntrospectionTimeout: Seconds(introspection, "TimeoutSeconds", 5));
    }

    private static IDictionary<string, object?> Section(IRouteContext context, IDictionary<string, object?>? section, string name) =>
        section ?? throw new InvalidOperationException(
            $"{name} is not configured for context '{context.ContextId}': see RedbIdentityApp.Module.config.json.");

    private static string? Optional(IDictionary<string, object?> section, string key) =>
        section.TryGetValue(key, out var value) && Convert.ToString(value, CultureInfo.InvariantCulture) is { Length: > 0 } text
            ? text.Trim()
            : null;

    private static string Required(IRouteContext context, IDictionary<string, object?> section, string sectionName, string key) =>
        Optional(section, key) ?? throw new InvalidOperationException(
            $"{sectionName}:{key} is not configured for context '{context.ContextId}': set it in " +
            $"RedbIdentityApp.Module.config.json or in Tsak:Contexts:{context.ContextId}:Override.");

    // A client secret: only from the environment, and long enough not to be guessed.
    private static string Secret(IRouteContext context, IDictionary<string, object?> section, string key)
    {
        var value = Optional(section, key) ?? throw new InvalidOperationException(
            $"Identity:{key} is not set for context '{context.ContextId}': set " +
            $"Tsak__Contexts__{context.ContextId}__Override__Identity__{key} in the worker's environment.");
        return value.Length >= 32
            ? value
            : throw new InvalidOperationException($"Identity:{key} of context '{context.ContextId}' must be at least 32 characters.");
    }

    // A list is written as one string, items separated by commas or spaces: the same text works in the
    // config file and in one environment variable.
    private static IReadOnlyList<string> List(IDictionary<string, object?> section, string key) =>
        Optional(section, key)?.Split([',', ' ', ';'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries) ?? [];

    private static TimeSpan Seconds(IDictionary<string, object?> section, string key, int fallback) =>
        TimeSpan.FromSeconds(Optional(section, key) is { } text
            ? int.Parse(text, CultureInfo.InvariantCulture)
            : fallback);
}
