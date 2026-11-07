using System.Globalization;
using redb.Route.Abstractions;

namespace RedbApp.Api;

/// <summary>
/// The module's settings, read from the properties of its route context. Both hosts fill them the same
/// way: RedbApp.Api.config.json first, then the <c>Tsak:Contexts:{context}:Override</c> section of the host
/// configuration, which wins. The signing key and the users belong only in the Override layer
/// (RedbApp.Host appsettings.json, or environment variables such as
/// <c>Tsak__Contexts__redbapp__Override__Auth__SigningKey</c>), never in the module's config file.
/// </summary>
public sealed record ModuleSettings(
    string ApiHost,
    int ApiPort,
    string CorsOrigins,
    string Issuer,
    int TokenLifetimeMinutes,
    string SigningKey,
    IReadOnlyDictionary<string, AppUser> Users)
{
    public static ModuleSettings FromContext(IRouteContext context)
    {
        var api = Section(context, "Api");
        var auth = Section(context, "Auth");

        var signingKey = Required(context, auth, "Auth", "SigningKey");
        // HMAC-SHA256 needs a key of at least 256 bits.
        if (System.Text.Encoding.UTF8.GetByteCount(signingKey) < 32)
            throw new InvalidOperationException($"Auth:SigningKey of context '{context.ContextId}' must be at least 32 characters.");

        var users = auth.TryGetValue("Users", out var raw) && raw is IDictionary<string, object?> section
            ? section.ToDictionary(
                u => u.Key,
                u => AppUser.From(context, u.Key, u.Value as IDictionary<string, object?>),
                StringComparer.OrdinalIgnoreCase)
            : throw Missing(context, "Auth:Users");

        return new(
            ApiHost: Required(context, api, "Api", "Host"),
            ApiPort: int.Parse(Required(context, api, "Api", "Port"), CultureInfo.InvariantCulture),
            // The origin of the Blazor client when it is served from another port or host. Behind one
            // reverse proxy (deploy/nginx.conf) the browser sees one origin and CORS is not used.
            CorsOrigins: Required(context, api, "Api", "CorsOrigins"),
            Issuer: Required(context, auth, "Auth", "Issuer"),
            TokenLifetimeMinutes: int.Parse(Required(context, auth, "Auth", "TokenLifetimeMinutes"), CultureInfo.InvariantCulture),
            SigningKey: signingKey,
            Users: users);
    }

    internal static IDictionary<string, object?> Section(IRouteContext context, string name) =>
        context.GetProperty<IDictionary<string, object?>>(name) ?? throw Missing(context, name);

    internal static string Required(IRouteContext context, IDictionary<string, object?>? section, string sectionName, string key) =>
        section is not null && section.TryGetValue(key, out var value)
            && Convert.ToString(value, CultureInfo.InvariantCulture) is { Length: > 0 } text
            ? text
            : throw Missing(context, $"{sectionName}:{key}");

    internal static InvalidOperationException Missing(IRouteContext context, string key) => new(
        $"{key} is not configured for context '{context.ContextId}': set it in RedbApp.Api.config.json " +
        $"or in Tsak:Contexts:{context.ContextId}:Override.");
}

/// <summary>
/// A user who can sign in. The users come from configuration: a stand-in that keeps the template small.
/// A real application keeps its users in a store of its own, for example redb.Identity.
/// </summary>
public sealed record AppUser(string Login, string Password, string Role)
{
    internal static AppUser From(IRouteContext context, string login, IDictionary<string, object?>? section) => new(
        login,
        ModuleSettings.Required(context, section, $"Auth:Users:{login}", "Password"),
        ModuleSettings.Required(context, section, $"Auth:Users:{login}", "Role"));
}
