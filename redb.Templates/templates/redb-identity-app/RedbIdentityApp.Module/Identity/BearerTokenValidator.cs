using System.Security.Claims;
using Microsoft.Extensions.Logging;
using redb.Route.Http;

namespace RedbIdentityApp.Module.Identity;

/// <summary>
/// The bearer check of the API endpoint (<c>inboundAuth=bearer&amp;tokenValidator=#...</c> in InitRoute):
/// redb.Route calls it before a controller runs.
/// <list type="bullet">
///   <item>No token, or one Identity calls inactive (expired, revoked, issued for another API): it
///   returns <c>null</c>, and redb.Route answers 401 with
///   <c>WWW-Authenticate: Bearer realm="...", error="invalid_token"</c>.</item>
///   <item>An active token: a principal with the token's subject, user, client and scopes.</item>
///   <item>Identity unreachable or refusing the module's own credentials: a principal marked
///   <see cref="UnavailableClaim"/>. The validator's interface knows only "principal or null", and
///   null would tell a client with a good token that its token is bad; <see cref="IdentityGate"/>
///   turns the mark into 503.</item>
/// </list>
/// </summary>
public sealed class BearerTokenValidator(TokenIntrospector introspector, ILogger logger) : IHttpTokenValidator
{
    public const string RegistryName = "identity-introspection";
    public const string AuthenticationType = "redb.Identity";

    /// <summary>Marks a principal whose token could not be checked; the value says why.</summary>
    public const string UnavailableClaim = "redb:identity-unavailable";

    public const string SubjectClaim = "sub";
    public const string UserIdClaim = "redb:user_id";
    public const string NameClaim = "name";
    public const string ClientIdClaim = "client_id";
    public const string ScopeClaim = "scope";
    public const string ExpiresClaim = "exp";

    public async Task<ClaimsPrincipal?> ValidateAsync(string token, CancellationToken ct)
    {
        Introspection result;
        try
        {
            result = await introspector.IntrospectAsync(token, ct).ConfigureAwait(false);
        }
        catch (IdentityUnavailableException ex)
        {
            logger.LogWarning(ex, "Bearer token not checked: {Reason}", ex.Message);
            return Unavailable("Identity is not available");
        }
        catch (IdentityRefusedException ex)
        {
            logger.LogError("Bearer token not checked: {Reason}", ex.Message);
            return Unavailable("Identity refused the module's client");
        }

        if (!result.Active || result.Subject is null)
            return null;

        var claims = new List<Claim> { new(SubjectClaim, result.Subject) };
        if (result.UserId is { } userId) claims.Add(new(UserIdClaim, userId));
        if (result.UserName is { } name) claims.Add(new(NameClaim, name));
        if (result.ClientId is { } clientId) claims.Add(new(ClientIdClaim, clientId));
        if (result.ExpiresAt is { } exp) claims.Add(new(ExpiresClaim, exp.ToUnixTimeSeconds().ToString(System.Globalization.CultureInfo.InvariantCulture)));
        claims.AddRange(result.Scopes.Select(scope => new Claim(ScopeClaim, scope)));

        return new ClaimsPrincipal(new ClaimsIdentity(claims, AuthenticationType, NameClaim, roleType: null));
    }

    // Authenticated on purpose: an unauthenticated principal is a 401 in redb.Route.
    private static ClaimsPrincipal Unavailable(string reason) =>
        new(new ClaimsIdentity([new Claim(UnavailableClaim, reason)], AuthenticationType));
}
