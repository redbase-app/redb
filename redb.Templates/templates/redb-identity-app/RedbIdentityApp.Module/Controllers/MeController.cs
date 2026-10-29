using redb.Route.Controllers.Attributes;
using RedbIdentityApp.Contracts;
using RedbIdentityApp.Module.Identity;

namespace RedbIdentityApp.Module.Controllers;

/// <summary><c>GET /api/me</c>: who the module sees behind the token. Handy to check a token by hand.</summary>
[Route("/api/me")]
public sealed class MeController : ApiController
{
    [HttpGet]
    public MeDto Get()
    {
        var caller = Caller;
        return new MeDto
        {
            Subject = Subject,
            UserId = long.TryParse(caller.FindFirst(BearerTokenValidator.UserIdClaim)?.Value, out var id) ? id : null,
            UserName = caller.FindFirst(BearerTokenValidator.NameClaim)?.Value,
            ClientId = caller.FindFirst(BearerTokenValidator.ClientIdClaim)?.Value,
            Scopes = caller.FindAll(BearerTokenValidator.ScopeClaim).Select(c => c.Value).ToList(),
            ExpiresAt = long.TryParse(caller.FindFirst(BearerTokenValidator.ExpiresClaim)?.Value, out var exp)
                ? DateTimeOffset.FromUnixTimeSeconds(exp)
                : null,
        };
    }
}
