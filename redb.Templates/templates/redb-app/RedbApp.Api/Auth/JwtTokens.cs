using System.Security.Claims;
using System.Text;
using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Tokens;
using redb.Route.Http;

namespace RedbApp.Api.Auth;

/// <summary>
/// Issues the sign-in token and checks it on every API call. One key signs and verifies (HMAC-SHA256), so
/// the API is the only party that can do either.
/// <para>
/// As an <see cref="IHttpTokenValidator"/> it is registered in the route registry and named by
/// <c>TokenValidator</c> on the REST declarations: a request without a valid token gets 401 and never
/// reaches the route; an accepted one reaches it with the principal on the exchange
/// (<c>ExchangePrincipal.Get</c>).
/// </para>
/// </summary>
public sealed class JwtTokens(ModuleSettings settings) : IHttpTokenValidator
{
    public const string RegistryName = "tokens";

    private readonly JsonWebTokenHandler _handler = new();
    private readonly SymmetricSecurityKey _key = new(Encoding.UTF8.GetBytes(settings.SigningKey));

    public (string Token, DateTimeOffset ExpiresAt) Issue(AppUser user)
    {
        var expires = DateTime.UtcNow.AddMinutes(settings.TokenLifetimeMinutes);
        var token = _handler.CreateToken(new SecurityTokenDescriptor
        {
            Issuer = settings.Issuer,
            Audience = settings.Issuer,
            Expires = expires,
            Subject = new ClaimsIdentity([
                new Claim(JwtRegisteredClaimNames.Sub, user.Login),
                new Claim("role", user.Role),
            ]),
            SigningCredentials = new SigningCredentials(_key, SecurityAlgorithms.HmacSha256),
        });
        return (token, expires);
    }

    public async Task<ClaimsPrincipal?> ValidateAsync(string token, CancellationToken ct)
    {
        var result = await _handler.ValidateTokenAsync(token, new TokenValidationParameters
        {
            ValidIssuer = settings.Issuer,
            ValidAudience = settings.Issuer,
            IssuerSigningKey = _key,
            ValidateLifetime = true,
            NameClaimType = JwtRegisteredClaimNames.Sub,
            RoleClaimType = "role",
        });
        return result.IsValid ? new ClaimsPrincipal(result.ClaimsIdentity) : null;
    }
}
