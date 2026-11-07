using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using redb.Route.Http;

namespace RedbBff.Backend;

/// <summary>
/// The backend has one caller: the web server. It sends the shared service key as a bearer token; any
/// other request gets 401 before a controller runs. The users sign in to the web server, not here.
/// </summary>
public sealed class ServiceKeyValidator(string serviceKey) : IHttpTokenValidator
{
    public const string RegistryName = "service-key";

    private readonly byte[] _expected = Encoding.UTF8.GetBytes(serviceKey);

    public Task<ClaimsPrincipal?> ValidateAsync(string token, CancellationToken ct)
    {
        // Constant-time comparison, so the answer time does not tell how much of the key was right.
        var accepted = CryptographicOperations.FixedTimeEquals(_expected, Encoding.UTF8.GetBytes(token));
        return Task.FromResult(accepted
            ? new ClaimsPrincipal(new ClaimsIdentity([new Claim(ClaimTypes.Name, "web")], authenticationType: "ServiceKey"))
            : null);
    }
}
