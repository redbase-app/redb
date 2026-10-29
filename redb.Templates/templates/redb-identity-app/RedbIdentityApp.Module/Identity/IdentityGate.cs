using System.Globalization;
using System.Text.Json;
using redb.Route.Abstractions;
using redb.Route.Http;

namespace RedbIdentityApp.Module.Identity;

/// <summary>
/// The first step of the API route, after redb.Route's bearer check. No controller runs unless it lets
/// the request through:
/// <list type="bullet">
///   <item>the token could not be checked because Identity is down or refuses the module
///   (<see cref="BearerTokenValidator.UnavailableClaim"/>): 503 with <c>Retry-After</c>;</item>
///   <item>the token is good but was not issued for this API's scope: 403 with
///   <c>WWW-Authenticate: Bearer error="insufficient_scope"</c> (RFC 6750 §3.1).</item>
/// </list>
/// A missing or bad token never gets here: redb.Route has answered 401 already.
/// </summary>
public sealed class IdentityGate(string realm, string apiScope)
{
    public const int RetryAfterSeconds = 5;

    public Task Process(IExchange exchange, CancellationToken ct)
    {
        var principal = ExchangePrincipal.Get(exchange);
        if (principal?.FindFirst(BearerTokenValidator.UnavailableClaim)?.Value is { } reason)
        {
            Answer(exchange, 503, new { message = $"The sign-in service is not available ({reason}). Try again shortly." });
            exchange.Out!.Headers["Retry-After"] = RetryAfterSeconds.ToString(CultureInfo.InvariantCulture);
            exchange.Stop();
        }
        else if (principal?.HasClaim(BearerTokenValidator.ScopeClaim, apiScope) != true)
        {
            InsufficientScope(exchange, realm, apiScope);
            exchange.Stop();
        }

        return Task.CompletedTask;
    }

    /// <summary>403 for a token without <paramref name="scope"/>; the body is what the controller returns.</summary>
    public static void InsufficientScope(IExchange exchange, string realm, string scope)
    {
        Answer(exchange, 403, new { message = $"The token lacks the scope {scope}." });
        exchange.Out!.Headers["WWW-Authenticate"] =
            $"Bearer realm=\"{realm}\", error=\"insufficient_scope\", error_description=\"The token lacks the scope {scope}\", scope=\"{scope}\"";
    }

    private static void Answer(IExchange exchange, int status, object body)
    {
        exchange.Out ??= exchange.In.Clone();
        exchange.Out.Headers[HttpHeaders.ResponseCode] = status;
        exchange.Out.Headers[HttpHeaders.ContentType] = "application/json";
        exchange.Out.Body = JsonSerializer.SerializeToUtf8Bytes(body, body.GetType());
    }
}
