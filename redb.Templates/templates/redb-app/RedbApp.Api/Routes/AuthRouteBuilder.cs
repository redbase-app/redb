using System.Security.Cryptography;
using System.Text;
using redb.Route.Abstractions;
using redb.Route.Core;
using redb.Route.Http;
using redb.Route.Http.Rest;
using RedbApp.Api.Auth;
using RedbApp.Models;

namespace RedbApp.Api.Routes;

/// <summary>
/// <c>POST /api/auth/login</c>: login and password in, a token out. The only API call without a token.
/// </summary>
public sealed class AuthRouteBuilder(ModuleSettings settings, JwtTokens tokens) : RouteBuilder
{
    protected override void Configure()
    {
        this.Rest("/api/auth", o => ApiOptions.Apply(o, settings, requireToken: false))
            .Post("/login").Consumes("application/json").Type<LoginRequest>().OutType<LoginResponse>()
            .To("direct:login");

        From("direct:login")
            .RouteId("login")
            .Process(Login);
    }

    private void Login(IExchange e)
    {
        var request = (LoginRequest)e.In.Body!;
        if (!settings.Users.TryGetValue(request.Login, out var user) || !SamePassword(user.Password, request.Password))
        {
            e.In.Headers[HttpHeaders.ResponseCode] = 401;
            e.In.Body = new ApiError { Message = "Wrong login or password." };
            return;
        }

        var (token, expiresAt) = tokens.Issue(user);
        e.In.Body = new LoginResponse { Token = token, Login = user.Login, Role = user.Role, ExpiresAt = expiresAt };
    }

    // Constant-time comparison, so the answer time does not tell how much of the password was right.
    private static bool SamePassword(string expected, string actual) =>
        CryptographicOperations.FixedTimeEquals(Encoding.UTF8.GetBytes(expected), Encoding.UTF8.GetBytes(actual));
}

/// <summary>The options every REST declaration of the module shares.</summary>
internal static class ApiOptions
{
    public static void Apply(RestOptions o, ModuleSettings settings, bool requireToken)
    {
        o.Host = settings.ApiHost;
        o.Port = settings.ApiPort;
        o.BindingMode = RestBindingMode.Json;
        // The Blazor client runs on another port in development, so the browser asks for CORS.
        o.ExtraConsumerOptions = $"cors=true&corsOrigins={Uri.EscapeDataString(settings.CorsOrigins)}";
        if (requireToken)
        {
            o.InboundAuth = HttpAuthScheme.Bearer;
            o.TokenValidator = JwtTokens.RegistryName;
        }
    }
}
