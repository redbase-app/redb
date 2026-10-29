using System.Collections.Concurrent;
using System.Net;
using System.Text.Json;
using Microsoft.AspNetCore.Authentication.OpenIdConnect;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.Protocols.OpenIdConnect;

namespace RedbIdentityApp.Web.Auth;

/// <summary>
/// A valid access token for the user's session, refreshed shortly before it expires.
/// <list type="bullet">
///   <item>Identity refuses the refresh (<c>invalid_grant</c>: the session was revoked, the user disabled,
///   the refresh token expired): the session is over, <see cref="SessionEndedException"/>.</item>
///   <item>Identity cannot be reached: while the current access token is still valid it is used; after
///   that <see cref="IdentityUnavailableException"/>. A network error never ends a session.</item>
/// </list>
/// One refresh per session at a time: Identity rotates refresh tokens, and two parallel refreshes with
/// the same one would race. The lock is per process; with several replicas, keep a user on one of them
/// or accept that the slower refresh is refused and the user signs in again.
/// </summary>
public sealed class AccessTokens(
    UserTokenStore store,
    IHttpClientFactory httpClients,
    IOptionsMonitor<OpenIdConnectOptions> oidc,
    IOptions<WebIdentityOptions> identity,
    ILogger<AccessTokens> logger)
{
    /// <summary>The name of the HttpClient that talks to Identity (Program.cs).</summary>
    public const string HttpClientName = "identity";

    private static readonly TimeSpan RefreshBefore = TimeSpan.FromSeconds(60);
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _locks = new(StringComparer.Ordinal);

    public async Task<string> GetAsync(string sessionKey, CancellationToken ct)
    {
        var tokens = await store.GetAsync(sessionKey, ct) ?? throw new SessionEndedException();
        if (tokens.ExpiresAt - DateTimeOffset.UtcNow > RefreshBefore)
            return tokens.AccessToken;

        var gate = _locks.GetOrAdd(sessionKey, _ => new SemaphoreSlim(1, 1));
        await gate.WaitAsync(ct);
        try
        {
            // Another request may have refreshed while this one waited.
            tokens = await store.GetAsync(sessionKey, ct) ?? throw new SessionEndedException();
            if (tokens.ExpiresAt - DateTimeOffset.UtcNow > RefreshBefore)
                return tokens.AccessToken;
            if (tokens.RefreshToken is null)
                return StillValid(tokens) ?? throw new SessionEndedException();

            var refreshed = await RefreshAsync(sessionKey, tokens, ct);
            return refreshed?.AccessToken ?? StillValid(tokens) ?? throw new IdentityUnavailableException();
        }
        finally
        {
            gate.Release();
        }
    }

    /// <summary>Ends the session here and at Identity: the refresh token is revoked (RFC 7009).</summary>
    public async Task<string?> EndSessionAsync(string sessionKey, CancellationToken ct)
    {
        var tokens = await store.GetAsync(sessionKey, ct);
        await store.RemoveAsync(sessionKey, ct);
        _locks.TryRemove(sessionKey, out _);
        if (tokens?.RefreshToken is { } refreshToken)
        {
            try
            {
                var configuration = await ConfigurationAsync(ct);
                if (configuration.AdditionalData.TryGetValue("revocation_endpoint", out var endpoint) && endpoint is string url)
                {
                    using var response = await PostAsync(url, new Dictionary<string, string>
                    {
                        ["token"] = refreshToken,
                        ["token_type_hint"] = "refresh_token",
                    }, ct);
                    if (!response.IsSuccessStatusCode)
                        logger.LogWarning("Identity answered {Status} to the revocation of a refresh token", (int)response.StatusCode);
                }
            }
            catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or InvalidOperationException)
            {
                // The session is over here anyway; the refresh token expires at Identity on its own.
                logger.LogWarning(ex, "The refresh token could not be revoked at Identity");
            }
        }
        return tokens?.IdToken;
    }

    private async Task<UserTokens?> RefreshAsync(string sessionKey, UserTokens tokens, CancellationToken ct)
    {
        HttpResponseMessage response;
        try
        {
            var configuration = await ConfigurationAsync(ct);
            response = await PostAsync(configuration.TokenEndpoint, new Dictionary<string, string>
            {
                ["grant_type"] = "refresh_token",
                ["refresh_token"] = tokens.RefreshToken!,
            }, ct);
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or InvalidOperationException)
        {
            logger.LogWarning(ex, "Token refresh failed: Identity is not reachable");
            return null;
        }

        using (response)
        {
            var body = await response.Content.ReadAsStringAsync(ct);
            if (response.IsSuccessStatusCode)
            {
                using var json = JsonDocument.Parse(body);
                var root = json.RootElement;
                var refreshed = new UserTokens(
                    AccessToken: root.GetProperty("access_token").GetString()!,
                    // Identity rotates refresh tokens; keep the old one only if no new one came.
                    RefreshToken: root.TryGetProperty("refresh_token", out var rt) ? rt.GetString() : tokens.RefreshToken,
                    IdToken: root.TryGetProperty("id_token", out var it) ? it.GetString() : tokens.IdToken,
                    ExpiresAt: DateTimeOffset.UtcNow.AddSeconds(root.TryGetProperty("expires_in", out var e) && e.TryGetInt32(out var s) ? s : 300),
                    Scopes: root.TryGetProperty("scope", out var sc) ? sc.GetString()!.Split(' ', StringSplitOptions.RemoveEmptyEntries) : tokens.Scopes);
                await store.SaveAsync(sessionKey, refreshed, ct);
                return refreshed;
            }

            // 400 with an OAuth error is Identity's answer about the grant; anything else is Identity unwell.
            if (response.StatusCode is HttpStatusCode.BadRequest or HttpStatusCode.Unauthorized && OAuthError(body) is { } error)
            {
                logger.LogInformation("Identity refused the refresh ({Error}): the session is over", error);
                await store.RemoveAsync(sessionKey, ct);
                throw new SessionEndedException();
            }

            logger.LogWarning("Token refresh failed: Identity answered {Status}", (int)response.StatusCode);
            return null;
        }
    }

    private async Task<HttpResponseMessage> PostAsync(string url, Dictionary<string, string> form, CancellationToken ct)
    {
        var options = identity.Value;
        form["client_id"] = options.ClientId;
        form["client_secret"] = options.ClientSecret;
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(TimeSpan.FromSeconds(10));
        return await httpClients.CreateClient(HttpClientName).PostAsync(url, new FormUrlEncodedContent(form), timeout.Token);
    }

    private Task<OpenIdConnectConfiguration> ConfigurationAsync(CancellationToken ct) =>
        oidc.Get(OpenIdConnectDefaults.AuthenticationScheme).ConfigurationManager?.GetConfigurationAsync(ct)
        ?? throw new InvalidOperationException("The OpenID Connect handler has no configuration manager yet.");

    // The "error" of an OAuth error answer (RFC 6749 §5.2), or null when the body is not one.
    private static string? OAuthError(string body)
    {
        try
        {
            using var json = JsonDocument.Parse(body);
            return json.RootElement.ValueKind == JsonValueKind.Object
                   && json.RootElement.TryGetProperty("error", out var error) && error.ValueKind == JsonValueKind.String
                ? error.GetString()
                : null;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static string? StillValid(UserTokens tokens) => tokens.ExpiresAt > DateTimeOffset.UtcNow ? tokens.AccessToken : null;
}

/// <summary>The user's session is over (revoked, expired, or never stored): sign in again.</summary>
public sealed class SessionEndedException() : Exception("Your session has ended. Sign in again.");

/// <summary>Identity cannot be reached and the access token has expired: try again shortly.</summary>
public sealed class IdentityUnavailableException() : Exception("The sign-in service is not available. Try again shortly.");
