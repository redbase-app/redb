using System.Globalization;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using redb.Identity.Contracts.Routes;

namespace RedbIdentityApp.Module.Identity;

/// <summary>
/// Identity's management API in process. Every <c>identity-manage-*</c> route is closed unless the same
/// exchange first passed Identity's management gate (<see cref="IdentityEndpoints.AuthManagement"/>) with
/// a token holding <c>identity:manage</c>; there is no in-process shortcut around it, on purpose.
/// <para>
/// The token is the module's service token: client credentials of the management client (Identity's
/// SeedBackchannelClient), kept until a minute before it expires. A token the gate refuses is dropped,
/// so the next call asks for a new one.
/// </para>
/// </summary>
public sealed class IdentityManagement(IdentityCalls calls, ModuleSettings settings, ILogger logger)
{
    private const string ManageScope = "identity:manage";
    private static readonly TimeSpan RenewBefore = TimeSpan.FromMinutes(1);

    private readonly SemaphoreSlim _tokenLock = new(1, 1);
    private string? _token;
    private DateTimeOffset _tokenExpiresAt;

    /// <summary>
    /// Runs <paramref name="operation"/> on a management route (<see cref="IdentityEndpoints.ManageUsers"/>,
    /// <see cref="IdentityEndpoints.ManageApps"/>, ...). Identity's own refusals come back as an answer with
    /// <see cref="IdentityAnswer.Error"/> set (<c>duplicate</c>, <c>not_found</c>, <c>validation_error</c>);
    /// a refused service token throws <see cref="IdentityRefusedException"/>, Identity being down throws
    /// <see cref="IdentityUnavailableException"/>.
    /// </summary>
    public async Task<IdentityAnswer> CallAsync(string endpoint, string operation, object? body, CancellationToken ct)
    {
        var token = await ServiceTokenAsync(ct).ConfigureAwait(false);
        var gateRefused = false;

        var answer = await calls.SendAsync(endpoint, body, async (exchange, c) =>
        {
            exchange.In.Headers["Authorization"] = "Bearer " + token;
            await calls.RequestOnAsync(IdentityEndpoints.AuthManagement, exchange, c).ConfigureAwait(false);
            if (!exchange.IsStopped && exchange.Exception is null)
            {
                exchange.In.Headers["operation"] = operation;
                return null;
            }

            gateRefused = true;
            var refused = IdentityAnswer.From(exchange);
            return refused.IsError ? refused : IdentityAnswer.Refused("access_denied", "The management gate refused the service token.");
        }, ct).ConfigureAwait(false);

        if (gateRefused)
        {
            Forget(token);
            throw new IdentityRefusedException(
                $"Identity's management gate refused the service token of '{settings.ManagementClientId}': {answer.ErrorDescription ?? answer.Error}. " +
                $"The client needs the scope {ManageScope}.");
        }

        return answer;
    }

    private async Task<string> ServiceTokenAsync(CancellationToken ct)
    {
        if (_token is { } cached && DateTimeOffset.UtcNow < _tokenExpiresAt - RenewBefore)
            return cached;

        await _tokenLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (_token is { } fresh && DateTimeOffset.UtcNow < _tokenExpiresAt - RenewBefore)
                return fresh;

            var request = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["grant_type"] = "client_credentials",
                ["client_id"] = settings.ManagementClientId,
                ["client_secret"] = settings.ManagementClientSecret,
                ["scope"] = ManageScope,
            };
            var answer = await calls.SendAsync(IdentityEndpoints.Token, request, (exchange, _) =>
            {
                // For Identity's audit and per-client throttle; the credentials themselves are in the body.
                exchange.In.Headers["client_id"] = settings.ManagementClientId;
                return Task.FromResult<IdentityAnswer?>(null);
            }, ct).ConfigureAwait(false);

            if (answer.IsError)
            {
                throw new IdentityRefusedException(
                    $"Identity refused client credentials of '{settings.ManagementClientId}': {answer.ErrorDescription ?? answer.Error}. " +
                    "Check Identity:ManagementClientSecret against Identity's SeedBackchannelClient:ClientSecret.");
            }

            var json = answer.Json;
            var token = json.GetProperty("access_token").GetString()
                ?? throw new IdentityRefusedException("Identity's token answer has no access_token.");
            var lifetime = json.TryGetProperty("expires_in", out var expiresIn) ? Seconds(expiresIn) : 300;

            _token = token;
            _tokenExpiresAt = DateTimeOffset.UtcNow.AddSeconds(lifetime);
            logger.LogDebug("Management token of {ClientId} obtained, valid for {Seconds} s", settings.ManagementClientId, lifetime);
            return token;
        }
        finally
        {
            _tokenLock.Release();
        }
    }

    private void Forget(string token)
    {
        if (ReferenceEquals(Volatile.Read(ref _token), token))
            Volatile.Write(ref _token, null);
    }

    // expires_in is a number, sometimes a string.
    private static long Seconds(JsonElement value) => value.ValueKind == JsonValueKind.Number
        ? value.GetInt64()
        : long.Parse(value.GetString() ?? "300", CultureInfo.InvariantCulture);
}

/// <summary>Identity answered, but refused the module's own credentials: a configuration error.</summary>
public sealed class IdentityRefusedException(string message) : Exception(message);
