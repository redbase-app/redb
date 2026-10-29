using System.Collections.Concurrent;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using redb.Identity.Contracts.Routes;

namespace RedbIdentityApp.Module.Identity;

/// <summary>
/// Checks a bearer token by asking Identity (RFC 7662 introspection over direct-vm) as the module's
/// resource client, and keeps the answer for a short while.
/// <para>
/// Identity decides whether the token is good: signature, expiry, revocation. What introspection leaves
/// to the resource server, this class checks: the token must be an access token, and its audience must
/// name this API's resource client (RFC 9068 §4). Identity answers <c>active: false</c> to a client outside
/// a token's audience as well; the check here keeps the API closed to a token issued for another client or
/// API even against an Identity that does not.
/// </para>
/// <para>
/// Cache: keyed by the SHA-256 of the token (the token itself is never stored). An active answer lives
/// <see cref="ModuleSettings.IntrospectionCacheTtl"/>, never past the token's own <c>exp</c>; an inactive
/// one lives <see cref="ModuleSettings.InactiveCacheTtl"/>. A revocation Identity reports
/// (<see cref="RevocationEvents"/>) bumps a generation, and every answer cached under an older
/// generation is asked again. Concurrent requests with the same token share one introspection.
/// When Identity cannot be reached, nothing is cached and the caller answers 503.
/// </para>
/// </summary>
public sealed class TokenIntrospector : IDisposable
{
    private readonly IdentityCalls _calls;
    private readonly ModuleSettings _settings;
    private readonly ILogger _logger;
    private readonly MemoryCache _cache = new(new MemoryCacheOptions { SizeLimit = 50_000 });
    private readonly ConcurrentDictionary<string, Task<Introspection>> _inFlight = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, long> _userGenerations = new(StringComparer.Ordinal);
    private long _globalGeneration;
    private long _revocations;

    public TokenIntrospector(IdentityCalls calls, ModuleSettings settings, ILogger logger)
    {
        _calls = calls;
        _settings = settings;
        _logger = logger;
    }

    /// <summary>The introspection of <paramref name="token"/>, from the cache or from Identity.</summary>
    public async Task<Introspection> IntrospectAsync(string token, CancellationToken ct)
    {
        var key = Hash(token);
        if (_cache.TryGetValue(key, out CachedIntrospection? cached) && cached is not null && IsCurrent(cached))
            return cached.Result;

        var pending = _inFlight.GetOrAdd(key, StartAsking, token);
        return await pending.WaitAsync(ct).ConfigureAwait(false);
    }

    /// <summary>Identity reported a revocation for a user: every answer cached for them is asked again.</summary>
    public void RevokeUser(string userKey)
    {
        _userGenerations.AddOrUpdate(userKey, 1, (_, generation) => generation + 1);
        Interlocked.Increment(ref _revocations);
    }

    /// <summary>A change that may concern any token (a client, a signing key): the whole cache is asked again.</summary>
    public void RevokeAll()
    {
        Interlocked.Increment(ref _globalGeneration);
        Interlocked.Increment(ref _revocations);
    }

    // One question per token at a time; the task leaves the table when it completes.
    private Task<Introspection> StartAsking(string key, string token)
    {
        var task = AskIdentityAsync(key, token);
        task.ContinueWith(t => _inFlight.TryRemove(new KeyValuePair<string, Task<Introspection>>(key, t)),
            CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        return task;
    }

    private async Task<Introspection> AskIdentityAsync(string key, string token)
    {
        // Never completes synchronously, so the task is in the table before it leaves it.
        await Task.Yield();

        // A revocation reported while Identity is answering: the answer is used once and not cached.
        var revocationsBefore = Interlocked.Read(ref _revocations);
        var global = Interlocked.Read(ref _globalGeneration);

        using var timeout = new CancellationTokenSource(_settings.IntrospectionTimeout);
        var request = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["token"] = token,
            ["token_type_hint"] = "access_token",
            ["client_id"] = _settings.ResourceClientId,
            ["client_secret"] = _settings.ResourceClientSecret,
        };
        var answer = await _calls.SendAsync(IdentityEndpoints.Introspect, request, (exchange, _) =>
        {
            exchange.In.Headers["client_id"] = _settings.ResourceClientId;   // audit and throttle only
            return Task.FromResult<IdentityAnswer?>(null);
        }, timeout.Token).ConfigureAwait(false);

        if (answer.IsError)
        {
            // invalid_client: the resource client or its secret is wrong, a configuration error of this
            // module, not a bad token. Anything else (a token Identity will not discuss with this client,
            // e.g. issued for another audience) is an inactive token.
            if (answer.Error == "invalid_client")
                throw new IdentityRefusedException(
                    $"Identity refused introspection by '{_settings.ResourceClientId}': {answer.ErrorDescription ?? answer.Error}. " +
                    "Check Identity:ResourceClientSecret.");

            _logger.LogDebug("Introspection refused: {Error} {Description}", answer.Error, answer.ErrorDescription);
            return Store(key, Introspection.Inactive, global, revocationsBefore, null);
        }

        var result = Introspection.Read(answer.Json);
        if (result.Active && result.TokenUsage is { } usage && usage != "access_token")
            result = Introspection.Inactive;   // a refresh or id token is not a bearer for an API
        if (result.Active && !result.Audiences.Contains(_settings.ResourceClientId, StringComparer.Ordinal))
        {
            // Issued for another API (RFC 9068 §4): Identity may still describe it to an introspecting
            // client, but this API is not its audience.
            _logger.LogDebug("Token of {Client} rejected: audience {Audiences} lacks {Resource}",
                result.ClientId, string.Join(' ', result.Audiences), _settings.ResourceClientId);
            result = Introspection.Inactive;
        }

        return Store(key, result, global, revocationsBefore, result.Active ? result.ExpiresAt : null);
    }

    private Introspection Store(string key, Introspection result, long global, long revocationsBefore, DateTimeOffset? expiresAt)
    {
        if (Interlocked.Read(ref _revocations) != revocationsBefore)
            return result;

        var ttl = result.Active ? _settings.IntrospectionCacheTtl : _settings.InactiveCacheTtl;
        if (expiresAt is { } exp && exp - DateTimeOffset.UtcNow < ttl)
            ttl = exp - DateTimeOffset.UtcNow;
        if (ttl <= TimeSpan.Zero)
            return result;

        var entry = new CachedIntrospection(result, global, Generation(result.Subject), Generation(result.UserId));
        _cache.Set(key, entry, new MemoryCacheEntryOptions { AbsoluteExpirationRelativeToNow = ttl, Size = 1 });
        return result;
    }

    private bool IsCurrent(CachedIntrospection cached) =>
        cached.GlobalGeneration == Interlocked.Read(ref _globalGeneration)
        && cached.SubjectGeneration == Generation(cached.Result.Subject)
        && cached.UserIdGeneration == Generation(cached.Result.UserId);

    private long Generation(string? userKey) =>
        userKey is not null && _userGenerations.TryGetValue(userKey, out var generation) ? generation : 0;

    private static string Hash(string token) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(token)));

    public void Dispose() => _cache.Dispose();

    private sealed record CachedIntrospection(Introspection Result, long GlobalGeneration, long SubjectGeneration, long UserIdGeneration);
}

/// <summary>What Identity said about a token (RFC 7662), reduced to what the API uses.</summary>
public sealed record Introspection(
    bool Active,
    string? Subject,
    string? UserId,
    string? UserName,
    string? ClientId,
    IReadOnlyList<string> Scopes,
    IReadOnlyList<string> Audiences,
    string? TokenUsage,
    DateTimeOffset? ExpiresAt)
{
    public static readonly Introspection Inactive = new(false, null, null, null, null, [], [], null, null);

    public static Introspection Read(JsonElement json)
    {
        if (json.ValueKind != JsonValueKind.Object || !json.TryGetProperty("active", out var active) || active.ValueKind != JsonValueKind.True)
            return Inactive;

        return new Introspection(
            Active: true,
            Subject: Text(json, "sub"),
            // The internal user id Identity adds for user tokens; client-credentials tokens have none.
            UserId: Text(json, "redb:user_id"),
            UserName: Text(json, "username"),
            ClientId: Text(json, "client_id"),
            Scopes: Text(json, "scope")?.Split(' ', StringSplitOptions.RemoveEmptyEntries) ?? [],
            Audiences: Strings(json, "aud"),
            TokenUsage: Text(json, "token_usage"),
            ExpiresAt: json.TryGetProperty("exp", out var exp) && exp.TryGetInt64(out var seconds)
                ? DateTimeOffset.FromUnixTimeSeconds(seconds)
                : null);
    }

    private static string? Text(JsonElement json, string name) =>
        json.TryGetProperty(name, out var value) ? value.ValueKind switch
        {
            JsonValueKind.String => value.GetString(),
            JsonValueKind.Number => value.GetRawText(),
            _ => null,
        } : null;

    // "aud" is a string or an array of strings.
    private static IReadOnlyList<string> Strings(JsonElement json, string name)
    {
        if (!json.TryGetProperty(name, out var value))
            return [];
        return value.ValueKind == JsonValueKind.Array
            ? value.EnumerateArray().Where(v => v.ValueKind == JsonValueKind.String).Select(v => v.GetString()!).ToList()
            : value.ValueKind == JsonValueKind.String ? [value.GetString()!] : [];
    }

    public override string ToString() => Active
        ? string.Create(CultureInfo.InvariantCulture, $"active sub={Subject} client={ClientId} scope={string.Join(' ', Scopes)}")
        : "inactive";
}
