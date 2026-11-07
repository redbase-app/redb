using System.Security.Cryptography;
using System.Text.Json;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.Extensions.Caching.Distributed;

namespace RedbIdentityApp.Web.Auth;

/// <summary>
/// The user's tokens, kept on this server: the session cookie carries only a random session key. A
/// token never reaches the browser, and the cookie stays small whatever the tokens weigh.
/// <para>
/// Storage is <see cref="IDistributedCache"/>, encrypted with DataProtection. Program.cs registers the
/// in-memory cache: one server, and a restart signs users out of this application (Identity's own
/// session signs them back in without a password). For several replicas register a shared cache
/// (Redis: AddStackExchangeRedisCache) and a shared DataProtection key ring.
/// </para>
/// </summary>
public sealed class UserTokenStore(IDistributedCache cache, IDataProtectionProvider protection)
{
    /// <summary>The cookie claim that holds the session key.</summary>
    public const string SessionClaim = "redbidentityapp:session";

    private readonly IDataProtector _protector = protection.CreateProtector("RedbIdentityApp.UserTokens");

    /// <summary>How long a session outlives its last use; the cookie uses the same.</summary>
    public static readonly TimeSpan SessionLifetime = TimeSpan.FromHours(8);

    public async Task<string> CreateAsync(UserTokens tokens, CancellationToken ct)
    {
        var key = Convert.ToHexString(RandomNumberGenerator.GetBytes(32));
        await SaveAsync(key, tokens, ct);
        return key;
    }

    public async Task<UserTokens?> GetAsync(string key, CancellationToken ct)
    {
        var protectedBytes = await cache.GetAsync(CacheKey(key), ct);
        if (protectedBytes is null)
            return null;
        try
        {
            return JsonSerializer.Deserialize<UserTokens>(_protector.Unprotect(protectedBytes));
        }
        catch (CryptographicException)
        {
            // Keys changed (a new DataProtection ring): the entry is unreadable, the session is over.
            await RemoveAsync(key, ct);
            return null;
        }
    }

    public Task SaveAsync(string key, UserTokens tokens, CancellationToken ct) =>
        cache.SetAsync(CacheKey(key), _protector.Protect(JsonSerializer.SerializeToUtf8Bytes(tokens)),
            new DistributedCacheEntryOptions { SlidingExpiration = SessionLifetime }, ct);

    public Task RemoveAsync(string key, CancellationToken ct) => cache.RemoveAsync(CacheKey(key), ct);

    private static string CacheKey(string key) => "redbidentityapp:tokens:" + key;
}

/// <summary>What the token endpoint gave the user's session.</summary>
public sealed record UserTokens(
    string AccessToken,
    string? RefreshToken,
    string? IdToken,
    DateTimeOffset ExpiresAt,
    string[] Scopes);
