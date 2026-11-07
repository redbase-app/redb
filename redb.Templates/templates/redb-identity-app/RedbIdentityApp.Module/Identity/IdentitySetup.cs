using System.Text.Json;
using Microsoft.Extensions.Logging;
using redb.Identity.Contracts.Applications;
using redb.Identity.Contracts.Common;
using redb.Identity.Contracts.Roles;
using redb.Identity.Contracts.Routes;
using redb.Identity.Contracts.Scopes;

namespace RedbIdentityApp.Module.Identity;

/// <summary>
/// Puts at Identity what the module needs, through the management API, and keeps it that way on every
/// start:
/// <list type="number">
///   <item>the API scope and the administrative scope, both with the resource client in their
///   <c>Resources</c>, so a token issued with them names that client in <c>aud</c>;</item>
///   <item>the resource client, allowed to introspect;</item>
///   <item>the web server's client (when configured): code flow with PKCE, refresh tokens, the redirect
///   addresses from the settings;</item>
///   <item>the administrative scope attached to Identity's <c>admin</c> role, so Identity issues it to
///   administrators only.</item>
/// </list>
/// A client or scope that exists already is brought to the settings (permissions, redirect addresses,
/// resources) instead of being skipped. A secret cannot be read back: when it differs from the setting,
/// Identity answers <c>invalid_client</c> and the log says which one to fix.
/// <para>
/// Runs in the background, after the context has started, and retries while Identity is still starting
/// or unreachable; the API answers 503 meanwhile, because introspection fails the same way.
/// </para>
/// </summary>
public sealed class IdentitySetup(IdentityManagement management, ModuleSettings settings, ILogger logger)
{
    private const string AdminRole = "admin";
    private static readonly JsonSerializerOptions Web = new(JsonSerializerDefaults.Web);
    private static readonly TimeSpan CallTimeout = TimeSpan.FromSeconds(15);

    public async Task RunAsync(CancellationToken ct)
    {
        var delay = TimeSpan.FromSeconds(2);
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await ApplyAsync(ct).ConfigureAwait(false);
                logger.LogInformation("Identity set up for the module: scopes {Api}, {Admin}; clients {Resource}{Web}",
                    settings.ApiScope, settings.AdminScope, settings.ResourceClientId,
                    settings.WebClientId is null ? "" : ", " + settings.WebClientId);
                return;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (IdentityUnavailableException ex)
            {
                logger.LogInformation("Identity is not available yet ({Reason}); next try in {Seconds} s", ex.Message, (int)delay.TotalSeconds);
            }
            catch (Exception ex)
            {
                // A refused client secret or an answer the module did not expect: retrying will not fix
                // it by itself, but the operator can, so keep trying, slowly and loudly.
                logger.LogError(ex, "Identity setup failed; next try in {Seconds} s", (int)delay.TotalSeconds);
            }

            await Task.Delay(delay, ct).ConfigureAwait(false);
            delay = TimeSpan.FromSeconds(Math.Min(delay.TotalSeconds * 2, 60));
        }
    }

    private async Task ApplyAsync(CancellationToken ct)
    {
        await EnsureScopeAsync(settings.ApiScope, "RedbIdentityApp API", ct).ConfigureAwait(false);
        var adminScope = await EnsureScopeAsync(settings.AdminScope, "RedbIdentityApp administration", ct).ConfigureAwait(false);

        // OpenIddict permission names, written out: the module does not reference OpenIddict.
        await EnsureClientAsync(new CreateApplicationRequest
        {
            ClientId = settings.ResourceClientId,
            ClientSecret = settings.ResourceClientSecret,
            DisplayName = "RedbIdentityApp API",
            ClientType = "confidential",
            ApplicationType = "web",
            Permissions = ["ept:introspection"],
        }, ct).ConfigureAwait(false);

        if (settings.WebClientId is { } webClientId)
        {
            await EnsureClientAsync(new CreateApplicationRequest
            {
                ClientId = webClientId,
                ClientSecret = settings.WebClientSecret,
                DisplayName = "RedbIdentityApp",
                ClientType = "confidential",
                ApplicationType = "web",
                // The application is first party: no consent page.
                ConsentType = "implicit",
                Permissions =
                [
                    // Pushed authorization requests: ASP.NET Core 9+ uses them whenever Identity offers them.
                    "ept:authorization", "ept:pushed_authorization", "ept:token", "ept:end_session", "ept:revocation",
                    "gt:authorization_code", "gt:refresh_token", "rst:code",
                    "scp:openid", "scp:profile", "scp:email", "scp:offline_access",
                    "scp:" + settings.ApiScope, "scp:" + settings.AdminScope,
                ],
                Requirements = ["ft:pkce"],
                RedirectUris = [.. settings.WebRedirectUris],
                PostLogoutRedirectUris = [.. settings.WebPostLogoutRedirectUris],
            }, ct).ConfigureAwait(false);
        }

        await AttachToAdminRoleAsync(adminScope, ct).ConfigureAwait(false);
    }

    private async Task<ScopeResponse> EnsureScopeAsync(string name, string displayName, CancellationToken ct)
    {
        var existing = await ReadAsync<ScopeResponse>(IdentityEndpoints.ManageScopes, new Dictionary<string, object?> { ["name"] = name }, ct)
            .ConfigureAwait(false);
        if (existing is null)
        {
            var created = await CallAsync(IdentityEndpoints.ManageScopes, "create",
                new CreateScopeRequest { Name = name, DisplayName = displayName, Resources = [settings.ResourceClientId] }, ct).ConfigureAwait(false);
            if (created.Error != "duplicate")   // another node created it at the same moment
            {
                logger.LogInformation("Scope {Scope} created", name);
                return Expect<ScopeResponse>(created, $"create scope {name}");
            }

            existing = await ReadAsync<ScopeResponse>(IdentityEndpoints.ManageScopes, new Dictionary<string, object?> { ["name"] = name }, ct).ConfigureAwait(false)
                ?? throw new InvalidOperationException($"Scope {name} is reported as a duplicate but cannot be read.");
        }

        if (existing.Resources?.Contains(settings.ResourceClientId, StringComparer.Ordinal) == true)
            return existing;

        var updated = await CallAsync(IdentityEndpoints.ManageScopes, "update", new UpdateScopeRequest
        {
            Id = existing.Id,
            DisplayName = existing.DisplayName,
            Description = existing.Description,
            Resources = [.. existing.Resources ?? [], settings.ResourceClientId],
            ConcurrencyToken = existing.ConcurrencyToken,
        }, ct).ConfigureAwait(false);
        logger.LogInformation("Scope {Scope}: resource {Resource} added", name, settings.ResourceClientId);
        return Expect<ScopeResponse>(updated, $"update scope {name}");
    }

    private async Task EnsureClientAsync(CreateApplicationRequest desired, CancellationToken ct)
    {
        var read = new Dictionary<string, object?> { ["clientId"] = desired.ClientId };
        var existing = await ReadAsync<ApplicationResponse>(IdentityEndpoints.ManageApps, read, ct).ConfigureAwait(false);
        if (existing is null)
        {
            var created = await CallAsync(IdentityEndpoints.ManageApps, "create", desired, ct).ConfigureAwait(false);
            if (created.Error != "duplicate")
            {
                Expect<ApplicationResponse>(created, $"create client {desired.ClientId}");
                logger.LogInformation("Client {ClientId} created", desired.ClientId);
                return;
            }

            existing = await ReadAsync<ApplicationResponse>(IdentityEndpoints.ManageApps, read, ct).ConfigureAwait(false)
                ?? throw new InvalidOperationException($"Client {desired.ClientId} is reported as a duplicate but cannot be read.");
        }

        if (Same(existing.Permissions, desired.Permissions) && Same(existing.Requirements, desired.Requirements)
            && Same(existing.RedirectUris, desired.RedirectUris) && Same(existing.PostLogoutRedirectUris, desired.PostLogoutRedirectUris))
            return;

        var updated = await CallAsync(IdentityEndpoints.ManageApps, "update", new UpdateApplicationRequest
        {
            Id = existing.Id,
            Permissions = desired.Permissions ?? [],
            Requirements = desired.Requirements ?? [],
            RedirectUris = desired.RedirectUris ?? [],
            PostLogoutRedirectUris = desired.PostLogoutRedirectUris ?? [],
            ConcurrencyToken = existing.ConcurrencyToken,
        }, ct).ConfigureAwait(false);
        Expect<ApplicationResponse>(updated, $"update client {desired.ClientId}");
        logger.LogInformation("Client {ClientId} brought to the settings (permissions, redirect addresses)", desired.ClientId);
    }

    private async Task AttachToAdminRoleAsync(ScopeResponse adminScope, CancellationToken ct)
    {
        var search = await CallAsync(IdentityEndpoints.ManageRoles, "search",
            new Dictionary<string, object?> { ["query"] = AdminRole, ["count"] = 50 }, ct).ConfigureAwait(false);
        var role = Expect<PagedResult<RoleResponse>>(search, "find the admin role").Items
            .FirstOrDefault(r => string.Equals(r.Name, AdminRole, StringComparison.Ordinal))
            ?? throw new InvalidOperationException($"Identity has no role '{AdminRole}'.");

        var attached = await CallAsync(IdentityEndpoints.ManageRoles, "list-scopes",
            new Dictionary<string, object?> { ["roleId"] = role.Id }, ct).ConfigureAwait(false);
        if (Expect<List<RoleScopeResponse>>(attached, "list the admin role's scopes")
            .Any(s => string.Equals(s.ScopeName, adminScope.Name, StringComparison.Ordinal)))
            return;

        var attach = await CallAsync(IdentityEndpoints.ManageRoles, "attach-scope",
            new Dictionary<string, object?> { ["roleId"] = role.Id, ["scopeId"] = long.Parse(adminScope.Id, System.Globalization.CultureInfo.InvariantCulture) },
            ct).ConfigureAwait(false);
        if (attach.IsError)
            throw new InvalidOperationException($"Identity refused to attach {adminScope.Name} to the role '{AdminRole}': {attach.ErrorDescription ?? attach.Error}");
        logger.LogInformation("Scope {Scope} attached to the role '{Role}'", adminScope.Name, AdminRole);
    }

    // A read that answers not_found is "nothing there", not an error.
    private async Task<T?> ReadAsync<T>(string endpoint, Dictionary<string, object?> key, CancellationToken ct) where T : class
    {
        var answer = await CallAsync(endpoint, "read", key, ct).ConfigureAwait(false);
        return answer.Error == "not_found" ? null : Expect<T>(answer, $"read {string.Join(",", key.Values)}");
    }

    private async Task<IdentityAnswer> CallAsync(string endpoint, string operation, object body, CancellationToken ct)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(CallTimeout);
        return await management.CallAsync(endpoint, operation, body, timeout.Token).ConfigureAwait(false);
    }

    // The typed answer; Identity answers in process with its contract types, so this is normally a cast.
    private static T Expect<T>(IdentityAnswer answer, string what) where T : class
    {
        if (answer.IsError)
            throw new InvalidOperationException($"Identity refused to {what}: {answer.Error} {answer.ErrorDescription}");
        return answer.Body as T
            ?? answer.Json.Deserialize<T>(Web)
            ?? throw new InvalidOperationException($"Identity gave no answer to {what}.");
    }

    private static bool Same(string[]? current, string[]? desired) =>
        new HashSet<string>(current ?? [], StringComparer.Ordinal).SetEquals(desired ?? []);
}
