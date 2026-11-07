using System.Text.Json;
using Microsoft.Extensions.Logging;
using redb.Identity.Contracts.Events;
using redb.Identity.Contracts.Routes;
using redb.Route.Abstractions;

namespace RedbIdentityApp.Module.Identity;

/// <summary>
/// Identity's audit events, delivered to the module's own direct-vm address
/// (<see cref="ModuleSettings.EventsEndpoint"/>). Identity sends them only when its operator lists that
/// address as an audit target (deploy/docker-compose.yml, README "Revocation"); without it the cache still
/// expires after <see cref="ModuleSettings.IntrospectionCacheTtl"/>.
/// <para>
/// A revocation for a user drops the cached answers for that user; a change to a client or a signing key
/// drops all of them. Every other event is ignored. The step only bumps counters, so it never holds up
/// Identity's event pipeline.
/// </para>
/// </summary>
public sealed class RevocationEvents(TokenIntrospector introspector, ILogger logger)
{
    // Events after which a token of the user may no longer be good.
    private static readonly HashSet<string> UserEvents = new(StringComparer.Ordinal)
    {
        IdentityAuditEventIds.TokenRevoked,
        IdentityAuditEventIds.TokensRevokedByUser,
        IdentityAuditEventIds.SessionRevoked,
        IdentityAuditEventIds.AllSessionsRevoked,
        IdentityAuditEventIds.SidRevoked,
        IdentityAuditEventIds.UserLoggedOut,
        IdentityAuditEventIds.UserUpdated,
        IdentityAuditEventIds.UserDeleted,
        IdentityAuditEventIds.PasswordChanged,
        IdentityAuditEventIds.PasswordResetCompleted,
        IdentityAuditEventIds.ConsentRevoked,
        IdentityAuditEventIds.AllConsentsRevoked,
        IdentityAuditEventIds.ScimUserReplaced,
        IdentityAuditEventIds.ScimUserPatched,
        IdentityAuditEventIds.ScimUserDeleted,
    };

    // Events after which any token may no longer be good.
    private static readonly HashSet<string> GlobalEvents = new(StringComparer.Ordinal)
    {
        IdentityAuditEventIds.ClientUpdated,
        IdentityAuditEventIds.ClientDeleted,
        IdentityAuditEventIds.ClientSecretRotated,
        IdentityAuditEventIds.SigningKeyRetired,
        IdentityAuditEventIds.ScopeUpdated,
        IdentityAuditEventIds.ScopeDeleted,
    };

    public Task Process(IExchange exchange, CancellationToken ct)
    {
        var identityEvent = Read(exchange.In.Body);
        if (identityEvent is null)
        {
            logger.LogWarning("An Identity event the module could not read: {Type}", exchange.In.Body?.GetType().Name ?? "null");
            return Task.CompletedTask;
        }

        if (GlobalEvents.Contains(identityEvent.EventType))
        {
            introspector.RevokeAll();
            logger.LogInformation("{Event}: cached token checks dropped", identityEvent.EventType);
        }
        else if (UserEvents.Contains(identityEvent.EventType))
        {
            // The user is named by the internal id or, from some emitters, by the subject; the cache
            // keeps both. An event that names nobody drops everything rather than nothing.
            if (string.IsNullOrEmpty(identityEvent.UserId))
                introspector.RevokeAll();
            else
                introspector.RevokeUser(identityEvent.UserId);
            logger.LogInformation("{Event} for user {User}: cached token checks dropped", identityEvent.EventType, identityEvent.UserId ?? "(unknown)");
        }

        return Task.CompletedTask;
    }

    // Identity hands its targets the event as JSON bytes; an in-process sender may pass the object.
    private static IdentityEvent? Read(object? body)
    {
        try
        {
            return body switch
            {
                IdentityEvent typed => typed,
                byte[] bytes => JsonSerializer.Deserialize<IdentityEvent>(bytes),
                string text => JsonSerializer.Deserialize<IdentityEvent>(text),
                _ => null,
            };
        }
        catch (JsonException)
        {
            return null;
        }
    }
}
