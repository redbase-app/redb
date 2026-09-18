using System;
using Microsoft.Extensions.DependencyInjection;

namespace redb.Core;

/// <summary>
/// Whether a service provider is the root of its container - the one whose lifetime is the container's. A service
/// resolved from it is captive: it lives as long as the process and the ambient frame it enters is inherited by every
/// flow. Such a service lends lazy loads its scope factory, never its one connection (review after 4.0.0, finding 4;
/// owner decision 2026-09-17): a lazy load resolved to a captive reader runs in a fresh scope of its container.
/// <para>
/// Microsoft.Extensions.DependencyInjection: the root scope's provider is its own <see cref="IServiceScopeFactory"/>,
/// a child scope's is not (pinned by a unit test). Another container is not recognised: its services are never captive.
/// </para>
/// </summary>
public static class RedbServiceProviders
{
    /// <summary>True when <paramref name="serviceProvider"/> is the root provider of a Microsoft DI container.</summary>
    public static bool IsRoot(IServiceProvider serviceProvider)
    {
        ArgumentNullException.ThrowIfNull(serviceProvider);
        return ReferenceEquals(serviceProvider, serviceProvider.GetService<IServiceScopeFactory>());
    }
}
