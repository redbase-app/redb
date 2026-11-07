using System;

namespace redb.Core;

/// <summary>
/// A service that opens scopes of the container it came from: for code that holds one service and needs another of
/// the same database per unit of work - a parallel flow, a background job, an audit written outside any request.
/// One <see cref="IRedbService"/> is one connection, so parallel work needs a service each; a service resolved from
/// the root provider lends its lazy loads a scope the same way (review after 4.0.0, finding 4). A service built
/// without a container has nothing to open a scope from (<see cref="CanCreateScope"/>).
/// </summary>
public interface IRedbScopeSource
{
    /// <summary>Whether <see cref="CreateScope"/> can open a scope: the service came from a container with a scope factory.</summary>
    bool CanCreateScope { get; }

    /// <summary>
    /// Opens a scope of the service's container and returns its service of the same database; the holder owns the
    /// scope and disposes it when the unit of work ends. Throws <see cref="InvalidOperationException"/> when there is
    /// no container (<see cref="CanCreateScope"/> is false), or when the scope's service reads another database - a
    /// container that registers <see cref="IRedbService"/> differently per resolution.
    /// </summary>
    RedbScope CreateScope();
}
