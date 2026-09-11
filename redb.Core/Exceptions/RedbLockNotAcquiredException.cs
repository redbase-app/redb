using System;
using System.Collections.Generic;
using System.Linq;

namespace redb.Core.Exceptions;

/// <summary>
/// The strict row lock (<c>LockForUpdateRequiredAsync</c>) found fewer rows than it was asked to
/// lock: the missing ids have no row in <c>_objects</c> - deleted, or never created. A CAS pattern
/// (lock -> re-read -> save) must not proceed on an unlocked row: with the default
/// <c>MissingObjectStrategy.AutoSwitchToInsert</c> the save would silently resurrect the deleted
/// object (BR-9, 2026-09-02). Catch this to abort the CAS, or to recreate the object deliberately.
/// The lenient <c>LockForUpdateAsync</c> reports a count instead of throwing.
/// </summary>
public class RedbLockNotAcquiredException : Exception
{
    /// <summary>The distinct ids the caller asked to lock.</summary>
    public IReadOnlyList<long> RequestedIds { get; }

    /// <summary>The ids that have no row in <c>_objects</c>, ascending.</summary>
    public IReadOnlyList<long> MissingIds { get; }

    public RedbLockNotAcquiredException(IReadOnlyList<long> requestedIds, IReadOnlyList<long> missingIds)
        : base(BuildMessage(requestedIds, missingIds))
    {
        RequestedIds = requestedIds;
        MissingIds = missingIds;
    }

    private static string BuildMessage(IReadOnlyList<long> requested, IReadOnlyList<long> missing)
        => $"Could not lock {missing.Count} of {requested.Count} requested objects: " +
           $"ids [{string.Join(", ", missing)}] have no row in _objects (deleted, or never created). " +
           "A CAS must not proceed on an unlocked row - abort, or recreate the object deliberately.";
}
