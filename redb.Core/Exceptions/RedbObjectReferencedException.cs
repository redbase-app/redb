namespace redb.Core.Exceptions;

/// <summary>
/// Objects of a trash container are still referenced by live objects - a <c>RedbObject&lt;T&gt;</c>
/// reference in their Props points at them - so they cannot be physically deleted without breaking
/// those references.
/// <para>
/// redb never repairs this on its own: nulling the references would hide the caller's mistake inside
/// other objects' data. The purge removes everything else of the container, marks the container
/// <c>failed</c> (the background worker no longer retries it) and names both sides here, so the
/// references can be removed or re-pointed deliberately. Purging the container again afterwards
/// finishes it.
/// </para>
/// </summary>
public class RedbObjectReferencedException : Exception
{
    /// <summary>The trash container that could not be purged completely.</summary>
    public long TrashId { get; }

    /// <summary>Objects still left in the container.</summary>
    public long RemainingCount { get; }

    /// <summary>Objects that live objects still reference (the first pairs found, at most 100).</summary>
    public IReadOnlyList<long> ReferencedObjectIds { get; }

    /// <summary>Live objects holding those references.</summary>
    public IReadOnlyList<long> ReferencingObjectIds { get; }

    public RedbObjectReferencedException(
        long trashId,
        long remainingCount,
        IReadOnlyList<long> referencedObjectIds,
        IReadOnlyList<long> referencingObjectIds)
        : base(BuildMessage(trashId, remainingCount, referencedObjectIds, referencingObjectIds))
    {
        TrashId = trashId;
        RemainingCount = remainingCount;
        ReferencedObjectIds = referencedObjectIds;
        ReferencingObjectIds = referencingObjectIds;
    }

    private static string BuildMessage(long trashId, long remaining, IReadOnlyList<long> referenced, IReadOnlyList<long> referencing) =>
        $"Trash container {trashId} cannot be purged completely: {remaining} object(s) are still referenced by " +
        $"live objects (referenced: {Ids(referenced)}; referencing: {Ids(referencing)}). Everything else was purged. " +
        "The container is marked 'failed' and is no longer retried automatically; remove or re-point those " +
        "references, then purge the container again.";

    private static string Ids(IReadOnlyList<long> ids) =>
        ids.Count <= 20
            ? string.Join(", ", ids)
            : string.Join(", ", ids.Take(20)) + $" and {ids.Count - 20} more";
}
