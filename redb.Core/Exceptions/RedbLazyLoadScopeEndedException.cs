using System;

namespace redb.Core.Exceptions;

/// <summary>
/// Thrown when the <c>Props</c> of an unloaded reference are asked for after the redb scope that
/// loaded it has ended: the reference still carries that scope's loader, whose context (one
/// connection) is disposed. Before this, the loader quietly took a fresh pooled connection nobody
/// would ever return. Objects served from the props cache never hit this - the cache hands their
/// references a loader that opens its own scope per load (V4, review).
/// </summary>
public class RedbLazyLoadScopeEndedException : InvalidOperationException
{
    /// <summary>Id of the object whose Props were asked for.</summary>
    public long ObjectId { get; }

    /// <summary>Scheme of that object.</summary>
    public long SchemeId { get; }

    public RedbLazyLoadScopeEndedException(long objectId, long schemeId)
        : base($"Object {objectId} (scheme {schemeId}) is an unloaded reference and the redb scope that loaded it " +
               "has ended (its context is disposed). Load it inside a live scope - redb.LoadAsync(id), " +
               "redb.LoadReferencesAsync(parent, p => p.Reference) - or load the parent with a larger depth " +
               "while the scope is alive.")
    {
        ObjectId = objectId;
        SchemeId = schemeId;
    }
}
