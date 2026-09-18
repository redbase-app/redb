using System;

namespace redb.Core.Exceptions;

/// <summary>
/// Thrown when a reference whose properties were never loaded is saved directly: the object has an id and no loaded
/// properties - a hand-written <c>new RedbObject&lt;T&gt; { id = x }</c>, a stub whose lazy load the getter did not
/// keep (a shared instance inside a transaction). It names another object; it is not that object. Saving it saved
/// whatever a fresh load returned, and the caller's edits were lost silently (review after 4.0.0). Reading the
/// <c>Props</c> of such a reference still answers null: plain System.Text.Json serialization of a graph reads the
/// getter, and an exception there would fail every API response carrying a hand-made reference.
/// </summary>
public class RedbUnloadedReferenceException : InvalidOperationException
{
    /// <summary>Id of the referenced object.</summary>
    public long ObjectId { get; }

    /// <summary>Scheme of that object; 0 when the reference does not carry it.</summary>
    public long SchemeId { get; }

    public RedbUnloadedReferenceException(long objectId, long schemeId)
        : base($"Object {objectId} (scheme {schemeId}) is a reference whose properties were never loaded: it names " +
               "another object, it is not that object, and saving it would save a fresh reload instead of your edits. " +
               "Load it explicitly - redb.LoadAsync(id), redb.LoadReferencesAsync(parent, p => p.Reference) - edit " +
               "and save the loaded object; a shared (cached) instance read inside a transaction keeps nothing it loads.")
    {
        ObjectId = objectId;
        SchemeId = schemeId;
    }
}
