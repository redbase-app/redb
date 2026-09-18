using System;

namespace redb.Core.Exceptions;

/// <summary>
/// Thrown when a lazy load - the <c>Props</c> of an unloaded reference, <c>RedbListItem.Object</c> - finds no live redb
/// scope to run on. A data object owns no connection: the load runs on the scope of whoever reads it (owner decision
/// 2026-09-15). Here none reads: the scope that loaded the object has ended, or the code runs outside any scope - a
/// background task, a static cache, a UI event handler. Before, the load quietly took a fresh scope and pooled connection
/// per read.
/// </summary>
public class RedbLazyLoadScopeEndedException : InvalidOperationException
{
    private const string WaysOut =
        " Read it inside a live scope: resolve IRedbService from a scope, or wrap the block in " +
        "using (redb.BeginAccess()) { ... } (UI event handlers, background work). Or load it explicitly while a scope " +
        "is alive - redb.LoadAsync(id), redb.LoadReferencesAsync(parent, p => p.Reference), redb.LoadLinkedObjectsAsync(items), " +
        "a larger depth - or set RedbServiceConfiguration.LazyLoadWithoutScope = FreshScope to open a scope per load.";

    /// <summary>Id of the object whose Props (or which, behind a list item) were asked for.</summary>
    public long ObjectId { get; }

    /// <summary>Scheme of that object; 0 when the load came from a list item and the scheme is not known yet.</summary>
    public long SchemeId { get; }

    public RedbLazyLoadScopeEndedException(long objectId, long schemeId)
        : base($"Object {objectId} (scheme {schemeId}) is an unloaded reference and no live redb scope reads it here." + WaysOut)
    {
        ObjectId = objectId;
        SchemeId = schemeId;
    }

    private RedbLazyLoadScopeEndedException(string message, long objectId)
        : base(message)
    {
        ObjectId = objectId;
    }

    /// <summary>A list item's linked object asked for with no live redb scope.</summary>
    public static RedbLazyLoadScopeEndedException ForListItem(long listItemId, long objectId)
        => new($"The object {objectId} linked to list item {listItemId} is loaded lazily and no live redb scope reads it here." + WaysOut,
            objectId);
}
