using System;

namespace redb.Core.Exceptions;

/// <summary>
/// Thrown by the <c>Props</c> getter of an unloaded reference when
/// <c>RedbServiceConfiguration.LazyReferenceAccess</c> is <c>Throw</c>: the host has said that a
/// hidden, blocking database round trip on property access is a defect (Blazor WebAssembly cannot
/// block at all; a UI thread should not), so the getter names the explicit way instead of loading.
/// </summary>
public class RedbSynchronousLazyLoadException : InvalidOperationException
{
    /// <summary>Id of the object whose Props were asked for.</summary>
    public long ObjectId { get; }

    /// <summary>Scheme of that object.</summary>
    public long SchemeId { get; }

    public RedbSynchronousLazyLoadException(long objectId, long schemeId)
        : base($"Object {objectId} (scheme {schemeId}) is an unloaded reference and LazyReferenceAccess is Throw: " +
               "synchronous loading on Props access is disabled for this host. Load it explicitly - " +
               "await obj.LoadPropsAsync(), or redb.LoadReferencesAsync(parent, p => p.Reference) for a batch - " +
               "or load the parent with a larger depth.")
    {
        ObjectId = objectId;
        SchemeId = schemeId;
    }
}
