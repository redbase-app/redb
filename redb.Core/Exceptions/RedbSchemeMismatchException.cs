namespace redb.Core.Exceptions;

/// <summary>
/// Thrown by <c>LoadAsync&lt;TProps&gt;</c> when the loaded object belongs to a different scheme than
/// <c>TProps</c> maps to, <b>and</b> <c>RedbServiceConfiguration.ThrowOnSchemeMismatch</c> is set.
/// By default the mismatch is not thrown — <c>LoadAsync</c> returns <c>null</c> instead, so a
/// soft-deleted object (scheme <c>-10</c>) reads as <c>null</c>. Set the flag to surface a genuine type
/// mistake loudly.
/// <para>
/// The check exists because loading an object under the wrong Props type deserializes garbage into the
/// fields and — with <c>EnablePropsCache</c> — caches it under that type, so a following
/// <c>GetWithoutHashValidation</c> would keep returning the garbage. Whether it throws or returns null,
/// the garbage never reaches the cache.
/// </para>
/// <para>
/// The untyped <c>LoadAsync(objectId)</c> (returning <c>IRedbObject</c> without Props) is never affected.
/// </para>
/// </summary>
public class RedbSchemeMismatchException : Exception
{
    /// <summary>Id of the object that was loaded.</summary>
    public long ObjectId { get; }

    /// <summary>The Props type the caller asked for.</summary>
    public Type RequestedType { get; }

    /// <summary>Scheme id that <see cref="RequestedType"/> maps to.</summary>
    public long ExpectedSchemeId { get; }

    /// <summary>Scheme id the loaded object actually has.</summary>
    public long ActualSchemeId { get; }

    public RedbSchemeMismatchException(long objectId, Type requestedType, long expectedSchemeId, long actualSchemeId)
        : base($"Object {objectId} belongs to scheme {actualSchemeId}, but was loaded as " +
               $"'{requestedType.FullName ?? requestedType.Name}' which maps to scheme {expectedSchemeId}. " +
               "Loading an object under the wrong Props type yields garbage and poisons the cache. " +
               "Use the correct TProps, load untyped via LoadAsync(objectId), or unset " +
               "RedbServiceConfiguration.ThrowOnSchemeMismatch to get null instead of this exception.")
    {
        ObjectId = objectId;
        RequestedType = requestedType;
        ExpectedSchemeId = expectedSchemeId;
        ActualSchemeId = actualSchemeId;
    }
}
