namespace redb.Core.Models.Configuration;

/// <summary>
/// How the <c>Props</c> getter of an unloaded reference behaves (V4, review). See
/// <see cref="RedbServiceConfiguration.LazyReferenceAccess"/>.
/// </summary>
public enum LazyReferenceAccessMode
{
    /// <summary>Load synchronously on first access (the default, the transparent form).</summary>
    Blocking = 0,

    /// <summary>
    /// Refuse with <see cref="Exceptions.RedbSynchronousLazyLoadException"/>; only the explicit async
    /// APIs load a reference. For hosts where a blocking query on property access is a defect.
    /// </summary>
    Throw = 1,
}
