namespace redb.Core.Exceptions;

/// <summary>
/// Which unique index rejected a save - first-class discrimination for
/// <see cref="RedbUniqueViolationException.Kind"/> (BR-8, Tsak report, 2026-09-02), so callers
/// branch on a value instead of matching provider-specific constraint-name strings.
/// </summary>
public enum RedbUniqueViolationKind
{
    /// <summary>The driver's report could not be classified; inspect
    /// <see cref="RedbUniqueViolationException.ConstraintName"/> / <c>Detail</c> / <c>Cause</c>.</summary>
    Unknown = 0,

    /// <summary>The object key: <c>UIX__objects__scheme_unique</c> over
    /// <c>(_id_scheme, _value_unique)</c> - another object of the scheme already holds this
    /// <c>ValueUnique</c>. <c>SaveByUniqueAsync</c> retries exactly this kind once onto the
    /// current winner's row.</summary>
    ObjectKey = 1,

    /// <summary>A <c>[RedbUnique]</c> property key: <c>UIX__values__structure_unique</c> - another
    /// object holds this value in the same property. Never retried by the object-key upsert.</summary>
    Property = 2,
}
