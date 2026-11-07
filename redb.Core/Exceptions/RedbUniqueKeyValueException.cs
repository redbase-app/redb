using System;

namespace redb.Core.Exceptions;

/// <summary>
/// A value written into a <c>[RedbUnique]</c> field has no canonical form — <c>NaN</c>, an
/// infinity — so no key can be computed for it. Raised at save time by <c>UniqueKeyEncoder</c>.
/// </summary>
public class RedbUniqueKeyValueException : Exception
{
    /// <summary>Structure (field) the value was meant for.</summary>
    public long StructureId { get; }

    /// <summary>Why the value cannot be a key.</summary>
    public string Reason { get; }

    public RedbUniqueKeyValueException(long structureId, string reason)
        : base($"Value for unique key structure id={structureId} cannot be stored: {reason}.")
    {
        StructureId = structureId;
        Reason = reason;
    }

    /// <summary>An object-level key (Э1, _objects._value_unique) that cannot be stored.</summary>
    public RedbUniqueKeyValueException(string reason)
        : base($"Object unique key cannot be stored: {reason}.")
    {
        Reason = reason;
    }
}
