namespace redb.Core.Exceptions;

/// <summary>
/// Thrown when two CLR types claim the same explicit scheme name. Both types are named in the
/// message — the whole point is that neither is more "correct" than the other, and the developer
/// has to pick.
/// <para>
/// Only conflicts involving an explicit <c>Name</c> are fatal. Alias collisions have always been
/// resolved last-write-wins and stay that way, so projects that predate explicit names are unaffected.
/// </para>
/// </summary>
public class RedbSchemeNameConflictException : Exception
{
    /// <summary>The contested scheme name.</summary>
    public string SchemeName { get; }

    /// <summary>The type already registered under this name.</summary>
    public Type ExistingType { get; }

    /// <summary>The type that tried to claim the same name.</summary>
    public Type ConflictingType { get; }

    public RedbSchemeNameConflictException(string schemeName, Type existingType, Type conflictingType)
        : base($"Scheme name '{schemeName}' is claimed by two types: " +
               $"'{existingType.FullName ?? existingType.Name}' and " +
               $"'{conflictingType.FullName ?? conflictingType.Name}'. " +
               "A scheme name is unique across the database — rename one of them or drop the explicit Name.")
    {
        SchemeName = schemeName;
        ExistingType = existingType;
        ConflictingType = conflictingType;
    }
}
