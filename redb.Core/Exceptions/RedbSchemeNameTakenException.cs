namespace redb.Core.Exceptions;

/// <summary>
/// Thrown when a type wants to rename its scheme to an explicit name that another scheme in the
/// database already occupies. Raised instead of letting the UNIQUE(_name) violation surface as a
/// provider-specific error code (23505 / 2627 / SQLITE_CONSTRAINT_UNIQUE), and the database is left
/// untouched.
/// </summary>
public class RedbSchemeNameTakenException : Exception
{
    /// <summary>The type that asked for the name.</summary>
    public Type DeclaringType { get; }

    /// <summary>The contested scheme name.</summary>
    public string SchemeName { get; }

    /// <summary>Id of the scheme already holding the name.</summary>
    public long OccupiedBySchemeId { get; }

    public RedbSchemeNameTakenException(Type declaringType, string schemeName, long occupiedBySchemeId)
        : base($"Type '{declaringType.FullName ?? declaringType.Name}' declares " +
               $"[RedbScheme(Name = \"{schemeName}\")], but scheme id={occupiedBySchemeId} already uses that name. " +
               "Scheme names are unique — pick another name, or remove the stale scheme first.")
    {
        DeclaringType = declaringType;
        SchemeName = schemeName;
        OccupiedBySchemeId = occupiedBySchemeId;
    }

    /// <summary>
    /// The type resolves to two different schemes at once: one already carries the explicit name, and
    /// another still sits under a previous name. That is the split-brain described in
    /// docs/SCHEME_EXPLICIT_NAME_PLAN.md — typically an older binary re-created the scheme under its
    /// old name after a newer one had renamed it, and objects are now being written to both.
    /// </summary>
    public RedbSchemeNameTakenException(
        Type declaringType, string schemeName, long occupiedBySchemeId, string previousName, long previousSchemeId)
        : base($"Type '{declaringType.FullName ?? declaringType.Name}' maps to two schemes at once: " +
               $"id={occupiedBySchemeId} named '{schemeName}' and id={previousSchemeId} named '{previousName}'. " +
               "The rename to the explicit name has already happened, yet the old scheme was re-created — " +
               "most likely by an application version that predates the explicit name. Objects may be split " +
               "across both schemes; merge them and remove the stale one before continuing.")
    {
        DeclaringType = declaringType;
        SchemeName = schemeName;
        OccupiedBySchemeId = occupiedBySchemeId;
    }
}
