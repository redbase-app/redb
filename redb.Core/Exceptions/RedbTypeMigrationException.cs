namespace redb.Core.Exceptions;

/// <summary>
/// Thrown by scheme synchronisation when a property's type changed but the stored values could not be
/// moved to the column the new type reads from.
///
/// <para>
/// <b>Why this stops synchronisation instead of warning.</b> The two failure modes are both silent if
/// allowed through. Updating <c>_structures._id_type</c> without moving the values leaves every row
/// reading as <c>null</c> — and with the default <c>PropsSaveStrategy.DeleteInsert</c>
/// the stranded values are physically deleted the next time the object is saved. Leaving the type alone
/// instead makes the CLR class and the structure disagree, which surfaces later and further from the
/// cause. Failing here is the only outcome that keeps the data intact and the state describable.
/// </para>
///
/// <para>
/// The type is deliberately distinct rather than a plain <c>InvalidOperationException</c>: the caller has
/// to be able to tell "values are waiting for a manual migration" from any other start-up failure, and
/// tell it from the exception type rather than by matching on message text.
/// </para>
/// </summary>
public class RedbTypeMigrationException : Exception
{
    /// <summary>Structure whose values could not be migrated.</summary>
    public long StructureId { get; }

    /// <summary>Name of the property backing that structure.</summary>
    public string PropertyName { get; }

    /// <summary>Scheme the structure belongs to.</summary>
    public long SchemeId { get; }

    /// <summary>Scheme name, when it could be resolved; the id is always present.</summary>
    public string? SchemeName { get; }

    /// <summary>REDB type name the structure had.</summary>
    public string OldTypeName { get; }

    /// <summary>REDB type name the property now maps to.</summary>
    public string NewTypeName { get; }

    /// <summary>Rows holding a value under the old type, as reported by the migration.</summary>
    public int AffectedRows { get; }

    /// <summary>Rows the migration did move. Zero when it never ran.</summary>
    public int MigratedRows { get; }

    /// <summary>Rows left behind: they still hold their original value.</summary>
    public int StrandedRows { get; }

    /// <summary>Verbatim diagnostics from the provider's migration, when it produced any.</summary>
    public string? MigrationErrors { get; }

    public RedbTypeMigrationException(
        long structureId,
        string propertyName,
        long schemeId,
        string? schemeName,
        string oldTypeName,
        string newTypeName,
        int affectedRows,
        int migratedRows,
        int strandedRows,
        string? migrationErrors)
        : base(BuildMessage(structureId, propertyName, schemeId, schemeName, oldTypeName, newTypeName,
                            affectedRows, migratedRows, strandedRows, migrationErrors))
    {
        StructureId = structureId;
        PropertyName = propertyName;
        SchemeId = schemeId;
        SchemeName = schemeName;
        OldTypeName = oldTypeName;
        NewTypeName = newTypeName;
        AffectedRows = affectedRows;
        MigratedRows = migratedRows;
        StrandedRows = strandedRows;
        MigrationErrors = migrationErrors;
    }

    private static string BuildMessage(
        long structureId, string propertyName, long schemeId, string? schemeName,
        string oldTypeName, string newTypeName,
        int affectedRows, int migratedRows, int strandedRows, string? migrationErrors)
    {
        var scheme = schemeName is null ? $"id={schemeId}" : $"'{schemeName}' (id={schemeId})";

        // The two cases differ in what the database now holds, so they must not share a wording.
        var state = migratedRows == 0
            ? $"nothing was moved: all {affectedRows} value(s) still hold their original content"
            : $"{migratedRows} of {affectedRows} value(s) were moved and {strandedRows} could not be — " +
              "no value was destroyed, but the two groups now live in different columns";

        var detail = string.IsNullOrWhiteSpace(migrationErrors) ? "" : $" Provider says: {migrationErrors}";

        return
            $"Scheme {scheme}: property '{propertyName}' changed type from '{oldTypeName}' to '{newTypeName}', " +
            $"but the migration did not complete — {state}. " +
            $"The structure type was left at '{oldTypeName}' on purpose, so this keeps failing until it is " +
            $"resolved rather than turning into silently missing values.{detail} " +
            $"Either migrate manually and start again: " +
            $"SELECT * FROM migrate_structure_type({structureId}, '{oldTypeName}', '{newTypeName}', false); " +
            $"— or revert the property to '{oldTypeName}' in the CLR class.";
    }
}
