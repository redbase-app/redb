namespace redb.Export.Models;

/// <summary>
/// Represents the footer (trailer) record of a <c>.redb</c> export file.
/// The footer is always the last line written to the JSONL stream
/// and contains aggregate statistics and the global-identity sequence value
/// required for a consistent restore.
/// </summary>
public sealed class ExportFooter
{
    /// <summary>Record type discriminator. Always <c>"footer"</c>.</summary>
    public string Type => "footer";

    /// <summary>Value of the <c>global_identity</c> sequence at the time of export.</summary>
    public long SequenceValue { get; init; }

    /// <summary>Total number of type definition records exported.</summary>
    public long TotalTypes { get; init; }

    /// <summary>Total number of role records exported.</summary>
    public long TotalRoles { get; init; }

    /// <summary>Total number of user records exported.</summary>
    public long TotalUsers { get; init; }

    /// <summary>Total number of user-role junction records exported.</summary>
    public long TotalUserRoles { get; init; }

    /// <summary>Total number of list definition records exported.</summary>
    public long TotalLists { get; init; }

    /// <summary>Total number of list item records exported.</summary>
    public long TotalListItems { get; init; }

    /// <summary>Total number of scheme definition records exported.</summary>
    public long TotalSchemes { get; init; }

    /// <summary>Total number of structure (field) definition records exported.</summary>
    public long TotalStructures { get; init; }

    /// <summary>Total number of object instance records exported.</summary>
    public long TotalObjects { get; init; }

    /// <summary>Total number of permission records exported.</summary>
    public long TotalPermissions { get; init; }

    /// <summary>Total number of property-value records exported.</summary>
    public long TotalValues { get; init; }

    /// <summary>SHA-256 checksum of the export file (hex-encoded, lowercase).</summary>
    public string Checksum { get; init; } = "";

    /// <summary>Wall-clock duration of the export operation.</summary>
    public TimeSpan Duration { get; init; }
}
