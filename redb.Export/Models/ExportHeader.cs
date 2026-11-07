namespace redb.Export.Models;

/// <summary>
/// Represents the header record of a <c>.redb</c> export file.
/// The header is always the first line written to the JSONL stream
/// and carries metadata about the export (provider, timestamp, filters).
/// </summary>
public sealed class ExportHeader
{
    /// <summary>Record type discriminator. Always <c>"header"</c>.</summary>
    public string Type => "header";

    /// <summary>Format version of the export file (currently <c>"1.0"</c>).</summary>
    public string Version { get; init; } = "1.0";

    /// <summary>Database provider name that produced the export (e.g. <c>"postgres"</c>, <c>"mssql"</c>).</summary>
    public string Provider { get; init; } = "";

    /// <summary>UTC timestamp when the export was started.</summary>
    public DateTime ExportedAt { get; init; } = DateTime.UtcNow;

    /// <summary>
    /// Scheme identifiers that were exported.
    /// An empty array means all schemes were included.
    /// </summary>
    public long[] SchemeIds { get; init; } = [];

    /// <summary>Optional human-readable description of the export.</summary>
    public string? Description { get; init; }
}
