using redb.Core.Attributes;

namespace redb.Tests.Integration.Models;

/// <summary>
/// A scheme the nodes of the scheme sync race test create and extend together (see SchemeSyncRace). Its name is
/// its own: the test deletes the scheme between rounds.
/// </summary>
[RedbScheme(Name = "schema_lock_sync_probe")]
public class SchemaLockSyncProbeProps
{
    public string Title { get; set; } = "";
    public int Count { get; set; }
    public string? Note { get; set; }
    public SchemaLockSyncProbePart? Part { get; set; }
}

/// <summary>A nested class: its structures hang under a parent, as most real schemes have.</summary>
public class SchemaLockSyncProbePart
{
    public string? City { get; set; }
    public decimal Weight { get; set; }
}
