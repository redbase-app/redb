using redb.Core.Attributes;
using redb.Core.Models.Entities;

namespace redb.Tests.Integration.Models;

/// <summary>
/// A node with two references to nodes of the same scheme: enough to build a graph that is not a tree - two parents
/// sharing one target - which is what the save's hashing and collection must handle.
/// </summary>
[RedbScheme]
public class GraphNodeProps
{
    public string Title { get; set; } = string.Empty;

    public RedbObject<GraphNodeProps>? Left { get; set; }

    public RedbObject<GraphNodeProps>? Right { get; set; }
}
