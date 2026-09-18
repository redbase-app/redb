using redb.Core.Attributes;
using redb.Core.Models.Entities;

namespace redb.Tests.Integration.Models;

/// <summary>An object that goes to the trash in the trash integrity suites.</summary>
[RedbScheme(Name = "TrashTarget")]
public class TrashTargetProps
{
    public string? Title { get; set; }
}

/// <summary>
/// An object that references trash targets - through a single reference and through a collection,
/// the two shapes a <c>_values._Object</c> row takes.
/// </summary>
[RedbScheme(Name = "TrashHolder")]
public class TrashHolderProps
{
    public string? Title { get; set; }

    public RedbObject<TrashTargetProps>? Target { get; set; }

    public List<RedbObject<TrashTargetProps>>? Targets { get; set; }
}
