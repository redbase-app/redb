using redb.Core.Attributes;
using redb.Core.Models.Entities;

namespace redb.Tests.Integration.Models;

/// <summary>
/// A list of references (<c>List&lt;RedbObject&lt;T&gt;&gt;</c>, not an array). The save collected the elements of arrays
/// only: references by id in a list got no hash, a new object in a list was never saved, and an edit to a loaded one
/// was lost. Synced by the tests that use it.
/// </summary>
[RedbScheme(Name = "ReferenceListProbe")]
public class ReferenceListProbeProps
{
    public string? Label { get; set; }

    public List<RedbObject<CtProbeChildProps>>? Items { get; set; }
}
