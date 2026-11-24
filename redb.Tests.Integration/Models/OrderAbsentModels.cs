using redb.Core.Attributes;

namespace redb.Tests.Integration.Models;

/// <summary>
/// Rows of one tag, sorted by a field some of them have no value for, and by a group then a name.
/// </summary>
[RedbScheme("TestOrderAbsent")]
public class OrderAbsentProps
{
    public string Tag { get; set; } = string.Empty;
    public string? Label { get; set; }
    public int? Rank { get; set; }
    public int Group { get; set; }
    public string Name { get; set; } = string.Empty;
}
