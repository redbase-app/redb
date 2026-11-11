using redb.Core.Attributes;

namespace redb.Tests.Integration.Models;

/// <summary>
/// A class whose own field and the field of its array elements share a name: a filter path that lost its array part
/// would read the field of the object instead of the field of the element.
/// </summary>
[RedbScheme("TestFieldPath")]
public class FieldPathProps
{
    public string Name { get; set; } = string.Empty;
    public int Rank { get; set; }
    public FieldPathItem[]? Items { get; set; }
    public List<FieldPathItem>? Parts { get; set; }
    public Dictionary<string, FieldPathItem>? ByKey { get; set; }
    public FieldPathItem? Main { get; set; }
}

public class FieldPathItem
{
    public string Name { get; set; } = string.Empty;
    public int Rank { get; set; }
    public int? Score { get; set; }
}
