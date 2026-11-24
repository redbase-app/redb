using redb.Core.Attributes;

namespace redb.Tests.Integration.Models;

/// <summary>An array and a list of classes, for Any() with a condition over the fields of an element.</summary>
[RedbScheme("TestArrayOfClassesAny")]
public class OrderSheetProps
{
    public string Tag { get; set; } = "";
    public int Number { get; set; }
    public OrderLine[]? Lines { get; set; }
    public List<OrderLine>? Extra { get; set; }
}

public class OrderLine
{
    public string? Kind { get; set; }
    public bool? Active { get; set; }
    public int Qty { get; set; }
    public decimal? Price { get; set; }
    public DateTime? Due { get; set; }
    public TimeSpan? Wait { get; set; }
}
