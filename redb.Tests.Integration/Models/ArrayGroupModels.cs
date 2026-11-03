using redb.Core.Attributes;
using redb.Core.Models.Entities;

namespace redb.Tests.Integration.Models;

/// <summary>
/// GroupByArray over arrays of every simple value column, over an array of classes whose element holds a class
/// and over elements with a missing key. Each test writes its own objects under a fresh <see cref="Marker"/> and
/// reads them back by it, so objects other tests leave in the shared database do not count.
/// </summary>
[RedbScheme("TestArrayValues")]
public class ArrayValuesProps
{
    public string Marker { get; set; } = string.Empty;
    public string[]? Words { get; set; }
    public long[]? Longs { get; set; }
    public double[]? Doubles { get; set; }
    public decimal[]? Decimals { get; set; }
    public bool[]? Flags { get; set; }
    public DateTime[]? Dates { get; set; }
    public Guid[]? Ids { get; set; }
    public ArrayItem[]? Items { get; set; }
}

public class ArrayItem
{
    /// <summary>Null on some elements: the group of the null key.</summary>
    public string? Kind { get; set; }
    public int Size { get; set; }
    public bool Active { get; set; }
    public ArrayItemPlace? Place { get; set; }
}

public class ArrayItemPlace
{
    public string City { get; set; } = string.Empty;
}

/// <summary>
/// Arrays GroupByArray refuses by name: list items and object references. Never saved; the refusal comes first.
/// </summary>
[RedbScheme("TestArrayRefusedElements")]
public class ArrayRefusedElementsProps
{
    public List<RedbListItem>? Roles { get; set; }
    public RedbObject<ArrayValuesProps>[]? Links { get; set; }
}
