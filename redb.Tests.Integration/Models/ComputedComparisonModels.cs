using redb.Core.Attributes;

namespace redb.Tests.Integration.Models;

/// <summary>Fields to compare with each other and with values computed from them.</summary>
[RedbScheme("TestComputedComparison")]
public class ComputedComparisonProps
{
    public string Tag { get; set; } = string.Empty;
    public string First { get; set; } = string.Empty;
    public string Last { get; set; } = string.Empty;
    public string Full { get; set; } = string.Empty;
    public int A { get; set; }
    public int B { get; set; }
    public decimal Price { get; set; }
    public decimal Limit { get; set; }
}
