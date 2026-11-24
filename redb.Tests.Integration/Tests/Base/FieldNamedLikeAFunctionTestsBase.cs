using redb.Core;
using redb.Core.Attributes;
using redb.Core.Models.Entities;
using redb.Core.Query;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>A model whose fields carry the names of collection members and LINQ functions.</summary>
[RedbScheme("TestFieldNamedLikeAFunction")]
public class FieldNamedLikeAFunctionProps
{
    public string Tag { get; set; } = string.Empty;
    public int Count { get; set; }
    public int Length { get; set; }
    public decimal Sum { get; set; }
    public int Min { get; set; }
    public int Max { get; set; }
    public bool Any { get; set; }
    public string First { get; set; } = string.Empty;
    public double Average { get; set; }
    public string Key { get; set; } = string.Empty;
    public string Value { get; set; } = string.Empty;
    public FieldNamedLikeAFunctionInner Inner { get; set; } = new();
    public List<string> Tags { get; set; } = [];
}

public class FieldNamedLikeAFunctionInner
{
    public int Count { get; set; }
    public int Length { get; set; }
}

/// <summary>
/// A field of the model named Count or Length is a field: a filter, a sort and an aggregate read it like any other.
/// Every member of those names was taken for the size function of a collection or a string, and a filter on such a
/// field failed. The other names a LINQ query uses (Sum, Min, Max, Any, First, Average, Key, Value) are checked too.
/// Count of a real collection stays its size.
/// </summary>
public abstract class FieldNamedLikeAFunctionTestsBase
{
    protected readonly IRedbService Redb;

    protected FieldNamedLikeAFunctionTestsBase(IRedbService redb) => Redb = redb;

    private async Task<string> SeedAsync()
    {
        await Redb.SyncSchemeAsync<FieldNamedLikeAFunctionProps>();
        var tag = "fnf-" + Guid.NewGuid().ToString("N")[..8];
        for (var i = 1; i <= 3; i++)
        {
            await Redb.SaveAsync(new RedbObject<FieldNamedLikeAFunctionProps>
            {
                name = $"{tag}-{i}",
                Props = new FieldNamedLikeAFunctionProps
                {
                    Tag = tag, Count = i * 10, Length = i * 100, Sum = i * 1.5m, Min = i, Max = i * 7, Any = i == 2,
                    First = $"f{i}", Average = i * 0.25, Key = $"k{i}", Value = $"v{i}",
                    Inner = new FieldNamedLikeAFunctionInner { Count = i, Length = i * 2 },
                    Tags = Enumerable.Range(0, i).Select(n => $"t{n}").ToList()
                }
            });
        }
        return tag;
    }

    private IRedbQueryable<FieldNamedLikeAFunctionProps> Q(string tag)
        => Redb.Query<FieldNamedLikeAFunctionProps>().Where(p => p.Tag == tag);

    private static async Task<int[]> MinOf(IRedbQueryable<FieldNamedLikeAFunctionProps> q)
        => (await q.ToListAsync()).Select(o => o.Props.Min).OrderBy(x => x).ToArray();

    [Fact]
    public async Task Filters_OnFieldsNamedLikeFunctions_ReadTheFields()
    {
        var tag = await SeedAsync();

        using var scope = new AwesomeAssertions.Execution.AssertionScope();
        (await MinOf(Q(tag).Where(p => p.Count == 20))).Should().Equal([2], "Count");
        (await MinOf(Q(tag).Where(p => p.Count > 10))).Should().Equal([2, 3], "Count >");
        (await MinOf(Q(tag).Where(p => p.Length >= 200))).Should().Equal([2, 3], "Length");
        (await MinOf(Q(tag).Where(p => p.Sum == 4.5m))).Should().Equal([3], "Sum");
        (await MinOf(Q(tag).Where(p => p.Min == 1))).Should().Equal([1], "Min");
        (await MinOf(Q(tag).Where(p => p.Max < 14))).Should().Equal([1], "Max");
        (await MinOf(Q(tag).Where(p => p.Any))).Should().Equal([2], "Any");
        (await MinOf(Q(tag).Where(p => p.First == "f3"))).Should().Equal([3], "First");
        (await MinOf(Q(tag).Where(p => p.Average > 0.3))).Should().Equal([2, 3], "Average");
        (await MinOf(Q(tag).Where(p => p.Key == "k1"))).Should().Equal([1], "Key");
        (await MinOf(Q(tag).Where(p => p.Value == "v2"))).Should().Equal([2], "Value");
        (await MinOf(Q(tag).Where(p => p.Inner.Count == 3))).Should().Equal([3], "Inner.Count");
        (await MinOf(Q(tag).Where(p => p.Inner.Length == 2))).Should().Equal([1], "Inner.Length");
        (await MinOf(Q(tag).Where(p => p.Tags.Count == 2))).Should().Equal([2], "Count of a list is its size");
        (await MinOf(Q(tag).Where(p => p.First.Length == 2))).Should().Equal([1, 2, 3], "Length of a string is its length");
    }

    [Fact]
    public async Task SortAndAggregates_OnFieldsNamedLikeFunctions_ReadTheFields()
    {
        var tag = await SeedAsync();

        using var scope = new AwesomeAssertions.Execution.AssertionScope();
        (await Q(tag).OrderByDescending(p => p.Count).ToListAsync()).Select(o => o.Props.Min).Should().Equal([3, 2, 1], "sort by Count");
        (await Q(tag).OrderBy(p => p.Length).ToListAsync()).Select(o => o.Props.Min).Should().Equal([1, 2, 3], "sort by Length");
        (await Q(tag).SumAsync(p => p.Count)).Should().Be(60, "sum of Count");
        (await Q(tag).SumAsync(p => p.Length)).Should().Be(600, "sum of Length");
        (await Q(tag).CountAsync()).Should().Be(3);
    }
}
