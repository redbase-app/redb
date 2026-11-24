using redb.Core;
using redb.Core.Models.Entities;
using redb.Core.Query;
using redb.Core.Query.Aggregation;
using redb.Core.Query.Window;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// The field path of an aggregate, a group key and a window key follows the filter's rule: a dictionary entry and the
/// Value of a nullable read the entry and the value, and a path that cannot be read is refused with
/// NotSupportedException. It must never fall back to another field (Items[0].Rank read as the object's own Rank).
///
/// Two objects of one tag, both Name "top", Rank 1; Items[0] is { "inner", 7 } and { "other", 7 };
/// Items[1] is { "zz-second", 70 }; ByKey["k"] is { "entry", 5 }, ByKey["z"] is { "zz-other-entry", 50 }; Main.Score
/// is 4; Scores is { a: 1, n: null, z: 90 }. The second element and the other entries hold larger values and names
/// that sort last, so an answer that reads the wrong element or entry (a MAX over all of them) does not match.
/// </summary>
public abstract class AggregatePathTestsBase
{
    protected readonly IRedbService Redb;

    protected AggregatePathTestsBase(IRedbService redb) => Redb = redb;

    private static FieldPathProps Row(string inner) => new()
    {
        Name = "top",
        Rank = 1,
        Items = [new FieldPathItem { Name = inner, Rank = 7 }, new FieldPathItem { Name = "zz-second", Rank = 70 }],
        Parts = [new FieldPathItem { Name = inner, Rank = 7 }],
        ByKey = new Dictionary<string, FieldPathItem>
        {
            ["k"] = new() { Name = "entry", Rank = 5 },
            ["z"] = new() { Name = "zz-other-entry", Rank = 50 }
        },
        Main = new FieldPathItem { Name = "main", Rank = 3, Score = 4 },
        Scores = new Dictionary<string, int?> { ["a"] = 1, ["n"] = null, ["z"] = 90 }
    };

    /// <summary>
    /// The two objects, as a flat query by their ids, or as the subtree of a root (TreeQuery) whose own values differ,
    /// so an answer that took the root in would not match either.
    /// </summary>
    private async Task<IRedbQueryable<FieldPathProps>> SeedAsync(bool tree)
    {
        await Redb.SyncSchemeAsync<FieldPathProps>();
        var tag = "ap-" + Guid.NewGuid().ToString("N")[..8];
        if (tree)
        {
            var root = new TreeRedbObject<FieldPathProps> { name = tag + "-root", Props = Row("root") };
            root.Props.Rank = 100;
            root.Props.Items![0].Rank = 100;
            root.id = await Redb.SaveAsync(root);
            foreach (var inner in new[] { "inner", "other" })
                await Redb.CreateChildAsync(new TreeRedbObject<FieldPathProps> { name = $"{tag}-{inner}", Props = Row(inner) }, root);
            return Redb.TreeQuery<FieldPathProps>(root.id);
        }

        var ids = new List<long>();
        foreach (var inner in new[] { "inner", "other" })
            ids.Add(await Redb.SaveAsync(new RedbObject<FieldPathProps> { name = $"{tag}-{inner}", Props = Row(inner) }));
        var a = ids[0];
        var b = ids[1];
        return Redb.Query<FieldPathProps>().WhereRedb(o => o.Id == a || o.Id == b);
    }

    public delegate Task<object?> Run(IRedbQueryable<FieldPathProps> q);

    /// <summary>(kind, run, expected, mustWork): mustWork means a refusal is a failure too.</summary>
    private static IEnumerable<(string Kind, Run Run, string Expected, bool MustWork)> Shapes()
    {
        // Single aggregates. A plain int? field must work: TField : struct makes the caller write .Value.
        yield return ("sum-nullable-value", async q => await q.SumAsync(p => p.Main!.Score!.Value), "8", true);
        yield return ("average-nullable-value", async q => await q.AverageAsync(p => p.Main!.Score!.Value), "4", true);
        yield return ("min-nullable-value", async q => await q.MinAsync(p => p.Main!.Score!.Value), "4", true);
        yield return ("max-nullable-value", async q => await q.MaxAsync(p => p.Main!.Score!.Value), "4", true);
        yield return ("statistics-nullable-value", async q => Stats(await q.GetStatisticsAsync(p => p.Main!.Score!.Value)), "8/4/4/4", true);

        // Through an element, an entry, an entry's Value: the right answer, or a refusal.
        yield return ("sum-array-index", async q => await q.SumAsync(p => p.Items![0].Rank), "14", false);
        yield return ("average-array-index", async q => await q.AverageAsync(p => p.Items![0].Rank), "7", false);
        yield return ("min-array-index", async q => await q.MinAsync(p => p.Items![0].Rank), "7", false);
        yield return ("max-array-index", async q => await q.MaxAsync(p => p.Items![0].Rank), "7", false);
        yield return ("statistics-array-index", async q => Stats(await q.GetStatisticsAsync(p => p.Items![0].Rank)), "14/7/7/7", false);
        yield return ("sum-dictionary-entry-field", async q => await q.SumAsync(p => p.ByKey!["k"].Rank), "10", false);
        yield return ("sum-dictionary-nullable-value", async q => await q.SumAsync(p => p.Scores!["a"]!.Value), "2", false);
        yield return ("aggregate-array-index", async q => (await q.AggregateAsync(x => new { S = Agg.Sum(x.Props.Items![0].Rank) })).S, "14", false);
        yield return ("aggregate-dictionary-nullable-value", async q => (await q.AggregateAsync(x => new { S = Agg.Sum(x.Props.Scores!["a"]!.Value) })).S, "2", false);
        yield return ("aggregate-nullable-value", async q => (await q.AggregateAsync(x => new { S = Agg.Sum(x.Props.Main!.Score!.Value) })).S, "8", true);

        // Group keys.
        yield return ("group-array-index", async q =>
            Keys((await q.GroupBy(p => p.Items![0].Name).SelectAsync(g => new { g.Key, C = Agg.Count(g) })).Select(r => (object?)r.Key)), "inner,other", false);
        yield return ("group-dictionary-entry-field", async q =>
            Keys((await q.GroupBy(p => p.ByKey!["k"].Name).SelectAsync(g => new { g.Key, C = Agg.Count(g) })).Select(r => (object?)r.Key)), "entry", false);
        yield return ("group-dictionary-nullable-value", async q =>
            Keys((await q.GroupBy(p => p.Scores!["a"]!.Value).SelectAsync(g => new { g.Key, C = Agg.Count(g) })).Select(r => (object?)r.Key)), "1", false);

        // Window keys: partitioned by Items[0].Name both rows are first; by the object's Name they would be 1 and 2.
        yield return ("window-partition-array-index", async q =>
            Keys((await q.WithWindow(w => w.PartitionBy(p => p.Items![0].Name).OrderBy(p => p.Rank))
                .SelectAsync(e => new { N = Win.RowNumber() })).Select(r => (object?)r.N)), "1,1", false);
        yield return ("window-partition-dictionary-nullable-value", async q =>
            Keys((await q.WithWindow(w => w.PartitionBy(p => p.Scores!["a"]!.Value).OrderBy(p => p.Rank))
                .SelectAsync(e => new { N = Win.RowNumber() })).Select(r => (object?)r.N)), "1,2", false);
        yield return ("window-order-nullable-value", async q =>
            Keys((await q.WithWindow(w => w.OrderBy(p => p.Main!.Score!.Value))
                .SelectAsync(e => new { N = Win.RowNumber() })).Select(r => (object?)r.N)), "1,2", true);
    }

    public static IEnumerable<object[]> Cases()
    {
        foreach (var tree in new[] { false, true })
            foreach (var s in Shapes())
                yield return [(tree ? "tree-" : "flat-") + s.Kind, tree, s.Run, s.Expected, s.MustWork];
    }

    private static string Stats<T>(Core.Query.Aggregation.FieldStatistics<T> s) where T : struct
        => string.Join("/", Fmt(s.Sum), Fmt(s.Average), Convert.ToString(s.Min, System.Globalization.CultureInfo.InvariantCulture),
            Convert.ToString(s.Max, System.Globalization.CultureInfo.InvariantCulture));

    private static string Fmt(object? v) => v is decimal d
        ? d.ToString("0.##########", System.Globalization.CultureInfo.InvariantCulture)
        : Convert.ToString(v, System.Globalization.CultureInfo.InvariantCulture) ?? "";

    private static string Keys(IEnumerable<object?> keys)
        => string.Join(",", keys.Select(k => Convert.ToString(k, System.Globalization.CultureInfo.InvariantCulture)).OrderBy(k => k, StringComparer.Ordinal));

    // The flat Pro answers on PostgreSQL and SQL Server were 0 for these three, with no error: the path came out as
    // Main.Score.Value or Scores[a].Value, no field of the scheme, and the aggregate dropped it from the statement.

    [Fact]
    public async Task AggSumOverANullableValue_ReadsTheField_NotASilentZero()
    {
        var q = await SeedAsync(tree: false);
        (await q.AggregateAsync(x => new { S = Agg.Sum(x.Props.Main!.Score!.Value) })).S.Should().Be(8);
    }

    [Fact]
    public async Task AggSumOverADictionaryEntryValue_ReadsTheEntry_NotASilentZero()
    {
        var q = await SeedAsync(tree: false);
        (await q.AggregateAsync(x => new { S = Agg.Sum(x.Props.Scores!["a"]!.Value) })).S.Should().Be(2);
    }

    [Fact]
    public async Task StatisticsOverANullableValue_ReadTheField_NotSilentZeros()
    {
        var q = await SeedAsync(tree: false);
        Stats(await q.GetStatisticsAsync(p => p.Main!.Score!.Value)).Should().Be("8/4/4/4");
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task PathThroughAnEntryAnElementOrANullable_ReadsThatField_OrIsRefused(
        string kind, bool tree, Run run, string expected, bool mustWork)
    {
        var q = await SeedAsync(tree);

        object? result;
        try
        {
            result = await run(q);
        }
        catch (NotSupportedException) when (!mustWork)
        {
            return; // refused: never a wrong answer
        }

        Fmt(result).Should().Be(expected, $"case {kind}");
    }
}