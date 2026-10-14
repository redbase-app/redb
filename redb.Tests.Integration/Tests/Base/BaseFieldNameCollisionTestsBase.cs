using redb.Core;
using redb.Core.Models.Entities;
using redb.Core.Query;
using redb.Core.Query.Aggregation;
using redb.Core.Query.Window;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// A Props field may be called like a base field of the object: Name, Key, Note, DateBegin.
/// <c>OrderBy(p => p.Name)</c> and <c>GroupBy(p => p.Name)</c> mean the Props field;
/// <c>OrderByRedb(o => o.Name)</c> and <c>GroupByRedb(o => o.Name)</c> mean the object's own.
///
/// Sorting and grouping used to decide by the name as well as by the flag the parser sets, so both
/// pairs read the object's column: a sort by Props.Name came back sorted by the object name, and
/// a grouping by Props.Name returned object names as keys. Filters decided by the flag alone and
/// were right, which is why the same field filtered correctly and sorted wrongly.
///
/// The seed makes the two orders opposite, so a query that reads the wrong column cannot pass.
/// </summary>
public abstract class BaseFieldNameCollisionTestsBase
{
    protected readonly IRedbService Redb;

    protected BaseFieldNameCollisionTestsBase(IRedbService redb) => Redb = redb;

    /// <summary>Three persons: object names a, b, c; Props.Name z, y, x.</summary>
    private async Task<string> SeedAsync()
    {
        var tag = Guid.NewGuid().ToString("N")[..8];
        foreach (var (objectName, propsName) in new[] { ("a", "z"), ("b", "y"), ("c", "x") })
        {
            await Redb.SaveAsync(new RedbObject<PersonProps>
            {
                name = $"{objectName}-{tag}",
                Props = new PersonProps { Name = $"{propsName}-{tag}", Age = 30, Email = $"{objectName}@collision-{tag}" }
            });
        }
        return tag;
    }

    private IRedbQueryable<PersonProps> Flat(string tag)
        => Redb.Query<PersonProps>().Where(p => p.Email.EndsWith($"@collision-{tag}"));

    private IRedbQueryable<PersonProps> Tree(string tag)
        => Redb.TreeQuery<PersonProps>().Where(p => p.Email.EndsWith($"@collision-{tag}"));

    private async Task EachFormAsync(Func<string, Task> check)
    {
        var restore = Redb.Configuration.EnablePvtPrefilter;
        try
        {
            foreach (var on in new[] { false, true })
            {
                Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = on);
                await check(on ? "planner on" : "planner off");
            }
        }
        finally
        {
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = restore);
        }
    }

    private static string[] PropsNames<T>(IEnumerable<T> rows) where T : RedbObject<PersonProps>
        => rows.Select(r => r.Props.Name[..1]).ToArray();

    [Fact]
    public async Task OrderBy_PropsFieldNamedLikeBaseField_SortsByProps()
    {
        var tag = await SeedAsync();

        await EachFormAsync(async form =>
        {
            PropsNames(await Flat(tag).OrderBy(p => p.Name).ToListAsync())
                .Should().Equal(["x", "y", "z"], $"OrderBy, {form}");
            PropsNames(await Flat(tag).OrderBy(p => p.Name).Take(10).ToListAsync())
                .Should().Equal(["x", "y", "z"], $"OrderBy with Take, {form}");
            PropsNames(await Flat(tag).OrderByDescending(p => p.Name).ToListAsync())
                .Should().Equal(["z", "y", "x"], $"OrderByDescending, {form}");
            PropsNames(await Flat(tag).OrderBy(p => p.Age).ThenBy(p => p.Name).ToListAsync())
                .Should().Equal(["x", "y", "z"], $"ThenBy, {form}");
            PropsNames(await Flat(tag).OrderBy(p => p.Name.ToLower()).ToListAsync())
                .Should().Equal(["x", "y", "z"], $"OrderBy over an expression, {form}");

            var tree = await Tree(tag).OrderBy(p => p.Name).ToListAsync();
            tree.Select(r => r.Props.Name[..1]).Should().Equal(["x", "y", "z"], $"tree OrderBy, {form}");
        });
    }

    [Fact]
    public async Task OrderByRedb_SortsByTheObjectsOwnField()
    {
        var tag = await SeedAsync();

        await EachFormAsync(async form =>
        {
            PropsNames(await Flat(tag).OrderByRedb(o => o.Name).ToListAsync())
                .Should().Equal(["z", "y", "x"], $"object names run a, b, c, {form}");

            var tree = await Tree(tag).OrderByRedb(o => o.Name).ToListAsync();
            tree.Select(r => r.Props.Name[..1]).Should().Equal(["z", "y", "x"], $"tree OrderByRedb, {form}");
        });
    }

    [Fact]
    public async Task GroupBy_PropsFieldNamedLikeBaseField_GroupsByProps()
    {
        var tag = await SeedAsync();

        await EachFormAsync(async form =>
        {
            var flat = await Flat(tag).GroupBy(p => p.Name).SelectAsync(g => new { g.Key, Count = Agg.Count(g) });
            flat.Select(g => g.Key[..1]).OrderBy(k => k).Should().Equal(["x", "y", "z"], $"GroupBy keys, {form}");

            var tree = await Tree(tag).GroupBy(p => p.Name).SelectAsync(g => new { g.Key, Count = Agg.Count(g) });
            tree.Select(g => g.Key[..1]).OrderBy(k => k).Should().Equal(["x", "y", "z"], $"tree GroupBy keys, {form}");
        });
    }

    [Fact]
    public async Task GroupByRedb_GroupsByTheObjectsOwnField()
    {
        var tag = await SeedAsync();

        await EachFormAsync(async form =>
        {
            var flat = await Flat(tag).GroupByRedb(o => o.Name).SelectAsync(g => new { g.Key, Count = Agg.Count(g) });
            flat.Select(g => g.Key[..1]).OrderBy(k => k).Should().Equal(["a", "b", "c"], $"GroupByRedb keys, {form}");
        });
    }

    [Fact]
    public async Task Where_PropsFieldNamedLikeBaseField_FiltersByProps()
    {
        var tag = await SeedAsync();

        var rows = await Flat(tag).Where(p => p.Name.StartsWith("x")).ToListAsync();
        rows.Should().ContainSingle().Which.name.Should().StartWith("c", "Props.Name x belongs to object c");
    }
    [Fact]
    public async Task Aggregates_TellPropsFieldFromTheObjectsOwn()
    {
        var tag = await SeedAsync();

        await EachFormAsync(async form =>
        {
            foreach (var (kind, query) in new[] { ("flat", Flat(tag)), ("tree", Tree(tag)) })
            {
                var props = await query.AggregateAsync(x => new { Max = Agg.Max(x.Props.Name), Min = Agg.Min(x.Props.Name), Count = Agg.Count() });
                props.Max[..1].Should().Be("z", $"Agg.Max(x.Props.Name), {kind}, {form}");
                props.Min[..1].Should().Be("x", $"Agg.Min(x.Props.Name), {kind}, {form}");
                props.Count.Should().Be(3, $"{kind}, {form}");

                var own = await query.AggregateAsync(x => new { Max = Agg.Max(x.Name), Min = Agg.Min(x.Name) });
                own.Max[..1].Should().Be("c", $"Agg.Max(x.Name) is the object's name, {kind}, {form}");
                own.Min[..1].Should().Be("a", $"Agg.Min(x.Name) is the object's name, {kind}, {form}");

                var mixed = await query.AggregateAsync(x => new { Props = Agg.Max(x.Props.Name), Own = Agg.Max(x.Name), Ages = Agg.Sum(x.Props.Age) });
                mixed.Props[..1].Should().Be("z", $"both in one batch, {kind}, {form}");
                mixed.Own[..1].Should().Be("c", $"both in one batch, {kind}, {form}");
                mixed.Ages.Should().Be(90, $"three persons aged 30, {kind}, {form}");
            }
        });
    }

    [Fact]
    public async Task AggregateInsideGroupBy_ReadsProps()
    {
        var tag = await SeedAsync();

        await EachFormAsync(async form =>
        {
            var flat = await Flat(tag).GroupBy(p => p.Age).SelectAsync(g => new { g.Key, Max = Agg.Max(g, e => e.Name) });
            flat.Should().ContainSingle().Which.Max[..1].Should().Be("z", $"flat, {form}");

            var tree = await Tree(tag).GroupBy(p => p.Age).SelectAsync(g => new { g.Key, Max = Agg.Max(g, e => e.Name) });
            tree.Should().ContainSingle().Which.Max[..1].Should().Be("z", $"tree, {form}");
        });
    }

    [Fact]
    public async Task GroupedWindow_KeyFollowsGroupByOrGroupByRedb()
    {
        var tag = await SeedAsync();

        await EachFormAsync(async form =>
        {
            foreach (var (kind, query) in new[] { ("flat", Flat(tag)), ("tree", Tree(tag)) })
            {
                var byProps = await query.GroupBy(p => p.Name)
                    .WithWindow(w => w.OrderByDesc(g => Agg.Sum(g, e => e.Age)))
                    .SelectAsync(g => new { g.Key, Ages = Agg.Sum(g, e => e.Age), Row = Win.RowNumber() });
                byProps.Select(g => g.Key[..1]).OrderBy(k => k).Should().Equal(["x", "y", "z"], $"GroupBy key, {kind}, {form}");

                var byObject = await query.GroupByRedb(o => o.Name)
                    .WithWindow(w => w.OrderByDesc(g => Agg.Sum(g, e => e.Age)))
                    .SelectAsync(g => new { g.Key, Ages = Agg.Sum(g, e => e.Age), Row = Win.RowNumber() });
                byObject.Select(g => g.Key[..1]).OrderBy(k => k).Should().Equal(["a", "b", "c"], $"GroupByRedb key, {kind}, {form}");
            }
        });
    }
    [Fact]
    public async Task GroupedWindow_OrdersByKeyAndByCount()
    {
        // Ages are equal in the seed, so the grouping here is by the Props name: three groups of one.
        var tag = await SeedAsync();

        await EachFormAsync(async form =>
        {
            foreach (var (kind, query) in new[] { ("flat", Flat(tag)), ("tree", Tree(tag)) })
            {
                var byKey = await query.GroupBy(p => p.Name)
                    .WithWindow(w => w.OrderBy(g => g.Key))
                    .SelectAsync(g => new { g.Key, Count = Agg.Count(g), Row = Win.RowNumber() });
                byKey.OrderBy(g => g.Row).Select(g => g.Key[..1])
                    .Should().Equal(["x", "y", "z"], $"window ordered by g.Key, {kind}, {form}");

                var byKeyDescending = await query.GroupBy(p => p.Name)
                    .WithWindow(w => w.OrderByDesc(g => g.Key))
                    .SelectAsync(g => new { g.Key, Count = Agg.Count(g), Row = Win.RowNumber() });
                byKeyDescending.OrderBy(g => g.Row).Select(g => g.Key[..1])
                    .Should().Equal(["z", "y", "x"], $"window ordered by g.Key descending, {kind}, {form}");

                var byCount = await query.GroupBy(p => p.Age)
                    .WithWindow(w => w.OrderByDesc(g => Agg.Count(g)))
                    .SelectAsync(g => new { g.Key, Count = Agg.Count(g), Row = Win.RowNumber() });
                byCount.Should().ContainSingle($"all three persons share an age, {kind}, {form}")
                    .Which.Count.Should().Be(3);
            }
        });
    }

    [Fact]
    public async Task GroupedWindow_OrderedByAnAggregateTheSelectorLacks_Works()
    {
        // Two persons aged 30 and one aged 40: sums 60 and 40, averages 30 and 40, counts 2 and 1.
        var tag = Guid.NewGuid().ToString("N")[..8];
        foreach (var (name, age) in new[] { ("a", 30), ("b", 30), ("c", 40) })
        {
            await Redb.SaveAsync(new RedbObject<PersonProps>
            {
                name = $"{name}-{tag}",
                Props = new PersonProps { Name = $"{name}-{tag}", Age = age, Email = $"{name}@collision-{tag}" }
            });
        }

        await EachFormAsync(async form =>
        {
            foreach (var (kind, query) in new[] { ("flat", Flat(tag)), ("tree", Tree(tag)) })
            {
                var bySum = await query.GroupBy(p => p.Age)
                    .WithWindow(w => w.OrderByDesc(g => Agg.Sum(g, e => e.Age)))
                    .SelectAsync(g => new { g.Key, Row = Win.RowNumber() });
                bySum.OrderBy(g => g.Row).Select(g => g.Key).Should().Equal([30, 40], $"Sum is not in the selector, {kind}, {form}");

                var byAverage = await query.GroupBy(p => p.Age)
                    .WithWindow(w => w.OrderByDesc(g => Agg.Average(g, e => e.Age)))
                    .SelectAsync(g => new { g.Key, Count = Agg.Count(g), Row = Win.RowNumber() });
                byAverage.OrderBy(g => g.Row).Select(g => g.Key).Should().Equal([40, 30], $"Average is not in the selector, {kind}, {form}");

                var byCount = await query.GroupBy(p => p.Age)
                    .WithWindow(w => w.OrderByDesc(g => Agg.Count(g)))
                    .SelectAsync(g => new { g.Key, Sum = Agg.Sum(g, e => e.Age), Row = Win.RowNumber() });
                byCount.OrderBy(g => g.Row).Select(g => g.Key).Should().Equal([30, 40], $"Count is not in the selector, {kind}, {form}");

                var averageShown = await query.GroupBy(p => p.Age)
                    .WithWindow(w => w.OrderBy(g => Agg.Average(g, e => e.Age)))
                    .SelectAsync(g => new { g.Key, Average = Agg.Average(g, e => e.Age), Row = Win.RowNumber() });
                averageShown.OrderBy(g => g.Row).Select(g => g.Key).Should().Equal([30, 40], $"Average in the selector, {kind}, {form}");
            }
        });
    }
}
