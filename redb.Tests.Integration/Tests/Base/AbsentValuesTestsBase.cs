using redb.Core;
using redb.Core.Models.Entities;
using redb.Core.Query;
using redb.Core.Query.Aggregation;
using redb.Core.Query.Window;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Analytics over objects that store no value for the fields a statement reads: a null field, an empty or missing array
/// and an unset list item have no props row. Every object of the source still counts, groups (the missing value is its
/// own group) and takes a window row, as the same LINQ over the objects in memory does.
///
/// Also the key of a grouping by a list item: by its Value it is the text of the item, never its id; by the item itself
/// it is the item a loaded object carries, with every field of it.
///
/// Each test reads only the objects it seeded (by id, or by the root of its tree), so the shared database stays out.
/// </summary>
public abstract class AbsentValuesTestsBase
{
    protected readonly IRedbService Redb;

    protected AbsentValuesTestsBase(IRedbService redb) => Redb = redb;

    // SkillLevels: two arrays with elements (average 1.75), an empty one and three with none; Rating: three set,
    // three null. Ages 30..35 average 32.5.
    private static readonly int[]?[] Levels = [[1, 2, 2], [2], [], null, null, null];
    private static readonly double?[] Ratings = [4.5, 4.5, null, null, 3.0, null];

    private sealed record SeededEmployees(long RootId, List<long> Ids, List<EmployeeProps> Props);

    private async Task<SeededEmployees> SeedEmployeesAsync()
    {
        await Redb.SyncSchemeAsync<EmployeeProps>();
        var tag = "av-" + Guid.NewGuid().ToString("N")[..8];
        var root = new TreeRedbObject<EmployeeProps>
        {
            name = $"{tag}-root",
            Props = TestDataFactory.CreateEmployee(0, department: $"{tag}-root").Props
        };
        root.id = await Redb.SaveAsync(root);

        var ids = new List<long>();
        var props = new List<EmployeeProps>();
        for (var i = 0; i < Levels.Length; i++)
        {
            var p = TestDataFactory.CreateEmployee(i + 1, firstName: $"{tag}-{i}", department: tag).Props;
            p.Age = 30 + i;
            p.SkillLevels = Levels[i];
            p.Rating = Ratings[i];
            ids.Add(await Redb.CreateChildAsync(new TreeRedbObject<EmployeeProps> { name = $"{tag}-{i}", Props = p }, root));
            props.Add(p);
        }
        return new SeededEmployees(root.id, ids, props);
    }

    private IRedbQueryable<EmployeeProps> Employees(SeededEmployees s, bool tree)
    {
        if (tree)
            return Redb.TreeQuery<EmployeeProps>(s.RootId).WhereRedb(o => o.Id != s.RootId);
        var idSet = s.Ids.ToHashSet();
        return Redb.Query<EmployeeProps>().WhereRedb(o => idSet.Contains(o.Id));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Aggregate_CountNextToArrayAggregate_CountsEveryObject(bool tree)
    {
        var s = await SeedEmployeesAsync();
        var elements = s.Props.Where(p => p.SkillLevels != null).SelectMany(p => p.SkillLevels!).ToList();

        var r = await Employees(s, tree).AggregateAsync(x => new
        {
            Sum = Agg.Sum(x.Props.SkillLevels!.Select(v => v)),
            Avg = Agg.Average(x.Props.SkillLevels!.Select(v => v)),
            Count = Agg.Count()
        });

        r.Count.Should().Be(s.Props.Count, "objects without elements are objects of the source too");
        r.Sum.Should().Be(elements.Sum());
        ((double)r.Avg).Should().BeApproximately(elements.Average(), 1e-9, "the average of integers is not an integer");
        (await Employees(s, tree).CountAsync()).Should().Be(s.Props.Count);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Aggregate_CountNextToNullableField_CountsEveryObject(bool tree)
    {
        var s = await SeedEmployeesAsync();

        var r = await Employees(s, tree).AggregateAsync(x => new
        {
            Max = Agg.Max(x.Props.Rating),
            Count = Agg.Count(),
            AvgAge = Agg.Average(x.Props.Age)
        });

        r.Count.Should().Be(s.Props.Count);
        r.Max.Should().Be(s.Props.Max(p => p.Rating));
        ((double)r.AvgAge).Should().BeApproximately(s.Props.Average(p => p.Age), 1e-9, "the average of integers is not an integer");
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task GroupBy_NullableField_KeepsTheNullGroup(bool tree)
    {
        var s = await SeedEmployeesAsync();

        var groups = await Employees(s, tree).GroupBy(e => e.Rating)
            .SelectAsync(g => new { Key = g.Key, Count = Agg.Count(g) });

        groups.Select(g => (g.Key, g.Count)).Should().BeEquivalentTo(
            s.Props.GroupBy(p => p.Rating).Select(g => (g.Key, g.Count())));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Window_OverNullableField_EveryObjectTakesARow(bool tree)
    {
        var s = await SeedEmployeesAsync();

        var rows = await Employees(s, tree).WithWindow(w => w.OrderBy(e => e.Rating))
            .SelectAsync(e => new { e.Props.FirstName, Row = Win.RowNumber() });

        rows.Select(r => r.FirstName).Should().BeEquivalentTo(s.Props.Select(p => p.FirstName));
        rows.Select(r => r.Row).Should().BeEquivalentTo(Enumerable.Range(1, s.Props.Count).Select(i => (long)i));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task GroupedWindow_NullableKey_KeepsTheNullGroup(bool tree)
    {
        var s = await SeedEmployeesAsync();

        var ranked = await Employees(s, tree).GroupBy(e => e.Rating).WithWindow(w => w.OrderBy(g => g.Key))
            .SelectAsync(g => new { Key = g.Key, Count = Agg.Count(g), Row = Win.RowNumber() });

        ranked.Select(g => (g.Key, g.Count)).Should().BeEquivalentTo(
            s.Props.GroupBy(p => p.Rating).Select(g => (g.Key, g.Count())));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task List_OrderedByNullableField_ReturnsEveryObject(bool tree)
    {
        var s = await SeedEmployeesAsync();

        var list = await Employees(s, tree).OrderBy(e => e.Rating).ToListAsync();

        list.Select(o => o.Props.FirstName).Should().BeEquivalentTo(s.Props.Select(p => p.FirstName));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task List_OrderedByNullableField_EveryFormThroughTheListPath(bool tree)
    {
        var s = await SeedEmployeesAsync();
        var unset = s.Ids.Where((_, i) => s.Props[i].Rating == null).ToHashSet();

        (await Employees(s, tree).OrderBy(e => e.Rating).Skip(0).Take(s.Props.Count).ToListAsync())
            .Should().HaveCount(s.Props.Count, "Take of every object");
        (await Employees(s, tree).OrderBy(e => e.Rating).Skip(3).Take(3).ToListAsync())
            .Should().HaveCount(3, "the second half of six");
        (await Employees(s, tree).OrderBy(e => e.Rating).CountAsync()).Should().Be(s.Props.Count);

        // Only the objects that leave Rating unset: before the fix nothing came back for them.
        var onlyUnset = Employees(s, tree).WhereRedb(o => unset.Contains(o.Id)).OrderBy(e => e.Rating);
        (await onlyUnset.CountAsync()).Should().Be(unset.Count);
        (await onlyUnset.AnyAsync()).Should().BeTrue();
        (await onlyUnset.ToListAsync()).Select(o => o.id).Should().BeEquivalentTo(unset);
    }

    // ── List items ──────────────────────────────────────────────────────────────

    private sealed record SeededPersons(List<long> Ids, List<RedbObject<PersonProps>> Loaded);

    /// <summary>Six persons: two Active, one Blocked (both items with an alias), three without a status.</summary>
    private async Task<SeededPersons> SeedPersonsAsync()
    {
        await Redb.SyncSchemeAsync<PersonProps>();
        var tag = "av-" + Guid.NewGuid().ToString("N")[..8];
        var list = await Redb.ListProvider.SaveListAsync(RedbList.Create(tag, tag));
        var items = await Redb.ListProvider.AddItemsAsync(list, ["Active", "Blocked"], ["a-alias", "b-alias"]);
        var active = items.First(i => i.Value == "Active");
        var blocked = items.First(i => i.Value == "Blocked");
        RedbListItem?[] statuses = [active, active, blocked, null, null, null];

        var persons = statuses.Select((st, i) => new RedbObject<PersonProps>
        {
            name = $"{tag}-{i}",
            Props = new PersonProps { Name = $"{tag}-{i}", Age = 20 + i, Email = $"{tag}-{i}@test", Status = st }
        }).ToList();
        var ids = await Redb.SaveAsync(persons);
        var idSet = ids.ToHashSet();
        var loaded = await Redb.Query<PersonProps>().WhereRedb(o => idSet.Contains(o.Id)).ToListAsync();
        return new SeededPersons(ids, loaded.ToList());
    }

    private IRedbQueryable<PersonProps> Persons(SeededPersons s)
    {
        var idSet = s.Ids.ToHashSet();
        return Redb.Query<PersonProps>().WhereRedb(o => idSet.Contains(o.Id));
    }

    [Fact]
    public async Task GroupBy_ListItemId_KeepsTheGroupWithoutAnItem()
    {
        var s = await SeedPersonsAsync();

        var groups = await Persons(s).GroupBy(p => p.Status!.Id)
            .SelectAsync(g => new { Key = g.Key, Count = Agg.Count(g) });

        // The key is a long: the group of persons without a status reads 0.
        groups.Select(g => (g.Key, g.Count)).Should().BeEquivalentTo(
            s.Loaded.GroupBy(o => o.Props.Status?.Id ?? 0).Select(g => (g.Key, g.Count())));
    }

    // Without any filter: a grouping over the whole scheme takes a shorter path on some engines than one with a
    // condition (PostgreSQL Free: an inline pivot of its own), and that path lost the group without an item too.
    [Fact]
    public async Task GroupBy_ListItem_WithoutAnyFilter_KeepsTheGroupWithoutAnItem()
    {
        await SeedPersonsAsync();
        var all = await Redb.Query<PersonProps>().ToListAsync();

        var byId = await Redb.Query<PersonProps>().GroupBy(p => p.Status!.Id)
            .SelectAsync(g => new { Key = g.Key, Count = Agg.Count(g) });
        var byValue = await Redb.Query<PersonProps>().GroupBy(p => p.Status!.Value)
            .SelectAsync(g => new { Key = g.Key, Count = Agg.Count(g) });

        byId.Select(g => (g.Key, g.Count)).Should().BeEquivalentTo(
            all.GroupBy(o => o.Props.Status?.Id ?? 0).Select(g => (g.Key, g.Count())));
        byValue.Select(g => (g.Key, g.Count)).Should().BeEquivalentTo(
            all.GroupBy(o => o.Props.Status?.Value).Select(g => (g.Key, g.Count())));
    }

    [Fact]
    public async Task GroupBy_ListItemValue_IsTheTextOfTheItem_AndKeepsTheNullGroup()
    {
        var s = await SeedPersonsAsync();

        var groups = await Persons(s).GroupBy(p => p.Status!.Value)
            .SelectAsync(g => new { Key = g.Key, Count = Agg.Count(g) });

        groups.Select(g => (g.Key, g.Count)).Should().BeEquivalentTo(
            s.Loaded.GroupBy(o => o.Props.Status?.Value).Select(g => (g.Key, g.Count())));
    }

    [Fact]
    public async Task GroupBy_ListItem_KeyIsTheItemALoadedObjectCarries()
    {
        var s = await SeedPersonsAsync();
        var query = Persons(s).GroupBy(p => p.Status);

        var groups = await query.SelectAsync(g => new { Key = g.Key, Count = Agg.Count(g) });

        static (long?, long?, string?, long?, string?) Shape(RedbListItem? i)
            => (i?.Id, i?.IdList, i?.Value, i?.IdObject, i?.Alias);
        groups.Select(g => (Shape(g.Key), g.Count)).Should().BeEquivalentTo(
            s.Loaded.GroupBy(o => o.Props.Status?.Id).Select(g => (Shape(g.First().Props.Status), g.Count())));
        (await query.ToSqlStringAsync(g => new { Key = g.Key, Count = Agg.Count(g) })).Should().NotBeNullOrWhiteSpace();
    }
}
