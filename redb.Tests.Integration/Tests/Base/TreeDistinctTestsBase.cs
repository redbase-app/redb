using redb.Core;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// DISTINCT ON a tree query, six hosts. A tree query ignored the distinct key and returned every node (SQLite Pro
/// and the three Free providers), where the same distinct on a flat query of the same objects returned one object
/// per key; the count of a distinct tree query was wrong on all three Pro (review 2026-09-24). Each tree fact has a
/// flat twin over the same seed, so a wrong expectation fails on both.
/// </summary>
public abstract class TreeDistinctTestsBase
{
    protected readonly IRedbService Redb;

    protected TreeDistinctTestsBase(IRedbService redb) => Redb = redb;

    /// <summary>A root and six children: names in three pairs, departments in two groups of three.</summary>
    private async Task<(long RootId, string Tag)> SeedAsync()
    {
        await Redb.SyncSchemeAsync<EmployeeProps>();
        var tag = Guid.NewGuid().ToString("N")[..8];
        var root = new TreeRedbObject<EmployeeProps>
        {
            name = $"distinct-root-{tag}",
            Props = TestDataFactory.CreateEmployee(0, department: $"Root-{tag}", salary: 1m).Props
        };
        root.id = await Redb.SaveAsync(root);
        for (var i = 0; i < 6; i++)
        {
            var child = new TreeRedbObject<EmployeeProps>
            {
                name = $"distinct-{tag}-{i / 2}",
                Props = TestDataFactory.CreateEmployee(i + 1, department: $"D{i % 2}-{tag}", salary: 100m * (i + 1)).Props
            };
            await Redb.CreateChildAsync(child, root);
        }
        return (root.id, tag);
    }

    [Fact]
    public async Task TreeDistinctByRedb_ReturnsOneNodePerName()
    {
        var (rootId, _) = await SeedAsync();

        var all = await Redb.TreeQuery<EmployeeProps>(rootId).ToListAsync();
        var distinct = await Redb.TreeQuery<EmployeeProps>(rootId).DistinctByRedb(o => o.Name).ToListAsync();

        var names = all.Select(o => o.Name).Distinct().ToList();
        all.Count.Should().BeGreaterThan(names.Count, "precondition: children share names");
        distinct.Select(o => o.Name).Should().OnlyHaveUniqueItems()
            .And.BeEquivalentTo(names, "one node per distinct name, and every name kept");
    }

    [Fact]
    public async Task FlatDistinctByRedb_ReturnsOneObjectPerName()
    {
        var (_, tag) = await SeedAsync();

        var distinct = await Redb.Query<EmployeeProps>()
            .WhereRedb(o => o.Name.StartsWith($"distinct-{tag}-"))
            .DistinctByRedb(o => o.Name)
            .ToListAsync();

        distinct.Select(o => o.Name).Should()
            .BeEquivalentTo(new[] { $"distinct-{tag}-0", $"distinct-{tag}-1", $"distinct-{tag}-2" });
    }

    [Fact]
    public async Task TreeDistinctByRedb_CountsOneNodePerName()
    {
        var (rootId, _) = await SeedAsync();

        var names = (await Redb.TreeQuery<EmployeeProps>(rootId).ToListAsync()).Select(o => o.Name).Distinct().Count();
        var count = await Redb.TreeQuery<EmployeeProps>(rootId).DistinctByRedb(o => o.Name).CountAsync();

        count.Should().Be(names, "the count of a distinct query is the number of distinct keys");
    }

    [Fact]
    public async Task TreeDistinctBy_ReturnsOneNodePerDepartment()
    {
        var (rootId, _) = await SeedAsync();

        var all = await Redb.TreeQuery<EmployeeProps>(rootId).ToListAsync();
        var distinct = await Redb.TreeQuery<EmployeeProps>(rootId).DistinctBy(e => e.Department).ToListAsync();

        var departments = all.Select(o => o.Props.Department).Distinct().ToList();
        all.Count.Should().BeGreaterThan(departments.Count, "precondition: children share departments");
        distinct.Select(o => o.Props.Department).Should().OnlyHaveUniqueItems()
            .And.BeEquivalentTo(departments, "one node per department, and every department kept");
    }
}
