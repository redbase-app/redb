using System.Linq.Expressions;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Core.Query;
using redb.Core.Query.Aggregation;
using redb.Core.Query.Window;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Every analytic form over a filter that reads an array, a dictionary entry or a field of a nested class, flat and
/// over a tree, compared with the same LINQ over the seeded objects in memory: SumAsync, AggregateAsync, GroupBy,
/// WithWindow, GroupBy.WithWindow, GroupByArray, and for the tree also ToListAsync and CountAsync.
///
/// Pro answers such a filter with the list statement of the same filter inside the analytic statement (a subquery on
/// PostgreSQL and SQLite, a temporary table of the same batch on SQL Server); it used to run that statement first and
/// inline the ids it returned. A filter that matches nothing and a filter that matches one object are part of the set:
/// the first must not turn into "every object", the second must give exactly one.
/// </summary>
public abstract class CollectionFilterAnalyticsTestsBase
{
    protected readonly IRedbService Redb;

    protected CollectionFilterAnalyticsTestsBase(IRedbService redb) => Redb = redb;

    private sealed record Seeded(long RootId, string Tag, List<EmployeeProps> Children);

    /// <summary>A root and eight children of one department, varied in skills, phone keys, floors and contacts.</summary>
    private async Task<Seeded> SeedAsync()
    {
        await Redb.SyncSchemeAsync<EmployeeProps>();
        var tag = "cf-" + Guid.NewGuid().ToString("N")[..8];

        var root = new TreeRedbObject<EmployeeProps>
        {
            name = $"{tag}-root",
            Props = TestDataFactory.CreateEmployee(0, department: $"{tag}-root", salary: 90_000m).Props
        };
        root.id = await Redb.SaveAsync(root);

        var children = new List<EmployeeProps>();
        for (var i = 1; i <= 8; i++)
        {
            var p = TestDataFactory.CreateEmployee(i, firstName: $"{tag}-n{i}", department: tag, salary: 1000m * i,
                position: i % 2 == 0 ? "Even" : "Odd").Props;
            p.Skills = i % 3 == 0 ? ["sql", "x"] : ["x"];
            p.PhoneDirectory = i % 2 == 0 ? new Dictionary<string, string> { ["desk"] = $"{i}" } : new Dictionary<string, string> { ["home"] = $"{i}" };
            p.HomeAddress!.Building!.Floor = i;
            // Email verified, phone not: the shape every seeded employee has, which ArrayGroupByDeepTestsBase counts on
            // over the shared database.
            p.Contacts =
            [
                new TestContact { Type = i % 2 == 0 ? "email" : "phone", IsVerified = i % 2 == 0, Value = $"{tag}-{i}-a" },
                new TestContact { Type = "email", IsVerified = true, Value = $"{tag}-{i}-b" }
            ];
            await Redb.CreateChildAsync(new TreeRedbObject<EmployeeProps> { name = $"{tag}-c{i}", Props = p }, root);
            children.Add(p);
        }

        return new Seeded(root.id, tag, children);
    }

    /// <summary>The filter of each case, always within the seeded department so other tests' data stays out.</summary>
    private static Expression<Func<EmployeeProps, bool>> Filter(string kind, string tag)
    {
        var narrowName = tag + "-n4";
        return kind switch
    {
        "nested" => e => e.Department == tag && e.HomeAddress!.Building!.Floor > 4,
        "array" => e => e.Department == tag && e.Skills!.Contains("sql"),
        "dict" => e => e.Department == tag && e.PhoneDirectory!.ContainsKey("desk"),
        "none" => e => e.Department == tag && e.Skills!.Contains("absent"),
        "narrow" => e => e.Department == tag && e.FirstName == narrowName && e.Skills!.Contains("x"),
        _ => throw new ArgumentOutOfRangeException(nameof(kind))
    };
    }

    public static IEnumerable<object[]> Kinds() => new[] { "nested", "array", "dict", "none", "narrow" }.Select(k => new object[] { k });

    private IRedbQueryable<EmployeeProps> Source(Seeded s, bool tree, string kind)
        => (tree ? Redb.TreeQuery<EmployeeProps>(s.RootId) : Redb.Query<EmployeeProps>()).Where(Filter(kind, s.Tag));

    private static List<EmployeeProps> Expected(Seeded s, string kind)
        => s.Children.Where(Filter(kind, s.Tag).Compile()).ToList();

    [Theory]
    [MemberData(nameof(Kinds))]
    public Task Flat_EveryForm_MatchesLinq(string kind) => EveryFormAsync(kind, tree: false);

    [Theory]
    [MemberData(nameof(Kinds))]
    public Task Tree_EveryForm_MatchesLinq(string kind) => EveryFormAsync(kind, tree: true);

    private async Task EveryFormAsync(string kind, bool tree)
    {
        var s = await SeedAsync();
        var expected = Expected(s, kind);
        var because = $"{(tree ? "tree" : "flat")} {kind}";
        if (kind == "none") expected.Should().BeEmpty("precondition");
        if (kind == "narrow") expected.Should().HaveCount(1, "precondition");

        // Single aggregate and AggregateAsync.
        (await Source(s, tree, kind).SumAsync(e => e.Salary)).Should().Be(expected.Sum(e => e.Salary), because);
        var batch = await Source(s, tree, kind).AggregateAsync(x => new { Count = Agg.Count(), Sum = Agg.Sum(x.Props.Salary) });
        batch.Count.Should().Be(expected.Count, because);
        batch.Sum.Should().Be(expected.Sum(e => e.Salary), because);

        // GroupBy.
        var groups = await Source(s, tree, kind).GroupBy(e => e.Position)
            .SelectAsync(g => new { Key = g.Key, Count = Agg.Count(g), Sum = Agg.Sum(g, e => e.Salary) });
        groups.Select(g => (g.Key, g.Count, g.Sum)).Should().BeEquivalentTo(
            expected.GroupBy(e => e.Position).Select(g => (g.Key, g.Count(), g.Sum(e => e.Salary))), because);

        // Window over the objects.
        var rows = await Source(s, tree, kind).WithWindow(w => w.OrderBy(e => e.Salary))
            .SelectAsync(e => new { e.Props.Salary, Row = Win.RowNumber() });
        rows.Select(r => r.Salary).Should().BeEquivalentTo(expected.Select(e => e.Salary), because);
        rows.Select(r => r.Row).Should().BeEquivalentTo(Enumerable.Range(1, expected.Count).Select(i => (long)i), because);

        // Window over the groups.
        var ranked = await Source(s, tree, kind).GroupBy(e => e.Position).WithWindow(w => w.OrderBy(g => g.Key))
            .SelectAsync(g => new { Key = g.Key, Count = Agg.Count(g), Row = Win.RowNumber() });
        ranked.Select(g => (g.Key, g.Count)).Should().BeEquivalentTo(
            expected.GroupBy(e => e.Position).Select(g => (g.Key, g.Count())), because);

        // GroupByArray over the contacts.
        var types = await Source(s, tree, kind).GroupByArray(e => e.Contacts!, c => c.Type)
            .SelectAsync(g => new { Key = g.Key, Count = Agg.Count(g) });
        types.Select(g => (g.Key, g.Count)).Should().BeEquivalentTo(
            expected.SelectMany(e => e.Contacts!).GroupBy(c => c.Type).Select(g => (g.Key, g.Count())), because);

        // The list and the count of the same filter.
        (await Source(s, tree, kind).CountAsync()).Should().Be(expected.Count, because);
        (await Source(s, tree, kind).ToListAsync()).Select(o => o.Props.FirstName)
            .Should().BeEquivalentTo(expected.Select(e => e.FirstName), because);
    }

    [Fact]
    public async Task GroupByArray_NotNullOfAMemberOnThePath_IsImplied()
    {
        // "Contacts != null" asks nothing of a grouping over Contacts.Metadata: an element there means Contacts is set.
        var s = await SeedAsync();
        var rows = await Redb.Query<EmployeeProps>()
            .Where(e => e.Department == s.Tag && e.Contacts != null && e.HomeAddress!.Building!.Floor > 4)
            .GroupByArray(e => e.Contacts!, c => c.Type)
            .SelectAsync(g => new { Key = g.Key, Count = Agg.Count(g) });

        rows.Select(g => (g.Key, g.Count)).Should().BeEquivalentTo(
            s.Children.Where(e => e.HomeAddress!.Building!.Floor > 4).SelectMany(e => e.Contacts!)
                .GroupBy(c => c.Type).Select(g => (g.Key, g.Count())));
    }
}
