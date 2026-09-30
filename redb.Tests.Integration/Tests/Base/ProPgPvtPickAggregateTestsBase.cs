using System.Text.RegularExpressions;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Core.Query;
using redb.Core.Query.Aggregation;
using redb.Core.Query.Window;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// PostgreSQL Pro pivot: the value of a scalar field is taken with the cheapest aggregate its column type allows.
///
/// <c>(array_agg(x) FILTER (...))[1]</c> builds an array per object and per field, which keeps the planner on a
/// sorted GroupAggregate. <c>max(x) FILTER (...)</c> lets it hash-aggregate instead: on 200 000 objects the whole
/// E015 page went from 1 106-1 367 ms to 838-975 ms (docs/v42/PVT_LEAD_FORM.md). The FILTER leaves one row per
/// object for a scalar field, so both return the same value.
///
/// PostgreSQL 14 has no max() for boolean, uuid or bytea: boolean takes bool_or (the same value on one row),
/// uuid and bytea keep array_agg. The second half of the class runs such queries end to end, so a max() over a
/// column that has none fails here and not in production.
///
/// Wrap this base in a Pro PG fixture only.
/// </summary>
public abstract class ProPgPvtPickAggregateTestsBase
{
    // array_agg(...)[1] over a column that has max() or bool_or(): the shape this class forbids.
    private static readonly Regex ArrayAggPickOverCheapColumn = new(
        @"\(array_agg\((?:\w+\.)?_(?:string|long|double|numeric|datetimeoffset|listitem|object|boolean)\)\s*FILTER\s*\(WHERE[^)]*\)\)\[1\]",
        RegexOptions.Compiled | RegexOptions.IgnoreCase);

    protected readonly IRedbService Redb;

    protected ProPgPvtPickAggregateTestsBase(IRedbService redb) => Redb = redb;

    private static void AssertCheapPick(string sql, string shape)
    {
        sql.Should().Contain("FILTER (WHERE", $"precondition: the {shape} SQL pivots Props fields");
        ArrayAggPickOverCheapColumn.Matches(sql).Select(m => m.Value).Should().BeEmpty(
            $"the {shape} pivot must take a scalar with max()/bool_or(), not array_agg(...)[1]");
    }

    // ── SQL text ────────────────────────────────────────────────────────────

    [Fact]
    public async Task FlatQuery_Pivot_UsesMaxAndBoolOr()
    {
        var sql = await Redb.Query<EmployeeProps>()
            .Where(e => e.Salary > 50_000m && e.Age >= 30 && e.Department != "" && e.IsRemote)
            .OrderByDescending(e => e.Salary)
            .Take(10)
            .ToSqlStringAsync();

        AssertCheapPick(sql, "flat query");
        sql.Should().Contain("max(").And.Contain("bool_or(");
    }

    [Fact]
    public async Task Aggregate_Pivot_UsesMax()
    {
        var sql = await Redb.Query<EmployeeProps>()
            .Where(e => e.Department == "Engineering" && e.IsRemote)
            .ToAggregateSqlStringAsync(x => new { Total = Agg.Sum(x.Props.Salary), Headcount = Agg.Count() });

        AssertCheapPick(sql, "aggregate");
    }

    [Fact]
    public async Task Grouping_Pivot_UsesMax()
    {
        var sql = await Redb.Query<EmployeeProps>()
            .Where(e => e.Age >= 30)
            .GroupBy(e => e.Department)
            .ToSqlStringAsync(g => new { Department = g.Key, Total = Agg.Sum(g, x => x.Salary) });

        AssertCheapPick(sql, "grouping");
    }

    [Fact]
    public async Task Window_Pivot_UsesMax()
    {
        var sql = await Redb.Query<EmployeeProps>()
            .Where(e => e.Age >= 30)
            .WithWindow(w => w.PartitionBy(e => e.Department).OrderByDesc(e => e.Salary))
            .ToSqlStringAsync(e => new { e.Props.Salary, RowNum = Win.RowNumber() });

        AssertCheapPick(sql, "window");
    }

    [Fact]
    public async Task TreeQuery_Pivot_UsesMax()
    {
        var sql = await Redb.TreeQuery<EmployeeProps>()
            .Where(e => e.Salary > 50_000m && e.IsRemote)
            .OrderBy(e => e.Age)
            .ToSqlStringAsync();

        AssertCheapPick(sql, "tree query");
    }

    [Fact]
    public async Task GuidField_KeepsArrayAgg_BoolField_UsesBoolOr()
    {
        var code = Guid.NewGuid();
        var sql = await Redb.Query<SimpleProps>()
            .Where(x => x.Code == code && x.IsActive)
            .OrderBy(x => x.Price)
            .ToSqlStringAsync();

        AssertCheapPick(sql, "flat query with a Guid field");
        sql.Should().MatchRegex(@"(?i)\(array_agg\((?:\w+\.)?_guid\)", "PostgreSQL 14 has no max(uuid)");
        sql.Should().Contain("bool_or(", "PostgreSQL has no max(boolean)");
    }

    // ── Execution: types without max() must still run ──────────────────────

    private async Task<string> SeedSimpleAsync()
    {
        await Redb.SyncSchemeAsync<SimpleProps>();
        var tag = $"pick-{Guid.NewGuid():N}"[..13];
        for (var i = 0; i < 4; i++)
        {
            await Redb.SaveAsync(new RedbObject<SimpleProps>
            {
                name = $"{tag}-{i}",
                Props = new SimpleProps
                {
                    Title = tag,
                    Count = i,
                    Price = 10m * (i + 1),
                    CreatedAt = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddDays(i),
                    IsActive = i % 2 == 0,
                    Code = Guid.NewGuid()
                }
            });
        }
        return tag;
    }

    [Fact]
    public async Task FlatQuery_FiltersAndSortsOnBoolAndGuid()
    {
        var tag = await SeedSimpleAsync();

        var rows = await Redb.Query<SimpleProps>()
            .Where(x => x.Title == tag && x.IsActive && x.Code != Guid.Empty)
            .OrderByDescending(x => x.Price)
            .ToListAsync();

        rows.Select(r => r.Props.Price).Should().Equal(30m, 10m);
    }

    [Fact]
    public async Task Grouping_ByBool_WithGuidInFilter()
    {
        var tag = await SeedSimpleAsync();

        var rows = await Redb.Query<SimpleProps>()
            .Where(x => x.Title == tag && x.Code != Guid.Empty)
            .GroupBy(x => x.IsActive)
            .SelectAsync(g => new { Active = g.Key, Count = Agg.Count(g), Total = Agg.Sum(g, x => x.Price) });

        rows.Should().HaveCount(2);
        rows.Single(r => r.Active).Total.Should().Be(40m);
        rows.Single(r => !r.Active).Total.Should().Be(60m);
    }

    [Fact]
    public async Task Window_PartitionByBool_WithGuidInFilter()
    {
        var tag = await SeedSimpleAsync();

        var rows = await Redb.Query<SimpleProps>()
            .Where(x => x.Title == tag && x.Code != Guid.Empty)
            .WithWindow(w => w.PartitionBy(x => x.IsActive).OrderByDesc(x => x.Price))
            .SelectAsync(x => new { x.Props.Price, RowNum = Win.RowNumber() });

        rows.Should().HaveCount(4);
        rows.Where(r => r.RowNum == 1).Select(r => r.Price).Should().BeEquivalentTo(new[] { 30m, 40m });
    }

    [Fact]
    public async Task Aggregate_WithBoolAndGuidInFilter()
    {
        var tag = await SeedSimpleAsync();

        var total = await Redb.Query<SimpleProps>()
            .Where(x => x.Title == tag && x.IsActive && x.Code != Guid.Empty)
            .SumAsync(x => x.Price);

        total.Should().Be(40m);
    }

    [Fact]
    public async Task TreeQuery_FiltersOnBool()
    {
        await Redb.SyncSchemeAsync<TreeNodeProps>();
        var tag = $"pick-{Guid.NewGuid():N}"[..13];
        var root = new TreeRedbObject<TreeNodeProps> { name = $"{tag}-root", Props = new TreeNodeProps { Name = tag, IsActive = true, Budget = 1m } };
        root.id = await Redb.SaveAsync(root);
        for (var i = 0; i < 3; i++)
            await Redb.CreateChildAsync(new TreeRedbObject<TreeNodeProps>
            {
                name = $"{tag}-{i}",
                Props = new TreeNodeProps { Name = tag, IsActive = i != 1, Budget = 10m * (i + 1) }
            }, root);

        var rows = await Redb.TreeQuery<TreeNodeProps>(root.id)
            .Where(x => x.IsActive && x.Budget > 5m)
            .OrderByDescending(x => x.Budget)
            .ToListAsync();

        rows.Select(r => r.Props.Budget).Should().Equal(30m, 10m);
    }
}
