using System.Text.RegularExpressions;
using redb.Core;
using redb.Core.Query;
using redb.Core.Query.Aggregation;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// PostgreSQL Pro purity tests: assert that inline SQL emitted by Pro PG provider
/// does not delegate to legacy <c>pvt_*</c> server-side composer functions
/// (audit 2026-05-24). The planner must see real parameterised SQL, not
/// <c>SELECT pvt_search_objects_with_facets(...)</c>.
///
/// Targeted leaks (regex <c>pvt_[a-z_]+\(</c>):
///   * pvt_search_objects_with_facets
///   * pvt_aggregate_grouped
///   * pvt_aggregate_array_grouped
///   * pvt_build_query_sql
///   * pvt_build_array_groupby_sql
///   * pvt_query_with_window
///
/// Wrap this base in a Pro PG fixture only.
/// </summary>
public abstract class ProPgPurityTestsBase
{
    private static readonly Regex PvtFunctionCall =
        new(@"\bpvt_[a-z_]+\s*\(", RegexOptions.Compiled | RegexOptions.IgnoreCase);

    protected readonly IRedbService Redb;

    protected ProPgPurityTestsBase(IRedbService redb) => Redb = redb;

    private static void AssertNoLegacyPvtCalls(string sql, string queryDescription)
    {
        sql.Should().NotBeNullOrWhiteSpace($"SQL preview for {queryDescription} must be non-empty");
        var matches = PvtFunctionCall.Matches(sql)
            .Select(m => m.Value)
            .Distinct()
            .ToArray();
        matches.Should().BeEmpty(
            $"Pro PG must inline parameterised SQL for {queryDescription}; found legacy " +
            $"pvt_* function calls: {string.Join(", ", matches)}");
    }

    [Fact]
    public async Task NoLegacyPvtCalls_PlainQuery()
    {
        var sql = await Redb.Query<EmployeeProps>()
            .Where(e => e.IsRemote && e.Salary > 50_000m)
            .ToSqlStringAsync();
        AssertNoLegacyPvtCalls(sql, "plain WHERE query");
    }

    [Fact]
    public async Task NoLegacyPvtCalls_OrderBySkipTake()
    {
        var sql = await Redb.Query<EmployeeProps>()
            .OrderBy(e => e.Salary)
            .Skip(5)
            .Take(10)
            .ToSqlStringAsync();
        AssertNoLegacyPvtCalls(sql, "OrderBy + Skip/Take");
    }

    [Fact]
    public async Task NoLegacyPvtCalls_AggregateOverArray()
    {
        var sql = await Redb.Query<EmployeeProps>()
            .ToAggregateSqlStringAsync(x => new
            {
                Total = Agg.Sum(x.Props.SkillLevels.Select(s => s)),
                Avg   = Agg.Average(x.Props.SkillLevels.Select(s => s))
            });
        AssertNoLegacyPvtCalls(sql, "aggregate over array");
    }

    [Fact]
    public async Task NoLegacyPvtCalls_AggregateScalarWithFilter()
    {
        var sql = await Redb.Query<EmployeeProps>()
            .Where(e => e.Department == "Engineering")
            .ToAggregateSqlStringAsync(x => new
            {
                TotalSalary = Agg.Sum(x.Props.Salary),
                Headcount   = Agg.Count()
            });
        AssertNoLegacyPvtCalls(sql, "scalar aggregate with WHERE");
    }
}
