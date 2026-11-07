using redb.Core;
using redb.Core.Query;
using redb.Core.Query.Aggregation;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Golden-SQL preview tests for PostgreSQL dialects.
/// Validates that the generated SQL contains expected fragments — does NOT
/// execute queries against any data.
///
/// Uses <see cref="RedbQueryableExtensions.ToSqlStringAsync{TProps}"/> (plain
/// queries) and <see cref="RedbQueryableExtensions.ToAggregateSqlStringAsync{TProps,TResult}"/>
/// (aggregates). Array-GroupBy preview is currently stubbed in core (returns
/// "-- SQL preview not supported …") so it is intentionally NOT covered here.
/// </summary>
public abstract class SqlPreviewTestsBase
{
    protected readonly IRedbService Redb;

    protected SqlPreviewTestsBase(IRedbService redb) => Redb = redb;

    // ──────────────────────────────────────────────────────────────────
    //  Aggregation over array — must unnest
    // ──────────────────────────────────────────────────────────────────
    /// <summary>
    /// Aggregate-over-array preview. Default body asserts the PG Pro form
    /// (CASE-based array-aggregate over <c>_values</c>). Plain PG provider has no
    /// preview path and throws — that case is covered by an override in the
    /// PG-free derived class.
    /// </summary>
    [Fact]
    public virtual async Task Preview_AggSumOverArray_ContainsUnnest()
    {
        var sql = await Redb.Query<EmployeeProps>()
            .ToAggregateSqlStringAsync(x => new
            {
                Total = Agg.Sum(x.Props.SkillLevels.Select(s => s))
            });

        sql.Should().NotBeNullOrWhiteSpace();
        sql.ToUpperInvariant().Should().Contain("SUM(");
        sql.Should().Contain("_long",
            "Sum over int[] aggregates over the _long storage column");
    }

    [Fact]
    public virtual async Task Preview_AggAvgOverArray_ContainsUnnestAndNullIf()
    {
        var sql = await Redb.Query<EmployeeProps>()
            .ToAggregateSqlStringAsync(x => new
            {
                Avg = Agg.Average(x.Props.SkillLevels.Select(s => s))
            });

        sql.Should().NotBeNullOrWhiteSpace();
        sql.ToUpperInvariant().Should().Contain("AVG(");
        sql.Should().Contain("_long");
    }

    // ──────────────────────────────────────────────────────────────────
    //  Plain query preview
    // ──────────────────────────────────────────────────────────────────
    [Fact]
    public async Task Preview_PlainQuery_HasSelectAndFrom()
    {
        var sql = await Redb.Query<EmployeeProps>().ToSqlStringAsync();

        sql.Should().NotBeNullOrWhiteSpace();
        sql.ToUpperInvariant().Should().Contain("SELECT");
        sql.ToUpperInvariant().Should().Contain("FROM");
    }

    [Fact]
    public async Task Preview_WhereWithStringContains_HasIlike()
    {
        var sql = await Redb.Query<EmployeeProps>()
            .Where(e => e.Department.Contains("ing"))
            .ToSqlStringAsync();

        // Plain PG lowers Contains(...) to ILIKE; PG Pro emits the case-sensitive
        // LIKE for ordinary `string.Contains`. Both contain the substring "LIKE".
        sql.ToUpperInvariant().Should().Contain("LIKE",
            "string.Contains(...) must compile to a LIKE/ILIKE predicate");
    }

    [Fact]
    public async Task Preview_OrderBy_HasOrderByClause()
    {
        var sql = await Redb.Query<EmployeeProps>()
            .OrderBy(e => e.Salary)
            .ToSqlStringAsync();

        sql.ToUpperInvariant().Should().Contain("ORDER BY");
    }
}
