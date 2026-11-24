using redb.Core;
using redb.Core.Query;
using redb.Core.Query.Aggregation;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// The aggregate SQL preview shows the query that runs. The Pro previews built it with no filter while
/// AggregateAsync passed the query's Where: the preview of a filtered sum showed a sum over the whole scheme. Free asked
/// for aggregate_batch_preview, a function of the removed legacy module, and threw NotSupportedException.
/// </summary>
public abstract class AggregatePreviewTestsBase
{
    protected readonly IRedbService Redb;

    protected AggregatePreviewTestsBase(IRedbService redb) => Redb = redb;

    [Fact]
    public async Task AggregatePreview_CarriesTheWhere()
    {
        await Redb.SyncSchemeAsync<EmployeeProps>();
        var department = $"preview-{Guid.NewGuid():N}";

        var sql = await Redb.Query<EmployeeProps>()
            .Where(e => e.Department == department && e.Salary > 1m)
            .ToAggregateSqlStringAsync(x => new { Total = Agg.Sum(x.Props.Salary), Headcount = Agg.Count() });

        sql.Should().Contain(department, "the preview must carry the Where the aggregate runs with");
    }
}

/// <summary>
/// On Free the preview is the statement the aggregate runs: pvt_build_aggregate_sql compiles it with every value inline,
/// so it runs as it is and gives what AggregateAsync returns.
/// </summary>
public abstract class FreeAggregatePreviewTestsBase : AggregatePreviewTestsBase
{
    protected FreeAggregatePreviewTestsBase(IRedbService redb) : base(redb) { }

    [Fact]
    public async Task AggregatePreview_RunAsItIs_GivesWhatTheAggregateReturns()
    {
        await Redb.SyncSchemeAsync<EmployeeProps>();
        var department = $"preview-{Guid.NewGuid():N}";
        await Redb.SaveAsync(Enumerable.Range(1, 3)
            .Select(i => TestDataFactory.CreateEmployee(i, salary: 100m * i, department: department)).ToList());
        IRedbQueryable<EmployeeProps> Tagged() => Redb.Query<EmployeeProps>().Where(e => e.Department == department);

        var aggregate = await Tagged().AggregateAsync(x => new { Total = Agg.Sum(x.Props.Salary) });
        var sql = await Tagged().ToAggregateSqlStringAsync(x => new { Total = Agg.Sum(x.Props.Salary) });

        aggregate.Total.Should().Be(600m);
        (await Redb.Context.ExecuteScalarAsync<decimal>(sql)).Should().Be(aggregate.Total, "the preview is the statement that runs");
    }
}

/// <summary>
/// The Pro preview reads the selector with the parser the execution uses. Its own parser took a dictionary's .Values
/// and an indexer key for nothing, and the preview showed SELECT 1 AS "_empty" for an aggregate that runs.
/// </summary>
public abstract class ProAggregatePreviewTestsBase : AggregatePreviewTestsBase
{
    protected ProAggregatePreviewTestsBase(IRedbService redb) : base(redb) { }

    [Fact]
    public async Task AggregatePreview_DictionaryValues_AggregatesTheValues()
    {
        await Redb.SyncSchemeAsync<EmployeeProps>();

        var sql = await Redb.Query<EmployeeProps>()
            .ToAggregateSqlStringAsync(x => new { Total = Agg.Sum(x.Props.BonusByYear!.Values) });

        sql.ToUpperInvariant().Should().Contain("SUM(");
        sql.Should().NotContain("_empty", "the values of the dictionary are aggregated");
    }

    [Fact]
    public async Task AggregatePreview_DictionaryKey_AggregatesThatKey()
    {
        await Redb.SyncSchemeAsync<EmployeeProps>();

        var sql = await Redb.Query<EmployeeProps>()
            .ToAggregateSqlStringAsync(x => new { Bonus = Agg.Sum(x.Props.BonusByYear![2023]) });

        sql.Should().ContainEquivalentOf("_array_index", "the key selects one entry of the dictionary");
        sql.Should().Contain("2023");
        sql.Should().NotContain("_empty");
    }
}
