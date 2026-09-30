using redb.Core;
using redb.Core.Query;
using redb.Core.Query.Aggregation;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// The aggregate SQL preview shows the query that runs. The Pro previews built it with no filter while
/// AggregateAsync passed the query's Where: the preview of a filtered sum showed a sum over the whole scheme.
/// </summary>
public abstract class ProAggregatePreviewTestsBase
{
    protected readonly IRedbService Redb;

    protected ProAggregatePreviewTestsBase(IRedbService redb) => Redb = redb;

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
