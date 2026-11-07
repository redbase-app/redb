using redb.Core.Query;
using redb.Core.Query.Aggregation;
using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Models;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

[Collection("Postgres")]
public class PostgresSqlPreviewTests : SqlPreviewTestsBase
{
    public PostgresSqlPreviewTests(PostgresFixture fixture) : base(fixture.Redb) { }

    // Plain PG provider has the legacy aggregate-preview SQL path removed
    // (see PostgreSqlDialect.Query_AggregateBatchPreviewSql). The wrapper in
    // ToAggregateSqlStringAsync re-throws NotSupportedException unchanged.
    public override async Task Preview_AggSumOverArray_ContainsUnnest()
    {
        var act = async () => await Redb.Query<EmployeeProps>()
            .ToAggregateSqlStringAsync(x => new
            {
                Total = Agg.Sum(x.Props.SkillLevels.Select(s => s))
            });

        await act.Should().ThrowAsync<System.NotSupportedException>()
            .WithMessage("*Legacy PostgreSQL SQL path is removed*");
    }

    public override async Task Preview_AggAvgOverArray_ContainsUnnestAndNullIf()
    {
        var act = async () => await Redb.Query<EmployeeProps>()
            .ToAggregateSqlStringAsync(x => new
            {
                Avg = Agg.Average(x.Props.SkillLevels.Select(s => s))
            });

        await act.Should().ThrowAsync<System.NotSupportedException>()
            .WithMessage("*Legacy PostgreSQL SQL path is removed*");
    }
}
