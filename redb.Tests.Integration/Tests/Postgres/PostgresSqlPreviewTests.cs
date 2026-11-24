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

    // The Free module averages the elements of an array as their sum over their count: it unnests the array and
    // guards the count with NULLIF, where Pro writes AVG over the element rows.
    public override async Task Preview_AggAvgOverArray_ContainsUnnestAndNullIf()
    {
        var sql = await Redb.Query<EmployeeProps>()
            .ToAggregateSqlStringAsync(x => new
            {
                Avg = Agg.Average(x.Props.SkillLevels.Select(s => s))
            });

        sql.ToUpperInvariant().Should().Contain("UNNEST(").And.Contain("NULLIF(");
        sql.Should().ContainEquivalentOf("_long");
    }
}
