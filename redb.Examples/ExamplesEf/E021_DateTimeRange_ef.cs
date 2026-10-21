using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E021_DateTimeRange"/>: hired between three and one year ago, up to 100 whole employees.
/// <para>OrderBy(_id) is EF's own need, the redb query has none: with split queries and Take, every child query must pick the same employees.</para>
/// <para>The date is marked UTC: redb stores a DateTime as a UTC clock reading, and Npgsql refuses a local DateTime for a timestamptz column.</para>
/// </summary>
[ExampleMeta("E021_ef", "EF: Where - DateTime Range", "Query", ExampleTier.Free, 1, "EF")]
public class E021_DateTimeRange_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();
        var startDate = DateTime.SpecifyKind(DateTime.Today.AddYears(-3), DateTimeKind.Utc);
        var endDate = DateTime.SpecifyKind(DateTime.Today.AddYears(-1), DateTimeKind.Utc);

        var result = await db.EmployeeProps.WithGraph()
            .Where(e => e.HireDate >= startDate && e.HireDate < endDate)
            .OrderBy(e => e._id)
            .Take(100)
            .ToBusinessListAsync();

        sw.Stop();

        return Ok("E021_ef", "EF: Where - DateTime Range", ExampleTier.Free, sw.ElapsedMilliseconds, result.Count,
            ["Filter: HireDate in [start, end)"]);
    }
}