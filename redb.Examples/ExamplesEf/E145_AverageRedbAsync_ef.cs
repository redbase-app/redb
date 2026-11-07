using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E145_AverageRedbAsync"/>: AVG, MIN and MAX of the employee keys, three queries as in redb.</summary>
[ExampleMeta("E145_ef", "EF: AverageRedbAsync - Base Field Avg", "Analytics", ExampleTier.Free, 1, "EF", "Average")]
public class E145_AverageRedbAsync_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var avgId = await db.EmployeeProps.AverageAsync(e => e._id);
        var minId = await db.EmployeeProps.MinAsync(e => (long?)e._id);
        var maxId = await db.EmployeeProps.MaxAsync(e => (long?)e._id);

        sw.Stop();

        return Ok("E145_ef", "EF: AverageRedbAsync - Base Field Avg", ExampleTier.Free, sw.ElapsedMilliseconds, 1,
            [$"AVG(_id) = {avgId:F2}",
             $"Range: {minId?.ToString() ?? "N/A"} - {maxId?.ToString() ?? "N/A"}"]);
    }
}
