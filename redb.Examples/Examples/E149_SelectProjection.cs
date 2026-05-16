using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Select projection - load only specific fields instead of full object.
/// Reduces data transfer and improves performance.
/// </summary>
[ExampleMeta("E149", "Select - Projection", "Query",
    ExampleTier.Free, 149, "Select", "Projection", "Performance", RelatedApis = ["IRedbQueryable.Select"])]
public class E149_SelectProjection : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Project only 3 fields from 20+ available
        var projected = await redb.Query<EmployeeProps>()
            .Take(100)
            .Select(x => new
            {
                x.id,
                x.Props.FirstName,
                x.Props.Salary
            })
            .ToListAsync();

        // Compare with full load
        var swFull = Stopwatch.StartNew();
        var full = await redb.Query<EmployeeProps>().Take(100).ToListAsync();
        swFull.Stop();

        sw.Stop();

        var first = projected.FirstOrDefault();

        return Ok("E149", "Select - Projection", ExampleTier.Free, sw.ElapsedMilliseconds, projected.Count,
            [$"Projected 3 fields: {sw.ElapsedMilliseconds}ms",
             $"Full load 20+ fields: {swFull.ElapsedMilliseconds}ms",
             first != null ? $"First: {first.FirstName} ${first.Salary:N0}" : "No data"]);
    }
}
