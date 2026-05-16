using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Demonstrates Math.Abs() in Where clause for range filtering around a target value.
/// Finds employees aged 30-40 (within 5 years of 35).
/// Pro feature: server-side ABS() function.
/// </summary>
[ExampleMeta("E156", "Where - Math.Abs", "Query",
    ExampleTier.Pro, 156, "Math", "Abs", "Range", RelatedApis = ["IRedbQueryable.Where", "Math.Abs"])]
public class E156_MathAbs : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Find employees aged 30-40 (within 5 years of 35)
        // |Age - 35| <= 5 means Age is between 30 and 40
        var query = redb.Query<EmployeeProps>()
            .Where(e => Math.Abs(e.Age - 35) <= 5)
            .Take(100);

        // Uncomment to see generated SQL (contains ABS):
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var results = await query.ToListAsync();
        var totalCount = await redb.Query<EmployeeProps>()
            .Where(e => Math.Abs(e.Age - 35) <= 5)
            .CountAsync();

        sw.Stop();

        var ages = results.Take(5).Select(e => e.Props.Age);
        var avgAge = results.Any() ? results.Average(e => e.Props.Age) : 0;

        return Ok("E156", "Where - Math.Abs", ExampleTier.Pro, sw.ElapsedMilliseconds, totalCount,
            [$"Filter: |Age - 35| <= 5 (ages 30-40)",
             $"Found: {totalCount} employees",
             $"Ages: {string.Join(", ", ages)}, Avg: {avgAge:F1}"]);
    }
}
