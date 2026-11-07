using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Demonstrates nested Math functions with arithmetic in Where clause.
/// Finds employees with salary close to $80,000 (within $10,000 range).
/// Pro feature: server-side ABS() with arithmetic expression inside.
/// </summary>
[ExampleMeta("E159", "Where - Nested Math", "Query",
    ExampleTier.Pro, 159, "Math", "Nested", "Range", RelatedApis = ["IRedbQueryable.Where", "Math.Abs"])]
public class E159_NestedMath : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Find employees with salary in range $70,000 - $90,000
        // |Salary - 80000| < 10000 means Salary is between 70k and 90k
        var query = redb.Query<EmployeeProps>()
            .Where(e => Math.Abs(e.Salary - 80000m) < 10000m)
            .Take(100);

        // Uncomment to see generated SQL (contains ABS with expression):
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var results = await query.ToListAsync();
        var totalCount = await redb.Query<EmployeeProps>()
            .Where(e => Math.Abs(e.Salary - 80000m) < 10000m)
            .CountAsync();

        sw.Stop();

        var salaries = results.Take(5).Select(e => $"${e.Props.Salary:N0}");
        var avgSalary = results.Any() ? results.Average(e => e.Props.Salary) : 0;

        return Ok("E159", "Where - Nested Math", ExampleTier.Pro, sw.ElapsedMilliseconds, totalCount,
            [$"Filter: |Salary - 80k| < 10k ($70k-$90k range)",
             $"Found: {totalCount} employees",
             $"Salaries: {string.Join(", ", salaries)}"]);
    }
}
