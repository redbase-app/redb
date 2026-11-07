using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Demonstrates arithmetic multiplication in Where clause.
/// Calculates annual salary (monthly * 12) and filters employees earning over 1M per year.
/// Pro feature: server-side arithmetic expressions.
/// </summary>
[ExampleMeta("E151", "Where - Arithmetic Multiply", "Query",
    ExampleTier.Pro, 151, "Arithmetic", "Multiply", "Expression", RelatedApis = ["IRedbQueryable.Where"])]
public class E151_ArithmeticMultiply : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Find employees with annual salary > 1,000,000
        // Server-side: Salary * 12 > 1000000
        var query = redb.Query<EmployeeProps>()
            .Where(e => e.Salary * 12 > 1_000_000m)
            .Take(100);

        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var highEarners = await query.ToListAsync();
        var totalCount = await redb.Query<EmployeeProps>()
            .Where(e => e.Salary * 12 > 1_000_000m)
            .CountAsync();

        sw.Stop();

        var first = highEarners.FirstOrDefault();
        var annualSalary = first?.Props.Salary * 12 ?? 0;

        return Ok("E151", "Where - Arithmetic Multiply", ExampleTier.Pro, sw.ElapsedMilliseconds, totalCount,
            [$"Filter: Salary * 12 > 1,000,000",
             $"Found: {totalCount} high earners",
             $"First: {first?.Props.FirstName} {first?.Props.LastName}, Annual: ${annualSalary:N0}"]);
    }
}
