using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Sort employees in descending order using OrderByDescending.
/// Get top earners and most experienced employees.
/// </summary>
[ExampleMeta("E083", "OrderByDescending - Reverse Sort", "Query",
    ExampleTier.Free, 83, "OrderByDescending", "Sort", "Descending", RelatedApis = ["IRedbQueryable.OrderByDescending"])]
public class E083_OrderByDescending : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Top earners (highest salary first)
        var query = redb.Query<EmployeeProps>()
            .OrderByDescending(e => e.Salary)
            .Take(5);
        
        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);
        
        var topEarners = await query.ToListAsync();

        // Oldest employees (highest age first)
        var oldest = await redb.Query<EmployeeProps>()
            .OrderByDescending(e => e.Age)
            .Take(5)
            .ToListAsync();

        sw.Stop();

        var topSalary = topEarners.FirstOrDefault()?.Props.Salary ?? 0;
        var oldestAge = oldest.FirstOrDefault()?.Props.Age ?? 0;

        return Ok("E083", "OrderByDescending - Reverse Sort", ExampleTier.Free, sw.ElapsedMilliseconds, 
            topEarners.Count + oldest.Count,
            [$"Top salary: ${topSalary:N0}", $"Oldest age: {oldestAge}"]);
    }
}
