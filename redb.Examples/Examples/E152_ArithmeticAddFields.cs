using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Demonstrates arithmetic operations combining multiple fields in Where clause.
/// Creates a "scoring" formula using Age and Salary to find senior valuable employees.
/// Pro feature: server-side arithmetic with multiple Props fields.
/// </summary>
[ExampleMeta("E152", "Where - Arithmetic Add Fields", "Query",
    ExampleTier.Pro, 152, "Arithmetic", "Add", "MultiField", RelatedApis = ["IRedbQueryable.Where"])]
public class E152_ArithmeticAddFields : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Combined scoring: Age * 1000 + Salary > 120000
        // Example: Age 40, Salary 85000 -> 40*1000 + 85000 = 125000 > 120000 ✓
        var query = redb.Query<EmployeeProps>()
            .Where(e => e.Age * 1000 + e.Salary > 120_000m)
            .Take(100);

        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var results = await query.ToListAsync();
        var totalCount = await redb.Query<EmployeeProps>()
            .Where(e => e.Age * 1000 + e.Salary > 120_000m)
            .CountAsync();

        sw.Stop();

        var first = results.FirstOrDefault();
        var score = first != null ? first.Props.Age * 1000 + (int)first.Props.Salary : 0;

        return Ok("E152", "Where - Arithmetic Add Fields", ExampleTier.Pro, sw.ElapsedMilliseconds, totalCount,
            [$"Formula: Age * 1000 + Salary > 120,000",
             $"Found: {totalCount} employees",
             $"First: {first?.Props.FirstName}, Age {first?.Props.Age}, Score: {score:N0}"]);
    }
}
