using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Project specific fields using Select (server-side projection).
/// Fetches only requested Props fields, reducing data transfer.
/// </summary>
[ExampleMeta("E075", "Select - Projection", "Query",
    ExampleTier.Free, 75, "Select", "Projection", "Fields", RelatedApis = ["IRedbQueryable.Select"])]
public class E075_Select : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Server-side projection - fetches only selected Props fields
        var query = redb.Query<EmployeeProps>()
            .Take(100)
            .Select(x => new { x.Props.FirstName, x.Props.LastName, x.Props.Salary });
        
        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);
        
        var results = await query.ToListAsync();

        sw.Stop();

        var first = results.FirstOrDefault();
        var fullName = first != null ? $"{first.FirstName} {first.LastName}" : "N/A";

        return Ok("E075", "Select - Projection", ExampleTier.Free, sw.ElapsedMilliseconds, 
            results.Count,
            [$"Projected: {results.Count}", $"First: {fullName}, Salary: {first?.Salary:N0}"]);
    }
}
