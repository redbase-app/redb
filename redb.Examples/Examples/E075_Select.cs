using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Project specific fields using Select (server-side projection).
/// Fetches only requested Props fields, reducing data transfer, from every depth: the employee itself,
/// nested classes one and two levels deep, and dictionary entries holding a scalar, a class and a class
/// inside that class.
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
            .Where(x => x.Salary > 60000m && x.HomeAddress!.Building!.Floor > 10)
            .OrderByDescending(x => x.Salary)
            .ThenBy(x => x.LastName)
            .Take(100)
            .Select(x => new
            {
                // Level 0: fields of the employee
                x.Props.FirstName,
                x.Props.LastName,
                x.Props.Salary,
                // Level 1: a nested class
                HomeCity = x.Props.HomeAddress!.City,
                WorkCity = x.Props.WorkAddress!.City,
                // Level 2: a class inside a nested class
                HomeFloor = x.Props.HomeAddress!.Building!.Floor,
                WorkBuilding = x.Props.WorkAddress!.Building!.Name,
                // Dictionary entries: a scalar, a class, a class inside it
                DeskPhone = x.Props.PhoneDirectory!["desk"],
                HqCity = x.Props.OfficeLocations!["HQ"].City,
                HqFloor = x.Props.OfficeLocations!["HQ"].Building!.Floor
            });
        
        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);
        
        var results = await query.ToListAsync();

        sw.Stop();

        var first = results.FirstOrDefault();
        var fullName = first != null ? $"{first.FirstName} {first.LastName}" : "N/A";

        return Ok("E075", "Select - Projection", ExampleTier.Free, sw.ElapsedMilliseconds, 
            results.Count,
            [$"Filter: Salary > 60000 AND HomeAddress.Building.Floor > 10, ORDER BY Salary DESC, LastName",
             $"Projected: {results.Count}",
             $"First: {fullName}, Salary: {first?.Salary:N0}",
             $"Home: {first?.HomeCity}, floor {first?.HomeFloor}; Work: {first?.WorkCity}, {first?.WorkBuilding}",
             $"Desk phone: {first?.DeskPhone}; HQ: {first?.HqCity}, floor {first?.HqFloor}"]);
    }
}
