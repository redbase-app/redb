using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E075_Select"/>: ten fields of 100 employees from every depth, a projection, no
/// whole object. Nested classes cost EF a join per level, dictionary entries a subquery by key. Without
/// tracking by nature: there is no entity to track.
/// </summary>
[ExampleMeta("E075_ef", "EF: Select - Projection", "Query", ExampleTier.Free, 1, "EF")]
public class E075_Select_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var results = await db.EmployeeProps
            .Where(x => x.Salary > 60000m && x.EmployeeProps_HomeAddress!.EmployeeProps_HomeAddress_Building!.Floor > 10)
            .OrderByDescending(x => x.Salary)
            .ThenBy(x => x.LastName)
            .Take(100)
            .Select(x => new
            {
                // Level 0: columns of the employee table
                x.FirstName,
                x.LastName,
                x.Salary,
                // Level 1: one join each
                HomeCity = x.EmployeeProps_HomeAddress!.City,
                WorkCity = x.EmployeeProps_WorkAddress!.City,
                // Level 2: two joins each
                HomeFloor = x.EmployeeProps_HomeAddress!.EmployeeProps_HomeAddress_Building!.Floor,
                WorkBuilding = x.EmployeeProps_WorkAddress!.EmployeeProps_WorkAddress_Building!.Name,
                // Dictionary entries: a subquery by key each
                DeskPhone = x.EmployeeProps_PhoneDirectory.Where(p => p._key == "desk").Select(p => p._value).FirstOrDefault(),
                HqCity = x.EmployeeProps_OfficeLocations.Where(o => o._key == "HQ").Select(o => o.City).FirstOrDefault(),
                HqFloor = x.EmployeeProps_OfficeLocations.Where(o => o._key == "HQ")
                    .Select(o => o.EmployeeProps_OfficeLocations_Building!.Floor).FirstOrDefault()
            })
            .ToListAsync();

        sw.Stop();

        var first = results.FirstOrDefault();
        var fullName = first != null ? $"{first.FirstName} {first.LastName}" : "N/A";
        return Ok("E075_ef", "EF: Select - Projection", ExampleTier.Free, sw.ElapsedMilliseconds, results.Count,
            [$"Filter: Salary > 60000 AND HomeAddress.Building.Floor > 10, ORDER BY Salary DESC, LastName",
             $"Projected: {results.Count}",
             $"First: {fullName}, Salary: {first?.Salary:N0}",
             $"Home: {first?.HomeCity}, floor {first?.HomeFloor}; Work: {first?.WorkCity}, {first?.WorkBuilding}",
             $"Desk phone: {first?.DeskPhone}; HQ: {first?.HqCity}, floor {first?.HqFloor}"]);
    }
}
