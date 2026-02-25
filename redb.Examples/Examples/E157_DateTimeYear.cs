using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Demonstrates DateTime property extraction in WhereRedb clause.
/// Finds objects created in the current year using DateCreate.Year.
/// Pro feature: server-side EXTRACT(YEAR FROM date) function.
/// </summary>
[ExampleMeta("E157", "WhereRedb - DateTime.Year", "Query",
    ExampleTier.Pro, 157, "DateTime", "Year", "Extract", RelatedApis = ["IRedbQueryable.WhereRedb", "DateTime.Year"])]
public class E157_DateTimeYear : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        var currentYear = DateTime.UtcNow.Year;

        // Find employees created in the current year
        // Uses base field DateCreate (not Props) via WhereRedb
        var query = redb.Query<EmployeeProps>()
            .WhereRedb(o => o.DateCreate.Year == currentYear)
            .Take(100);

        // Uncomment to see generated SQL (contains EXTRACT):
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var results = await query.ToListAsync();
        var totalCount = await redb.Query<EmployeeProps>()
            .WhereRedb(o => o.DateCreate.Year == currentYear)
            .CountAsync();

        sw.Stop();

        var dates = results.Take(3).Select(e => e.date_create.ToString("yyyy-MM-dd"));

        return Ok("E157", "WhereRedb - DateTime.Year", ExampleTier.Pro, sw.ElapsedMilliseconds, totalCount,
            [$"Filter: DateCreate.Year == {currentYear}",
             $"Found: {totalCount} objects created this year",
             $"Dates: {string.Join(", ", dates)}"]);
    }
}
