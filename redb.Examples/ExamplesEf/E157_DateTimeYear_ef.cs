using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E157_DateTimeYear"/>: objects created this year and older than 30, up to 100 whole
/// objects, and the count of this year's objects. The creation date is a field of the <c>objects</c> header.
/// </summary>
[ExampleMeta("E157_ef", "EF: WhereRedb - DateTime.Year", "Query", ExampleTier.Pro, 1, "EF", "Where", "DateTime")]
public class E157_DateTimeYear_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var currentYear = DateTime.UtcNow.Year;
        var thisYear = db.EmployeeProps.Where(e => e.objects._date_create.Year == currentYear);

        var results = await thisYear.Where(e => e.Age > 30)
            .WithGraph().OrderBy(e => e._id).Take(100).ToBusinessListAsync();
        var totalCount = await thisYear.CountAsync();

        sw.Stop();

        var dates = results.Take(3).Select(e => e.date_create.ToString("yyyy-MM-dd"));
        return Ok("E157_ef", "EF: WhereRedb - DateTime.Year", ExampleTier.Pro, sw.ElapsedMilliseconds, totalCount,
            [$"Filter: DateCreate.Year == {currentYear}",
             $"Found: {totalCount} objects created this year",
             $"Dates: {string.Join(", ", dates)}"]);
    }
}
