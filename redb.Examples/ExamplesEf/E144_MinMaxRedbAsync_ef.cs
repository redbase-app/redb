using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E144_MinMaxRedbAsync"/>: first and last creation date of the employees, two
/// queries; the date is a field of the <c>objects</c> header.
/// </summary>
[ExampleMeta("E144_ef", "EF: Min/MaxRedbAsync - Date Range", "Analytics", ExampleTier.Free, 1, "EF", "Min", "Max")]
public class E144_MinMaxRedbAsync_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var minDate = await db.EmployeeProps.MinAsync(e => (DateTime?)e.objects._date_create);
        var maxDate = await db.EmployeeProps.MaxAsync(e => (DateTime?)e.objects._date_create);

        sw.Stop();

        var range = minDate.HasValue && maxDate.HasValue ? (maxDate.Value - minDate.Value).Days : 0;
        return Ok("E144_ef", "EF: Min/MaxRedbAsync - Date Range", ExampleTier.Free, sw.ElapsedMilliseconds, 2,
            [$"First: {minDate?.ToString("yyyy-MM-dd") ?? "N/A"}",
             $"Last: {maxDate?.ToString("yyyy-MM-dd") ?? "N/A"}",
             $"Range: {range} days"]);
    }
}
