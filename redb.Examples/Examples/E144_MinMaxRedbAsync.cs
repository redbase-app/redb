using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// MinRedbAsync/MaxRedbAsync - min/max of base IRedbObject fields WITHOUT JOIN.
/// Perfect for finding date ranges (DateCreate, DateModify).
/// </summary>
[ExampleMeta("E144", "Min/MaxRedbAsync - Date Range", "Analytics",
    ExampleTier.Free, 144, "MinRedbAsync", "MaxRedbAsync", "DateRange", RelatedApis = ["IRedbQueryable.MinRedbAsync", "IRedbQueryable.MaxRedbAsync"])]
public class E144_MinMaxRedbAsync : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Min/Max of DateCreate (from _objects table, NO JOIN!)
        var minDate = await redb.Query<EmployeeProps>().MinRedbAsync(x => x.DateCreate);
        var maxDate = await redb.Query<EmployeeProps>().MaxRedbAsync(x => x.DateCreate);

        sw.Stop();

        var range = minDate.HasValue && maxDate.HasValue 
            ? (maxDate.Value - minDate.Value).Days 
            : 0;

        return Ok("E144", "Min/MaxRedbAsync - Date Range", ExampleTier.Free, sw.ElapsedMilliseconds, 2,
            [$"First: {minDate?.ToString("yyyy-MM-dd") ?? "N/A"}", 
             $"Last: {maxDate?.ToString("yyyy-MM-dd") ?? "N/A"}", 
             $"Range: {range} days (NO JOIN!)"]);
    }
}
