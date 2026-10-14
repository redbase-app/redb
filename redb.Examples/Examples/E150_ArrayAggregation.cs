using System.Diagnostics;
using redb.Core;
using redb.Core.Query.Aggregation;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Array aggregation - aggregate all elements of an array field.
/// SkillLevels[] contains numeric values (1-5) for aggregation.
/// For one element by its index see E150A.
/// </summary>
[ExampleMeta("E150", "Array Aggregation - All Elements", "Analytics",
    ExampleTier.Free, 150, "Array", "Aggregation", "Sum", RelatedApis = ["Agg.Sum", "Agg.Average"])]
public class E150_ArrayAggregation : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Aggregate ALL elements of SkillLevels[] array
        var allElements = await redb.Query<EmployeeProps>()
            .AggregateAsync(x => new
            {
                TotalSkillPoints = Agg.Sum(x.Props.SkillLevels.Select(s => s)),
                AvgSkillLevel = Agg.Average(x.Props.SkillLevels.Select(s => s)),
                Count = Agg.Count()
            });

        sw.Stop();

        return Ok("E150", "Array Aggregation - All Elements", ExampleTier.Free, sw.ElapsedMilliseconds, (int)allElements.Count,
            [$"All SkillLevels[]: Sum={allElements.TotalSkillPoints}, Avg={allElements.AvgSkillLevel:F2}",
             $"Employees: {allElements.Count}"]);
    }
}
