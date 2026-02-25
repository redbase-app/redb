using System.Diagnostics;
using redb.Core;
using redb.Core.Query.Aggregation;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Array aggregation - aggregate all elements of array field or specific index.
/// SkillLevels[] contains numeric values (1-5) for aggregation.
/// </summary>
[ExampleMeta("E150", "Array Aggregation", "Analytics",
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

        // Aggregate specific index: SkillLevels[0] (first skill)
        var firstElement = await redb.Query<EmployeeProps>()
            .AggregateAsync(x => new
            {
                SumFirstSkill = Agg.Sum(x.Props.SkillLevels[0]),
                AvgFirstSkill = Agg.Average(x.Props.SkillLevels[0])
            });

        sw.Stop();

        return Ok("E150", "Array Aggregation", ExampleTier.Free, sw.ElapsedMilliseconds, (int)allElements.Count,
            [$"All SkillLevels[]: Sum={allElements.TotalSkillPoints}, Avg={allElements.AvgSkillLevel:F2}",
             $"SkillLevels[0]: Sum={firstElement.SumFirstSkill}, Avg={firstElement.AvgFirstSkill:F2}",
             $"Employees: {allElements.Count}"]);
    }
}
