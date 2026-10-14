using System.Diagnostics;
using redb.Core;
using redb.Core.Query.Aggregation;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Array aggregation - aggregate one element of an array field by its index.
/// SkillLevels[0] is the level of the first skill of each employee.
/// For all elements see E150.
/// </summary>
[ExampleMeta("E150A", "Array Aggregation - By Index", "Analytics",
    ExampleTier.Free, 150, "Array", "Aggregation", "Index", RelatedApis = ["Agg.Sum", "Agg.Average"])]
public class E150A_ArrayAggregationByIndex : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Aggregate specific index: SkillLevels[0] (first skill)
        var firstElement = await redb.Query<EmployeeProps>()
            .AggregateAsync(x => new
            {
                SumFirstSkill = Agg.Sum(x.Props.SkillLevels[0]),
                AvgFirstSkill = Agg.Average(x.Props.SkillLevels[0])
            });

        sw.Stop();

        return Ok("E150A", "Array Aggregation - By Index", ExampleTier.Free, sw.ElapsedMilliseconds, 1,
            [$"SkillLevels[0]: Sum={firstElement.SumFirstSkill}, Avg={firstElement.AvgFirstSkill:F2}"]);
    }
}
