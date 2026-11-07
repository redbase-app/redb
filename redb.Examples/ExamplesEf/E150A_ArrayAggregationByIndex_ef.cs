using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E150A_ArrayAggregationByIndex"/>: SUM and AVG of SkillLevels[0], one query over the array table.</summary>
[ExampleMeta("E150A_ef", "EF: Array Aggregation - By Index", "Analytics", ExampleTier.Free, 1, "EF", "Array", "Aggregation")]
public class E150A_ArrayAggregationByIndex_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var firstElement = await db.EmployeeProps_SkillLevels
            .Where(s => s._idx == 0)
            .GroupBy(_ => 1)
            .Select(g => new { SumFirstSkill = g.Sum(s => s._value), AvgFirstSkill = g.Average(s => s._value) })
            .FirstAsync();

        sw.Stop();

        return Ok("E150A_ef", "EF: Array Aggregation - By Index", ExampleTier.Free, sw.ElapsedMilliseconds, 1,
            [$"SkillLevels[0]: Sum={firstElement.SumFirstSkill}, Avg={firstElement.AvgFirstSkill:F2}"]);
    }
}
