using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E150_ArrayAggregation"/>: SUM and AVG over every element of SkillLevels and the
/// employee count. The elements and the employees are two tables, so EF asks twice; redb asks once.
/// </summary>
[ExampleMeta("E150_ef", "EF: Array Aggregation - All Elements", "Analytics", ExampleTier.Free, 1, "EF", "Array", "Aggregation")]
public class E150_ArrayAggregation_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var allElements = await db.EmployeeProps_SkillLevels
            .GroupBy(_ => 1)
            .Select(g => new { TotalSkillPoints = g.Sum(s => s._value), AvgSkillLevel = g.Average(s => s._value) })
            .FirstAsync();
        var count = await db.EmployeeProps.CountAsync();

        sw.Stop();

        return Ok("E150_ef", "EF: Array Aggregation - All Elements", ExampleTier.Free, sw.ElapsedMilliseconds, count,
            [$"All SkillLevels[]: Sum={allElements.TotalSkillPoints}, Avg={allElements.AvgSkillLevel:F2}",
             $"Employees: {count}"]);
    }
}
