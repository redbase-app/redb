using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E168_ArrayContains"/>: Skills contains "C#", up to 100 whole objects and the count.</summary>
[ExampleMeta("E168_ef", "EF: Array - Contains", "Query", ExampleTier.Free, 1, "EF", "Array", "Contains")]
public class E168_ArrayContains_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var filtered = db.EmployeeProps.Where(e => e.EmployeeProps_Skills.Any(s => s._value == "C#"));
        var results = await filtered.WithGraph().OrderBy(e => e._id).Take(100).ToBusinessListAsync();
        var totalCount = await filtered.CountAsync();

        sw.Stop();

        var skillSamples = results.Take(3)
            .Select(e => $"{e.Props.FirstName}: [{string.Join(", ", e.Props.Skills?.Take(3) ?? [])}]");
        return Ok("E168_ef", "EF: Array - Contains", ExampleTier.Free, sw.ElapsedMilliseconds, totalCount,
            ["Filter: Skills.Contains(\"C#\")",
             $"Found: {totalCount} employees",
             $"Samples: {string.Join("; ", skillSamples)}"]);
    }
}
