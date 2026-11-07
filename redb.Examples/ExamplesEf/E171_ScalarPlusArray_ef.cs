using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E171_ScalarPlusArray"/>: Age above 30 and Skills contains "C#", up to 100 whole objects and the count.</summary>
[ExampleMeta("E171_ef", "EF: Scalar + Array Combined", "Query", ExampleTier.Free, 1, "EF", "Array", "Where")]
public class E171_ScalarPlusArray_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var filtered = db.EmployeeProps.Where(e => e.Age > 30 && e.EmployeeProps_Skills.Any(s => s._value == "C#"));
        var results = await filtered.WithGraph().OrderBy(e => e._id).Take(100).ToBusinessListAsync();
        var totalCount = await filtered.CountAsync();

        sw.Stop();

        var avgAge = results.Any() ? results.Average(e => e.Props.Age) : 0;
        var sample = results.FirstOrDefault();
        return Ok("E171_ef", "EF: Scalar + Array Combined", ExampleTier.Free, sw.ElapsedMilliseconds, totalCount,
            ["Filter: Age > 30 AND Skills.Contains(\"C#\")",
             $"Found: {totalCount} senior C# developers",
             $"Avg age: {avgAge:F1}, Sample: {sample?.Props.FirstName} ({sample?.Props.Age})"]);
    }
}
