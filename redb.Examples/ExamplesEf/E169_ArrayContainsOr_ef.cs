using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E169_ArrayContainsOr"/>: Skills contains "C#" or "Python", up to 100 whole objects and the count.</summary>
[ExampleMeta("E169_ef", "EF: Array - Contains OR", "Query", ExampleTier.Free, 1, "EF", "Array", "Contains")]
public class E169_ArrayContainsOr_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var filtered = db.EmployeeProps.Where(e =>
            e.EmployeeProps_Skills.Any(s => s._value == "C#") || e.EmployeeProps_Skills.Any(s => s._value == "Python"));
        var results = await filtered.WithGraph().OrderBy(e => e._id).Take(100).ToBusinessListAsync();
        var totalCount = await filtered.CountAsync();

        sw.Stop();

        var csharpCount = results.Count(e => e.Props.Skills?.Contains("C#") == true);
        var pythonCount = results.Count(e => e.Props.Skills?.Contains("Python") == true);
        return Ok("E169_ef", "EF: Array - Contains OR", ExampleTier.Free, sw.ElapsedMilliseconds, totalCount,
            ["Filter: Skills.Contains(\"C#\") OR Skills.Contains(\"Python\")",
             $"Found: {totalCount} employees",
             $"C# devs: {csharpCount}, Python devs: {pythonCount}"]);
    }
}
