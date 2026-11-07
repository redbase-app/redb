using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E162_SqlFunctionPower"/>: POWER(Age, 2) above 900, up to 100 whole objects and the
/// count. EF writes POWER for <c>Math.Pow</c>.
/// </summary>
[ExampleMeta("E162_ef", "EF: Sql.Function - POWER", "Query", ExampleTier.Pro, 1, "EF", "Where", "Function")]
public class E162_SqlFunctionPower_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var filtered = db.EmployeeProps.Where(e => Math.Pow(e.Age!.Value, 2) > 900);
        var results = await filtered.WithGraph().OrderBy(e => e._id).Take(100).ToBusinessListAsync();
        var totalCount = await filtered.CountAsync();

        sw.Stop();

        var ages = results.Take(5).Select(e => $"{e.Props.Age} (^2={e.Props.Age * e.Props.Age})");
        return Ok("E162_ef", "EF: Sql.Function - POWER", ExampleTier.Pro, sw.ElapsedMilliseconds, totalCount,
            ["SQL: POWER(Age, 2) > 900 (Age > 30)",
             $"Found: {totalCount} employees",
             $"Ages: {string.Join(", ", ages)}"]);
    }
}
