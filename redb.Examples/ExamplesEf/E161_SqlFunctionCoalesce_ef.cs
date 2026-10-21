using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E161_SqlFunctionCoalesce"/>: COALESCE(Age, 0) above 30, up to 100 whole objects and
/// the count. EF writes COALESCE for the C# <c>??</c> operator.
/// </summary>
[ExampleMeta("E161_ef", "EF: Sql.Function - COALESCE", "Query", ExampleTier.Pro, 1, "EF", "Where", "Function")]
public class E161_SqlFunctionCoalesce_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var filtered = db.EmployeeProps.Where(e => (e.Age ?? 0) > 30);
        var results = await filtered.WithGraph().OrderBy(e => e._id).Take(100).ToBusinessListAsync();
        var totalCount = await filtered.CountAsync();

        sw.Stop();

        var ages = results.Take(5).Select(e => e.Props.Age);
        return Ok("E161_ef", "EF: Sql.Function - COALESCE", ExampleTier.Pro, sw.ElapsedMilliseconds, totalCount,
            ["SQL: COALESCE(Age, 0) > 30",
             $"Found: {totalCount} employees",
             $"Ages: {string.Join(", ", ages)}"]);
    }
}
