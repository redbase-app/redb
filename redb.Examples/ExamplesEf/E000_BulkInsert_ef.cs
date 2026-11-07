using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E000_BulkInsert"/>: the same 100 complex employees, mapped to the entity graph by
/// hand and written in one <c>SaveChangesAsync</c> with change tracking on.
/// </summary>
[ExampleMeta("E000_ef", "EF: Bulk Insert - Complex Objects", "Setup", ExampleTier.Free, 1, "EF", "BulkInsert")]
public class E000_BulkInsert_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        const int count = 100;
        var headers = E000_BulkInsert.CreateEmployees(count).Select(EmployeeEfMapping.ToEntity).ToList();
        db.objects.AddRange(headers);
        var rows = await db.SaveChangesAsync();

        sw.Stop();

        return Ok("E000_ef", "EF: Bulk Insert - Complex Objects", ExampleTier.Free, sw.ElapsedMilliseconds, count,
        [
            $"Inserted: {headers.Count} complex objects, {rows} rows",
            $"Rate: {headers.Count * 1000 / Math.Max(sw.ElapsedMilliseconds, 1)} obj/sec"
        ]);
    }
}
