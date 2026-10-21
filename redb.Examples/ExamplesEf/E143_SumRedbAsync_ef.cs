using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E143_SumRedbAsync"/>: SUM of the employee keys and the count, two queries.</summary>
[ExampleMeta("E143_ef", "EF: SumRedbAsync - Base Field Sum", "Analytics", ExampleTier.Free, 1, "EF", "Sum")]
public class E143_SumRedbAsync_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var sumId = await db.EmployeeProps.SumAsync(e => e._id);
        var count = await db.EmployeeProps.CountAsync();

        sw.Stop();

        return Ok("E143_ef", "EF: SumRedbAsync - Base Field Sum", ExampleTier.Free, sw.ElapsedMilliseconds, count,
            [$"SUM(_id) = {sumId:N0}", $"Count: {count}"]);
    }
}
