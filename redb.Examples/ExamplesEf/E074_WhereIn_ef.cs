using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E074_WhereIn"/>: employees of three departments, up to 100 whole objects,
/// and their total count.
/// <para>OrderBy(_id) is EF's own need, the redb query has none: with split queries and Take, every child query must pick the same employees.</para>
/// </summary>
[ExampleMeta("E074_ef", "EF: WhereIn - Filter by List", "Query", ExampleTier.Free, 1, "EF")]
public class E074_WhereIn_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var targetDepartments = new[] { "Engineering", "Sales", "Marketing" };

        var results = await db.EmployeeProps.WithGraph()
            .Where(e => targetDepartments.Contains(e.Department))
            .OrderBy(e => e._id)
            .Take(100)
            .ToBusinessListAsync();
        var totalCount = await db.EmployeeProps.Where(e => targetDepartments.Contains(e.Department)).CountAsync();

        sw.Stop();

        return Ok("E074_ef", "EF: WhereIn - Filter by List", ExampleTier.Free, sw.ElapsedMilliseconds, totalCount,
            [$"Loaded: {results.Count}, Total: {totalCount}", $"Depts: {string.Join(", ", targetDepartments)}"]);
    }
}
