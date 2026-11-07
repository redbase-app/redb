using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E002_LoadAsync"/>: find any employee, then load it by id, both times as the
/// whole business object.
/// </summary>
[ExampleMeta("E002_ef", "EF: Load Employee by ID", "CRUD", ExampleTier.Free, 1, "EF", "Read", "CRUD")]
public class E002_LoadAsync_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        // Same first step as E002: one employee, as a whole object. OrderBy is EF's own need: with split
        // queries and Take, every child query must pick the same rows.
        var items = await db.EmployeeProps.WithGraph().OrderBy(e => e._id).Take(1).ToListAsync();
        var first = items.Select(EmployeeEfGraph.ToBusiness).ToList();

        if (first.Count == 0)
        {
            sw.Stop();
            return Fail("E002_ef", "EF: Load Employee by ID", ExampleTier.Free, sw.ElapsedMilliseconds,
                "No employees found. Run E203_ef first.");
        }

        var id = first[0].Id;
        var entity = await db.EmployeeProps.WithGraph().FirstOrDefaultAsync(e => e._id == id);
        var loaded = entity == null ? null : EmployeeEfGraph.ToBusiness(entity);

        sw.Stop();

        return Ok("E002_ef", "EF: Load Employee by ID", ExampleTier.Free, sw.ElapsedMilliseconds,
            [$"Loaded ID: {loaded?.Id}", $"Employee: {loaded?.Props?.FirstName} {loaded?.Props?.LastName}, {loaded?.Props?.Position}"]);
    }
}
