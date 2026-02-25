using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Example: Load employee by ID.
/// </summary>
[ExampleMeta("E002", "LoadAsync - Load Employee by ID", "CRUD",
    ExampleTier.Free, 1, "LoadAsync", "Read", "CRUD")]
public class E002_LoadAsync : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Get any existing employee ID
        var query = redb.Query<EmployeeProps>().Take(1);
        var items = await query.ToListAsync();

        if (items.Count == 0)
        {
            sw.Stop();
            return Fail("E002", "LoadAsync - Load Employee by ID", ExampleTier.Free, sw.ElapsedMilliseconds,
                "No employees found. Run E000 first.");
        }

        var id = items[0].Id;
        var loaded = await redb.LoadAsync<EmployeeProps>(id);

        if (loaded != null)
        {
            EmployeeProps? employee = (EmployeeProps)loaded;
        }
        
        sw.Stop();

        return Ok("E002", "LoadAsync - Load Employee by ID", ExampleTier.Free, sw.ElapsedMilliseconds,
            [$"Loaded ID: {loaded.Id}", $"Employee: {loaded.Props?.FirstName} {loaded.Props?.LastName}, {loaded.Props?.Position}"]);
    }
}
