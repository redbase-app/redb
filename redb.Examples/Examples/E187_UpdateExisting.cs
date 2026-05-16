using System.Diagnostics;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Demonstrates updating an existing object by loading, modifying, and saving it.
/// Shows the standard update pattern in REDB: Load -> Modify -> Save.
/// </summary>
[ExampleMeta("E187", "Update - Existing Object", "CRUD",
    ExampleTier.Free, 187, "Update", "Modify", "Save", RelatedApis = ["IRedbService.LoadAsync", "IRedbService.SaveAsync"])]
public class E187_UpdateExisting : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Create an object to update
        var scheme = await redb.EnsureSchemeFromTypeAsync<EmployeeProps>();
        var original = new RedbObject<EmployeeProps>
        {
            name = $"ToUpdate_{DateTime.UtcNow.Ticks}",
            scheme_id = scheme.Id,
            Props = new EmployeeProps
            {
                FirstName = "Original",
                LastName = "Name",
                Age = 30,
                Salary = 60000m,
                Department = "Sales"
            }
        };
        await redb.SaveAsync(original);
        var originalSalary = original.Props.Salary;

        // Load the object
        var loaded = await redb.LoadAsync<EmployeeProps>(original.Id);
        if (loaded == null)
        {
            sw.Stop();
            return Fail("E187", "Update - Existing Object", ExampleTier.Free, sw.ElapsedMilliseconds,
                "Failed to load object for update.");
        }

        // Modify Props
        loaded.Props.FirstName = "Updated";
        loaded.Props.Salary = 75000m; // Raise!
        loaded.Props.Department = "Engineering";

        // Save changes (same ID = update)
        await redb.SaveAsync(loaded);

        // Verify changes
        var verified = await redb.LoadAsync<EmployeeProps>(original.Id);
        
        sw.Stop();

        // Cleanup
        await redb.DeleteAsync(original.Id);

        if (verified == null)
        {
            return Fail("E187", "Update - Existing Object", ExampleTier.Free, sw.ElapsedMilliseconds,
                "Failed to verify updated object.");
        }

        return Ok("E187", "Update - Existing Object", ExampleTier.Free, sw.ElapsedMilliseconds, 1,
            [$"Original: FirstName='Original', Salary=${originalSalary:N0}",
             $"Updated: FirstName='{verified.Props.FirstName}', Salary=${verified.Props.Salary:N0}",
             $"Department changed: Sales -> {verified.Props.Department}"]);
    }
}
