using System.Diagnostics;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Demonstrates batch deletion of multiple objects.
/// Creates temporary objects, then deletes them all at once.
/// </summary>
[ExampleMeta("E182", "Delete - Batch", "Core",
    ExampleTier.Free, 182, "Delete", "Batch", "Multiple", RelatedApis = ["IRedbService.DeleteAsync"])]
public class E182_DeleteBatch : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Create temporary objects to delete
        var scheme = await redb.EnsureSchemeFromTypeAsync<EmployeeProps>();
        var batchId = DateTime.UtcNow.Ticks;
        var tempObjects = new List<RedbObject<EmployeeProps>>();

        for (int i = 0; i < 5; i++)
        {
            tempObjects.Add(new RedbObject<EmployeeProps>
            {
                name = $"BatchDelete_{batchId}_{i}",
                scheme_id = scheme.Id,
                Props = new EmployeeProps
                {
                    FirstName = $"Temp{i}",
                    LastName = "Employee",
                    Age = 20 + i,
                    Salary = 40000m + i * 1000,
                    Department = "Test"
                }
            });
        }

        // Save all
        var ids = await redb.SaveAsync(tempObjects.Cast<Core.Models.Contracts.IRedbObject>());

        // Verify count before delete
        var countBefore = (await redb.LoadAsync(ids)).Count;

        // Delete all by IDs
        foreach (var id in ids)
        {
            await redb.DeleteAsync(id);
        }

        // Verify count after delete
        var countAfter = (await redb.LoadAsync(ids)).Count;

        sw.Stop();

        return Ok("E182", "Delete - Batch", ExampleTier.Free, sw.ElapsedMilliseconds, ids.Count,
            [$"Created: {ids.Count} objects",
             $"Before delete: {countBefore}",
             $"After delete: {countAfter}"]);
    }
}
