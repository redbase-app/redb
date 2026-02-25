using System.Diagnostics;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Demonstrates deleting a single object by ID.
/// Creates a temporary object, then deletes it.
/// </summary>
[ExampleMeta("E181", "Delete - Single Object", "CRUD",
    ExampleTier.Free, 181, "Delete", "Single", "Remove", RelatedApis = ["IRedbService.DeleteAsync"])]
public class E181_DeleteSingle : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Create a temporary object to delete
        var scheme = await redb.EnsureSchemeFromTypeAsync<EmployeeProps>();
        var tempObj = new RedbObject<EmployeeProps>
        {
            name = $"ToDelete_{DateTime.UtcNow.Ticks}",
            scheme_id = scheme.Id,
            Props = new EmployeeProps
            {
                FirstName = "Temporary",
                LastName = "Employee",
                Age = 25,
                Salary = 50000m,
                Department = "Test"
            }
        };

        var id = await redb.SaveAsync(tempObj);

        // Verify it exists
        var beforeDelete = await redb.LoadAsync<EmployeeProps>(id);
        var existsBefore = beforeDelete != null;

        // Delete the object
        await redb.DeleteAsync(id);

        // Verify it's deleted
        var afterDelete = await redb.LoadAsync<EmployeeProps>(id);
        var existsAfter = afterDelete != null;

        sw.Stop();

        if (!existsBefore || existsAfter)
        {
            return Fail("E181", "Delete - Single Object", ExampleTier.Free, sw.ElapsedMilliseconds,
                $"Delete verification failed. Before: {existsBefore}, After: {existsAfter}");
        }

        return Ok("E181", "Delete - Single Object", ExampleTier.Free, sw.ElapsedMilliseconds, 1,
            [$"Created object ID: {id}",
             $"Existed before delete: {existsBefore}",
             $"Existed after delete: {existsAfter}"]);
    }
}
