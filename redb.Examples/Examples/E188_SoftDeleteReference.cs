using System.Diagnostics;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Verifies the soft-delete read-path fix (_id_scheme = -10).
///
/// A surviving Employee references a Project via the RedbObject _Object edge
/// (EmployeeProps.CurrentProject). When the Project is **soft-deleted**
/// (moved to trash, _id_scheme = -10 — NOT purged), loading the Employee must
/// NOT materialize the tombstone through the nested reference.
///
/// Invariant checked (edition-agnostic):
///   loaded.CurrentProject?.Props == null
/// Free nulls the reference outright (get_object_json returns NULL for the
/// trashed target); Pro leaves an id-only placeholder with Props == null
/// (the trashed target is filtered out of Materialization_SelectObjectsByIds,
/// so it is never substituted). Either way the trashed object is not loaded as
/// live data. The _values._Object pointer is left intact, so the reference
/// returns once the target is restored from trash.
/// </summary>
[ExampleMeta("E188", "Soft-Delete - Nested Reference Not Materialized", "Core",
    ExampleTier.Free, 188, "SoftDelete", "Reference", "Trash", "-10",
    RelatedApis = ["IRedbService.SoftDeleteAsync", "RedbObject<TProps>"])]
public class E188_SoftDeleteReference : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        var projectScheme = await redb.EnsureSchemeFromTypeAsync<ProjectMetricsProps>();
        var employeeScheme = await redb.EnsureSchemeFromTypeAsync<EmployeeProps>();

        // Project that the employee will reference.
        var project = new RedbObject<ProjectMetricsProps>
        {
            name = $"SD_Project_{DateTime.UtcNow.Ticks}",
            scheme_id = projectScheme.Id,
            Props = new ProjectMetricsProps
            {
                ProjectId = 2001,
                TasksCompleted = 10,
                TasksTotal = 20,
                Budget = 250000
            }
        };
        await redb.SaveAsync(project);

        // Surviving employee with an _Object reference to the project.
        var employee = new RedbObject<EmployeeProps>
        {
            name = $"SD_Employee_{DateTime.UtcNow.Ticks}",
            scheme_id = employeeScheme.Id,
            Props = new EmployeeProps
            {
                FirstName = "Jane",
                LastName = "Survivor",
                Age = 40,
                Salary = 95000m,
                Department = "Engineering",
                CurrentProject = project
            }
        };
        await redb.SaveAsync(employee);

        // Sanity: before soft-delete the nested reference materializes fully.
        var before = await redb.LoadAsync<EmployeeProps>(employee.Id);
        var beforeOk = before?.Props?.CurrentProject?.Props != null;

        // Soft-delete the referenced project: it moves to trash (_id_scheme = -10),
        // it is NOT physically purged, and the _values._Object pointer stays intact.
        await redb.SoftDeleteAsync(new[] { project.Id });

        // Reload the survivor (cache is off in the examples, so this re-materializes).
        var after = await redb.LoadAsync<EmployeeProps>(employee.Id);
        var nested = after?.Props?.CurrentProject;
        var tombstoneLeaked = nested?.Props != null;          // trashed object materialized as live data
        var placeholderLeaked = nested != null;               // id-only placeholder left behind (no Free/Pro parity)

        sw.Stop();

        // Cleanup: drop the survivor first (removes the _Object pointer), then the
        // trashed project (now unreferenced, so the hard delete is unblocked).
        await redb.DeleteAsync(employee.Id);
        await redb.DeleteAsync(project.Id);

        if (!beforeOk)
        {
            return Fail("E188", "Soft-Delete - Nested Reference Not Materialized", ExampleTier.Free,
                sw.ElapsedMilliseconds,
                "Sanity check failed: CurrentProject.Props was null BEFORE soft-delete (reference never loaded).");
        }

        if (tombstoneLeaked)
        {
            return Fail("E188", "Soft-Delete - Nested Reference Not Materialized", ExampleTier.Free,
                sw.ElapsedMilliseconds,
                "REGRESSION: soft-deleted (_id_scheme=-10) project was still materialized through the nested reference.");
        }

        if (placeholderLeaked)
        {
            return Fail("E188", "Soft-Delete - Nested Reference Not Materialized", ExampleTier.Free,
                sw.ElapsedMilliseconds,
                "PARITY GAP: trashed reference left as an id-only placeholder instead of null (Free returns null).");
        }

        return Ok("E188", "Soft-Delete - Nested Reference Not Materialized", ExampleTier.Free,
            sw.ElapsedMilliseconds, 1,
            ["Before soft-delete: CurrentProject materialized",
             "After soft-delete:  CurrentProject == null",
             "Free / Pro parity: reference nulled in both editions"]);
    }
}
