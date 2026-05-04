using System.Diagnostics;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Demonstrates loading objects with RedbObject references.
/// EmployeeProps has CurrentProject field of type RedbObject&lt;ProjectMetricsProps&gt;.
/// Shows how referenced objects are loaded.
/// </summary>
[ExampleMeta("E186", "Object Reference - Load", "Core",
    ExampleTier.Free, 186, "Reference", "Related", "Nested", RelatedApis = ["RedbObject<TProps>"])]
public class E186_ObjectReference : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Ensure schemes exist
        var projectScheme = await redb.EnsureSchemeFromTypeAsync<ProjectMetricsProps>();
        var employeeScheme = await redb.EnsureSchemeFromTypeAsync<EmployeeProps>();

        // Create a project
        var project = new RedbObject<ProjectMetricsProps>
        {
            name = $"RefProject_{DateTime.UtcNow.Ticks}",
            scheme_id = projectScheme.Id,
            Props = new ProjectMetricsProps
            {
                ProjectId = 1001,
                TasksCompleted = 50,
                TasksTotal = 100,
                Budget = 500000
            }
        };

        var id1 = await redb.SaveAsync(project);
        //Console.WriteLine($"First object id: {id1}");

        // Create employee with project reference
        var employee = new RedbObject<EmployeeProps>
        {
            name = $"RefEmployee_{DateTime.UtcNow.Ticks}",
            scheme_id = employeeScheme.Id,
            Props = new EmployeeProps
            {
                FirstName = "John",
                LastName = "ProjectManager",
                Age = 35,
                Salary = 90000m,
                Department = "Engineering",
                CurrentProject = project // Reference to another RedbObject
            }
        };

        var id2 = await redb.SaveAsync(employee);
        //Console.WriteLine($"Second object id: {id2}");

        // Load employee and access referenced project
        var loaded = await redb.LoadAsync<EmployeeProps>(employee.Id);

        sw.Stop();

        if (loaded?.Props?.CurrentProject == null)
        {
            // Cleanup
            await redb.DeleteAsync(employee.Id);
            await redb.DeleteAsync(project.Id);
            
            return Fail("E186", "Object Reference - Load", ExampleTier.Free, sw.ElapsedMilliseconds,
                "CurrentProject reference not loaded.");
        }

        var projName = loaded.Props.CurrentProject.name;
        var projBudget = loaded.Props.CurrentProject.Props?.Budget ?? 0;

        // Cleanup
        await redb.DeleteAsync(employee.Id);
        await redb.DeleteAsync(project.Id);

        return Ok("E186", "Object Reference - Load", ExampleTier.Free, sw.ElapsedMilliseconds, 1,
            [$"Employee: {loaded.Props.FirstName} {loaded.Props.LastName}",
             $"Project: {projName}",
             $"Project Budget: ${projBudget:N0}"]);
    }
}
