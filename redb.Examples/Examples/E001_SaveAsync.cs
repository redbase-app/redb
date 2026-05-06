using System.Diagnostics;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Example: Create and save a new employee.
/// </summary>
[ExampleMeta("E001", "SaveAsync - Create Employee", "CRUD",
    ExampleTier.Free, 1, "SaveAsync", "Create", "CRUD")]
public class E001_SaveAsync : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        var employee = new RedbObject<EmployeeProps>
        {
            name = "New Developer",
            Props = new EmployeeProps
            {
                FirstName = "Alice",
                LastName = "Johnson",
                Age = 28,
                Position = "Developer",
                Department = "Engineering",
                Salary = 85000m,
                HireDate = DateTime.Today,
                EmployeeCode = "EMP-NEW-001",
                Skills = ["C#", "React", "SQL"]
            }
        };

        var id = await redb.SaveAsync(employee);
        sw.Stop();

        return Ok("E001", "SaveAsync - Create Employee", ExampleTier.Free, sw.ElapsedMilliseconds,
            [$"Created employee ID: {id}", $"Name: {employee.Props.FirstName} {employee.Props.LastName}"]);
    }
}
