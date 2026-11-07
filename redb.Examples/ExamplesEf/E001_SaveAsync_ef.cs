using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Core.Models.Entities;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;
using Biz = redb.Examples.Models;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E001_SaveAsync"/>: one new employee, mapped by hand and saved.</summary>
[ExampleMeta("E001_ef", "EF: SaveAsync - Create Employee", "CRUD", ExampleTier.Free, 1, "EF", "Save")]
public class E001_SaveAsync_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var employee = new RedbObject<Biz.EmployeeProps>
        {
            name = "New Developer",
            Props = new Biz.EmployeeProps
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
        var header = EmployeeEfMapping.ToEntity(employee);
        db.objects.Add(header);
        await db.SaveChangesAsync();

        sw.Stop();

        return Ok("E001_ef", "EF: SaveAsync - Create Employee", ExampleTier.Free, sw.ElapsedMilliseconds,
            [$"Created employee ID: {header._id}", $"Name: {employee.Props.FirstName} {employee.Props.LastName}"]);
    }
}
