using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Core.Models.Entities;
using redb.Examples.Ef;
using redb.Examples.Ef.Entities;
using redb.Examples.Examples;
using redb.Examples.Output;
using Biz = redb.Examples.Models;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E186_ObjectReference"/>: a project, an employee whose CurrentProject points at it,
/// and the employee loaded whole with the referenced project and its header included. Both are deleted after.
/// </summary>
[ExampleMeta("E186_ef", "EF: Object Reference - Load", "Core", ExampleTier.Free, 1, "EF", "Reference")]
public class E186_ObjectReference_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();

        var now = DateTime.UtcNow;
        var project = new objects
        {
            _id_scheme = EfSchemeIds.ProjectMetrics,
            _date_create = now,
            _date_modify = now,
            _name = $"RefProject_{now.Ticks}",
            ProjectMetricsProps = new ProjectMetricsProps
            {
                ProjectId = 1001,
                TasksCompleted = 50,
                TasksTotal = 100,
                Budget = 500000
            }
        };
        await using (var db = await contexts.CreateDbContextAsync())
        {
            db.objects.Add(project);
            await db.SaveChangesAsync();
        }

        var employee = EmployeeEfMapping.ToEntity(new RedbObject<Biz.EmployeeProps>
        {
            name = $"RefEmployee_{now.Ticks}",
            Props = new Biz.EmployeeProps
            {
                FirstName = "John",
                LastName = "ProjectManager",
                Age = 35,
                Salary = 90000m,
                Department = "Engineering"
            }
        });
        employee.EmployeeProps!.CurrentProject = project._id;
        await using (var db = await contexts.CreateDbContextAsync())
        {
            db.objects.Add(employee);
            await db.SaveChangesAsync();
        }

        EmployeeProps? loaded;
        await using (var db = await contexts.CreateDbContextAsync())
        {
            loaded = await db.EmployeeProps.WithGraph()
                .Include(e => e.CurrentProjectNavigation!).ThenInclude(p => p.objects)
                .FirstOrDefaultAsync(e => e._id == employee._id);
        }

        sw.Stop();

        await using (var db = await contexts.CreateDbContextAsync())
        {
            await db.objects.Where(o => o._id == employee._id).ExecuteDeleteAsync();
            await db.objects.Where(o => o._id == project._id).ExecuteDeleteAsync();
        }

        if (loaded?.CurrentProjectNavigation == null)
            return Fail("E186_ef", "EF: Object Reference - Load", ExampleTier.Free, sw.ElapsedMilliseconds,
                "CurrentProject reference not loaded.");

        var business = EmployeeEfGraph.ToBusiness(loaded);
        return Ok("E186_ef", "EF: Object Reference - Load", ExampleTier.Free, sw.ElapsedMilliseconds, 1,
            [$"Employee: {business.Props.FirstName} {business.Props.LastName}",
             $"Project: {loaded.CurrentProjectNavigation.objects._name}",
             $"Project Budget: ${loaded.CurrentProjectNavigation.Budget ?? 0:N0}"]);
    }
}
