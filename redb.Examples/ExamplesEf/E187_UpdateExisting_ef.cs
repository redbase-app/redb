using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Core.Models.Entities;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;
using Biz = redb.Examples.Models;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E187_UpdateExisting"/>: an employee saved, loaded whole with tracking, three fields
/// changed and saved, loaded again to verify, then deleted.
/// </summary>
[ExampleMeta("E187_ef", "EF: Update - Existing Object", "CRUD", ExampleTier.Free, 1, "EF", "Update")]
public class E187_UpdateExisting_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();

        var original = EmployeeEfMapping.ToEntity(new RedbObject<Biz.EmployeeProps>
        {
            name = $"ToUpdate_{DateTime.UtcNow.Ticks}",
            Props = new Biz.EmployeeProps
            {
                FirstName = "Original",
                LastName = "Name",
                Age = 30,
                Salary = 60000m,
                Department = "Sales"
            }
        });
        await using (var db = await contexts.CreateDbContextAsync())
        {
            db.objects.Add(original);
            await db.SaveChangesAsync();
        }
        var id = original._id;
        var originalSalary = original.EmployeeProps!.Salary;

        await using (var db = await contexts.CreateDbContextAsync())
        {
            var loaded = await db.EmployeeProps.WithGraph().FirstOrDefaultAsync(e => e._id == id);
            if (loaded == null)
            {
                sw.Stop();
                return Fail("E187_ef", "EF: Update - Existing Object", ExampleTier.Free, sw.ElapsedMilliseconds,
                    "Failed to load object for update.");
            }

            loaded.FirstName = "Updated";
            loaded.Salary = 75000m;
            loaded.Department = "Engineering";
            loaded.objects._date_modify = DateTime.UtcNow;
            await db.SaveChangesAsync();
        }

        RedbObject<Biz.EmployeeProps>? verified;
        await using (var db = await contexts.CreateDbContextAsync())
            verified = (await db.EmployeeProps.WithGraph().Where(e => e._id == id).ToBusinessListAsync()).FirstOrDefault();

        sw.Stop();

        await using (var db = await contexts.CreateDbContextAsync())
            await db.objects.Where(o => o._id == id).ExecuteDeleteAsync();

        if (verified == null)
            return Fail("E187_ef", "EF: Update - Existing Object", ExampleTier.Free, sw.ElapsedMilliseconds,
                "Failed to verify updated object.");

        return Ok("E187_ef", "EF: Update - Existing Object", ExampleTier.Free, sw.ElapsedMilliseconds, 1,
            [$"Original: FirstName='Original', Salary=${originalSalary:N0}",
             $"Updated: FirstName='{verified.Props.FirstName}', Salary=${verified.Props.Salary:N0}",
             $"Department changed: Sales -> {verified.Props.Department}"]);
    }
}
