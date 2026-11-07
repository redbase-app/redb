using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Core.Models.Entities;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;
using Biz = redb.Examples.Models;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E205_WhereRedbSystemFields"/>: saves three employees with a service key and a
/// string lookup key in the <c>objects</c> header, finds them by those header fields as whole objects, and
/// deletes them again.
/// </summary>
[ExampleMeta("E205_ef", "EF: WhereRedb - System Fields and Composition", "Query", ExampleTier.Free, 1, "EF", "Where", "Base")]
public class E205_WhereRedbSystemFields_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var headers = new List<Ef.Entities.objects>();
        for (var i = 0; i < 3; i++)
        {
            var header = EmployeeEfMapping.ToEntity(new RedbObject<Biz.EmployeeProps>
            {
                name = $"e205-employee-{i}",
                Props = new Biz.EmployeeProps
                {
                    FirstName = $"E205-{i}",
                    LastName = "Sample",
                    Department = i == 2 ? "Sales" : "IT",
                    Salary = 50000m + i * 10000m,
                },
            });
            header._key = 7000 + i;
            header._value_string = i == 0 ? "e205:head-of-desk" : null;
            headers.Add(header);
        }
        db.objects.AddRange(headers);
        await db.SaveChangesAsync();
        var seeded = headers.Select(h => h._id).ToList();

        var head = (await db.EmployeeProps.WithGraph()
            .Where(e => e.objects._value_string == "e205:head-of-desk")
            .OrderBy(e => e._id)
            .Take(1)
            .ToBusinessListAsync()).FirstOrDefault();

        var byIds = await db.EmployeeProps.WithGraph()
            .Where(e => seeded.Contains(e._id))
            .ToBusinessListAsync();

        var itByKey = await db.EmployeeProps.WithGraph()
            .Where(e => e.objects._key >= 7000 && e.objects._key <= 7002)
            .Where(e => e.Department == "IT")
            .ToBusinessListAsync();

        sw.Stop();

        // Cleanup outside the measurement, as in redb: the cascade removes the Props rows with the headers.
        await db.objects.Where(o => seeded.Contains(o._id)).ExecuteDeleteAsync();

        return Ok("E205_ef", "EF: WhereRedb - System Fields and Composition", ExampleTier.Free,
            sw.ElapsedMilliseconds, byIds.Count + itByKey.Count + (head != null ? 1 : 0),
            [
                $"ValueString lookup: {(head != null ? head.name : "<none>")}",
                $"Contains(Id): {byIds.Count} of {seeded.Count}",
                $"Key range and Department == IT: {itByKey.Count}",
            ]);
    }
}
