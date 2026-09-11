using System.Diagnostics;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// WhereRedb: filtering on SYSTEM fields of _objects (id, parent_id, key, value_string,
/// date_modify and friends) as opposed to Where, which filters on your Props. The two
/// compose freely in one query - the system predicate and the Props predicate both run
/// server-side. Patterns shown here mirror production usage: a lookup by the service
/// value_string key, an id-set prefilter with Contains, and the composite
/// WhereRedb().Where() shape (discussion #12, item on WhereRedb docs).
/// </summary>
[ExampleMeta("E205", "WhereRedb - System Fields and Composition", "Query",
    ExampleTier.Free, 1, "WhereRedb", "Query", "SystemFields", "Composite",
    RelatedApis = ["IRedbQueryable.WhereRedb", "IRedbQueryable.Where", "IRedbObject.Key", "IRedbObject.ValueString"])]
public class E205_WhereRedbSystemFields : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();
        await redb.SyncSchemeAsync<EmployeeProps>();

        // Seed: three employees carrying SYSTEM fields alongside Props. The service fields
        // (key, value_string, value_long...) live on the _objects row itself - no scheme
        // change needed - and are ideal for lookup keys next to your business data.
        var seeded = new List<long>();
        for (var i = 0; i < 3; i++)
        {
            var employee = new RedbObject<EmployeeProps>
            {
                name = $"e205-employee-{i}",
                key = 7000 + i,                        // numeric service key
                value_string = i == 0 ? "e205:head-of-desk" : null, // string lookup key
                Props = new EmployeeProps
                {
                    FirstName = $"E205-{i}",
                    LastName = "Sample",
                    Department = i == 2 ? "Sales" : "IT",
                    Salary = 50000m + i * 10000m,
                },
            };
            seeded.Add(await redb.SaveAsync(employee));
        }

        // 1. Lookup by the service string key - the settings/singleton pattern:
        //    one indexed system column instead of a dedicated Props field.
        var head = await redb.Query<EmployeeProps>()
            .WhereRedb(o => o.ValueString == "e205:head-of-desk")
            .FirstOrDefaultAsync();

        // 2. Id-set prefilter with Contains - "load these exact objects, filtered further".
        var byIds = await redb.Query<EmployeeProps>()
            .WhereRedb(o => seeded.Contains(o.Id))
            .ToListAsync();

        // 3. The composite shape: a SYSTEM predicate and a Props predicate in one query.
        //    Both are translated to SQL - the numeric key range narrows on _objects, the
        //    department condition narrows on Props values.
        var itByKey = await redb.Query<EmployeeProps>()
            .WhereRedb(o => o.Key >= 7000 && o.Key <= 7002)
            .Where(e => e.Department == "IT")
            .ToListAsync();

        sw.Stop();

        // Cleanup: the example leaves no residue behind.
        await redb.DeleteAsync(seeded);

        return Ok("E205", "WhereRedb - System Fields and Composition", ExampleTier.Free,
            sw.ElapsedMilliseconds, byIds.Count + itByKey.Count + (head != null ? 1 : 0),
            [
                $"ValueString lookup: {(head != null ? head.name : "<none>")}",
                $"Contains(Id) prefilter: {byIds.Count} of {seeded.Count}",
                $"WhereRedb(Key range).Where(Department==IT): {itByKey.Count}",
            ]);
    }
}
