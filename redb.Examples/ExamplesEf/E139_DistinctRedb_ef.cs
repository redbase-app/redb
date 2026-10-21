using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E139_DistinctRedb"/>: up to 100 employees unique by every header field except
/// the key, as whole objects. EF has no DISTINCT ON; its usual way is to group by those fields, keep the
/// smallest key of each group, then load the objects by those keys.
/// </summary>
[ExampleMeta("E139_ef", "EF: DistinctRedb - Find Duplicates", "Analytics", ExampleTier.Pro, 1, "EF", "Distinct")]
public class E139_DistinctRedb_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var ids = await db.EmployeeProps
            .GroupBy(e => new
            {
                e.objects._id_parent,
                e.objects._id_scheme,
                e.objects._id_owner,
                e.objects._id_who_change,
                e.objects._date_create,
                e.objects._date_modify,
                e.objects._date_begin,
                e.objects._date_complete,
                e.objects._key,
                e.objects._name,
                e.objects._note,
                e.objects._value_long,
                e.objects._value_string,
                e.objects._value_guid,
                e.objects._value_bool,
                e.objects._value_double,
                e.objects._value_numeric,
                e.objects._value_datetime,
                e.objects._value_bytes
            })
            .Select(g => g.Min(e => e._id))
            .OrderBy(id => id)
            .Take(100)
            .ToListAsync();

        var unique = await db.EmployeeProps.WithGraph()
            .Where(e => ids.Contains(e._id))
            .ToBusinessListAsync();

        sw.Stop();

        return Ok("E139_ef", "EF: DistinctRedb - Find Duplicates", ExampleTier.Pro, sw.ElapsedMilliseconds, unique.Count,
            ["GROUP BY header fields except _id, then load by key", $"Unique: {unique.Count} objects"]);
    }
}
