using System.Diagnostics;
using redb.Core;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// WhereRedb narrows first. A condition on the object's own columns (here the parent) is checked
/// on _objects before any Props field is read, so the query reads the fields of the few objects
/// that pass and nothing else. The same Props filter and page over the whole scheme reads every
/// employee. This is the shape of "the points of one route sorted by their number" or "the lines
/// of one order": a narrow parent, a Props filter, a page sorted by a Props field.
/// </summary>
[ExampleMeta("E206", "WhereRedb - Narrow by Parent First", "Query",
    ExampleTier.Free, 1, "WhereRedb", "ParentId", "Performance", "Composite",
    RelatedApis = ["IRedbQueryable.WhereRedb", "IRedbQueryable.Where", "IRedbQueryable.ToSqlStringAsync"])]
public class E206_WhereRedbNarrowsFirst : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();
        await redb.SyncSchemeAsync<EmployeeProps>();

        // A team lead and twenty employees under it: the parent is a system column (_id_parent),
        // no Props field is needed to tell the team apart.
        var lead = new RedbObject<EmployeeProps>
        {
            name = "e206-team-lead",
            Props = new EmployeeProps { FirstName = "E206", LastName = "Lead", Department = "Sales", Salary = 150000m, Age = 45 },
        };
        var leadId = await redb.SaveAsync(lead);

        var team = Enumerable.Range(0, 20).Select(i => (IRedbObject)new RedbObject<EmployeeProps>
        {
            name = $"e206-member-{i}",
            parent_id = leadId,
            Props = new EmployeeProps
            {
                FirstName = $"E206-{i}",
                LastName = "Member",
                Department = "Sales",
                Age = 25 + i,
                Salary = 60000m + i * 2500m,
            },
        }).ToList();
        var memberIds = await redb.SaveAsync(team);

        // The same Props filter and page, once over the whole scheme and once under the parent.
        var whole = Stopwatch.StartNew();
        var anywhere = await redb.Query<EmployeeProps>()
            .Where(e => e.Salary > 70000m && e.Age >= 30)
            .OrderByDescending(e => e.Salary)
            .Take(10)
            .ToListAsync();
        var wholeMs = whole.ElapsedMilliseconds;

        var underLead = redb.Query<EmployeeProps>()
            .WhereRedb(o => o.ParentId == leadId)
            .Where(e => e.Salary > 70000m && e.Age >= 30)
            .OrderByDescending(e => e.Salary)
            .Take(10);

        var narrow = Stopwatch.StartNew();
        var teamPage = await underLead.ToListAsync();
        var narrowMs = narrow.ElapsedMilliseconds;

        // The SQL shows where the parent goes: into the scan of _objects that drives the query,
        // ahead of every Props field.
        var sql = await underLead.ToSqlStringAsync();
        var drivingScan = sql.Split('\n').FirstOrDefault(l => l.Contains("_id_parent"))?.Trim() ?? "<parent condition not found>";

        sw.Stop();

        // Cleanup: the example leaves no residue behind.
        await redb.DeleteAsync(memberIds.Append(leadId).ToList());

        return Ok("E206", "WhereRedb - Narrow by Parent First", ExampleTier.Free,
            sw.ElapsedMilliseconds, teamPage.Count,
            [
                $"Whole scheme, Salary > 70000 and Age >= 30, top 10 by Salary: {wholeMs} ms ({anywhere.Count} rows)",
                $"Same, WhereRedb(ParentId == lead): {narrowMs} ms ({teamPage.Count} of {team.Count} team members)",
                $"Driving scan: {drivingScan}",
            ],
            sql);
    }
}
