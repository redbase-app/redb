using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Core.Models.Entities;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;
using Biz = redb.Examples.Models;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E206_WhereRedbNarrowsFirst"/>: a lead with 20 team members under it, then the same
/// Props filter and page over the whole set and under the lead (<c>objects._id_parent</c>), as whole objects.
/// The team is deleted at the end.
/// </summary>
[ExampleMeta("E206_ef", "EF: WhereRedb - Narrow by Parent First", "Query", ExampleTier.Free, 1, "EF", "Where", "Parent")]
public class E206_WhereRedbNarrowsFirst_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var lead = EmployeeEfMapping.ToEntity(new RedbObject<Biz.EmployeeProps>
        {
            name = "e206-team-lead",
            Props = new Biz.EmployeeProps { FirstName = "E206", LastName = "Lead", Department = "Sales", Salary = 150000m, Age = 45 },
        });
        var team = Enumerable.Range(0, 20).Select(i =>
        {
            var member = EmployeeEfMapping.ToEntity(new RedbObject<Biz.EmployeeProps>
            {
                name = $"e206-member-{i}",
                Props = new Biz.EmployeeProps
                {
                    FirstName = $"E206-{i}",
                    LastName = "Member",
                    Department = "Sales",
                    Age = 25 + i,
                    Salary = 60000m + i * 2500m,
                },
            });
            member._id_parentNavigation = lead;
            return member;
        }).ToList();

        db.objects.Add(lead);
        db.objects.AddRange(team);
        await db.SaveChangesAsync();
        var leadId = lead._id;

        var whole = Stopwatch.StartNew();
        var anywhere = await db.EmployeeProps.WithGraph()
            .Where(e => e.Salary > 70000m && e.Age >= 30)
            .OrderByDescending(e => e.Salary).ThenBy(e => e._id)
            .Take(10)
            .ToBusinessListAsync();
        var wholeMs = whole.ElapsedMilliseconds;

        var narrow = Stopwatch.StartNew();
        var teamPage = await db.EmployeeProps.WithGraph()
            .Where(e => e.objects._id_parent == leadId)
            .Where(e => e.Salary > 70000m && e.Age >= 30)
            .OrderByDescending(e => e.Salary).ThenBy(e => e._id)
            .Take(10)
            .ToBusinessListAsync();
        var narrowMs = narrow.ElapsedMilliseconds;

        sw.Stop();

        // Cleanup: the members go with the lead through the cascade on _id_parent.
        await db.objects.Where(o => o._id == leadId).ExecuteDeleteAsync();

        return Ok("E206_ef", "EF: WhereRedb - Narrow by Parent First", ExampleTier.Free,
            sw.ElapsedMilliseconds, teamPage.Count,
            [
                $"Whole set, Salary > 70000 and Age >= 30, top 10 by Salary: {wholeMs} ms ({anywhere.Count} rows)",
                $"Same, objects._id_parent == lead: {narrowMs} ms ({teamPage.Count} of {team.Count} team members)",
            ]);
    }
}
