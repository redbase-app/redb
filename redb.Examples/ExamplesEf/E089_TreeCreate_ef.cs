using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Ef.Entities;
using redb.Examples.Examples;
using redb.Examples.Output;
using Tree = redb.Examples.Examples.E088_TreeCreateBulk;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E089_TreeCreate"/>: the same tree saved node by node, one <c>SaveChangesAsync</c>
/// per node, the antipattern on purpose as in redb.
/// </summary>
[ExampleMeta("E089_ef", "EF: Tree Create Sequential - ANTIPATTERN (slow by design)", "Trees", ExampleTier.Free, 1, "EF", "Tree")]
public class E089_TreeCreate_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var count = 0;
        async Task<objects> Save(objects node)
        {
            db.objects.Add(node);
            await db.SaveChangesAsync();
            count++;
            return node;
        }

        var root = await Save(DepartmentEfMapping.ToEntity("TechCorp", "CORP", "Headquarters", 50_000_000m, null));

        var offices = new objects[Tree.OfficeCount];
        for (int i = 0; i < Tree.OfficeCount; i++)
        {
            var city = Tree.Cities[i % Tree.Cities.Length];
            offices[i] = await Save(DepartmentEfMapping.ToEntity($"{city} Office", $"OFF-{i + 1:D2}", $"Regional office {city}", 5_000_000m - i * 200_000m, root));
        }

        var depts = new List<objects>();
        for (int o = 0; o < Tree.OfficeCount; o++)
        {
            for (int d = 0; d < Tree.DeptPerOffice; d++)
            {
                var deptName = Tree.Departments[d % Tree.Departments.Length];
                depts.Add(await Save(DepartmentEfMapping.ToEntity($"{deptName} {o + 1}-{d + 1}", $"DEPT-{o + 1:D2}-{d + 1:D2}", deptName, 1_000_000m - d * 100_000m, offices[o])));
            }
        }

        for (int d = 0; d < Tree.TeamsForFirstDepts && d < depts.Count; d++)
        {
            for (int t = 0; t < Tree.TeamsPerDept; t++)
            {
                var teamName = Tree.Teams[t % Tree.Teams.Length];
                await Save(DepartmentEfMapping.ToEntity($"{teamName} Team {d + 1}-{t + 1}", $"TEAM-{d + 1:D2}-{t + 1:D2}", teamName, 300_000m - t * 50_000m, depts[d]));
            }
        }

        sw.Stop();

        var rate = count * 1000 / Math.Max(sw.ElapsedMilliseconds, 1);
        return Ok("E089_ef", "EF: Tree Create Sequential - ANTIPATTERN (slow by design)", ExampleTier.Free, sw.ElapsedMilliseconds, count,
            [
                "ANTIPATTERN: one SaveChanges round-trip per node, slow on purpose.",
                $"Sequential created: {count} tree nodes in 4 levels",
                $"Rate: {rate} nodes/sec - compare with E088_ef (one SaveChanges)"
            ]);
    }
}
