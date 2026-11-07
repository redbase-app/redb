using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Ef.Entities;
using redb.Examples.Examples;
using redb.Examples.Output;
using Tree = redb.Examples.Examples.E088_TreeCreateBulk;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E088_TreeCreateBulk"/>: the same four-level department tree, built as one entity
/// graph (children point at their parent entity) and written by one <c>SaveChangesAsync</c>.
/// </summary>
[ExampleMeta("E088_ef", "EF: Tree Create BULK - Fast", "Trees", ExampleTier.Free, 1, "EF", "Tree", "BulkInsert")]
public class E088_TreeCreateBulk_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var nodes = new List<objects>();
        var root = DepartmentEfMapping.ToEntity("TechCorp", "CORP", "Headquarters", 50_000_000m, null);
        nodes.Add(root);

        var offices = new objects[Tree.OfficeCount];
        for (int i = 0; i < Tree.OfficeCount; i++)
        {
            var city = Tree.Cities[i % Tree.Cities.Length];
            offices[i] = DepartmentEfMapping.ToEntity($"{city} Office", $"OFF-{i + 1:D2}", $"Regional office {city}", 5_000_000m - i * 200_000m, root);
            nodes.Add(offices[i]);
        }

        var depts = new List<objects>();
        for (int o = 0; o < Tree.OfficeCount; o++)
        {
            for (int d = 0; d < Tree.DeptPerOffice; d++)
            {
                var deptName = Tree.Departments[d % Tree.Departments.Length];
                var dept = DepartmentEfMapping.ToEntity($"{deptName} {o + 1}-{d + 1}", $"DEPT-{o + 1:D2}-{d + 1:D2}", deptName, 1_000_000m - d * 100_000m, offices[o]);
                depts.Add(dept);
                nodes.Add(dept);
            }
        }

        for (int d = 0; d < Tree.TeamsForFirstDepts && d < depts.Count; d++)
        {
            for (int t = 0; t < Tree.TeamsPerDept; t++)
            {
                var teamName = Tree.Teams[t % Tree.Teams.Length];
                nodes.Add(DepartmentEfMapping.ToEntity($"{teamName} Team {d + 1}-{t + 1}", $"TEAM-{d + 1:D2}-{t + 1:D2}", teamName, 300_000m - t * 50_000m, depts[d]));
            }
        }

        db.objects.AddRange(nodes);
        await db.SaveChangesAsync();

        sw.Stop();

        var rate = nodes.Count * 1000 / Math.Max(sw.ElapsedMilliseconds, 1);
        return Ok("E088_ef", "EF: Tree Create BULK - Fast", ExampleTier.Free, sw.ElapsedMilliseconds, nodes.Count,
            [$"Bulk created: {nodes.Count} tree nodes in 4 levels", $"Rate: {rate} nodes/sec"]);
    }
}
