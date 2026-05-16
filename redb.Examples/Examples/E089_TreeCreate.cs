using System.Diagnostics;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Sequential create tree hierarchy (Pro feature) - SLOW version.
/// 
/// Creates ~100 tree nodes using sequential CreateChildAsync.
/// Compare with E088 (bulk AddNewObjectsAsync).
/// 
/// Structure:
/// - 1 root (TechCorp)
/// - 10 regional offices
/// - 50 departments (5 per office)
/// - 40 teams (4 per first 10 departments)
/// 
/// Each CreateChildAsync = separate DB round-trip.
/// </summary>
[ExampleMeta("E089", "Tree Create Sequential - Slow", "Trees",
    ExampleTier.Free, 3, "Tree", "CreateChildAsync", "Sequential", "Pro", Order = 89)]
public class E089_TreeCreate : ExampleBase
{
    private const int OfficeCount = 10;
    private const int DeptPerOffice = 5;
    private const int TeamsForFirstDepts = 10;
    private const int TeamsPerDept = 4;

    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        await redb.SyncSchemeAsync<DepartmentProps>();
        // Run E087 first to cleanup existing data

        // Measure sequential creation
        var sw = Stopwatch.StartNew();
        var count = await CreateTreeSequentially(redb);
        sw.Stop();

        var rate = count * 1000 / Math.Max(sw.ElapsedMilliseconds, 1);

        return Ok("E089", "Tree Create Sequential - Slow", ExampleTier.Free, sw.ElapsedMilliseconds, count,
            [$"Sequential created: {count} tree nodes in 4 levels", $"Rate: {rate} nodes/sec"]);
    }

    /// <summary>Creates tree using sequential CreateChildAsync calls.</summary>
    private static async Task<int> CreateTreeSequentially(IRedbService redb)
    {
        var count = 0;

        // Level 0: Root
        var root = CreateDept("TechCorp", "CORP", "Headquarters", 50_000_000m);
        root.id = await redb.SaveAsync(root);
        count++;

        // Level 1: Regional offices
        var offices = new TreeRedbObject<DepartmentProps>[OfficeCount];
        for (int i = 0; i < OfficeCount; i++)
        {
            var city = Cities[i % Cities.Length];
            offices[i] = CreateDept($"{city} Office", $"OFF-{i + 1:D2}", $"Regional office {city}", 5_000_000m - i * 200_000m);
            offices[i].id = await redb.CreateChildAsync(offices[i], root);
            count++;
        }

        // Level 2: Departments in each office
        var depts = new List<TreeRedbObject<DepartmentProps>>();
        for (int o = 0; o < OfficeCount; o++)
        {
            for (int d = 0; d < DeptPerOffice; d++)
            {
                var deptName = Departments[d % Departments.Length];
                var dept = CreateDept($"{deptName} {o + 1}-{d + 1}", $"DEPT-{o + 1:D2}-{d + 1:D2}", deptName, 1_000_000m - d * 100_000m);
                dept.id = await redb.CreateChildAsync(dept, offices[o]);
                depts.Add(dept);
                count++;
            }
        }

        // Level 3: Teams in first 10 departments
        for (int d = 0; d < TeamsForFirstDepts && d < depts.Count; d++)
        {
            for (int t = 0; t < TeamsPerDept; t++)
            {
                var teamName = Teams[t % Teams.Length];
                var team = CreateDept($"{teamName} Team {d + 1}-{t + 1}", $"TEAM-{d + 1:D2}-{t + 1:D2}", teamName, 300_000m - t * 50_000m);
                await redb.CreateChildAsync(team, depts[d]);
                count++;
            }
        }

        return count;
    }

    private static readonly string[] Cities = ["Moscow", "SPB", "Kazan", "Novosibirsk", "Yekaterinburg", "Nizhny", "Samara", "Omsk", "Chelyabinsk", "Rostov"];
    private static readonly string[] Departments = ["IT", "Sales", "HR", "Finance", "Marketing"];
    private static readonly string[] Teams = ["Alpha", "Beta", "Gamma", "Delta"];

    private static TreeRedbObject<DepartmentProps> CreateDept(string name, string code, string desc, decimal budget)
        => new()
        {
            name = name,
            Props = new DepartmentProps { Name = name, Code = code, Description = desc, Budget = budget, IsActive = true }
        };
}
