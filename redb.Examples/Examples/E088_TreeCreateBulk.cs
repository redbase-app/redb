using System.Diagnostics;
using redb.Core;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// BULK create tree hierarchy (Pro feature) - FAST version.
/// 
/// Creates ~100 tree nodes using pre-generated IDs + AddNewObjectsAsync.
/// Compare with E089 (sequential CreateChildAsync).
/// 
/// Structure:
/// - 1 root (TechCorp)
/// - 10 regional offices
/// - 50 departments (5 per office)
/// - 40 teams (4 per first 10 departments)
/// 
/// Optimization:
/// - **NextObjectIdBatchAsync** - get all IDs in 1 query
/// - **AddNewObjectsAsync** - bulk insert all nodes in 1 operation
/// </summary>
[ExampleMeta("E088", "Tree Create BULK - Fast", "Trees",
    ExampleTier.Free, 3, "Tree", "Bulk", "AddNewObjectsAsync", "Pro", Order = 88)]
public class E088_TreeCreateBulk : ExampleBase
{
    private const int OfficeCount = 10;
    private const int DeptPerOffice = 5;
    private const int TeamsForFirstDepts = 10; // Teams only for first 10 departments
    private const int TeamsPerDept = 4;

    // Total: 1 + 10 + 50 + 40 = 101 nodes

    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        await redb.SyncSchemeAsync<DepartmentProps>();
        // Run E087 first to cleanup existing data

        var totalNodes = 1 + OfficeCount + (OfficeCount * DeptPerOffice) + (TeamsForFirstDepts * TeamsPerDept);

        // Measure bulk creation
        var sw = Stopwatch.StartNew();

        // 1. Get all IDs in ONE query
        var ids = await redb.Context.NextObjectIdBatchAsync(totalNodes);

        // 2. Create all objects with pre-assigned IDs and parent_id
        var nodes = GenerateTree(ids);

        // 3. Bulk insert ALL nodes in ONE operation
        await redb.AddNewObjectsAsync(nodes.Cast<IRedbObject<DepartmentProps>>().ToList());

        sw.Stop();

        var rate = nodes.Count * 1000 / Math.Max(sw.ElapsedMilliseconds, 1);

        return Ok("E088", "Tree Create BULK - Fast", ExampleTier.Free, sw.ElapsedMilliseconds, nodes.Count,
            [$"Bulk created: {nodes.Count} tree nodes in 4 levels", $"Rate: {rate} nodes/sec"]);
    }

    /// <summary>Generates tree with ~100 nodes using pre-assigned IDs.</summary>
    private static List<TreeRedbObject<DepartmentProps>> GenerateTree(long[] ids)
    {
        var nodes = new List<TreeRedbObject<DepartmentProps>>();
        var idx = 0;

        // Level 0: Root
        var rootId = ids[idx++];
        nodes.Add(CreateNode(rootId, null, "TechCorp", "CORP", "Headquarters", 50_000_000m));

        // Level 1: Regional offices
        var officeIds = new long[OfficeCount];
        for (int i = 0; i < OfficeCount; i++)
        {
            officeIds[i] = ids[idx++];
            var city = Cities[i % Cities.Length];
            nodes.Add(CreateNode(officeIds[i], rootId, $"{city} Office", $"OFF-{i + 1:D2}", $"Regional office {city}", 5_000_000m - i * 200_000m));
        }

        // Level 2: Departments in each office
        var deptIds = new List<long>();
        for (int o = 0; o < OfficeCount; o++)
        {
            for (int d = 0; d < DeptPerOffice; d++)
            {
                var deptId = ids[idx++];
                deptIds.Add(deptId);
                var deptName = Departments[d % Departments.Length];
                nodes.Add(CreateNode(deptId, officeIds[o], $"{deptName} {o + 1}-{d + 1}", $"DEPT-{o + 1:D2}-{d + 1:D2}", deptName, 1_000_000m - d * 100_000m));
            }
        }

        // Level 3: Teams in first 10 departments
        for (int d = 0; d < TeamsForFirstDepts && d < deptIds.Count; d++)
        {
            for (int t = 0; t < TeamsPerDept; t++)
            {
                var teamId = ids[idx++];
                var teamName = Teams[t % Teams.Length];
                nodes.Add(CreateNode(teamId, deptIds[d], $"{teamName} Team {d + 1}-{t + 1}", $"TEAM-{d + 1:D2}-{t + 1:D2}", teamName, 300_000m - t * 50_000m));
            }
        }

        return nodes;
    }

    private static readonly string[] Cities = ["Moscow", "SPB", "Kazan", "Novosibirsk", "Yekaterinburg", "Nizhny", "Samara", "Omsk", "Chelyabinsk", "Rostov"];
    private static readonly string[] Departments = ["IT", "Sales", "HR", "Finance", "Marketing"];
    private static readonly string[] Teams = ["Alpha", "Beta", "Gamma", "Delta"];

    private static TreeRedbObject<DepartmentProps> CreateNode(long id, long? parentId, string name, string code, string desc, decimal budget)
        => new()
        {
            id = id,
            parent_id = parentId,
            name = name,
            Props = new DepartmentProps { Name = name, Code = code, Description = desc, Budget = budget, IsActive = true }
        };
}
