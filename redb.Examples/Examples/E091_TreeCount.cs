using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Count tree nodes with TreeQuery.
/// 
/// **Requires E089 to run first (creates tree data).**
/// 
/// CountAsync on TreeQuery - counts all nodes in tree.
/// </summary>
[ExampleMeta("E091", "Tree Count - CountAsync", "Trees",
    ExampleTier.Free, 1, "Tree", "TreeQuery", "CountAsync", "Pro", Order = 91)]
public class E091_TreeCount : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();
        var count = await redb.TreeQuery<DepartmentProps>().CountAsync();
        sw.Stop();

        if (count == 0)
            return Fail("E091", "Tree Count - CountAsync", ExampleTier.Free, sw.ElapsedMilliseconds, "No tree. Run E089 first.");

        return Ok("E091", "Tree Count - CountAsync", ExampleTier.Free, sw.ElapsedMilliseconds, count,
            [$"TreeQuery<DepartmentProps>().CountAsync()", $"Total: {count} nodes"]);
    }
}
