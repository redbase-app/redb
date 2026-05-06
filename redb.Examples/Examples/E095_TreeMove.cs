using System.Diagnostics;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Move node to new parent.
/// Demonstrates MoveObjectAsync - moves entire subtree.
/// </summary>
[ExampleMeta("E095", "Tree Move - Relocate Node", "Trees",
    ExampleTier.Free, 3, "Tree", "MoveObjectAsync", "Pro")]
public class E095_TreeMove : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Find a department to move - use TreeQuery!
        var nodes = await redb.TreeQuery<DepartmentProps>()
            .Where(d => d.Code.StartsWith("DEPT-01-"))
            .Take(1)
            .ToListAsync();

        if (nodes.Count == 0)
        {
            return Ok("E095", "Tree Move - Relocate Node", ExampleTier.Free, sw.ElapsedMilliseconds, 0,
                ["No tree found. Run E088 or E089 first."]);
        }

        // Find another office as new parent - use TreeQuery!
        var officeNodes = await redb.TreeQuery<DepartmentProps>()
            .Where(d => d.Code == "OFF-02")
            .Take(1)
            .ToListAsync();

        if (officeNodes.Count == 0)
        {
            return Ok("E095", "Tree Move - Relocate Node", ExampleTier.Free, sw.ElapsedMilliseconds, 0,
                ["Target office not found."]);
        }

        var dept = (TreeRedbObject<DepartmentProps>)nodes[0];
        var targetOffice = (TreeRedbObject<DepartmentProps>)officeNodes[0];
        var originalParentId = dept.parent_id;

        // Move department to another office
        await redb.MoveObjectAsync(dept, targetOffice);

        // Move back to original parent
        if (originalParentId.HasValue)
        {
            var originalParent = await redb.TreeQuery<DepartmentProps>()
                .Where(d => d.Code == "OFF-01")
                .Take(1)
                .ToListAsync();

            if (originalParent.Count > 0)
            {
                var origOffice = (TreeRedbObject<DepartmentProps>)originalParent[0];
                await redb.MoveObjectAsync(dept, origOffice);
            }
        }

        sw.Stop();

        return Ok("E095", "Tree Move - Relocate Node", ExampleTier.Free, sw.ElapsedMilliseconds, 1,
        [
            $"Moved: {dept.name} to {targetOffice.name}",
            "Moved back to original office",
            "Subtree preserved during move"
        ]);
    }
}
