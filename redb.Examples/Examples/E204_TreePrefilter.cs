using System.Diagnostics;
using System.Text;
using redb.Core;
using redb.Core.Query;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Tree counterpart of E202: dumps the SQL a tree query generates with and without the PVT
/// prefilter, and checks that the row count is unchanged. See <c>docs/PVT_PREFILTER_PLAN.md</c>.
///
/// <code>
///   dotnet run --project redb.Examples -- E204                       (off)
///   REDB_PVT_PREFILTER=1 dotnet run --project redb.Examples -- E204  (on)
/// </code>
///
/// A subtree already bounds the work, unlike a flat query which always scans the whole scheme,
/// so the prefilter only starts paying once the subtree itself is large. Measured on a 6-level
/// tree of 99200 nodes: 1189 -> 401 ms and 716677 -> 333755 buffers, identical result sets.
///
/// Needs a tree with real depth under the Employee scheme. E203 seeds objects but leaves them
/// flat; the tree used for the measurement above was built by re-parenting them into a 10-ary
/// tree. Without depth this example still runs, it just measures very little.
///
/// The differential check here is by COUNT rather than by id set: a tree query over a large
/// subtree returns tens of thousands of objects and materialising them all would dwarf the
/// query being measured. Full id-set equality was verified separately in psql.
/// </summary>
[ExampleMeta("E204", "PVT prefilter in tree queries", "Trees",
    ExampleTier.Pro, 200, "Sql", "Debug", "Prefilter", "Pro", "Pvt", "Tree",
    RelatedApis = ["IRedbService.TreeQuery", "IRedbQueryable.ToSqlStringAsync"])]
public class E204_TreePrefilter : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        var enabled = Environment.GetEnvironmentVariable("REDB_PVT_PREFILTER") == "1";

        var root = await redb.TreeQuery<EmployeeProps>().WhereRoots().FirstOrDefaultAsync();
        if (root == null)
        {
            sw.Stop();
            return Fail("E204", "PVT prefilter in tree queries", ExampleTier.Pro, sw.ElapsedMilliseconds,
                "No Employee tree found. Seed with E000/E203, then give the objects parents.");
        }

        var log = new StringBuilder();
        var summary = new List<string>();
        var withPrefilter = 0;

        log.AppendLine($"-- REDB_PVT_PREFILTER = {(enabled ? "1 (enabled)" : "<unset> (disabled)")}");
        log.AppendLine($"-- root id = {root.Id}");
        log.AppendLine();

        async Task CaseAsync(string label, string expectation, IRedbQueryable<EmployeeProps> query)
        {
            log.AppendLine("-----------------------------------------------------------------");
            log.AppendLine($"-- {label}");
            log.AppendLine($"-- expected when enabled: {expectation}");
            log.AppendLine("-----------------------------------------------------------------");

            string sql;
            try
            {
                sql = await query.ToSqlStringAsync();
            }
            catch (Exception ex)
            {
                log.AppendLine($"-- SQL BUILD FAIL: {ex.GetType().Name}: {ex.Message}");
                log.AppendLine();
                summary.Add($"{label}: SQL build failed");
                Console.WriteLine($"[BUILD-FAIL] {label}: {ex.Message}");
                return;
            }

            log.AppendLine(sql);
            Console.WriteLine();
            Console.WriteLine($"--- {label} (expected when enabled: {expectation}) ---");
            Console.WriteLine(sql);

            var hasRow = sql.Contains("AND ((v._id_structure");
            // Dropping _objects oo is the second half of the change and only fires together
            // with the prefilter; surfacing it separately makes a half-applied change visible.
            var droppedObjects = sql.Contains("FROM tree t\n") || sql.Contains("    FROM tree t");
            var form = hasRow ? (droppedObjects ? "Row+" : "Row") : "none";
            if (hasRow) withPrefilter++;

            string count;
            var timer = Stopwatch.StartNew();
            try
            {
                count = (await query.CountAsync()).ToString();
            }
            catch (Exception ex)
            {
                count = $"EXEC FAIL {ex.GetType().Name}: {ex.Message.Split('\n')[0]}";
            }
            timer.Stop();

            log.AppendLine($"-- prefilter form: {form}");
            log.AppendLine($"-- count: {count} in {timer.ElapsedMilliseconds} ms");
            log.AppendLine();

            summary.Add($"{label}: {form}, count={count}");
            Console.WriteLine($"[{form,5}] {label} -> count={count} in {timer.ElapsedMilliseconds} ms");
        }

        // The text-search shape: one needle across two string fields. Row form plus the dropped
        // _objects join, and pg_trgm finally reaches the column.
        await CaseAsync(
            "OR: two string fields, same needle",
            "Row+ (prefilter and _objects join dropped)",
            redb.TreeQuery<EmployeeProps>(root.Id)
                .Where(e => e.Position.Contains("Design") || e.Department.Contains("Design")));

        // Two predicates on one field merge into a single selective branch.
        await CaseAsync(
            "AND: range on one field",
            "Row+ (both bounds merged)",
            redb.TreeQuery<EmployeeProps>(root.Id)
                .Where(e => e.Salary >= 60000m && e.Salary < 90000m));

        // Dates use _datetimeoffset and a different parameter round-trip.
        await CaseAsync(
            "AND: date range",
            "Row+",
            redb.TreeQuery<EmployeeProps>(root.Id)
                .Where(e => e.HireDate >= new DateTime(2030, 1, 1) && e.HireDate < new DateTime(2031, 1, 1)));

        // Pivot not covered: Position and Age have no branch of their own, so the guard suppresses
        // the prefilter rather than nulling their columns out.
        await CaseAsync(
            "AND: three fields, one selective",
            "none (pivot not covered)",
            redb.TreeQuery<EmployeeProps>(root.Id)
                .Where(e => e.Position != "" && e.Age >= 30 && e.Salary > 70000m));

        // Null check switches the CTE to the LEFT JOIN form, where dropping rows would blur
        // "absent" and "present but not matching".
        await CaseAsync(
            "AND: null check plus selective field",
            "none (hasNullCheck)",
            redb.TreeQuery<EmployeeProps>(root.Id)
                .Where(e => e.EmployeeCode == null && e.Salary > 70000m));

        string? outPath = null;
        try
        {
            var dir = Path.Combine(AppContext.BaseDirectory, "Output");
            Directory.CreateDirectory(dir);
            outPath = Path.Combine(dir, enabled ? "tree_prefilter_on.txt" : "tree_prefilter_off.txt");
            await File.WriteAllTextAsync(outPath, log.ToString());
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[WARN] failed to write dump file: {ex.Message}");
        }

        sw.Stop();

        var output = new List<string>(summary)
        {
            $"Prefilter: {(enabled ? "ENABLED" : "disabled")}, applied in {withPrefilter} of 5 cases",
            outPath != null ? $"Output: {outPath}" : "Output file: <not written>",
            "Diff tree_prefilter_off.txt against tree_prefilter_on.txt: counts must match exactly"
        };

        return Ok("E204", "PVT prefilter in tree queries", ExampleTier.Pro,
            sw.ElapsedMilliseconds, 5, output.ToArray());
    }
}
