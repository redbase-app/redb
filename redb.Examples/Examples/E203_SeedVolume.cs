using System.Diagnostics;
using redb.Core;
using redb.Core.Models.Contracts;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Seeds a large company: Employee objects arranged as a reporting tree.
///
/// Every employee but the first reports to a manager, and every manager has
/// <see cref="DirectReports"/> direct reports, so 100000 employees form a tree six levels deep.
/// The same objects answer both kinds of query:
///
/// <code>
///   redb.Query&lt;EmployeeProps&gt;().Where(e => e.Salary > 90000)              // everyone
///   redb.TreeQuery&lt;EmployeeProps&gt;(managerId).Where(e => e.Salary > 90000)  // one manager's people
/// </code>
///
/// <code>
///   dotnet run --project redb.Examples -- E203
///   REDB_SEED_COUNT=20000 dotnet run --project redb.Examples -- E203
/// </code>
///
/// <b>The whole-suite sweep never seeds.</b> Seeding runs only when the example is NAMED on the
/// command line (default 100000 employees). REDB_SEED_COUNT overrides the size (0 disables even an
/// explicit run), REDB_SEED_BATCH the batch (default 1000).
///
/// Objects have the shape E000 builds: about 60 _values rows each, with arrays, nested classes
/// and dictionaries, so 100000 employees land around 6M rows in _values.
///
/// This example WRITES a lot. It appends and never clears: every run adds one more company with
/// its own head, so running it twice doubles the data. It ends by refreshing planner statistics
/// through <c>redb.Maintenance.AnalyzeAsync()</c>; after a bulk load the planner's estimates are
/// stale until that is done.
/// </summary>
[ExampleMeta("E203", "Seed volume: employees as a reporting tree", "Setup",
    ExampleTier.Pro, 200, "Setup", "BulkInsert", "Tree", "Benchmark",
    RelatedApis = ["IRedbService.AddNewObjectsAsync", "IRedbContext.NextObjectIdBatchAsync"])]
public class E203_SeedVolume : ExampleBase
{
    /// <summary>How many people report to one manager.</summary>
    private const int DirectReports = 10;

    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        // Seeding volume is a decision, not a side effect. Being NAMED on the command line is
        // that decision; being reached by the whole-suite sweep is not - the sweep always skips.
        var isExplicitRun = ExplicitlyRequestedIds?.Contains("E203") == true;
        var total = ReadInt("REDB_SEED_COUNT", isExplicitRun ? 100_000 : 0);
        var batchSize = ReadInt("REDB_SEED_BATCH", 1_000);

        if (total == 0)
        {
            return Ok("E203", "Seed volume: employees as a reporting tree", ExampleTier.Pro, 0,
            [
                "Skipped: the whole-suite sweep never seeds.",
                "To seed: dotnet run --project redb.Examples -- E203   (default 100000)",
                "REDB_SEED_COUNT overrides the size, REDB_SEED_BATCH the batch (default 1000).",
                "100000 employees land around 6M rows in _values and take ~20 minutes.",
                "It appends and never clears, so running it twice doubles the data."
            ]);
        }

        if (total < 0 || batchSize <= 0)
            return Fail("E203", "Seed volume: employees as a reporting tree", ExampleTier.Pro, 0,
                "REDB_SEED_COUNT must not be negative and REDB_SEED_BATCH must be positive");

        // Continue numbering after whatever is already there, so names and codes stay unique
        // across runs.
        var existing = await redb.Query<EmployeeProps>().CountAsync();

        Console.WriteLine();
        Console.WriteLine($"Seeding {total:N0} employees in batches of {batchSize:N0}, {DirectReports} direct reports per manager");
        Console.WriteLine($"Already in scheme: {existing:N0}. New objects start at index {existing:N0}.");
        Console.WriteLine($"Expect roughly {total * 60L:N0} rows in _values. This appends, it never clears.");
        Console.WriteLine();

        var sw = Stopwatch.StartNew();

        // All ids up front: a manager's id has to be known before the reports are created.
        var ids = await redb.Context.NextObjectIdBatchAsync(total);

        var batchTimer = new Stopwatch();
        var seeded = 0;
        var batches = 0;

        while (seeded < total)
        {
            var take = Math.Min(batchSize, total - seeded);

            batchTimer.Restart();
            var employees = E000_BulkInsert.CreateEmployees(take, existing + seeded);
            for (var i = 0; i < employees.Count; i++)
            {
                // Position n reports to position (n - 1) / DirectReports; position 0 heads the
                // company. A manager always comes earlier than the reports, so it is already saved
                // or sits in the same batch.
                var position = seeded + i;
                employees[i].id = ids[position];
                employees[i].parent_id = position == 0 ? null : ids[(position - 1) / DirectReports];
            }

            await redb.AddNewObjectsAsync(employees.Cast<IRedbObject<EmployeeProps>>().ToList());
            batchTimer.Stop();

            seeded += employees.Count;
            batches++;

            var pct = seeded * 100.0 / total;
            var overallRate = seeded * 1000.0 / Math.Max(sw.ElapsedMilliseconds, 1);
            var remaining = overallRate > 0
                ? TimeSpan.FromSeconds((total - seeded) / overallRate)
                : TimeSpan.Zero;

            Console.WriteLine(
                $"[{seeded,8:N0}/{total:N0}] {pct,5:F1}%  " +
                $"batch {batchTimer.ElapsedMilliseconds,6:N0} ms  " +
                $"avg {overallRate,7:F0} obj/s  " +
                $"elapsed {sw.Elapsed:hh\\:mm\\:ss}  " +
                $"ETA {remaining:hh\\:mm\\:ss}");
        }

        sw.Stop();

        var finalCount = await redb.Query<EmployeeProps>().CountAsync();
        var rate = seeded * 1000.0 / Math.Max(sw.ElapsedMilliseconds, 1);

        // A bulk load leaves the planner's statistics behind reality. The Maintenance facade runs
        // the provider's own refresh (ANALYZE / sp_updatestats / PRAGMA analysis_limit + ANALYZE),
        // so the seed hands back a database that is ready to be queried and measured.
        Console.WriteLine();
        Console.WriteLine("Refreshing planner statistics (redb.Maintenance.AnalyzeAsync)...");
        var analyzeSw = Stopwatch.StartNew();
        await redb.Maintenance.AnalyzeAsync();
        analyzeSw.Stop();
        Console.WriteLine();

        var levels = 1;
        for (long reach = 1, width = 1; reach < total; levels++)
        {
            width *= DirectReports;
            reach += width;
        }

        return Ok("E203", "Seed volume: employees as a reporting tree", ExampleTier.Pro,
            sw.ElapsedMilliseconds, seeded,
            [
                $"Seeded {seeded:N0} in {batches:N0} batches of {batchSize:N0}",
                $"Rate: {rate:F0} obj/sec | scheme now holds {finalCount:N0} employees",
                $"Reporting tree: {levels} levels, head of the company id {ids[0]}",
                $"Try: redb.TreeQuery<EmployeeProps>({ids[0]}) and redb.Query<EmployeeProps>()",
                $"Planner statistics refreshed: Maintenance.AnalyzeAsync in {analyzeSw.ElapsedMilliseconds:N0} ms"
            ]);
    }

    private static int ReadInt(string name, int fallback)
    {
        var raw = Environment.GetEnvironmentVariable(name);
        return int.TryParse(raw, out var parsed) ? parsed : fallback;
    }
}
