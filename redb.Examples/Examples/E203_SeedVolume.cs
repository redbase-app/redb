using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Seeds Employee objects in batches so the PVT prefilter can be measured on real volume.
/// See <c>docs/PVT_PREFILTER_PLAN.md</c> and <c>docs/PVT_PREFILTER_EXPLAIN.sql</c>.
///
/// Below roughly 10^5 objects in a scheme every plan collapses to the same shape, pg_trgm
/// stays unused and sub-millisecond timings measure cache warmth rather than work. This
/// example exists to get past that floor.
///
/// <code>
///   REDB_SEED_COUNT=100000 REDB_SEED_BATCH=1000 dotnet run --project redb.Examples -- E203
/// </code>
///
/// <b>The whole-suite sweep never seeds.</b> Seeding runs only when the example is NAMED on the
/// command line (<c>dotnet run -- E203</c>, default 100000) - being reached by the sweep is not a
/// request for a twenty-minute, six-million-row write. REDB_SEED_COUNT overrides the size in
/// either mode (and 0 disables even an explicit run); batches default to 1000.
/// Objects reuse the exact shape E000 builds
/// (~60 _values rows each, arrays, nested classes, dictionaries), with an index offset so
/// batches do not collide. 100000 employees therefore land around 6M rows in _values.
///
/// This example WRITES a lot. It appends, it never clears, so running it twice doubles the
/// data. The tail refreshes planner statistics via <c>redb.Maintenance.AnalyzeAsync()</c> -
/// after a bulk load, without that refresh every estimate you read is fiction.
/// </summary>
[ExampleMeta("E203", "Seed volume for prefilter measurement", "Setup",
    ExampleTier.Pro, 200, "Setup", "BulkInsert", "Prefilter", "Benchmark",
    RelatedApis = ["IRedbService.SaveAsync"])]
public class E203_SeedVolume : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        // Seeding volume is a decision, not a side effect. Being NAMED on the command line is
        // that decision (dotnet run -- E203, default 100000); being reached by the whole-suite
        // sweep is not - the sweep always skips. REDB_SEED_COUNT overrides the size in either
        // mode, and 0 disables even an explicit run.
        var isExplicitRun = ExplicitlyRequestedIds?.Contains("E203") == true;
        var total = ReadInt("REDB_SEED_COUNT", isExplicitRun ? 100_000 : 0);
        var batchSize = ReadInt("REDB_SEED_BATCH", 1_000);

        if (total == 0)
        {
            return Ok("E203", "Seed volume for prefilter measurement", ExampleTier.Pro, 0,
            [
                "Skipped: the whole-suite sweep never seeds.",
                "To seed: dotnet run --project redb.Examples -- E203   (default 100000)",
                "REDB_SEED_COUNT overrides the size, REDB_SEED_BATCH the batch (default 1000).",
                "100000 employees land around 6M rows in _values and take ~20 minutes.",
                "It appends and never clears, so running it twice doubles the data."
            ]);
        }

        if (total < 0 || batchSize <= 0)
            return Fail("E203", "Seed volume for prefilter measurement", ExampleTier.Pro, 0,
                "REDB_SEED_COUNT must not be negative and REDB_SEED_BATCH must be positive");

        // Continue numbering after whatever is already there, so identities stay unique
        // across runs and the batches of a single run.
        var existing = await redb.Query<EmployeeProps>().CountAsync();

        Console.WriteLine();
        Console.WriteLine($"Seeding {total:N0} employees in batches of {batchSize:N0}");
        Console.WriteLine($"Already in scheme: {existing:N0}. New objects start at index {existing:N0}.");
        Console.WriteLine($"Expect roughly {total * 60L:N0} rows in _values. This appends, it never clears.");
        Console.WriteLine();

        var sw = Stopwatch.StartNew();
        var batchTimer = new Stopwatch();
        var seeded = 0;
        var batches = 0;

        while (seeded < total)
        {
            var take = Math.Min(batchSize, total - seeded);

            batchTimer.Restart();
            var employees = E000_BulkInsert.CreateEmployees(take, existing + seeded);
            var saved = await redb.SaveAsync(employees);
            batchTimer.Stop();

            seeded += saved.Count;
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

            // A short batch means the save silently did less than asked; stop rather than spin.
            if (saved.Count == 0)
            {
                Console.WriteLine("[WARN] batch saved 0 objects, aborting to avoid an endless loop");
                break;
            }
        }

        sw.Stop();

        var finalCount = await redb.Query<EmployeeProps>().CountAsync();
        var rate = seeded * 1000.0 / Math.Max(sw.ElapsedMilliseconds, 1);

        // A bulk load leaves the planner's statistics behind reality - every estimate (and
        // every EXPLAIN you would trust) is fiction until they are refreshed. The Maintenance
        // facade runs the provider's own incantation (ANALYZE / sp_updatestats / PRAGMA
        // analysis_limit + ANALYZE), so the seed hands back a database ready to be measured.
        Console.WriteLine();
        Console.WriteLine("Refreshing planner statistics (redb.Maintenance.AnalyzeAsync)...");
        var analyzeSw = Stopwatch.StartNew();
        await redb.Maintenance.AnalyzeAsync();
        analyzeSw.Stop();
        Console.WriteLine();

        return Ok("E203", "Seed volume for prefilter measurement", ExampleTier.Pro,
            sw.ElapsedMilliseconds, seeded,
            [
                $"Seeded {seeded:N0} in {batches:N0} batches of {batchSize:N0}",
                $"Rate: {rate:F0} obj/sec | scheme now holds {finalCount:N0} employees",
                $"Planner statistics refreshed: Maintenance.AnalyzeAsync in {analyzeSw.ElapsedMilliseconds:N0} ms"
            ]);
    }

    private static int ReadInt(string name, int fallback)
    {
        var raw = Environment.GetEnvironmentVariable(name);
        return int.TryParse(raw, out var parsed) ? parsed : fallback;
    }
}
