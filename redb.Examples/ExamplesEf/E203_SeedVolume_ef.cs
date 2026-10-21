using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Ef.Entities;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E203_SeedVolume"/>: seeds the same employees, arranged as the same reporting
/// tree, into the flat database, with EF Core's own means only.
/// <para>
/// Same generator (<c>E000_BulkInsert.CreateEmployees</c>, deterministic), same batch size, same tree
/// (every manager has <see cref="DirectReports"/> direct reports), same environment variables. What is
/// EF's own: the business class is mapped to the entity graph by hand (<see cref="EmployeeEfMapping"/>),
/// each batch is one unit of work in a new context with change tracking on, written by
/// <c>SaveChangesAsync</c>; keys come from the database sequence. No bulk copy, no raw SQL in the
/// measured part.
/// </para>
/// <code>
///   dotnet run --project redb.Examples -- E203_ef
///   REDB_SEED_COUNT=20000 dotnet run --project redb.Examples -- E203_ef
/// </code>
/// <para>
/// Like E203, it seeds only when named on the command line, appends and never clears.
/// </para>
/// </summary>
[ExampleMeta("E203_ef", "EF: seed volume, employees as a reporting tree", "Setup",
    ExampleTier.Pro, 200, "Setup", "BulkInsert", "Tree", "Benchmark", "EF")]
public class E203_SeedVolume_ef : EfExampleBase
{
    /// <summary>How many people report to one manager.</summary>
    private const int DirectReports = 10;

    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        const string id = "E203_ef";
        const string title = "EF: seed volume, employees as a reporting tree";

        var isExplicitRun = ExplicitlyRequestedIds?.Contains("E203_EF") == true;
        var total = ReadInt("REDB_SEED_COUNT", isExplicitRun ? 100_000 : 0);
        var batchSize = ReadInt("REDB_SEED_BATCH", 1_000);

        if (total == 0)
        {
            return Ok(id, title, ExampleTier.Pro, 0,
            [
                "Skipped: the whole-suite sweep never seeds.",
                "To seed: dotnet run --project redb.Examples -- E203_ef   (default 100000)",
                "REDB_SEED_COUNT overrides the size, REDB_SEED_BATCH the batch (default 1000)."
            ]);
        }

        if (total < 0 || batchSize <= 0)
            return Fail(id, title, ExampleTier.Pro, 0,
                "REDB_SEED_COUNT must not be negative and REDB_SEED_BATCH must be positive");

        // Continue numbering after whatever is already there, as E203 does.
        int existing;
        await using (var db = await contexts.CreateDbContextAsync())
            existing = await db.EmployeeProps.CountAsync();

        Console.WriteLine();
        Console.WriteLine($"Seeding {total:N0} employees through EF Core in batches of {batchSize:N0}, {DirectReports} direct reports per manager");
        Console.WriteLine($"Already in the flat database: {existing:N0}. New objects start at index {existing:N0}.");
        Console.WriteLine();

        var sw = Stopwatch.StartNew();

        // As E203 sets redb's plain indexes aside before its load, the developer's indexes go before this
        // one; they are created again after it, inside the measured time.
        await DropDeveloperIndexesAsync(contexts);

        // Header key of every seeded position, known once its batch is saved. A manager always comes
        // earlier than the reports, so it is already saved or sits in the same batch.
        var keys = new long[total];

        var batchTimer = new Stopwatch();
        var seeded = 0;
        var batches = 0;

        while (seeded < total)
        {
            var take = Math.Min(batchSize, total - seeded);

            batchTimer.Restart();
            var employees = E000_BulkInsert.CreateEmployees(take, existing + seeded);

            // One unit of work per batch, in a context of its own: a context that tracked every batch
            // would slow down with each SaveChanges.
            await using var db = await contexts.CreateDbContextAsync();

            var headers = new objects[take];
            for (var i = 0; i < take; i++)
            {
                var position = seeded + i;
                var header = EmployeeEfMapping.ToEntity(employees[i]);

                if (position > 0)
                {
                    var managerPosition = (position - 1) / DirectReports;
                    if (managerPosition >= seeded)
                        header._id_parentNavigation = headers[managerPosition - seeded]; // same batch: EF orders the inserts
                    else
                        header._id_parent = keys[managerPosition];                       // an earlier batch: key is known
                }

                headers[i] = header;
            }

            db.objects.AddRange(headers);
            await db.SaveChangesAsync();

            for (var i = 0; i < take; i++)
                keys[seeded + i] = headers[i]._id;

            batchTimer.Stop();

            seeded += take;
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

        // The developer's indexes for the EF twins' queries (Ef/developer_indexes.sql), built after the seed and
        // inside the measured time, as E203 rebuilds redb's indexes after its seed.
        await CreateDeveloperIndexesAsync(contexts);

        sw.Stop();

        int finalCount;
        await using (var db = await contexts.CreateDbContextAsync())
        {
            finalCount = await db.EmployeeProps.CountAsync();

            // Same step as E203, outside the measured time. EF has no API to refresh planner statistics,
            // so this one maintenance statement goes as SQL; it is not a query and not part of the seed.
            Console.WriteLine();
            Console.WriteLine("Refreshing planner statistics (ANALYZE)...");
            await db.Database.ExecuteSqlRawAsync("ANALYZE");
        }

        var rate = seeded * 1000.0 / Math.Max(sw.ElapsedMilliseconds, 1);

        return Ok(id, title, ExampleTier.Pro, sw.ElapsedMilliseconds, seeded,
            [
                $"Seeded {seeded:N0} in {batches:N0} batches of {batchSize:N0}",
                $"Rate: {rate:F0} obj/sec | flat database now holds {finalCount:N0} employees",
                $"Reporting tree head: objects._id {keys[0]}"
            ]);
    }

    /// <summary>The statements of Ef/developer_indexes.sql, one per line; lines starting with -- are skipped.</summary>
    private static List<string> DeveloperIndexStatements()
    {
        var path = Path.Combine(AppContext.BaseDirectory, "Ef", "developer_indexes.sql");
        return File.ReadAllLines(path)
            .Select(line => line.Trim())
            .Where(line => line.Length > 0 && !line.StartsWith("--", StringComparison.Ordinal))
            .ToList();
    }

    /// <summary>
    /// Drops the indexes Ef/developer_indexes.sql creates, when they are there: the name is the quoted
    /// identifier after <c>CREATE INDEX</c>.
    /// </summary>
    private static async Task DropDeveloperIndexesAsync(IDbContextFactory<FlatContext> contexts)
    {
        var names = DeveloperIndexStatements()
            .Where(s => s.StartsWith("CREATE INDEX \"", StringComparison.OrdinalIgnoreCase))
            .Select(s => s[(s.IndexOf('"') + 1)..])
            .Select(rest => rest[..rest.IndexOf('"')])
            .ToList();

        Console.WriteLine($"Dropping {names.Count} developer indexes before the load (Ef/developer_indexes.sql)...");

        await using var db = await contexts.CreateDbContextAsync();
        foreach (var name in names)
        {
            var drop = "DROP INDEX IF EXISTS \"" + name.Replace("\"", "\"\"") + "\"";
            await db.Database.ExecuteSqlRawAsync(drop);
        }
    }

    /// <summary>
    /// Runs Ef/developer_indexes.sql. EF has no API for indexes outside migrations, so the script goes as SQL:
    /// schema maintenance, not a query.
    /// </summary>
    private static async Task CreateDeveloperIndexesAsync(IDbContextFactory<FlatContext> contexts)
    {
        var statements = DeveloperIndexStatements();

        Console.WriteLine();
        Console.WriteLine($"Creating {statements.Count} developer indexes (Ef/developer_indexes.sql)...");
        var timer = Stopwatch.StartNew();

        await using var db = await contexts.CreateDbContextAsync();
        foreach (var statement in statements)
            await db.Database.ExecuteSqlRawAsync(statement);

        timer.Stop();
        Console.WriteLine($"Developer indexes created in {timer.ElapsedMilliseconds:N0} ms");
    }

    private static int ReadInt(string name, int fallback)
    {
        var raw = Environment.GetEnvironmentVariable(name);
        return int.TryParse(raw, out var parsed) ? parsed : fallback;
    }
}
