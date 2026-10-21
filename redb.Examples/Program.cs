using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Npgsql.EntityFrameworkCore.PostgreSQL.Infrastructure;
using redb.Examples.Ef;
using redb.Examples.ExamplesEf;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Pro.Extensions;
using redb.Postgres.Pro.Extensions;
using redb.SQLite.Pro.Extensions;        // UseSqlite is tier-agnostic: AddRedb -> Free, AddRedbPro -> Pro
                                         // (do NOT also import redb.SQLite.Extensions — its UseSqlite
                                         //  would make the call ambiguous; switch tiers via AddRedb/AddRedbPro)
using redb.SQLite.Data;                  // SqliteDataSource.NativeExtensionPath (Free native ext)
using redb.Examples.Examples;
using redb.Examples.Output;
using redb.Core.Models.Configuration;
using redb.Core.Models.Entities;
using redb.Core.Models.Security;
using redb.Postgres;
using redb.Core.Extensions;
using redb.MSSql.Pro.Extensions;
using redb.Postgres.Providers;
using redb.Core.Providers;
using redb.Core.Serialization;
using redb.Examples.Models;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace redb.Examples;

class Program
{
    // Test license for Pro features
    //private const string TestLicense = "...";

    /// <summary>
    /// Results storage, a SQLite file of its own (created on first use), whatever provider the examples run
    /// on: one object per example, every platform and pass of every run under its key, so results of
    /// different runs and databases sit side by side.
    /// </summary>
    private const string ResultsConnection = "Data Source=redb_examples_results.db";

    static async Task Main(string[] args)
    {
        Console.OutputEncoding = System.Text.Encoding.UTF8;

        var services = new ServiceCollection();
        ConfigureServices(services);

        await using var provider = services.BuildServiceProvider();

        // One scope per unit of work. An IRedbService is one connection: a service kept for the life of the process

        // cannot serve parallel flows (a web request, a job, a message handler each resolve their own from a scope).

        // A console does its work in one flow, so the scope here only shows the shape that carries over to hosts.

        await using var scope = provider.CreateAsyncScope();

        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();

        // Get DB type and platform key
        var ctxName = redb.Context.GetType().Name;
        var dbType = ctxName.Contains("Npgsql") ? "PostgreSQL"
            : ctxName.Contains("Sqlite") ? "SQLite"   // check before "Sql": "Sqlite".Contains("Sql") == true
            : "SQL Server";
        var platformKey = GetPlatformKey(redb);

        // --results [--platform <key>] [E...]: print the saved results and run nothing. The platform decides which
        // passes come first and what EF is compared with; by default the configured provider's, as in a run.
        if (args.Contains("--results", StringComparer.OrdinalIgnoreCase))
        {
            var platformAt = Array.FindIndex(args, a => a.Equals("--platform", StringComparison.OrdinalIgnoreCase));
            if (platformAt >= 0 && platformAt + 1 < args.Length)
                platformKey = args[platformAt + 1].ToLowerInvariant();

            var requested = args.Where(a => a.StartsWith('E') || a.StartsWith('e')).Select(a => BaseId(a.ToUpperInvariant())).ToHashSet();
            Console.WriteLine($"Saved results ({ResultsConnection}) | Platform: {platformKey}");
            await WithResultsDbAsync(resultsDb => PrintSavedResultsAsync(resultsDb, requested.Count > 0 ? requested : null, platformKey));
            return;
        }

        Console.WriteLine($"DB: {dbType} | Platform: {platformKey} | redb.Examples");
        Console.WriteLine($"redb {VersionOf(typeof(IRedbService))} | EF Core {VersionOf(typeof(DbContext))} | " +
                          $"Npgsql EF {VersionOf(typeof(NpgsqlDbContextOptionsBuilder))} | Npgsql {VersionOf(typeof(NpgsqlConnection))}");
        Console.WriteLine();

        // Initialize REDB for tests
        Console.WriteLine("=== REDB INITIALIZATION ===");
        var initSw = System.Diagnostics.Stopwatch.StartNew();
        await redb.InitializeAsync(ensureCreated:true);

        initSw.Stop();
        Console.WriteLine($"Initialization complete: {initSw.ElapsedMilliseconds} ms");
        Console.WriteLine();

        // Discover examples
        var examples = DiscoverExamples();

        // Parse args (stray "-" tokens tolerated: `dotnet run --no-build - E203` forwards the
        // lone dash to the app)
        var exampleIds = args.Length > 0
            ? args.Select(a => a.ToUpperInvariant()).Where(a => a.StartsWith('E')).ToHashSet()
            : null;
        if (exampleIds is { Count: 0 }) exampleIds = null;

        // Examples that behave differently when asked for by name (E203 seeds only then).
        ExampleBase.ExplicitlyRequestedIds = exampleIds;

        // redb examples and their EF Core twins (*_ef). Naming an example runs its twin with it; naming a
        // twin runs the twin alone (E203_ef seeds the flat database by itself).
        bool Selected(ExampleMetaAttribute meta) =>
            exampleIds == null
            || exampleIds.Contains(meta.Id.ToUpperInvariant())
            || exampleIds.Contains(BaseId(meta.Id).ToUpperInvariant());

        var redbToRun = examples.Where(e => e.example is not EfExampleBase && Selected(e.meta!)).ToList();
        var efToRun = examples.Where(e => e.example is EfExampleBase && Selected(e.meta!)).ToList();

        // The EF twins work on the flat PostgreSQL database only: next to redb on another provider their
        // numbers would compare different databases, so they do not run.
        if (efToRun.Count > 0 && !RunsOnEfProvider(platformKey))
        {
            Console.WriteLine($"EF Core twins skipped: they run on PostgreSQL only ({EfPlatformKey}), redb runs on {platformKey}.");
            Console.WriteLine();
            efToRun.Clear();
        }

        if (redbToRun.Count == 0 && efToRun.Count == 0)
        {
            Console.WriteLine("No examples to run. Available:");
            foreach (var (ex, meta) in examples.Where(e => e.meta != null))
                Console.WriteLine($"  {meta!.Id} - {meta.Title} [{meta.Tier}]");
            return;
        }

        // EF Core twins work on the flat database. Like redb.InitializeAsync above, the EF model is built
        // and a pooled connection opened here, before any example is timed. Only when a twin is selected,
        // so a redb-only run does not need the flat database.
        if (efToRun.Count > 0)
        {
            var efContexts = scope.ServiceProvider.GetRequiredService<IDbContextFactory<FlatContext>>();
            EfExampleBase.Contexts = efContexts;

            Console.WriteLine("=== EF CORE INITIALIZATION ===");
            var efInitSw = System.Diagnostics.Stopwatch.StartNew();
            await using (var warm = await efContexts.CreateDbContextAsync())
            {
                _ = warm.Model;
                await warm.Database.OpenConnectionAsync();
                await warm.Database.CloseConnectionAsync();
            }
            efInitSw.Stop();
            Console.WriteLine($"EF Core initialization complete: {efInitSw.ElapsedMilliseconds} ms");
            Console.WriteLine();
        }

        // Round trips are counted on both sides: redb through its ActivitySource, EF through an interceptor.
        CommandCounters.StartRedb();

        var efKey = EfPlatformKey;
        var runs = new List<(string Platform, ExampleResult Result)>();

        // Pass 1: redb, default settings. Pass 2: EF Core.
        await RunPassAsync(platformKey, redbToRun, redb, () => CommandCounters.Redb, runs);
        await RunPassAsync(efKey, efToRun, redb, () => CommandCounters.Ef, runs);

        // Passes 3 to 5 repeat the same examples. Setup examples (seeding, bulk insert) are not repeated:
        // they would only add the same data again.
        var redbRepeat = redbToRun.Where(e => e.meta!.Category != "Setup").ToList();
        var efRepeat = efToRun.Where(e => e.meta!.Category != "Setup").ToList();

        if (redbRepeat.Count > 0)
        {
            // redb with the props cache: a service of its own, initialized outside the measurement. Its
            // first pass is a repeat that fills the cache, its second pass reads through it.
            var cacheServices = new ServiceCollection();
            ConfigureServices(cacheServices, enablePropsCache: true);
            await using var cacheProvider = cacheServices.BuildServiceProvider();
            await using var cacheScope = cacheProvider.CreateAsyncScope();
            var cachedRedb = cacheScope.ServiceProvider.GetRequiredService<IRedbService>();
            await cachedRedb.InitializeAsync();

            await RunPassAsync($"{platformKey}.repeat", redbRepeat, cachedRedb, () => CommandCounters.Redb, runs);
            await RunPassAsync($"{platformKey}.cache", redbRepeat, cachedRedb, () => CommandCounters.Redb, runs);
        }

        await RunPassAsync($"{efKey}.repeat", efRepeat, redb, () => CommandCounters.Ef, runs);

        Console.WriteLine();

        // The direct table of the run, kept for reference; the results now come from the results database.
        // PrintResults(runs.Where(r => r.Platform == platformKey).Select(r => r.Result).ToList());

        // Save results to the separate results database and print them from there
        await WithResultsDbAsync(async resultsDb =>
        {
            await SaveResultsAsync(resultsDb, runs);
            await PrintSavedResultsAsync(resultsDb, runs.Select(r => BaseId(r.Result.Id)).ToHashSet(), platformKey);
        });
    }

    /// <summary>Opens the results storage (<see cref="ResultsConnection"/>, created on first use) for one unit of work.</summary>
    private static async Task WithResultsDbAsync(Func<IRedbService, Task> work)
    {
        var resultsServices = new ServiceCollection();
        resultsServices.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        resultsServices.AddRedbPro(options => options.WithLicense("").UseSqlite(ResultsConnection));
        await using var resultsProvider = resultsServices.BuildServiceProvider();
        await using var resultsScope = resultsProvider.CreateAsyncScope();
        var resultsDb = resultsScope.ServiceProvider.GetRequiredService<IRedbService>();
        await resultsDb.InitializeAsync(ensureCreated: true);
        await work(resultsDb);
    }

    /// <summary>
    /// Runs one pass over <paramref name="toRun"/> and adds each result, with the round trips it made, to
    /// <paramref name="runs"/> under <paramref name="platform"/>.
    /// </summary>
    private static async Task RunPassAsync(
        string platform,
        List<(ExampleBase example, ExampleMetaAttribute? meta)> toRun,
        IRedbService redb,
        Func<long> commands,
        List<(string Platform, ExampleResult Result)> runs)
    {
        if (toRun.Count == 0)
            return;

        Console.WriteLine($"=== PASS {platform} ===");
        var current = 0;

        foreach (var (example, meta) in toRun.OrderBy(e => e.meta?.Id))
        {
            if (meta == null) continue;
            current++;

            // Progress indicator
            Console.Write($"[{current}/{toRun.Count}] {meta.Id} {meta.Title}...");

            var before = commands();
            try
            {
                var result = await example.RunAsync(redb);
                runs.Add((platform, result with { Queries = result.NotSupported ? null : (int)(commands() - before) }));
                Console.WriteLine(result.NotSupported ? " N/A" : result.Success ? " OK" : " FAIL");
            }
            catch (Exception ex)
            {
                runs.Add((platform, new ExampleResult
                {
                    Id = meta.Id,
                    Title = meta.Title,
                    Tier = meta.Tier,
                    Success = false,
                    ElapsedMs = 0,
                    Output = [],
                    Error = ex.Message
                }));
                Console.WriteLine($" ERROR: {ex.Message}");
            }
        }

        Console.WriteLine();
    }

    /// <summary>The id of the redb example a twin belongs to: <c>E010_ef</c> gives <c>E010</c>.</summary>
    private static string BaseId(string id) => IsTwin(id) ? id[..^3] : id;

    private static bool IsTwin(string id) => id.EndsWith("_ef", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Platform key of the EF Core passes. The EF context works on the flat PostgreSQL database
    /// (<see cref="FlatEfConnection"/>) whatever provider redb runs on, so the key names that database.
    /// </summary>
    private const string EfPlatformKey = "postgres.ef";

    /// <summary>
    /// The kind of a pass from its platform key: <c>first</c> for <c>postgres.pro</c> or <c>postgres.ef</c>,
    /// otherwise the last part (<c>repeat</c>, <c>cache</c>).
    /// </summary>
    private static string PassKind(string platformKey)
    {
        var parts = platformKey.Split('.');
        return parts.Length > 2 ? parts[^1] : "first";
    }

    /// <summary>Whether redb runs on the provider of the EF twins, so that the two sides are comparable.</summary>
    private static bool RunsOnEfProvider(string platformKey) =>
        platformKey.StartsWith("postgres.", StringComparison.OrdinalIgnoreCase);


    /// <summary>
    /// The flat database of the EF Core twins (redb.Export/doc/PLAN_EF_BENCHMARK.md): the schema from
    /// `redb flat --ddl-only`, the data seeded by EF itself (E203_ef). Same server and session options as
    /// the redb database below, so both sides run under the same settings.
    /// </summary>
    private const string FlatEfConnection =
        "Host=localhost;Port=5432;Username=postgres;Password=1;Database=redb_examples_flat_ef;Pooling=true;Timeout=600;Command Timeout=600;Include Error Detail=true;Options=-c jit=off";

    /// <summary>Product version of the assembly that defines <paramref name="type"/>, without the commit suffix.</summary>
    private static string VersionOf(Type type) =>
        type.Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion.Split('+')[0]
        ?? type.Assembly.GetName().Version?.ToString()
        ?? "?";

    /// <param name="services">Container to fill.</param>
    /// <param name="enablePropsCache">Turns redb's props cache on: the pass that shows what the cache gives.</param>
    private static void ConfigureServices(ServiceCollection services, bool enablePropsCache = false)
    {
        // EF Core side: a context factory, so every EF unit of work gets a new context, as EF recommends.
        // The interceptor only counts round trips for the comparison table.
        services.AddDbContextFactory<FlatContext>(options => options
            .UseNpgsql(FlatEfConnection)
            .AddInterceptors(CommandCounters.EfInterceptor));

        services.AddLogging(b => b
            .AddConsole()
            .SetMinimumLevel(LogLevel.Warning));

        // FREE SQLite needs the native extension loaded on every connection (it hosts
        // get_object_json / pvt_build_*_sql in-DB). Make the example run out-of-the-box:
        // honor REDB_SQLITE_EXTENSION if it's set (that's the env default), otherwise
        // locate redb.SQLite/native/build/redbsqlite.{dll,so,dylib} by walking up from the app
        // base dir. (Harmless for the Pro run — Pro never calls those functions.)
        SqliteDataSource.NativeExtensionPath ??= ResolveSqliteNativeExtension();

        // SQLite reads through a 2 MB page cache per connection by default; on the seeded examples
        // database (2.5 GB) that is what most of a query's time goes into. Map the file instead,
        // and give each connection a moderate cache on top of the mapping.
        SqliteDataSource.MmapSizeBytes = 4L * 1024 * 1024 * 1024;
        SqliteDataSource.CacheSizeBytes = 64L * 1024 * 1024;

        // Pro is free from 3.3.0 — no license key required for the entire 3.x line.
        // Empty string = no key; set a JWT here only against a 4.x+ build that re-enables licensing.
        var TestLicense=@"";
        services.AddRedbPro(options => options
            .WithLicense(TestLicense)
            .Configure(c =>
            {
                c.PropsSaveStrategy = PropsSaveStrategy.ChangeTracking; // FREE tier: DeleteInsert (ChangeTracking is a Pro feature). Flip to ChangeTracking for the Pro run.
                //c.SkipHashValidationOnCacheCheck = false;
                c.EnablePropsCache = enablePropsCache;
                c.PropsCacheMaxSize = 100000;
                //c.PropsCacheTtl = TimeSpan.FromMinutes(60);
                // PVT prefilter (Pro + PostgreSQL, opt-in). Flip to true and re-run E202 to
                // diff the SQL with and without the cutting step. See docs/PVT_PREFILTER_PLAN.md.
                c.EnablePvtPrefilter = true;// Environment.GetEnvironmentVariable("REDB_PVT_PREFILTER") == "1";
            })
            // Examples live in their OWN databases (redb_examples), never in the test/Identity
            // `redb`: the integration-test fixtures wipe their database CLEAN on start, and a
            // seeded 8M-row _values under the cascade trigger turns that wipe into hours
            // (2026-09-10: a seeded database was lost to exactly this).
            .UsePostgres("Host=localhost;Port=5432;Username=postgres;Password=1;Database=redb_examples;Pooling=true;Timeout=600;Command Timeout=600;Include Error Detail=true;Options=-c jit=off")
            //.UseMsSql("Server=127.0.0.1,1433;Database=redb_examples;User Id=sa;Password=1;TrustServerCertificate=true;Command Timeout=600;")  // 127.0.0.1 (not localhost): localhost->::1 hits docker [::]:1433 and hangs ~63s
            //.UseSqlite(@"Data Source=redb_examples.db")
            );
    }

    /// <summary>
    /// Locate the FREE SQLite native extension (redbsqlite.dll/.so/.dylib) by walking up the
    /// directory tree from the running app to the repo's redb.SQLite/native/build folder.
    /// Returns the full path (suffix included) or null if not found / not yet built.
    /// </summary>
    private static string? ResolveSqliteNativeExtension()
    {
        var suffix = OperatingSystem.IsWindows() ? ".dll"
                   : OperatingSystem.IsMacOS()   ? ".dylib"
                   : ".so";
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir != null; dir = dir.Parent)
        {
            var candidate = Path.Combine(dir.FullName, "redb.SQLite", "native", "build", "redbsqlite" + suffix);
            if (File.Exists(candidate))
                return candidate;
        }
        Console.WriteLine(
            "[WARN] FREE SQLite native extension not found (redb.SQLite/native/build/redbsqlite" + suffix +
            "). Build it (cmake --build redb.SQLite/native/build) or set REDB_SQLITE_EXTENSION. " +
            "Without it, get_object_json / pvt_build_*_sql are missing and most examples FAIL.");
        return null;
    }

    private static List<(ExampleBase example, ExampleMetaAttribute? meta)> DiscoverExamples()
    {
        return Assembly.GetExecutingAssembly()
            .GetTypes()
            .Where(t => t.IsSubclassOf(typeof(ExampleBase)) && !t.IsAbstract)
            .Select(t => (
                example: (ExampleBase)Activator.CreateInstance(t)!,
                meta: t.GetCustomAttribute<ExampleMetaAttribute>()
            ))
            .Where(e => e.meta != null)
            .OrderBy(e => e.meta!.Id)
            .ToList();
    }

    /// <summary>
    /// Determines current platform key based on database type and Pro/Free version.
    /// </summary>
    private static string GetPlatformKey(IRedbService redb)
    {
        var ctxName = redb.Context.GetType().Name;
        var isPro = redb.GetType().Assembly.FullName?.Contains(".Pro") == true;

        var db = ctxName.Contains("Npgsql") ? "postgres"
            : ctxName.Contains("Sqlite") ? "sqlite"   // check before "Sql": "Sqlite".Contains("Sql") == true
            : "mssql";
        return isPro ? $"{db}.pro" : db;
    }

    /// <summary>
    /// Saves or updates the results of this run in the results database, one object per example: its
    /// key (E001, E002...) in ValueString, its title in Name, every pass under its platform key. An EF
    /// twin (E010_ef) is stored with its redb example (E010), under the EF platform key, so both sides
    /// sit in one object. Other platforms already stored for an example are kept.
    /// </summary>
    private static async Task SaveResultsAsync(IRedbService redb, List<(string Platform, ExampleResult Result)> runs)
    {
        Console.WriteLine();
        Console.WriteLine("=== SAVING RESULTS ===");

        // Load all existing results at once (with Props)
        var existingResults = await redb.Query<ExampleResultProps>()
            .ToListAsync();

        var existingByKey = existingResults.ToDictionary(r => r.ValueString ?? "", r => r);

        var toSave = new List<RedbObject<ExampleResultProps>>();
        var saved = 0;
        var updated = 0;

        foreach (var example in runs.GroupBy(r => BaseId(r.Result.Id)))
        {
            // Title and tier come from the redb example when it ran, from the twin otherwise.
            var lead = example.FirstOrDefault(r => !IsTwin(r.Result.Id)).Result ?? example.First().Result;
            var title = IsTwin(lead.Id) && lead.Title.StartsWith("EF: ", StringComparison.Ordinal) ? lead.Title[4..] : lead.Title;
            var tierString = lead.Tier switch
            {
                ExampleTier.Free => "Free",
                ExampleTier.Pro => "Pro",
                ExampleTier.Enterprise => "Enterprise",
                _ => "Free"
            };

            if (!existingByKey.TryGetValue(example.Key, out var target))
            {
                target = new RedbObject<ExampleResultProps>
                {
                    Name = title,
                    ValueString = example.Key,
                    Props = new ExampleResultProps()
                };
                existingByKey[example.Key] = target;
                saved++;
            }
            else
            {
                updated++;
            }

            // Update existing object - preserve other platforms!
            target.Props ??= new ExampleResultProps();
            target.Props.Tier = tierString;
            target.Props.Results ??= new Dictionary<string, PlatformResult>();

            foreach (var (platform, result) in example)
            {
                target.Props.Results[platform] = new PlatformResult
                {
                    Count = result.Count,
                    Time = result.NotSupported ? null : (int)result.ElapsedMs,
                    Queries = result.Queries,
                    Status = result.NotSupported ? $"N/A: {result.Output.FirstOrDefault()}"
                        : result.Success ? "OK" : (result.Error ?? "FAIL")
                };
            }

            toSave.Add(target);
        }

        // Batch save all at once
        if (toSave.Count > 0)
        {
            await redb.SaveAsync(toSave);
        }

        Console.WriteLine($"Results saved: {saved} new, {updated} updated (batch)");
    }

    /// <summary>
    /// Loads the results of the given examples from the results database and prints them, each pass on a
    /// line of its own: redb, redb with the props cache, EF Core. An EF line also shows how its time
    /// compares with redb's (EF against the first redb pass, EF repeat against redb with the cache).
    /// </summary>
    private static async Task PrintSavedResultsAsync(IRedbService redb, IReadOnlySet<string>? exampleKeys, string platformKey)
    {
        Console.WriteLine();
        Console.WriteLine("=== RESULTS ===");

        // exampleKeys null: every saved example.
        var allResults = (await redb.Query<ExampleResultProps>()
                .OrderByRedb(r => r.ValueString)
                .ToListAsync())
            .Where(r => r.ValueString != null && (exampleKeys == null || exampleKeys.Contains(r.ValueString)))
            .ToList();

        if (allResults.Count == 0)
        {
            Console.WriteLine("No saved results in database.");
            return;
        }

        // Passes of this platform first, in the order they run; whatever else an example carries after.
        var efKey = EfPlatformKey;
        var order = new[] { platformKey, $"{platformKey}.repeat", $"{platformKey}.cache", efKey, $"{efKey}.repeat" };

        // The ratio to EF only where redb ran on the same provider.
        var compareWithEf = RunsOnEfProvider(platformKey);

        // Table with platforms as subrows for easy comparison
        // No TIER column: the tier is redb's licence tier and says nothing about an EF line.
        var table = new TablePrinter(8, 44, 8, 10, 7, 50);
        table.Header("ID", "TITLE", "COUNT", "TIME", "SQL", "PLATFORM: STATUS");

        for (int i = 0; i < allResults.Count; i++)
        {
            var r = allResults[i];
            var id = r.ValueString ?? "-";
            var title = r.Name ?? "-";
            var platforms = r.Props?.Results ?? new Dictionary<string, PlatformResult>();

            // First row: ID, Title
            table.Row(
                new TableCell(id),
                new TableCell(title),
                new TableCell(""),
                new TableCell(""),
                new TableCell(""),
                new TableCell("")
            );

            // The fastest and the slowest within each kind of pass (first, repeat, cache), across providers and
            // libraries: yellow and red. A cold first pass is never set against a pass over a warm cache.
            var best = new HashSet<string>();
            var worst = new HashSet<string>();
            var byKind = platforms
                .Where(p => p.Value.Time is > 0 && p.Value.Status?.StartsWith("OK", StringComparison.Ordinal) == true)
                .GroupBy(p => PassKind(p.Key));
            foreach (var kind in byKind.Where(g => g.Count() > 1))
            {
                best.Add(kind.MinBy(p => p.Value.Time)!.Key);
                worst.Add(kind.MaxBy(p => p.Value.Time)!.Key);
            }

            // Platform subrows
            foreach (var platform in order.Where(platforms.ContainsKey).Concat(platforms.Keys.Except(order).Order()))
            {
                var pr = platforms[platform];
                var count = pr.Count?.ToString() ?? "-";
                var time = pr.Time != null ? $"{pr.Time}ms" : "-";
                var queries = pr.Queries?.ToString() ?? "-";
                var status = pr.Status ?? "-";

                // EF against redb pass for pass: the first EF pass against the first redb pass, the EF repeat
                // against the redb repeat.
                var against = !compareWithEf ? null
                    : platform == efKey ? platformKey
                    : platform == $"{efKey}.repeat" ? $"{platformKey}.repeat"
                    : null;
                if (against != null && pr.Time is > 0 && platforms.GetValueOrDefault(against)?.Time is > 0 and var redbTime)
                {
                    // The pass without the provider prefix keeps the line inside the column; a ratio below 1
                    // keeps two decimals, so a fast EF line does not print as x0.0.
                    var ratio = (double)pr.Time.Value / redbTime;
                    var pass = against[(against.IndexOf('.') + 1)..];
                    status += $"  x{ratio.ToString(ratio < 1 ? "F2" : "F1")} vs {pass}";
                }

                var color = status.StartsWith("N/A", StringComparison.Ordinal) ? ConsoleColor.DarkYellow
                    : !status.StartsWith("OK", StringComparison.Ordinal) ? ConsoleColor.Red
                    : best.Contains(platform) ? ConsoleColor.Yellow
                    : worst.Contains(platform) ? ConsoleColor.Red
                    : ConsoleColor.Green;

                table.Row(
                    new TableCell(""),
                    new TableCell(""),
                    new TableCell(count, color),
                    new TableCell(time, color),
                    new TableCell(queries, color),
                    new TableCell($"  {platform,-20}: {status}", color)
                );
            }

            // Separator between examples (except last)
            if (i < allResults.Count - 1)
                table.Separator();
        }

        Console.WriteLine();
        table.Print();

        // Summary by platform
        Console.WriteLine();
        foreach (var platform in allResults.SelectMany(r => r.Props?.Results?.Keys ?? Enumerable.Empty<string>()).Distinct()
                     .OrderBy(p => Array.IndexOf(order, p) is var index and >= 0 ? index : int.MaxValue).ThenBy(p => p))
        {
            var statuses = allResults.Select(r => r.Props?.Results?.GetValueOrDefault(platform)?.Status).Where(s => s != null).ToList();
            var ok = statuses.Count(s => s == "OK");
            var notSupported = statuses.Count(s => s!.StartsWith("N/A", StringComparison.Ordinal));
            var fail = statuses.Count - ok - notSupported;
            Console.WriteLine($"  {platform,-20}: {ok} OK, {fail} FAIL, {notSupported} N/A (of {statuses.Count})");
        }
    }

    private static void PrintResults(List<ExampleResult> results)
    {
        var table = new TablePrinter(6, 50, 6, 6, 10, 8);
        table.Header("ID", "TITLE", "TIER", "COUNT", "TIME", "STATUS");

        var sorted = results.OrderBy(r => r.Id).ToList();
        for (int i = 0; i < sorted.Count; i++)
        {
            var r = sorted[i];
            var tier = r.Tier switch { ExampleTier.Free => "FREE", ExampleTier.Pro => "PRO", _ => "ENT" };
            var status = r.Success ? "OK" : "FAIL";
            var countVal = r.Count ?? 0;
            var count = r.Count?.ToString() ?? "-";

            // Highlight: red if time > 1000ms or count = 0
            var timeColor = r.ElapsedMs > 1000 ? ConsoleColor.Red : (ConsoleColor?)null;
            var countColor = countVal == 0 ? ConsoleColor.Red : (ConsoleColor?)null;
            var statusColor = r.Success ? (ConsoleColor?)null : ConsoleColor.Red;

            table.Row(
                new TableCell(r.Id),
                new TableCell(r.Title),
                new TableCell(tier),
                new TableCell(count, countColor),
                new TableCell($"{r.ElapsedMs}ms", timeColor),
                new TableCell(status, statusColor)
            );

            // Output lines
            foreach (var line in r.Output.Take(2))
                table.Row("", $"  {line}", "", "", "", "");

            // Error
            if (!r.Success && r.Error != null)
                table.Row(new TableCell(""), new TableCell($"  ERROR: {r.Error}", ConsoleColor.Red), 
                    new TableCell(""), new TableCell(""), new TableCell(""), new TableCell(""));

            // Separator between examples (except last)
            if (i < sorted.Count - 1)
                table.Separator();
        }

        Console.WriteLine();
        table.Print();

        // Summary
        var ok = results.Count(r => r.Success);
        var fail = results.Count(r => !r.Success);
        var totalMs = results.Sum(r => r.ElapsedMs);
        Console.WriteLine();
        Console.WriteLine($"Results: {ok} OK, {fail} FAIL | Total time: {totalMs}ms");
    }
}
