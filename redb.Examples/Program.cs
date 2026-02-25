using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Pro.Extensions;
using redb.Postgres.Pro.Extensions;
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

    // Connection string for results storage (separate DB for all platforms)
    // Set via environment variable or replace with your own connection string
    // private const string RedbDocConnection = "Server=localhost;Database=redbDoc;User Id=sa;Password=YourPassword;TrustServerCertificate=true;Command Timeout=600;";

    static async Task Main(string[] args)
    {
        Console.OutputEncoding = System.Text.Encoding.UTF8;

        var services = new ServiceCollection();
        ConfigureServices(services);

        var provider = services.BuildServiceProvider();
        var redb = provider.GetRequiredService<IRedbService>();

        // Get DB type and platform key
        var dbType = redb.Context.GetType().Name.Contains("Npgsql") ? "PostgreSQL" : "SQL Server";
        var platformKey = GetPlatformKey(redb);
        Console.WriteLine($"DB: {dbType} | Platform: {platformKey} | redb.Examples");
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

        // Parse args
        var exampleIds = args.Length > 0
            ? args.Select(a => a.ToUpperInvariant()).ToHashSet()
            : null;

        // Filter examples
        var toRun = exampleIds == null
            ? examples
            : examples.Where(e => e.meta != null && exampleIds.Contains(e.meta.Id.ToUpperInvariant())).ToList();

        if (toRun.Count == 0)
        {
            Console.WriteLine("No examples to run. Available:");
            foreach (var (ex, meta) in examples.Where(e => e.meta != null))
                Console.WriteLine($"  {meta!.Id} - {meta.Title} [{meta.Tier}]");
            return;
        }

        // Run examples
        var results = new List<ExampleResult>();
        var total = toRun.Count;
        var current = 0;

        foreach (var (example, meta) in toRun.OrderBy(e => e.meta?.Id))
        {
            if (meta == null) continue;
            current++;

            // Progress indicator
            Console.Write($"[{current}/{total}] {meta.Id} {meta.Title}...");

            try
            {
                var result = await example.RunAsync(redb);
                results.Add(result);
                Console.WriteLine(result.Success ? " OK" : " FAIL");
            }
            catch (Exception ex)
            {
                results.Add(new ExampleResult
                {
                    Id = meta.Id,
                    Title = meta.Title,
                    Tier = meta.Tier,
                    Success = false,
                    ElapsedMs = 0,
                    Output = [],
                    Error = ex.Message
                });
                Console.WriteLine($" ERROR: {ex.Message}");
            }
        }

        Console.WriteLine();

        PrintResults(results);
        // Save results to separate redbDoc database
        // var redbDoc = await CreateRedbDocServiceAsync();
        // await SaveResultsAsync(redbDoc, results, platformKey);
    }

    // /// <summary>
    // /// Creates separate IRedbService for results storage in redbDoc database.
    // /// </summary>
    // private static async Task<IRedbService> CreateRedbDocServiceAsync()
    // {
    //     var services = new ServiceCollection();
    //     services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
    //     services.AddRedbPro(options => options.UseMsSql(RedbDocConnection));
    //     
    //     var provider = services.BuildServiceProvider();
    //     var redbDoc = provider.GetRequiredService<IRedbService>();
    //     await redbDoc.InitializeAsync();
    //     
    //     return redbDoc;
    // }

    private static void ConfigureServices(ServiceCollection services)
    {
        services.AddLogging(b => b
            .AddConsole()
            .SetMinimumLevel(LogLevel.Warning));

        services.AddRedb(options => options
            //.WithLicense(TestLicense)
            .Configure(c =>
            {
                c.EavSaveStrategy = EavSaveStrategy.DeleteInsert;
                //c.SkipHashValidationOnCacheCheck = false;
                //c.EnableLazyLoadingForProps = false;
                //c.EnablePropsCache = false;
                //c.PropsCacheMaxSize = 10000;
                //c.PropsCacheTtl = TimeSpan.FromMinutes(60);
            })
            //.UsePostgres("Host=localhost;Port=5432;Username=postgres;Password=1;Database=redb;Pooling=true;Include Error Detail=true;Options=-c jit=off")
            .UseMsSql("Server=localhost;Database=redb;User Id=sa;Password=1;TrustServerCertificate=true;Command Timeout=600;")
            );
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
        var isPostgres = redb.Context.GetType().Name.Contains("Npgsql");
        var isPro = redb.GetType().Assembly.FullName?.Contains(".Pro") == true;
        
        var db = isPostgres ? "postgres" : "mssql";
        return isPro ? $"{db}.pro" : db;
    }

    // /// <summary>
    // /// Saves or updates test results in redb database.
    // /// Creates new objects for new examples, updates existing ones.
    // /// Uses batch operations for efficiency.
    // /// </summary>
    // private static async Task SaveResultsAsync(IRedbService redb, List<ExampleResult> results, string platformKey)
    // {
    //     Console.WriteLine();
    //     Console.WriteLine($"=== SAVING RESULTS ({platformKey}) ===");
    //
    //     // Load all existing results at once (with Props)
    //     var existingResults = await redb.Query<ExampleResultProps>()
    //         .ToListAsync();
    //
    //     var existingByKey = existingResults.ToDictionary(r => r.ValueString ?? "", r => r);
    //
    //     var toSave = new List<RedbObject<ExampleResultProps>>();
    //     var saved = 0;
    //     var updated = 0;
    //
    //     foreach (var result in results)
    //     {
    //         var tierString = result.Tier switch
    //         {
    //             ExampleTier.Free => "Free",
    //             ExampleTier.Pro => "Pro",
    //             ExampleTier.Enterprise => "Enterprise",
    //             _ => "Free"
    //         };
    //
    //         var platformResult = new PlatformResult
    //         {
    //             Count = result.Count,
    //             Time = (int)result.ElapsedMs,
    //             Status = result.Success ? "OK" : (result.Error ?? "FAIL")
    //         };
    //
    //         if (existingByKey.TryGetValue(result.Id, out var existing))
    //         {
    //             // Update existing object - preserve other platforms!
    //             existing.Props ??= new ExampleResultProps();
    //             existing.Props.Tier = tierString;
    //             existing.Props.Results ??= new Dictionary<string, PlatformResult>();
    //             existing.Props.Results[platformKey] = platformResult;
    //
    //             toSave.Add(existing);
    //             updated++;
    //         }
    //         else
    //         {
    //             // Create new object
    //             var newResult = new RedbObject<ExampleResultProps>
    //             {
    //                 Name = result.Title,
    //                 ValueString = result.Id,
    //                 Props = new ExampleResultProps
    //                 {
    //                     Tier = tierString,
    //                     Results = new Dictionary<string, PlatformResult>
    //                     {
    //                         [platformKey] = platformResult
    //                     }
    //                 }
    //             };
    //
    //             toSave.Add(newResult);
    //             saved++;
    //         }
    //     }
    //
    //     // Batch save all at once
    //     if (toSave.Count > 0)
    //     {
    //         await redb.SaveAsync(toSave);
    //     }
    //
    //     Console.WriteLine($"Results saved: {saved} new, {updated} updated (batch)");
    //
    //     // Print all saved results from database
    //     await PrintSavedResultsAsync(redb);
    // }

    // /// <summary>
    // /// Loads and prints all saved results from database with all platforms.
    // /// Shows each platform on separate line for easy comparison.
    // /// </summary>
    // private static async Task PrintSavedResultsAsync(IRedbService redb)
    // {
    //     Console.WriteLine();
    //     Console.WriteLine("=== ALL SAVED RESULTS ===");
    //
    //     var allResults = await redb.Query<ExampleResultProps>()
    //         .OrderByRedb(r => r.ValueString)
    //         .ToListAsync();
    //
    //     if (allResults.Count == 0)
    //     {
    //         Console.WriteLine("No saved results in database.");
    //         return;
    //     }
    //
    //     // Table with platforms as subrows for easy comparison
    //     var table = new TablePrinter(6, 40, 6, 8, 10, 30);
    //     table.Header("ID", "TITLE", "TIER", "COUNT", "TIME", "PLATFORM: STATUS");
    //
    //     var platforms = new[] { "mssql", "mssql.pro", "postgres", "postgres.pro" };
    //
    //     for (int i = 0; i < allResults.Count; i++)
    //     {
    //         var r = allResults[i];
    //         var id = r.ValueString ?? "-";
    //         var title = r.Name ?? "-";
    //         var tier = r.Props?.Tier ?? "-";
    //
    //         // First row: ID, Title, Tier
    //         table.Row(
    //             new TableCell(id),
    //             new TableCell(title),
    //             new TableCell(tier),
    //             new TableCell(""),
    //             new TableCell(""),
    //             new TableCell("")
    //         );
    //
    //         // Platform subrows
    //         foreach (var platform in platforms)
    //         {
    //             var pr = r.Props?.Results?.GetValueOrDefault(platform);
    //             if (pr == null)
    //             {
    //                 table.Row(
    //                     new TableCell(""),
    //                     new TableCell(""),
    //                     new TableCell(""),
    //                     new TableCell("-", ConsoleColor.DarkGray),
    //                     new TableCell("-", ConsoleColor.DarkGray),
    //                     new TableCell($"  {platform,-12}: -", ConsoleColor.DarkGray)
    //                 );
    //             }
    //             else
    //             {
    //                 var count = pr.Count?.ToString() ?? "-";
    //                 var time = pr.Time != null ? $"{pr.Time}ms" : "-";
    //                 var status = pr.Status ?? "-";
    //                 var color = status == "OK" ? ConsoleColor.Green : ConsoleColor.Red;
    //
    //                 table.Row(
    //                     new TableCell(""),
    //                     new TableCell(""),
    //                     new TableCell(""),
    //                     new TableCell(count, color),
    //                     new TableCell(time, color),
    //                     new TableCell($"  {platform,-12}: {status}", color)
    //                 );
    //             }
    //         }
    //
    //         // Separator between examples (except last)
    //         if (i < allResults.Count - 1)
    //             table.Separator();
    //     }
    //
    //     Console.WriteLine();
    //     table.Print();
    //
    //     // Summary by platform
    //     Console.WriteLine();
    //     foreach (var platform in platforms)
    //     {
    //         var total = allResults.Count(r => r.Props?.Results?.ContainsKey(platform) == true);
    //         var ok = allResults.Count(r => r.Props?.Results?.GetValueOrDefault(platform)?.Status == "OK");
    //         var fail = total - ok;
    //         if (total > 0)
    //             Console.WriteLine($"  {platform,-12}: {ok} OK, {fail} FAIL (of {total})");
    //     }
    // }

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
