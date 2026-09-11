using System.Diagnostics;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// CT performance benchmark harness (perf waves F1..F9, docs/V4/CT_DEEP_REVIEW_PERF.md).
/// No-op unless REDB_BENCH=1: results are wall-clock numbers, not assertions, so they must
/// never run (or fail) in regular suites. Results are appended to the file named by
/// REDB_BENCH_OUT (semicolon-separated: provider;scenario;ops;total_ms;ms_per_op).
/// </summary>
public abstract class CtPerfBenchmarkTestsBase
{
    protected readonly IRedbService Redb;
    protected abstract string ProviderName { get; }

    protected CtPerfBenchmarkTestsBase(IRedbService redb) => Redb = redb;

    private static bool Enabled => Environment.GetEnvironmentVariable("REDB_BENCH") == "1";

    private static void Report(string provider, string scenario, int ops, double totalMs)
    {
        var outPath = Environment.GetEnvironmentVariable("REDB_BENCH_OUT");
        if (string.IsNullOrEmpty(outPath)) return;
        var line = FormattableString.Invariant(
            $"{provider};{scenario};{ops};{totalMs:F1};{totalMs / ops:F2}");
        lock (typeof(CtPerfBenchmarkTestsBase))
            File.AppendAllText(outPath, line + Environment.NewLine);
    }

    private async Task CleanupAsync()
    {
        await Redb.Context.ExecuteAsync(
            "DELETE FROM _objects WHERE _id_scheme IN (SELECT _id FROM _schemes WHERE _name = 'CtProbe') AND _name LIKE 'bench-%'");
    }

    private async Task<long> SeedAsync(string name, CtProbeProps props)
    {
        await Redb.SyncSchemeAsync<CtProbeProps>();
        return await Redb.SaveAsync(new RedbObject<CtProbeProps> { name = name, Props = props });
    }

    private static CtProbeProps ArrayProps(int arrayLen, string label) => new()
    {
        Label = label,
        Longs = [.. Enumerable.Range(1, arrayLen).Select(i => (long)i)],
    };

    [Fact]
    public async Task B1_BatchMostlyUnchanged_Resave()
    {
        if (!Enabled) return;
        await CleanupAsync();

        // 60 objects x (scalar + 50-element array), 3 of them edited, resave the whole batch.
        // The F1 target: unchanged objects must not pay for the value pipeline.
        var objects = new List<RedbObject<CtProbeProps>>();
        await Redb.SyncSchemeAsync<CtProbeProps>();
        for (var i = 0; i < 60; i++)
            objects.Add(new RedbObject<CtProbeProps> { name = $"bench-b1-{i}", Props = ArrayProps(50, $"o{i}") });
        var ids = await Redb.SaveAsync(objects);
        for (var i = 0; i < objects.Count; i++) objects[i].id = ids[i];

        var reloaded = new List<RedbObject<CtProbeProps>>();
        foreach (var id in ids)
            reloaded.Add((await Redb.LoadAsync<CtProbeProps>(id, depth: 2))!);
        reloaded[5].Props.Longs![7] = 9999;
        reloaded[25].Props.Label = "edited";
        reloaded[45].Props.Longs![49] = 8888;

        const int rounds = 5;
        var sw = Stopwatch.StartNew();
        for (var r = 0; r < rounds; r++)
            await Redb.SaveAsync(reloaded.Cast<Core.Models.Contracts.IRedbObject>().ToList());
        sw.Stop();
        Report(ProviderName, "B1_batch60_3edited", rounds, sw.Elapsed.TotalMilliseconds);
        await CleanupAsync();
    }

    [Fact]
    public async Task B2_ResaveUnchangedLargeArray()
    {
        if (!Enabled) return;
        await CleanupAsync();

        // Single object with a 500-element array, resaved untouched. F1/F2 target.
        var id = await SeedAsync("bench-b2", ArrayProps(500, "big"));
        var loaded = await Redb.LoadAsync<CtProbeProps>(id, depth: 2);

        const int rounds = 20;
        var sw = Stopwatch.StartNew();
        for (var r = 0; r < rounds; r++)
            await Redb.SaveAsync(loaded!);
        sw.Stop();
        Report(ProviderName, "B2_resave_array500", rounds, sw.Elapsed.TotalMilliseconds);
        await CleanupAsync();
    }

    [Fact]
    public async Task B3_SingleElementEdit_LargeArray()
    {
        if (!Enabled) return;
        await CleanupAsync();

        // Single object with a 500-element array, one element changed per save. F2/F5 target.
        var id = await SeedAsync("bench-b3", ArrayProps(500, "big"));
        var loaded = await Redb.LoadAsync<CtProbeProps>(id, depth: 2);

        const int rounds = 20;
        var sw = Stopwatch.StartNew();
        for (var r = 0; r < rounds; r++)
        {
            loaded!.Props.Longs![r % 500] = 100000 + r;
            await Redb.SaveAsync(loaded);
        }
        sw.Stop();
        Report(ProviderName, "B3_edit1_array500", rounds, sw.Elapsed.TotalMilliseconds);
        await CleanupAsync();
    }

    [Fact]
    public async Task B5_ScalarEdit_UntouchedLargeArray()
    {
        if (!Enabled) return;
        await CleanupAsync();

        // Object changes (so F1 cannot skip it) but the 500-element array is untouched:
        // the F2 target - the array's base hash matches and elements need no walk.
        var id = await SeedAsync("bench-b5", ArrayProps(500, "big"));
        var loaded = await Redb.LoadAsync<CtProbeProps>(id, depth: 2);

        const int rounds = 20;
        var sw = Stopwatch.StartNew();
        for (var r = 0; r < rounds; r++)
        {
            loaded!.Props.Label = "big-" + r;
            await Redb.SaveAsync(loaded);
        }
        sw.Stop();
        Report(ProviderName, "B5_scalar_edit_array500", rounds, sw.Elapsed.TotalMilliseconds);
        await CleanupAsync();
    }

    [Fact]
    public async Task B4_SmallObjectResave()
    {
        if (!Enabled) return;
        await CleanupAsync();

        // The most frequent real-world case: one small object saved repeatedly. F7 target.
        var id = await SeedAsync("bench-b4", new CtProbeProps
        {
            Label = "small",
            Longs = [1, 2, 3],
            Dict = new() { ["a"] = "1" },
        });
        var loaded = await Redb.LoadAsync<CtProbeProps>(id, depth: 2);

        const int rounds = 100;
        var sw = Stopwatch.StartNew();
        for (var r = 0; r < rounds; r++)
        {
            loaded!.Props.Label = "small-" + r;
            await Redb.SaveAsync(loaded);
        }
        sw.Stop();
        Report(ProviderName, "B4_small_edit_x100", rounds, sw.Elapsed.TotalMilliseconds);
        await CleanupAsync();
    }
}
