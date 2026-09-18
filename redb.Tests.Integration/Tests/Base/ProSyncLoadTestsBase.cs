using System.Globalization;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Extensions;
using redb.Core.Models.Configuration;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Plan docs/V4/LISTITEM_OBJECT_WALKERS_AND_PRO_SYNC_LOAD_PLAN.md, part B. The synchronous <c>Load&lt;T&gt;</c> is the
/// thread-pool-free twin of <c>LoadAsync</c> (the sync getter of <see cref="RedbListItem.Object"/> runs on it). On Pro it
/// took the Free in-database JSON builder instead of the Pro C# materializer: Pro SQLite has no such function and
/// failed, Pro MSSQL/PostgreSQL built a different object (list items lost their object link).
/// <para>
/// Detector: the SQL recorded by <see cref="RecordingRedbContext"/> - no command of the sync load may call
/// <c>get_object_json</c>. It does not depend on the database lacking the function, so the suite is red on every
/// provider in any run order (the Free native SQLite extension is process-wide state). Then the same object as the async
/// load, field by field, and no list-item load on the way: the item is still not loaded, and an explicit read on the live
/// scope loads it.
/// </para>
/// </summary>
public abstract class ProSyncLoadTestsBase
{
    private readonly SqlRecorder _recorder = new();

    protected abstract void UseProvider(RedbOptionsBuilder options);

    protected virtual void Register(IServiceCollection services, Action<RedbOptionsBuilder> configure)
        => services.AddRedb(configure);

    private ServiceProvider Build(Action<RedbServiceConfiguration> configure)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        Register(services, options =>
        {
            UseProvider(options);
            options.Configure(c =>
            {
                // Cache off: every load materializes, on both roads.
                c.EnablePropsCache = false;
                c.CacheDomain = "pro-sync-load";
                configure(c);
            });
        });
        RecordingRedbContext.Decorate(services, _recorder);
        return services.BuildServiceProvider();
    }

    private sealed record Seed(long RootId, long LinkedObjectId);

    private static async Task<Seed> SeedAsync(ServiceProvider sp, bool onlyTrashedReference)
    {
        var tag = Guid.NewGuid().ToString("N")[..8];
        var redb = sp.GetRequiredService<IRedbService>();
        await redb.InitializeAsync(ensureCreated: true);
        await redb.SyncSchemeAsync<SimpleProps>();
        await redb.SyncSchemeAsync<CtProbeChildProps>();
        await redb.SyncSchemeAsync<CtProbeProps>();
        await redb.InitializeTypeRegistryAsync();

        var linkedObjectId = await redb.SaveAsync(new RedbObject<SimpleProps>
            { name = $"prosync-linked-{tag}", Props = new SimpleProps { Title = "linked" } });
        var list = await redb.ListProvider.SaveListAsync(RedbList.Create($"prosync-{tag}", "prosync"));
        var linked = await redb.ListProvider.SaveListItemAsync(new RedbListItem { IdList = list.Id, Value = "linked", IdObject = linkedObjectId });
        var plain = await redb.ListProvider.SaveListItemAsync(new RedbListItem { IdList = list.Id, Value = "plain" });
        var c1 = await redb.SaveAsync(new RedbObject<CtProbeChildProps> { name = $"prosync-c1-{tag}", Props = new CtProbeChildProps { Tag = "one" } });
        var c2 = await redb.SaveAsync(new RedbObject<CtProbeChildProps> { name = $"prosync-c2-{tag}", Props = new CtProbeChildProps { Tag = "two" } });

        var props = new CtProbeProps
        {
            Label = "root",
            Longs = [3, 1, 2],
            Decimals = [1.5m, 2.25m],
            Tags = ["a", "b"],
            Dict = new() { ["k1"] = "v1", ["k2"] = "v2" },
            TupleDict = new() { [(2024, "Q1")] = "first", [(2025, "Q2")] = "second" },
            Items =
            [
                new CtProbeItem { Name = "i1", Price = 9.99m, Codes = [7, 8], Meta = new() { ["m"] = "1" } },
                new CtProbeItem { Name = "i2", Price = 0.5m }
            ],
            Status = linked,
            Roles = [linked, plain]
        };
        if (onlyTrashedReference)
        {
            props.Refs = [new RedbObject<CtProbeChildProps> { id = c2 }];
        }
        else
        {
            props.Refs = [new RedbObject<CtProbeChildProps> { id = c1 }, new RedbObject<CtProbeChildProps> { id = c2 }];
            props.RefDict = new() { ["first"] = new RedbObject<CtProbeChildProps> { id = c1 } };
        }

        var rootId = await redb.SaveAsync(new RedbObject<CtProbeProps> { name = $"prosync-root-{tag}", Props = props });
        if (onlyTrashedReference)
            await redb.SoftDeleteAsync(new[] { c2 });
        return new Seed(rootId, linkedObjectId);
    }

    /// <summary>The loaded object field by field, without waking anything (a stub is described, never loaded).</summary>
    private static string Describe(RedbObject<CtProbeProps> o)
    {
        var p = o.Props;
        var s = new StringBuilder();
        s.AppendLine($"header: id={o.id} scheme={o.scheme_id} name={o.name} hash={o.hash}");
        s.AppendLine($"label: {p.Label}");
        s.AppendLine($"longs: {Seq(p.Longs)}");
        s.AppendLine($"decimals: {Seq(p.Decimals?.Select(d => d.ToString(CultureInfo.InvariantCulture)))}");
        s.AppendLine($"tags: {Seq(p.Tags?.Select(t => t ?? "<null>"))}");
        s.AppendLine($"dict: {Seq(Pairs(p.Dict))}");
        s.AppendLine($"tupleDict: {Seq(p.TupleDict?.Select(e => $"{e.Key.Year}/{e.Key.Quarter}={e.Value}").OrderBy(x => x, StringComparer.Ordinal))}");
        s.AppendLine($"items: {Seq(p.Items?.Select(i => i == null ? "<null>"
            : $"{i.Name}:{i.Price.ToString(CultureInfo.InvariantCulture)}:[{Seq(i.Codes)}]:[{Seq(Pairs(i.Meta))}]"))}");
        s.AppendLine($"refs: {Seq(p.Refs?.Select(Reference))}");
        s.AppendLine($"refDict: {Seq(p.RefDict?.OrderBy(e => e.Key, StringComparer.Ordinal).Select(e => $"{e.Key}={Reference(e.Value)}"))}");
        s.AppendLine($"status: {Item(p.Status)}");
        s.AppendLine($"roles: {Seq(p.Roles?.Select(Item))}");
        return s.ToString();
    }

    private static string Seq<T>(IEnumerable<T>? items) => items == null ? "<null>" : string.Join(",", items);

    private static IEnumerable<string>? Pairs(Dictionary<string, string>? dict)
        => dict?.OrderBy(e => e.Key, StringComparer.Ordinal).Select(e => $"{e.Key}={e.Value}");

    private static string Reference(RedbObject<CtProbeChildProps>? r)
        => r == null ? "<null>"
            : r.IsPropsLoaded ? $"{r.id}:{r.name}:{r.Props.Tag}"
            : $"stub {r.id}:{r.name}:hash={r.hash != null}";

    private static string Item(RedbListItem? i)
        => i == null ? "<null>" : $"{i.Id}:{i.IdList}:{i.Value}:{i.Alias}:object={i.IdObject}";

    private async Task SyncLoadMatchesAsyncLoadAsync(string path, Action<RedbServiceConfiguration> configure, int depth,
        bool onlyTrashedReference = false)
    {
        await using var sp = Build(configure);
        var seed = await SeedAsync(sp, onlyTrashedReference);

        string expected;
        await using (var asyncScope = sp.CreateAsyncScope())
        {
            var viaAsync = await asyncScope.ServiceProvider.GetRequiredService<IRedbService>().LoadAsync<CtProbeProps>(seed.RootId, depth);
            expected = Describe(viaAsync!);
        }
        expected.Should().Contain($"object={seed.LinkedObjectId}",
            $"{path}: precondition - the async load carries the list-item link, or comparing it proves nothing");

        await using var scope = sp.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        var callingThread = Environment.CurrentManagedThreadId;
        _recorder.Start();
        var loaded = redb.Load<CtProbeProps>(seed.RootId, depth);
        var commands = _recorder.Stop();

        AssertSyncRoad(path, commands, callingThread);
        loaded.Should().NotBeNull();
        Describe(loaded!).Should().Be(expected,
            $"{path}: the sync load is the thread-pool-free twin of LoadAsync - the same object, list-item links included");

        var status = loaded!.Props.Status!;
        status.IsObjectLoaded.Should().BeFalse($"{path}: the sync materialization reads no RedbListItem.Object");
        status.Object.Should().NotBeNull($"{path}: positive control - an explicit read of Object loads it on the live scope");
        status.IsObjectLoaded.Should().BeTrue();
    }

    /// <summary>
    /// The synchronous road: commands went through the recording context (positive control), none of them is the Free
    /// JSON builder, and every one ran on the calling thread - a hop to the pool (Task.Run, Parallel.ForEach) parks the
    /// caller behind pool capacity, the anatomy of the tsum freeze (2026-09-09).
    /// </summary>
    private static void AssertSyncRoad(string path, IReadOnlyList<RecordedCommand> commands, int callingThread)
    {
        commands.Should().NotBeEmpty($"{path}: positive control - the load runs its commands through the recording context");
        commands.Should().NotContain(c => c.Sql.Contains("get_object_json", StringComparison.OrdinalIgnoreCase),
            $"{path}: Pro materializes Props in C#; the synchronous road must not take the Free in-database JSON builder (Pro SQLite has none)");
        commands.Should().OnlyContain(c => c.ThreadId == callingThread,
            $"{path}: every command of the synchronous road runs on the calling thread, never on a thread-pool thread");
    }

    [Fact]
    public async Task LazyStubPropsGetter_LoadsOnTheCallingThread_WithoutFreeJson()
    {
        // The synchronous Props getter of a lazy reference stub (the other synchronous road of Pro materialization).
        await using var sp = Build(c =>
        {
            c.EnableLazyReferences = true;
            c.LazyReferenceAccess = LazyReferenceAccessMode.Blocking; // the transparent synchronous load, stated explicitly
        });
        var seed = await SeedAsync(sp, onlyTrashedReference: false);

        await using var scope = sp.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        var root = await redb.LoadAsync<CtProbeProps>(seed.RootId, depth: 1);
        var stub = root!.Props.Refs![0];
        stub.IsPropsLoaded.Should().BeFalse("precondition: at depth 1 the reference is a lazy stub");

        var callingThread = Environment.CurrentManagedThreadId;
        _recorder.Start();
        var tag = stub.Props.Tag;
        var commands = _recorder.Stop();

        tag.Should().BeOneOf("one", "two");
        AssertSyncRoad("lazy stub Props getter", commands, callingThread);
        root.Props.Status!.IsObjectLoaded.Should().BeFalse("the stub load reads no RedbListItem.Object");
    }

    [Fact]
    public Task SyncLoad_WithReferencesAndListItems_MatchesAsync_WithoutFreeJson()
        => SyncLoadMatchesAsyncLoadAsync("references and list items", _ => { }, depth: 10);

    [Fact]
    public Task SyncLoad_WithTrashedReferenceTarget_MatchesAsync_WithoutFreeJson()
        => SyncLoadMatchesAsyncLoadAsync("trashed reference target", _ => { }, depth: 10, onlyTrashedReference: true);

    [Fact]
    public Task SyncLoad_WithLazyReferences_MatchesAsync_WithoutFreeJson()
        => SyncLoadMatchesAsyncLoadAsync("lazy references", c => c.EnableLazyReferences = true, depth: 1);
}
