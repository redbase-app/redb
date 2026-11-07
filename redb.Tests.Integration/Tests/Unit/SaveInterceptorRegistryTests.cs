using Microsoft.Extensions.DependencyInjection;
using redb.Core.Extensions;
using redb.Core.Interception;
using redb.SQLite.Extensions;

namespace redb.Tests.Integration.Tests.Unit;

/// <summary>
/// The run-time registry of save interceptors and the pipeline a storage provider reads it through: subscription
/// and removal, the order DI first then registry, snapshots that do not move under a running operation, no
/// allocation when nobody listens, and the TryAdd registration that lets several containers share one registry.
/// No database: the save path itself is covered by SaveInterceptorTestsBase.
/// </summary>
public class SaveInterceptorRegistryTests
{
    private sealed class Probe(string name) : IRedbSaveInterceptor
    {
        public override string ToString() => name;
    }

    [Fact]
    public void Add_ThenDispose_RemovesOnlyThatSubscription()
    {
        var registry = new RedbSaveInterceptorRegistry();
        var a = new Probe("a");
        var b = new Probe("b");

        var ha = registry.Add(a);
        using var hb = registry.Add(b);
        registry.Current.Should().Equal(a, b);

        ha.Dispose();
        registry.Current.Should().Equal(b);
    }

    [Fact]
    public void Dispose_Twice_IsANoOp()
    {
        var registry = new RedbSaveInterceptorRegistry();
        var a = new Probe("a");
        var h = registry.Add(a);

        h.Dispose();
        h.Dispose();

        registry.Current.Should().BeEmpty();
    }

    [Fact]
    public void SameInstanceAddedTwice_RunsTwice_AndEachHandleRemovesOne()
    {
        var registry = new RedbSaveInterceptorRegistry();
        var a = new Probe("a");
        var h1 = registry.Add(a);
        var h2 = registry.Add(a);
        registry.Current.Should().Equal(a, a);

        h1.Dispose();
        h1.Dispose();
        registry.Current.Should().Equal(a);

        h2.Dispose();
        registry.Current.Should().BeEmpty();
    }

    [Fact]
    public void Current_IsASnapshot_ThatLaterChangesDoNotTouch()
    {
        var registry = new RedbSaveInterceptorRegistry();
        var a = new Probe("a");
        using var ha = registry.Add(a);

        var before = registry.Current;
        using var hb = registry.Add(new Probe("b"));
        ha.Dispose();

        before.Should().Equal(a);
    }

    [Fact]
    public void Add_Null_Throws()
    {
        var registry = new RedbSaveInterceptorRegistry();
        var act = () => registry.Add(null!);
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public async Task ConcurrentAddAndDispose_LoseNothing()
    {
        var registry = new RedbSaveInterceptorRegistry();
        var keep = Enumerable.Range(0, 200).Select(i => new Probe($"keep{i}")).ToArray();

        await Task.WhenAll(Enumerable.Range(0, 200).Select(i => Task.Run(() =>
        {
            registry.Add(keep[i]);
            var temp = registry.Add(new Probe($"temp{i}"));
            temp.Dispose();
            temp.Dispose();
        })));

        registry.Current.Should().HaveCount(200).And.OnlyHaveUniqueItems();
        registry.Current.Should().OnlyContain(p => keep.Contains(p), "every temporary subscription is gone");
    }

    [Fact]
    public void Pipeline_RunsDiFirst_ThenTheRegistryInSubscriptionOrder()
    {
        var registry = new RedbSaveInterceptorRegistry();
        var di1 = new Probe("di1");
        var di2 = new Probe("di2");
        var r1 = new Probe("r1");
        var r2 = new Probe("r2");
        using var h1 = registry.Add(r1);
        using var h2 = registry.Add(r2);

        var pipeline = new RedbSaveInterceptorPipeline([di1, di2], registry);

        pipeline.Snapshot().Should().Equal(di1, di2, r1, r2);
    }

    [Fact]
    public void Pipeline_Snapshot_DoesNotMoveWhenTheRegistryChanges()
    {
        var registry = new RedbSaveInterceptorRegistry();
        var di = new Probe("di");
        var r = new Probe("r");
        var pipeline = new RedbSaveInterceptorPipeline([di], registry);

        var h = registry.Add(r);
        var during = pipeline.Snapshot();
        h.Dispose();
        using var late = registry.Add(new Probe("late"));

        during.Should().Equal(di, r);
        pipeline.Snapshot().Should().HaveCount(2).And.NotContain(r);
    }

    [Fact]
    public void Pipeline_WithNobodyListening_ReturnsTheSameEmptyList()
    {
        var withRegistry = new RedbSaveInterceptorPipeline(null, new RedbSaveInterceptorRegistry());

        var first = withRegistry.Snapshot();
        var second = withRegistry.Snapshot();

        first.Should().BeEmpty();
        second.Should().BeSameAs(first, "an empty snapshot allocates nothing");
        RedbSaveInterceptorPipeline.Empty.Snapshot().Should().BeEmpty();
    }

    [Fact]
    public void Pipeline_WithOneSourceOnly_HandsItOutWithoutCopying()
    {
        var registry = new RedbSaveInterceptorRegistry();
        using var h = registry.Add(new Probe("r"));

        var registryOnly = new RedbSaveInterceptorPipeline(null, registry);
        registryOnly.Snapshot().Should().BeSameAs(registry.Current);

        var diOnly = new RedbSaveInterceptorPipeline([new Probe("di")], new RedbSaveInterceptorRegistry());
        diOnly.Snapshot().Should().BeSameAs(diOnly.Snapshot());
    }

    [Fact]
    public void AddRedb_RegistersOneRegistry_AndKeepsOneRegisteredBefore()
    {
        var services = new ServiceCollection();
        services.AddRedb(o => o.UseSqlite("Data Source=unused.db"));
        using (var sp = services.BuildServiceProvider())
        {
            var registry = sp.GetRequiredService<IRedbSaveInterceptorRegistry>();
            registry.Should().BeOfType<RedbSaveInterceptorRegistry>();
            sp.GetRequiredService<IRedbSaveInterceptorRegistry>().Should().BeSameAs(registry, "a singleton");
        }

        var shared = new RedbSaveInterceptorRegistry();
        var withShared = new ServiceCollection();
        withShared.AddSingleton<IRedbSaveInterceptorRegistry>(shared);
        withShared.AddRedb(o => o.UseSqlite("Data Source=unused.db"));
        using var sp2 = withShared.BuildServiceProvider();
        sp2.GetRequiredService<IRedbSaveInterceptorRegistry>().Should().BeSameAs(shared,
            "TryAdd keeps the instance a host hands over, so its named instances share the subscriptions");
    }
}
