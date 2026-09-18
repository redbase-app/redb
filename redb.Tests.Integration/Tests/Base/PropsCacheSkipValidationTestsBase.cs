using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Extensions;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// The props cache under <c>SkipHashValidationOnCacheCheck</c> (plan docs/V4/PROPS_CACHE_PROD_AND_TAILS_PLAN.md §11): a
/// standalone application that trusts its own writes. Every write of the process must still reach the cache, and a load
/// by a list of ids must use the cache in both modes with one query for the whole batch - it is the road of the list-item
/// preload and of <c>LoadLinkedObjectsAsync</c>.
/// </summary>
public abstract class PropsCacheSkipValidationTestsBase
{
    private readonly SqlRecorder _recorder = new();

    protected abstract void UseProvider(RedbOptionsBuilder options);

    protected virtual void Register(IServiceCollection services, Action<RedbOptionsBuilder> configure)
        => services.AddRedb(configure);

    private ServiceProvider Build(bool skipHashValidation)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        Register(services, options =>
        {
            UseProvider(options);
            options.Configure(c =>
            {
                c.EnablePropsCache = true;
                c.SkipHashValidationOnCacheCheck = skipHashValidation;
                // A props cache lives per domain for the process: one domain per suite and per mode.
                c.CacheDomain = $"props-cache-skip-{GetType().Name}-{(skipHashValidation ? "skip" : "validate")}";
            });
        });
        RecordingRedbContext.Decorate(services, _recorder);
        return services.BuildServiceProvider();
    }

    private static string NewTag() => Guid.NewGuid().ToString("N")[..8];

    private static async Task BootAsync(IRedbService redb)
    {
        await redb.InitializeAsync(ensureCreated: true);
        await redb.SyncSchemeAsync<SimpleProps>();
        await redb.SyncSchemeAsync<TreeNodeProps>();
        await redb.InitializeTypeRegistryAsync();
    }

    [Fact]
    public async Task MovedObject_LoadsWithItsNewParent_WhenHashValidationIsSkipped()
    {
        await using var sp = Build(skipHashValidation: true);
        await using var scope = sp.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        await BootAsync(redb);

        var tag = NewTag();
        var parentA = new RedbObject<SimpleProps> { name = $"skip-parent-a-{tag}", Props = new SimpleProps { Title = "a" } };
        parentA.id = await redb.SaveAsync(parentA);
        var parentB = new RedbObject<SimpleProps> { name = $"skip-parent-b-{tag}", Props = new SimpleProps { Title = "b" } };
        parentB.id = await redb.SaveAsync(parentB);
        // A plain object under a parent: what CreateChildAsync writes, saved as RedbObject<T> so that the stored hash
        // is the one the loaded object recomputes (a TreeRedbObject<T> stores a different hash - see the plan, §11).
        var child = new RedbObject<SimpleProps> { name = $"skip-child-{tag}", parent_id = parentA.id, Props = new SimpleProps { Title = "child" } };
        child.id = await redb.SaveAsync(child);

        var before = (await redb.LoadAsync<SimpleProps>(child.id))!;
        before.parent_id.Should().Be(parentA.id, "precondition: the child hangs under its first parent");
        (await redb.LoadAsync<SimpleProps>(child.id)).Should().BeSameAs(before,
            "precondition: the second load is a cache hit - the scenario is about a cached object");

        await redb.MoveObjectAsync(child, parentB);

        var after = (await redb.LoadAsync<SimpleProps>(child.id))!;
        after.parent_id.Should().Be(parentB.id,
            "a move bypasses the save path: the database drops the object's hash, and the cached copy must go with it - " +
            "without hash validation a load never asks the database");
    }

    /// <summary>
    /// An object saved as <see cref="TreeRedbObject{TProps}"/> - every <c>CreateChildAsync</c>, every tree node - stored a
    /// header-only hash: the save recognised the exact generic type <c>RedbObject&lt;&gt;</c> and nothing derived from it.
    /// The loaded <c>RedbObject&lt;T&gt;</c> hashes header and Props, so the cache never served such objects.
    /// </summary>
    [Theory]
    [InlineData("SaveAsync")]
    [InlineData("CreateChildAsync")]
    public async Task ObjectSavedAsATreeObject_IsServedFromTheCache(string road)
    {
        await using var sp = Build(skipHashValidation: false);
        await using var scope = sp.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        await BootAsync(redb);

        var tag = NewTag();
        var node = new TreeRedbObject<TreeNodeProps> { name = $"tree-{road}-{tag}", Props = new TreeNodeProps { Name = "n", Code = "N", Budget = 1000000m } };
        if (road == "SaveAsync")
        {
            node.id = await redb.SaveAsync(node);
        }
        else
        {
            var parent = new RedbObject<SimpleProps> { name = $"tree-parent-{tag}", Props = new SimpleProps { Title = "p" } };
            parent.id = await redb.SaveAsync(parent);
            node.id = await redb.CreateChildAsync(node, parent);
        }

        var first = (await redb.LoadAsync<TreeNodeProps>(node.id))!;
        first.hash.Should().Be(first.ComputeHash(),
            "the hash the save stored is the one the loaded object recomputes - otherwise every cache probe of the object is a miss");
        var second = (await redb.LoadAsync<TreeNodeProps>(node.id))!;
        second.Should().BeSameAs(first, "the second load of an unchanged object is a cache hit");
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task BatchLoadOfCachedObjects_IssuesOneQuery_AndServesTheCachedInstances(bool skipHashValidation)
    {
        await using var sp = Build(skipHashValidation);
        await using var scope = sp.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        await BootAsync(redb);

        var tag = NewTag();
        var ids = new long[5];
        for (var i = 0; i < ids.Length; i++)
            ids[i] = await redb.SaveAsync(new RedbObject<SimpleProps> { name = $"skip-batch-{tag}-{i}", Props = new SimpleProps { Title = $"batch-{i}" } });

        // Warm the cache the way any single load does.
        var warmed = new Dictionary<long, IRedbObject>();
        foreach (var id in ids)
            warmed[id] = (await redb.LoadAsync<SimpleProps>(id))!;

        _recorder.Start();
        var loaded = await redb.LoadAsync(ids);
        var commands = _recorder.Stop();

        loaded.Should().HaveCount(ids.Length);
        loaded.Should().OnlyContain(o => ReferenceEquals(o, warmed[o.Id]),
            "every object of the batch is in the cache: the load serves the cached instances instead of materializing again");
        commands.Should().HaveCount(1,
            "one query resolves the hashes of the whole batch (the polymorphic load needs the schemes anyway); the cache answers the rest");
    }
}
