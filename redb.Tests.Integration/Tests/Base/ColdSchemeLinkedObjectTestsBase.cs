using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Extensions;
using redb.Core.Models.Configuration;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// The object behind a list item loads typed on a node whose metadata cache has never seen its scheme - a fresh
/// process, another cluster node. The single linked-object loaders took the CLR type from that cache only; on a miss
/// they fell back to get_object_json with an untyped result, which a Pro SQLite database does not even have. The
/// batch preload path already resolved cold schemes by id, so the preload is off here.
/// </summary>
public abstract class ColdSchemeLinkedObjectTestsBase
{
    protected abstract void UseProvider(RedbOptionsBuilder options);

    protected virtual void Register(IServiceCollection services, Action<RedbOptionsBuilder> configure)
        => services.AddRedb(configure);

    private ServiceProvider Build(string cacheDomain)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        Register(services, options =>
        {
            UseProvider(options);
            options.Configure(c =>
            {
                c.EnablePropsCache = false;
                c.PreloadListItemLinkedObjects = false;
                c.CacheDomain = cacheDomain;
            });
        });
        return services.BuildServiceProvider();
    }

    private sealed record Seed(long ItemId, long LinkedObjectId, long SchemeId);

    private static async Task<Seed> SeedAsync(ServiceProvider writer)
    {
        var tag = Guid.NewGuid().ToString("N")[..8];
        var redb = writer.GetRequiredService<IRedbService>();
        await redb.InitializeAsync(ensureCreated: true);
        await redb.SyncSchemeAsync<SimpleProps>();
        await redb.InitializeTypeRegistryAsync();

        var linkedObjectId = await redb.SaveAsync(new RedbObject<SimpleProps>
            { name = $"coldscheme-linked-{tag}", Props = new SimpleProps { Title = "linked" } });
        var list = await redb.ListProvider.SaveListAsync(RedbList.Create($"coldscheme-{tag}", "coldscheme"));
        var item = await redb.ListProvider.SaveListItemAsync(new RedbListItem { IdList = list.Id, Value = "linked", IdObject = linkedObjectId });
        var schemeId = (await redb.LoadAsync<SimpleProps>(linkedObjectId))!.scheme_id;
        return new Seed(item.Id, linkedObjectId, schemeId);
    }

    /// <summary>A reader with a cache domain of its own takes the item through its list provider.</summary>
    private async Task RunOnColdReaderAsync(Func<RedbListItem, Seed, Task> touch)
    {
        var run = Guid.NewGuid().ToString("N")[..8];
        await using var writer = Build($"cold-scheme-writer-{run}");
        var seed = await SeedAsync(writer);

        await using var reader = Build($"cold-scheme-reader-{run}");
        await using var scope = reader.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        redb.Cache.GetClrType(seed.SchemeId).Should().BeNull("precondition: the reader's metadata cache has never seen the scheme");

        var item = await redb.ListProvider.GetListItemAsync(seed.ItemId);
        item.Should().NotBeNull();
        item!.IdObject.Should().Be(seed.LinkedObjectId);
        item.IsObjectLoaded.Should().BeFalse("precondition: no preload, the single loader resolves the object");

        await touch(item, seed);
    }

    private static void AssertTyped(IRedbObject? loaded, Seed seed)
    {
        loaded.Should().BeOfType<RedbObject<SimpleProps>>("a cold scheme is resolved by its id, not loaded untyped");
        var typed = (RedbObject<SimpleProps>)loaded!;
        typed.id.Should().Be(seed.LinkedObjectId);
        typed.Props.Title.Should().Be("linked");
    }

    [Fact]
    public Task SyncGetter_ColdSchemeCache_LoadsTheLinkedObjectTyped()
        => RunOnColdReaderAsync((item, seed) =>
        {
            AssertTyped(item.Object, seed);
            return Task.CompletedTask;
        });

    [Fact]
    public Task AsyncLoad_ColdSchemeCache_LoadsTheLinkedObjectTyped()
        => RunOnColdReaderAsync(async (item, seed) => AssertTyped(await item.GetObjectAsync(), seed));
}
