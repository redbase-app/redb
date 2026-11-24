using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Extensions;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// DeleteSubtreeAsync answers how many objects of the subtree it deleted, the start object included. It collects the
/// subtree first and deletes it after; it used to answer the size of what it collected, so an object another session
/// deleted in between was counted too. The delete is put exactly after the collection through the test context.
/// </summary>
public abstract class DeleteSubtreeCountTestsBase
{
    private readonly SqlRecorder _recorder = new();

    protected abstract void UseProvider(RedbOptionsBuilder options);

    protected virtual void Register(IServiceCollection services, Action<RedbOptionsBuilder> configure)
        => services.AddRedb(configure);

    private ServiceProvider Build(bool decorate)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        Register(services, options =>
        {
            UseProvider(options);
            options.Configure(c => c.CacheDomain = $"subtree-count-{GetType().Name}-{Guid.NewGuid():N}");
        });
        if (decorate)
            RecordingRedbContext.Decorate(services, _recorder);
        return services.BuildServiceProvider();
    }

    /// <summary>The statement that collects the subtree (Tree_SelectSubtreeRows).</summary>
    private static bool CollectsSubtree(string sql)
        => sql.Contains("AS IdParent, _id_scheme AS IdScheme, depth AS Depth", StringComparison.Ordinal);

    /// <summary>A root, three children, a grandchild under the first child: five objects.</summary>
    private static async Task<(long Root, long Grandchild)> SeedAsync(IRedbService redb)
    {
        await redb.SyncSchemeAsync<SimpleProps>();
        var tag = "subc-" + Guid.NewGuid().ToString("N")[..8];
        var root = new TreeRedbObject<SimpleProps> { name = $"{tag}-root", Props = new SimpleProps { Title = tag } };
        root.id = await redb.SaveAsync(root);
        long firstChild = 0;
        for (var i = 0; i < 3; i++)
        {
            var child = new TreeRedbObject<SimpleProps> { name = $"{tag}-c{i}", Props = new SimpleProps { Title = tag, Count = i } };
            child.id = await redb.CreateChildAsync(child, root);
            if (i == 0) firstChild = child.id;
        }
        var grandchild = new TreeRedbObject<SimpleProps> { name = $"{tag}-g", Props = new SimpleProps { Title = tag, Count = 9 } };
        var parent = new TreeRedbObject<SimpleProps> { id = firstChild };
        grandchild.id = await redb.CreateChildAsync(grandchild, parent);
        return (root.id, grandchild.id);
    }

    [Fact]
    public async Task DeleteSubtree_CountsEveryObjectOfTheSubtree()
    {
        await using var host = Build(decorate: false);
        var redb = host.GetRequiredService<IRedbService>();
        await redb.InitializeAsync(ensureCreated: true);
        var (root, _) = await SeedAsync(redb);

        var deleted = await redb.DeleteSubtreeAsync(new RedbObject { id = root });

        deleted.Should().Be(5);
        (await redb.LoadAsync<SimpleProps>(root)).Should().BeNull();
    }

    [Fact]
    public async Task ObjectDeletedAfterTheSubtreeWasCollected_IsNotCounted()
    {
        await using var writerHost = Build(decorate: false);
        var writer = writerHost.GetRequiredService<IRedbService>();
        await writer.InitializeAsync(ensureCreated: true);
        var (root, grandchild) = await SeedAsync(writer);

        await using var deleterHost = Build(decorate: true);
        var deleter = deleterHost.GetRequiredService<IRedbService>();
        await deleter.InitializeAsync(ensureCreated: true);

        var interleaved = 0;
        _recorder.AfterQuery = async sql =>
        {
            if (!CollectsSubtree(sql) || Interlocked.Exchange(ref interleaved, 1) == 1) return;
            (await writer.DeleteAsync(grandchild)).Should().BeTrue("precondition: the other session deletes the grandchild");
        };
        int deleted;
        try
        {
            deleted = await deleter.DeleteSubtreeAsync(new RedbObject { id = root });
        }
        finally
        {
            _recorder.AfterQuery = null;
        }

        interleaved.Should().Be(1, "precondition: the delete ran between the collection and the delete of the subtree");
        deleted.Should().Be(4, "the grandchild was deleted by the other session, not by this call");
        (await writer.LoadAsync<SimpleProps>(root)).Should().BeNull();
    }
}
