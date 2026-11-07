using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Extensions;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// The object behind a list item whose scheme has no CLR type loads untyped, through the Free in-database JSON builder.
/// The SQLite service sent the PostgreSQL text of that call (<c>::text</c> cast included), which SQLite cannot parse, so
/// on Free SQLite such a load never worked. Free hosts only: the Pro answer for an untyped scheme is a separate decision.
/// </summary>
public abstract class UntypedLinkedObjectTestsBase
{
    protected abstract void UseProvider(RedbOptionsBuilder options);

    private ServiceProvider Build()
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        services.AddRedb(options =>
        {
            UseProvider(options);
            options.Configure(c =>
            {
                c.EnablePropsCache = false;
                c.PreloadListItemLinkedObjects = false;
                c.CacheDomain = "untyped-linked-object";
            });
        });
        return services.BuildServiceProvider();
    }

    private async Task RunAsync(Func<RedbListItem, long, string, Task> touch)
    {
        var tag = Guid.NewGuid().ToString("N")[..8];
        await using var sp = Build();
        var redb = sp.GetRequiredService<IRedbService>();
        await redb.InitializeAsync(ensureCreated: true);
        await redb.SyncSchemeAsync<SimpleProps>();
        await redb.InitializeTypeRegistryAsync();

        // An object of a scheme no CLR type maps to: saved typed, then moved to an untyped object scheme.
        var name = $"untyped-linked-{tag}";
        var objectId = await redb.SaveAsync(new RedbObject<SimpleProps> { name = name, Props = new SimpleProps { Title = "untyped" } });
        var untypedScheme = await redb.EnsureObjectSchemeAsync($"untyped_linked_scheme_{tag}");
        await redb.Context.ExecuteAsync($"UPDATE _objects SET _id_scheme = {untypedScheme.Id} WHERE _id = {objectId}");

        var list = await redb.ListProvider.SaveListAsync(RedbList.Create($"untyped-linked-{tag}", "untyped"));
        var item = await redb.ListProvider.SaveListItemAsync(new RedbListItem { IdList = list.Id, Value = "untyped", IdObject = objectId });

        await using var scope = sp.CreateAsyncScope();
        var reader = scope.ServiceProvider.GetRequiredService<IRedbService>();
        reader.Cache.GetClrType(untypedScheme.Id).Should().BeNull("precondition: no CLR type maps to the scheme");
        var handed = await reader.ListProvider.GetListItemAsync(item.Id);
        handed.Should().NotBeNull();
        handed!.IsObjectLoaded.Should().BeFalse("precondition: no preload, the single loader resolves the object");

        await touch(handed, objectId, name);
    }

    private static void AssertLoaded(IRedbObject? loaded, long objectId, string name)
    {
        loaded.Should().NotBeNull("an object of an untyped scheme loads untyped rather than failing");
        loaded!.Id.Should().Be(objectId);
        loaded.Name.Should().Be(name);
    }

    [Fact]
    public Task SyncGetter_SchemeWithoutClrType_LoadsTheLinkedObjectUntyped()
        => RunAsync((item, objectId, name) =>
        {
            AssertLoaded(item.Object, objectId, name);
            return Task.CompletedTask;
        });

    [Fact]
    public Task AsyncLoad_SchemeWithoutClrType_LoadsTheLinkedObjectUntyped()
        => RunAsync(async (item, objectId, name) => AssertLoaded(await item.GetObjectAsync(), objectId, name));
}
