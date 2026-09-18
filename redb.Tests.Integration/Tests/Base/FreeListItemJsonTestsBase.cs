using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Extensions;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Plan docs/V4/LISTITEM_OBJECT_WALKERS_AND_PRO_SYNC_LOAD_PLAN.md, part C. The Free in-database JSON builders wrote a list
/// item as <c>{id, idList, value, alias[, object]}</c>, while the model reads <c>id_list</c> and <c>id_object</c>: every
/// Free load lost both the list link and the object link of the list items in Props, and PostgreSQL / SQLite built the
/// whole linked object for a key the deserializer ignores. Owner decision 2026-09-15: a list item is
/// <c>{id, id_list, value, alias, id_object}</c>, with no nested object.
/// </summary>
public abstract class FreeListItemJsonTestsBase
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
                c.CacheDomain = "free-listitem-json";
            });
        });
        return services.BuildServiceProvider();
    }

    private sealed record Seed(long RootId, long ListId, long LinkedItemId, long PlainItemId, long LinkedObjectId);

    private static async Task<Seed> SeedAsync(ServiceProvider sp)
    {
        var tag = Guid.NewGuid().ToString("N")[..8];
        var redb = sp.GetRequiredService<IRedbService>();
        await redb.InitializeAsync(ensureCreated: true);
        await redb.SyncSchemeAsync<SimpleProps>();
        await redb.SyncSchemeAsync<CtProbeChildProps>();
        await redb.SyncSchemeAsync<CtProbeProps>();
        await redb.InitializeTypeRegistryAsync();

        var linkedObjectId = await redb.SaveAsync(new RedbObject<SimpleProps>
            { name = $"lijson-linked-{tag}", Props = new SimpleProps { Title = "linked" } });
        var list = await redb.ListProvider.SaveListAsync(RedbList.Create($"lijson-{tag}", "lijson"));
        var linked = await redb.ListProvider.SaveListItemAsync(new RedbListItem { IdList = list.Id, Value = "linked", IdObject = linkedObjectId });
        var plain = await redb.ListProvider.SaveListItemAsync(new RedbListItem { IdList = list.Id, Value = "plain" });
        var rootId = await redb.SaveAsync(new RedbObject<CtProbeProps>
        {
            name = $"lijson-root-{tag}",
            Props = new CtProbeProps { Label = "root", Status = linked, Roles = [linked, plain] }
        });
        return new Seed(rootId, list.Id, linked.Id, plain.Id, linkedObjectId);
    }

    private static void AssertLinks(string path, CtProbeProps props, Seed seed)
    {
        AssertItem($"{path}, Status", props.Status, seed.LinkedItemId, seed.ListId, "linked", seed.LinkedObjectId);
        props.Roles.Should().HaveCount(2, $"{path}: both list items are materialized");
        AssertItem($"{path}, Roles[0]", props.Roles![0], seed.LinkedItemId, seed.ListId, "linked", seed.LinkedObjectId);
        AssertItem($"{path}, Roles[1]", props.Roles[1], seed.PlainItemId, seed.ListId, "plain", null);
    }

    private static void AssertItem(string path, RedbListItem? item, long id, long listId, string value, long? objectId)
    {
        item.Should().NotBeNull($"{path}: the list item is materialized");
        item!.Id.Should().Be(id, $"{path}: id");
        item.Value.Should().Be(value, $"{path}: value");
        item.IdList.Should().Be(listId, $"{path}: the item keeps the list it belongs to");
        item.IdObject.Should().Be(objectId, $"{path}: the item keeps its object link");
    }

    private static readonly string[] ItemJsonKeys = ["id", "id_list", "value", "alias", "id_object"];

    private static void AssertItemJson(string path, JsonElement item, long? objectId)
    {
        item.EnumerateObject().Select(p => p.Name).Should().BeEquivalentTo(ItemJsonKeys,
            $"{path}: a list item in the object JSON is exactly id, id_list, value, alias, id_object - no idList, no nested object");
        var link = item.GetProperty("id_object");
        if (objectId == null)
            link.ValueKind.Should().Be(JsonValueKind.Null, $"{path}: the item has no object link");
        else
            link.GetInt64().Should().Be(objectId.Value, $"{path}: the object link");
    }

    [Fact]
    public async Task LoadAsync_ListItems_KeepListAndObjectLinks()
    {
        await using var sp = Build();
        var seed = await SeedAsync(sp);

        await using var scope = sp.CreateAsyncScope();
        var root = await scope.ServiceProvider.GetRequiredService<IRedbService>().LoadAsync<CtProbeProps>(seed.RootId, depth: 10);

        AssertLinks("async load", root!.Props, seed);
    }

    [Fact]
    public async Task SyncLoad_ListItems_KeepListAndObjectLinks()
    {
        await using var sp = Build();
        var seed = await SeedAsync(sp);

        await using var scope = sp.CreateAsyncScope();
        var root = scope.ServiceProvider.GetRequiredService<IRedbService>().Load<CtProbeProps>(seed.RootId, depth: 10);

        AssertLinks("sync load", root!.Props, seed);
    }

    [Fact]
    public async Task ObjectJson_ListItem_IsIdListValueAliasObjectLink_WithoutNestedObject()
    {
        await using var sp = Build();
        var seed = await SeedAsync(sp);

        await using var scope = sp.CreateAsyncScope();
        var json = await scope.ServiceProvider.GetRequiredService<IRedbService>().LoadJsonAsync(seed.RootId, depth: 10);

        using var doc = JsonDocument.Parse(json!);
        var properties = doc.RootElement.GetProperty("properties");
        AssertItemJson("Status", properties.GetProperty("Status"), seed.LinkedObjectId);
        var roles = properties.GetProperty("Roles");
        AssertItemJson("Roles[0]", roles[0], seed.LinkedObjectId);
        AssertItemJson("Roles[1]", roles[1], null);
    }
}
