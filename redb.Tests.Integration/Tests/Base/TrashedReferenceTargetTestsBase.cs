using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Extensions;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// A reference whose target left for the trash loads as null in its place, in a collection too: an array keeps its
/// length and a dictionary keeps the key. The PostgreSQL and SQLite Free builders and every Pro materializer agree; the
/// MSSQL Free builder dropped the element (STRING_AGG skips the NULL of the nested get_object_json) and the dictionary
/// key with it. Found by the Free host of <see cref="ListItemObjectWalkTestsBase"/> (plan
/// docs/V4/LISTITEM_OBJECT_WALKERS_AND_PRO_SYNC_LOAD_PLAN.md).
/// </summary>
public abstract class TrashedReferenceTargetTestsBase
{
    protected abstract void UseProvider(RedbOptionsBuilder options);

    protected virtual void Register(IServiceCollection services, Action<RedbOptionsBuilder> configure)
        => services.AddRedb(configure);

    private ServiceProvider Build()
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        Register(services, options =>
        {
            UseProvider(options);
            options.Configure(c =>
            {
                c.EnablePropsCache = false;
                c.CacheDomain = "trashed-reference-target";
            });
        });
        return services.BuildServiceProvider();
    }

    private sealed record Seed(long RootId, long KeptId);

    private static async Task<Seed> SeedAsync(ServiceProvider sp)
    {
        var tag = Guid.NewGuid().ToString("N")[..8];
        var redb = sp.GetRequiredService<IRedbService>();
        await redb.InitializeAsync(ensureCreated: true);
        await redb.SyncSchemeAsync<CtProbeChildProps>();
        await redb.SyncSchemeAsync<CtProbeProps>();
        await redb.InitializeTypeRegistryAsync();

        var kept = await redb.SaveAsync(new RedbObject<CtProbeChildProps> { name = $"trashref-kept-{tag}", Props = new CtProbeChildProps { Tag = "kept" } });
        var gone = await redb.SaveAsync(new RedbObject<CtProbeChildProps> { name = $"trashref-gone-{tag}", Props = new CtProbeChildProps { Tag = "gone" } });
        var rootId = await redb.SaveAsync(new RedbObject<CtProbeProps>
        {
            name = $"trashref-root-{tag}",
            Props = new CtProbeProps
            {
                Label = "root",
                Refs = [new RedbObject<CtProbeChildProps> { id = kept }, new RedbObject<CtProbeChildProps> { id = gone }],
                RefDict = new()
                {
                    ["kept"] = new RedbObject<CtProbeChildProps> { id = kept },
                    ["gone"] = new RedbObject<CtProbeChildProps> { id = gone }
                }
            }
        });
        await redb.SoftDeleteAsync(new[] { gone });
        return new Seed(rootId, kept);
    }

    private async Task<(CtProbeProps Props, Seed Seed)> LoadAsync()
    {
        await using var sp = Build();
        var seed = await SeedAsync(sp);

        await using var scope = sp.CreateAsyncScope();
        var root = await scope.ServiceProvider.GetRequiredService<IRedbService>().LoadAsync<CtProbeProps>(seed.RootId, depth: 10);
        return (root!.Props, seed);
    }

    [Fact]
    public async Task Load_TrashedTargetInArray_IsNullInItsPlace()
    {
        var (props, seed) = await LoadAsync();

        props.Refs.Should().HaveCount(2, "the array keeps its length: a trashed target is null in its place, never dropped");
        props.Refs![0].id.Should().Be(seed.KeptId);
        props.Refs[0].Props.Tag.Should().Be("kept");
        props.Refs[1].Should().BeNull("the target left for the trash");
    }

    [Fact]
    public async Task Load_TrashedTargetInDictionary_KeepsItsKeyWithNull()
    {
        var (props, seed) = await LoadAsync();

        props.RefDict.Should().NotBeNull();
        props.RefDict!.Keys.Should().BeEquivalentTo(new[] { "kept", "gone" },
            "the dictionary keeps the key of a trashed target, with null as its value");
        props.RefDict["kept"].id.Should().Be(seed.KeptId);
        props.RefDict["kept"].Props.Tag.Should().Be("kept");
        props.RefDict["gone"].Should().BeNull("the target left for the trash");
    }
}
