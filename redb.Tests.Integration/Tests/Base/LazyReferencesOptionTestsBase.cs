using redb.Core;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// V4 wave 5 (Л2): the `virtual` marker + the lazy-references option. The marker lands in
/// `_structures._lazy` at synchronisation (the code is the source of truth); with the option on the
/// builders emit a stub for a marked reference REGARDLESS of depth, while a non-virtual reference
/// stays eager. The per-query override `WithLazyReferences(bool)` rides the session flag of the
/// context's connection — the Free builders honour it; the Pro materializer reads only the global
/// option (a recorded Л2 boundary), which subclasses express via <see cref="PerQueryOverrideWorks"/>.
/// </summary>
public abstract class LazyReferencesOptionTestsBase
{
    protected readonly IRedbService Redb;

    protected LazyReferencesOptionTestsBase(IRedbService redb) => Redb = redb;

    /// <summary>Free builders read the session flag; the Pro materializer does not (Л2 boundary).</summary>
    protected virtual bool PerQueryOverrideWorks => true;

    /// <summary>Pro reads the GLOBAL option live from the configuration object; the Free channel is armed at connection open.</summary>
    protected virtual bool GlobalMutationAffects => false;

    private async Task<(long rootId, long nextId, long plainId)> SaveTriangleAsync(string tag)
    {
        var nextId = await Redb.SaveAsync(new RedbObject<LazyOptionNodeProps>
            { name = $"lopt-next-{tag}", Props = new LazyOptionNodeProps { Label = $"next-{tag}" } });
        var plainId = await Redb.SaveAsync(new RedbObject<LazyOptionNodeProps>
            { name = $"lopt-plain-{tag}", Props = new LazyOptionNodeProps { Label = $"plain-{tag}" } });
        var rootId = await Redb.SaveAsync(new RedbObject<LazyOptionNodeProps>
        {
            name = $"lopt-root-{tag}",
            Props = new LazyOptionNodeProps
            {
                Label = $"root-{tag}",
                Next = new RedbObject<LazyOptionNodeProps> { id = nextId },
                Plain = new RedbObject<LazyOptionNodeProps> { id = plainId },
                Children = new List<RedbObject<LazyOptionNodeProps>> { new() { id = nextId } },
            }
        });
        return (rootId, nextId, plainId);
    }

    private Task<long?> StructureLazyAsync(string fieldName)
        => Redb.Context.ExecuteScalarAsync<long?>(
            "SELECT CAST(_lazy AS INT) FROM _structures WHERE _name = '" + fieldName + "' AND _id_scheme = " +
            "(SELECT _id FROM _schemes WHERE _name = '" + typeof(LazyOptionNodeProps).FullName + "')");

    [Fact]
    public async Task Sync_WritesLazyFromVirtual_AndRestoresItOverManualEdits()
    {
        await Redb.SyncSchemeAsync<LazyOptionNodeProps>();

        (await StructureLazyAsync("Next")).Should().Be(1, "virtual reference -> _lazy");
        (await StructureLazyAsync("Children")).Should().Be(1, "virtual collection of references -> _lazy");
        ((await StructureLazyAsync("Plain")) ?? 0).Should().Be(0, "a non-virtual reference is not lazy");
        ((await StructureLazyAsync("Label")) ?? 0).Should().Be(0, "virtual matters only on references");

        // The code is the source of truth: a hand-flipped flag is restored on the next sync.
        await Redb.Context.ExecuteAsync(
            "UPDATE _structures SET _lazy = NULL WHERE _name = 'Next' AND _id_scheme = " +
            "(SELECT _id FROM _schemes WHERE _name = '" + typeof(LazyOptionNodeProps).FullName + "')");
        Redb.Cache.Clear();
        await Redb.SyncSchemeAsync<LazyOptionNodeProps>();
        (await StructureLazyAsync("Next")).Should().Be(1);
    }

    [Fact]
    public async Task PerQueryOverride_VirtualBecomesStub_PlainStaysEager()
    {
        var (rootId, nextId, plainId) = await SaveTriangleAsync("override");

        var results = await Redb.Query<LazyOptionNodeProps>()
            .WhereRedb(o => o.Id == rootId)
            .WithLazyReferences(true)
            .ToListAsync();
        results.Should().HaveCount(1);
        var root = results[0];
        root.Props.Should().NotBeNull();

        var next = root.Props.Next!;
        var plain = root.Props.Plain!;
        next.Should().NotBeNull();
        plain.Should().NotBeNull();
        plain.id.Should().Be(plainId);
        next.id.Should().Be(nextId);

        if (PerQueryOverrideWorks)
        {
            next.IsPropsLoaded.Should().BeFalse("a virtual reference under WithLazyReferences(true) is a stub");
            next.hash.Should().NotBeNull("the stub carries the hash (W1 shape)");
            plain.IsPropsLoaded.Should().BeTrue("a non-virtual reference stays eager whatever the option says");
            root.Props.Children![0].IsPropsLoaded.Should().BeFalse("the collection branch of the builders' gate honours the marker too");

            next.Props!.Label.Should().Be("next-override", "the stub is loadable on first access");
        }
        else
        {
            // Pro: the materializer reads only the global option — the override is a recorded boundary.
            next.IsPropsLoaded.Should().BeTrue("the per-query override does not reach the Pro materializer (Л2 boundary)");
        }
    }

    [Fact]
    public async Task GlobalOption_ProMaterializerEmitsEnrichedStubs()
    {
        if (!GlobalMutationAffects)
        {
            // The Free channel is armed when the context's connection opens; mutating the shared
            // configuration here cannot re-arm it. The Free global path is covered by
            // LazyHostTestsBase.GlobalOption_HoldsOnEveryScope (own host, option on from the start).
            return;
        }

        var (rootId, nextId, plainId) = await SaveTriangleAsync("global");

        Redb.Configuration.EnableLazyReferences = true;
        try
        {
            var root = await Redb.LoadAsync<LazyOptionNodeProps>(rootId, depth: 10);
            var next = root!.Props.Next!;
            next.Should().NotBeNull();
            next.IsPropsLoaded.Should().BeFalse("with the option on, a virtual reference is a stub at ANY depth");
            next.hash.Should().NotBeNull("the loader enriches the placeholder to the W1 stub shape");
            root.Props.Plain!.IsPropsLoaded.Should().BeTrue("non-virtual stays eager");
            root.Props.Children![0].IsPropsLoaded.Should().BeFalse("the array branch of the materializer gate honours the marker too");

            next.Props!.Label.Should().Be("next-global", "the stub is loadable on first access");
        }
        finally
        {
            Redb.Configuration.EnableLazyReferences = false;
        }
    }

    [Fact]
    public async Task CollectionOfVirtualReferences_LoadReferencesAsync_OneBatch()
    {
        var tag = "batch";
        var c1 = await Redb.SaveAsync(new RedbObject<LazyOptionNodeProps>
            { name = $"lopt-c1-{tag}", Props = new LazyOptionNodeProps { Label = $"c1-{tag}" } });
        var c2 = await Redb.SaveAsync(new RedbObject<LazyOptionNodeProps>
            { name = $"lopt-c2-{tag}", Props = new LazyOptionNodeProps { Label = $"c2-{tag}" } });
        var parentId = await Redb.SaveAsync(new RedbObject<LazyOptionNodeProps>
        {
            name = $"lopt-parent-{tag}",
            Props = new LazyOptionNodeProps
            {
                Label = $"parent-{tag}",
                Children =
                [
                    new RedbObject<LazyOptionNodeProps> { id = c1 },
                    new RedbObject<LazyOptionNodeProps> { id = c2 },
                ]
            }
        });

        // depth 1 gives stubs by depth alone — the reload API must work without the option too.
        var parent = await Redb.LoadAsync<LazyOptionNodeProps>(parentId, depth: 1);
        var children = parent!.Props.Children!;
        children.Should().OnlyContain(c => !c.IsPropsLoaded);

        await Redb.LoadReferencesAsync(parent, p => p.Children);

        children.Should().OnlyContain(c => c.IsPropsLoaded, "one batch loads the whole collection (§4.6)");
        children.Select(c => c.Props!.Label).Should().BeEquivalentTo($"c1-{tag}", $"c2-{tag}");
    }

    [Fact]
    public async Task OptionOff_BehaviourUnchanged()
    {
        var (rootId, _, _) = await SaveTriangleAsync("off");

        var root = await Redb.LoadAsync<LazyOptionNodeProps>(rootId, depth: 2);
        root!.Props.Next!.IsPropsLoaded.Should().BeTrue("without the option, depth alone decides");
        root.Props.Plain!.IsPropsLoaded.Should().BeTrue();
    }
}
