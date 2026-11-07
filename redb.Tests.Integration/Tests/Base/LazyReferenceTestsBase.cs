using System.Text.Json;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Core.Serialization;
using redb.Core.Utils;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// V4 wave 4 (Л1, LAZY_REFERENCES_PLAN §3.6/§6): with the old Props-level lazy mechanism removed,
/// `LoadAsync(depth: 1)` gives working lazy references for free — every reference at the boundary
/// is a stub (W1 contract) that now carries a loader (L.3). First access to its Props loads exactly
/// that object, with depth 1, so its own references are stubs again: laziness is transitive and
/// independent of the original depth. The parent's hash takes each reference's own hash (L.2), so
/// eager and lazy hashes agree and computing one never wakes the graph. Serialization goes through
/// the SerializedProps bridge (L.4) and never wakes it either.
/// </summary>
public abstract class LazyReferenceTestsBase
{
    protected readonly IRedbService Redb;

    protected LazyReferenceTestsBase(IRedbService redb) => Redb = redb;

    /// <summary>root → mid → leaf, references by id (never re-saved by the parent — W1).</summary>
    private async Task<(long rootId, long midId, long leafId)> SaveChainAsync(string tag)
    {
        var leafId = await Redb.SaveAsync(new RedbObject<LazyNodeProps>
        {
            name = $"lazy-leaf-{tag}",
            Props = new LazyNodeProps { Label = $"leaf-{tag}" }
        });
        var midId = await Redb.SaveAsync(new RedbObject<LazyNodeProps>
        {
            name = $"lazy-mid-{tag}",
            Props = new LazyNodeProps { Label = $"mid-{tag}", Next = new RedbObject<LazyNodeProps> { id = leafId } }
        });
        var rootId = await Redb.SaveAsync(new RedbObject<LazyNodeProps>
        {
            name = $"lazy-root-{tag}",
            Props = new LazyNodeProps { Label = $"root-{tag}", Next = new RedbObject<LazyNodeProps> { id = midId } }
        });
        return (rootId, midId, leafId);
    }

    [Fact]
    public async Task FirstAccess_LoadsThatObject_ItsReferencesAreStubsAgain()
    {
        var (rootId, midId, leafId) = await SaveChainAsync("transitive");

        var root = await Redb.LoadAsync<LazyNodeProps>(rootId, depth: 1);
        var midStub = root!.Props.Next!;
        midStub.IsPropsLoaded.Should().BeFalse("at depth 1 the reference is a stub");
        midStub.id.Should().Be(midId);

        // First access loads exactly this object…
        var midProps = midStub.Props;
        midProps.Should().NotBeNull();
        midProps!.Label.Should().Be("mid-transitive");
        midStub.IsPropsLoaded.Should().BeTrue();

        // …and the references under it are stubs again (the loader passes depth 1 — §4.7),
        // which are themselves loadable: laziness is transitive.
        var leafStub = midProps.Next!;
        leafStub.Should().NotBeNull("the boundary yields a stub, never null (W1 contract)");
        leafStub.IsPropsLoaded.Should().BeFalse("the reloaded object's own references are stubs again");
        leafStub.id.Should().Be(leafId);
        leafStub.Props!.Label.Should().Be("leaf-transitive");
    }

    [Fact]
    public async Task FirstAccess_LoadsNestedClassesCompletely()
    {
        var leafId = await Redb.SaveAsync(new RedbObject<LazyNodeProps>
        {
            name = "lazy-leaf-meta",
            Props = new LazyNodeProps { Label = "leaf-meta" }
        });
        var midId = await Redb.SaveAsync(new RedbObject<LazyNodeProps>
        {
            name = "lazy-mid-meta",
            Props = new LazyNodeProps
            {
                Label = "mid-meta",
                Meta = new LazyNodeMeta { Tag = "m" },
                Next = new RedbObject<LazyNodeProps> { id = leafId }
            }
        });
        var rootId = await Redb.SaveAsync(new RedbObject<LazyNodeProps>
        {
            name = "lazy-root-meta",
            Props = new LazyNodeProps
            {
                Label = "root-meta",
                Meta = new LazyNodeMeta { Tag = "r" },
                Next = new RedbObject<LazyNodeProps> { id = midId }
            }
        });

        // depth counts reference hops, not class nesting: a class field is loaded completely at
        // any depth by every builder, and the Pro materializer must agree.
        var root = await Redb.LoadAsync<LazyNodeProps>(rootId, depth: 1);
        root!.Props.Meta.Should().NotBeNull();
        root.Props.Meta!.Tag.Should().Be("r", "a nested class is not a reference hop");

        var mid = root.Props.Next!;
        mid.IsPropsLoaded.Should().BeFalse("the reference at the boundary is a stub");
        mid.Props!.Meta.Should().NotBeNull();
        mid.Props.Meta!.Tag.Should().Be("m", "the lazy reload (depth 1) must not cut the nested class either");
        mid.Props.Next!.IsPropsLoaded.Should().BeFalse("while the reference under it is a stub again");
    }

    [Fact]
    public async Task HashParity_EagerAndLazy_AndComputingNeverWakesTheStub()
    {
        var (rootId, _, _) = await SaveChainAsync("hashparity");

        var lazyRoot = await Redb.LoadAsync<LazyNodeProps>(rootId, depth: 1);
        var lazyHash = RedbHash.ComputeFor(lazyRoot!);
        lazyHash.Should().NotBeNull();
        lazyRoot!.Props.Next!.IsPropsLoaded.Should().BeFalse("computing the parent's hash must not wake the reference (L.2)");

        var eagerRoot = await Redb.LoadAsync<LazyNodeProps>(rootId, depth: 10);
        var eagerHash = RedbHash.ComputeFor(eagerRoot!);

        eagerHash.Should().Be(lazyHash, "the parent hashes the reference's own hash, present on stub and full object alike");
    }

    [Fact]
    public async Task FirstSave_WritesTheHashEveryReloadComputes_AndResavesKeepIt()
    {
        var (rootId, _, _) = await SaveChainAsync("stablehash");

        // The chain is saved with hand-made `{ id = x }` references that carry no hash in memory;
        // the save resolves their persisted hashes, so the very first _objects._hash is the one
        // every reload computes - the props cache lives on exactly that equality.
        var persisted = (await Redb.LoadAsync<LazyNodeProps>(rootId, depth: 1))!.hash;
        persisted.Should().NotBeNull();
        RedbHash.ComputeFor((await Redb.LoadAsync<LazyNodeProps>(rootId, depth: 10))!)
            .Should().Be(persisted, "the persisted hash must be reproducible from the loaded object");

        // An eager re-save…
        var eager = await Redb.LoadAsync<LazyNodeProps>(rootId, depth: 10);
        await Redb.SaveAsync(eager!);
        (await Redb.LoadAsync<LazyNodeProps>(rootId, depth: 1))!.hash
            .Should().Be(persisted, "re-saving an unmodified object must not shift _objects._hash");

        // …and a lazy re-save agree byte for byte: the stub carries the same id and hash.
        var lazy = await Redb.LoadAsync<LazyNodeProps>(rootId, depth: 1);
        await Redb.SaveAsync(lazy!);
        (await Redb.LoadAsync<LazyNodeProps>(rootId, depth: 1))!.hash
            .Should().Be(persisted, "the lazy and the eager form must write the same parent hash");
    }

    [Fact]
    public async Task NewNestedObjectInOneSave_ParentHashIsReproducible()
    {
        var root = new RedbObject<LazyNodeProps>
        {
            name = "lazy-root-newnested",
            Props = new LazyNodeProps
            {
                Label = "root-newnested",
                Next = new RedbObject<LazyNodeProps>
                {
                    name = "lazy-mid-newnested",
                    Props = new LazyNodeProps { Label = "mid-newnested" }
                }
            }
        };
        var rootId = await Redb.SaveAsync(root);
        root.Props.Next!.id.Should().BeGreaterThan(0, "the cascade save assigned the nested id");

        // The parent hashes "id:hash" of its reference; both exist only after the ids are handed
        // out, so the hashes are computed then, children first - never with "0:" inside.
        var persisted = (await Redb.LoadAsync<LazyNodeProps>(rootId, depth: 1))!.hash;
        RedbHash.ComputeFor((await Redb.LoadAsync<LazyNodeProps>(rootId, depth: 10))!)
            .Should().Be(persisted, "the persisted parent hash must be reproducible from a reload");
        root.hash.Should().Be(persisted, "the in-memory object carries what was written");
    }

    [Fact]
    public async Task CollectionWithOneTargetTwice_EveryInstanceLoads()
    {
        var leafId = await Redb.SaveAsync(new RedbObject<LazyNodeProps>
        {
            name = "lazy-leaf-dup",
            Props = new LazyNodeProps { Label = "leaf-dup" }
        });
        var rootId = await Redb.SaveAsync(new RedbObject<LazyNodeProps>
        {
            name = "lazy-root-dup",
            Props = new LazyNodeProps
            {
                Label = "root-dup",
                Children = Enumerable.Range(0, 6).Select(_ => new RedbObject<LazyNodeProps> { id = leafId }).ToList()
            }
        });

        // Batch form: one target referenced six times is six stub instances - all of them load.
        var root = await Redb.LoadAsync<LazyNodeProps>(rootId, depth: 1);
        root!.Props.Children.Should().HaveCount(6);
        root.Props.Children.Should().OnlyContain(c => !c.IsPropsLoaded);
        await Redb.LoadReferencesAsync(root, p => p.Children);
        root.Props.Children.Should().OnlyContain(c => c.IsPropsLoaded && c.Props.Label == "leaf-dup",
            "the batch de-duplicates by id, but every instance in the graph must end up loaded");

        // Getter form: the same six stubs touched one after another through the sync getter. (One
        // context is one connection; concurrent use of it is refused by the connection guard, so
        // the parallel variant is not a supported shape.)
        var again = await Redb.LoadAsync<LazyNodeProps>(rootId, depth: 1);
        again!.Props.Children!.Select(c => c.Props?.Label).Should().HaveCount(6).And.OnlyContain(l => l == "leaf-dup",
            "every stub of one target loads on first access, whichever instance goes first");
    }

    [Fact]
    public async Task LoadReferencesAsync_ReloadsAtDepthOne_LikeFirstAccess()
    {
        var (rootId, midId, _) = await SaveChainAsync("batchdepth");
        var holderId = await Redb.SaveAsync(new RedbObject<LazyNodeProps>
        {
            name = "lazy-holder-batchdepth",
            Props = new LazyNodeProps
            {
                Label = "holder-batchdepth",
                Children = new List<RedbObject<LazyNodeProps>> { new() { id = rootId } }
            }
        });

        var holder = await Redb.LoadAsync<LazyNodeProps>(holderId, depth: 1);
        await Redb.LoadReferencesAsync(holder!, p => p.Children);

        var rootRef = holder!.Props.Children![0];
        rootRef.IsPropsLoaded.Should().BeTrue();
        rootRef.Props.Next!.id.Should().Be(midId);
        rootRef.Props.Next.IsPropsLoaded.Should().BeFalse(
            "the batch reload passes depth 1 like a stub's first access: laziness stays transitive (§4.7)");
    }

    [Fact]
    public async Task SavingTheParent_DoesNotWakeOrDamageTheReference()
    {
        var (rootId, midId, _) = await SaveChainAsync("nowake");

        var root = await Redb.LoadAsync<LazyNodeProps>(rootId, depth: 1);
        root!.Props.Label = "renamed-nowake";
        await Redb.SaveAsync(root);

        root.Props.Next!.IsPropsLoaded.Should().BeFalse("saving the parent must not wake its references (§4.2)");

        (await Redb.LoadAsync<LazyNodeProps>(rootId, depth: 1))!.Props.Label.Should().Be("renamed-nowake");
        var midReloaded = await Redb.LoadAsync<LazyNodeProps>(midId, depth: 1);
        midReloaded!.Props.Should().NotBeNull("the referenced object must be untouched (W1 reference-wipe guard)");
        midReloaded.Props!.Label.Should().Be("mid-nowake");
    }

    [Fact]
    public async Task Serialization_WritesTheStub_AndDoesNotLoadIt()
    {
        var (rootId, midId, _) = await SaveChainAsync("serialize");

        var root = await Redb.LoadAsync<LazyNodeProps>(rootId, depth: 1);
        var json = JsonSerializer.Serialize(root, SystemTextJsonRedbSerializer.Options);

        root!.Props.Next!.IsPropsLoaded.Should().BeFalse("serializing the parent must not pull the graph (§4.3)");
        json.Should().Contain($"\"id\":{midId}", "the stub serializes as its base fields");
        json.Should().NotContain("\"Label\":\"mid-serialize\"", "the reference's CONTENT is not serialized (its name base field is)");
        json.Should().Contain("root-serialize", "the loaded root serializes in full");
    }
    [Fact]
    public async Task Deserialization_NullProperties_IsNotLoaded_AndSavingTheParentKeepsTheTarget()
    {
        var (rootId, midId, _) = await SaveChainAsync("nullprops");
        var root = await Redb.LoadAsync<LazyNodeProps>(rootId, depth: 1);
        var json = JsonSerializer.Serialize(root, SystemTextJsonRedbSerializer.Options);

        // What a foreign serializer makes of a loader-less stub: the getter answers null, so the
        // JSON carries "properties": null for the reference.
        var node = System.Text.Json.Nodes.JsonNode.Parse(json)!;
        node["properties"]!["Next"]!["properties"] = null;
        var foreign = node.ToJsonString();
        foreign.Should().Contain("\"properties\":null");

        var back = JsonSerializer.Deserialize<RedbObject<LazyNodeProps>>(foreign, SystemTextJsonRedbSerializer.Options)!;
        back.Props.Next!.IsPropsLoaded.Should().BeFalse("null Props after reading mean not loaded, never loaded-with-nothing");

        // A client round-trip: the parent comes back edited and is saved.
        back.Props.Label = "root-nullprops-edited";
        await Redb.SaveAsync(back);

        var target = await Redb.LoadAsync<LazyNodeProps>(midId, depth: 1);
        target!.Props.Label.Should().Be("mid-nullprops", "saving the parent must not wipe a reference that arrived without Props");
        (await Redb.LoadAsync<LazyNodeProps>(rootId, depth: 1))!.Props.Label.Should().Be("root-nullprops-edited");
    }

    [Fact]
    public async Task CollectionOfReferences_ArrivesAsStubs_EachLoadable()
    {
        var tag = "collection";
        var child1 = await Redb.SaveAsync(new RedbObject<LazyNodeProps>
            { name = $"lazy-c1-{tag}", Props = new LazyNodeProps { Label = $"c1-{tag}" } });
        var child2 = await Redb.SaveAsync(new RedbObject<LazyNodeProps>
            { name = $"lazy-c2-{tag}", Props = new LazyNodeProps { Label = $"c2-{tag}" } });
        var parentId = await Redb.SaveAsync(new RedbObject<LazyNodeProps>
        {
            name = $"lazy-parent-{tag}",
            Props = new LazyNodeProps
            {
                Label = $"parent-{tag}",
                Children =
                [
                    new RedbObject<LazyNodeProps> { id = child1 },
                    new RedbObject<LazyNodeProps> { id = child2 },
                ]
            }
        });

        var parent = await Redb.LoadAsync<LazyNodeProps>(parentId, depth: 1);
        var children = parent!.Props.Children!;
        children.Should().HaveCount(2);
        children.Should().OnlyContain(c => !c.IsPropsLoaded, "a collection of references arrives as a list of stubs");

        children.Select(c => c.id).Should().BeEquivalentTo(new[] { child1, child2 });
        children.First(c => c.id == child1).Props!.Label.Should().Be($"c1-{tag}");
        children.First(c => c.id == child2).Props!.Label.Should().Be($"c2-{tag}");
    }

    [Fact]
    public async Task TargetInTheTrash_StubReloadsAsNull_NotAnException()
    {
        var (rootId, midId, _) = await SaveChainAsync("trash");

        var root = await Redb.LoadAsync<LazyNodeProps>(rootId, depth: 1);
        var stub = root!.Props.Next!;
        stub.IsPropsLoaded.Should().BeFalse();

        await Redb.SoftDeleteAsync(new[] { midId });

        // The stub was obtained before the deletion; loading it now must yield null Props (§4.8).
        var props = stub.Props;
        props.Should().BeNull("a reference whose target left for the trash reloads as null, not an exception");
        stub.IsPropsLoaded.Should().BeTrue("the answer is definite — no retry storm on every access");
    }
}
