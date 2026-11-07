using System.Reflection;
using redb.Core.Attributes;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Core.Pro.Materialization;
using redb.Core.Providers;
using redb.Core.Utils;

namespace redb.Tests.Integration.Tests.Unit;

/// <summary>
/// Plan docs/V4/LISTITEM_OBJECT_WALKERS_AND_PRO_SYNC_LOAD_PLAN.md, item 4 (owner decision 2026-09-15: "as the save"). The
/// scheme and the save take every public instance property except <see cref="RedbIgnoreAttribute"/> and read it without
/// a catch - a throwing getter fails the save. The graph walkers of a load read <c>[RedbIgnore]</c> properties too and
/// swallowed whatever a getter threw. Now they walk the save's property set and let a getter's exception surface.
/// </summary>
public class WalkerPropertyRuleTests
{
    private sealed class Counter { public int Value; }

    private sealed class PropsWithIgnoredGetter
    {
        public string Label { get; set; } = "props";

        [RedbIgnore] public Counter Reads { get; } = new();

        [RedbIgnore]
        public string Technical
        {
            get { Reads.Value++; return "technical"; }
        }
    }

    public sealed class PropsWithThrowingGetter
    {
        public string Label { get; set; } = "props";

        public int Total => throw new InvalidOperationException("computed getter");
    }

    private static void InvokeCollectBareStubs(object node)
    {
        var method = typeof(ProLazyPropsLoader).GetMethod("CollectBareStubs", BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new InvalidOperationException("ProLazyPropsLoader.CollectBareStubs not found - update this test");
        method.Invoke(null, new object[]
        {
            node,
            new Dictionary<long, List<IRedbObject>>(),
            new HashSet<object>(ReferenceEqualityComparer.Instance)
        });
    }

    public static IEnumerable<object[]> Walkers() =>
    [
        ["loader install", (Action<object>)(props => LazyReferenceInstaller.InstallInto(props, new NoopLoader()))],
        ["mark unloaded", (Action<object>)(props => LazyReferenceInstaller.MarkUnloadedWherePropsAreNull(props))],
        ["dirty guard", (Action<object>)(props => LoadedGraphInspector.HasDirtyLoadedReference(props))],
        ["Pro stub collector", (Action<object>)InvokeCollectBareStubs],
    ];

    [Theory]
    [MemberData(nameof(Walkers))]
    public void Walker_DoesNotReadRedbIgnoreProperties(string walker, Action<object> walk)
    {
        var props = new PropsWithIgnoredGetter();

        walk(props);

        props.Reads.Value.Should().Be(0, $"{walker}: a [RedbIgnore] property is not redb's - the scheme and the save never read it");
    }

    [Theory]
    [MemberData(nameof(Walkers))]
    public void Walker_ThrowingGetter_Surfaces(string walker, Action<object> walk)
    {
        var act = () => walk(new PropsWithThrowingGetter());

        act.Should().Throw<Exception>($"{walker}: a getter that throws fails the walk as it fails the save, it is not swallowed")
            .Which.GetBaseException().Should().BeOfType<InvalidOperationException>();
    }

    [Fact]
    public void PropsHash_ThrowingGetter_Surfaces()
    {
        var act = () => RedbHash.ComputeForProps(new PropsWithThrowingGetter());

        act.Should().Throw<Exception>("the hash does not stand in an empty string for a getter that threw")
            .Which.GetBaseException().Should().BeOfType<InvalidOperationException>();
    }

    private sealed class NoopLoader : ILazyPropsLoader
    {
        public TProps? LoadProps<TProps>(long objectId, long schemeId) where TProps : class, new() => null;
        public Task<TProps?> LoadPropsAsync<TProps>(long objectId, long schemeId, CancellationToken cancellationToken = default) where TProps : class, new() => Task.FromResult<TProps?>(null);
        public Task LoadPropsForManyAsync<TProps>(List<RedbObject<TProps>> objects, CancellationToken cancellationToken = default) where TProps : class, new() => Task.CompletedTask;
        public Task LoadPropsForManyAsync<TProps>(List<RedbObject<TProps>> objects, HashSet<long>? projectedStructureIds, CancellationToken cancellationToken = default) where TProps : class, new() => Task.CompletedTask;
        public Task LoadPropsForManyAsync<TProps>(List<RedbObject<TProps>> objects, int? propsDepth, CancellationToken cancellationToken = default) where TProps : class, new() => Task.CompletedTask;
        public Task LoadPropsForManyAsync<TProps>(List<RedbObject<TProps>> objects, HashSet<long>? projectedStructureIds, int? propsDepth, CancellationToken cancellationToken = default) where TProps : class, new() => Task.CompletedTask;
    }
}
