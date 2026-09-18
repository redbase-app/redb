using redb.Core.Models.Entities;
using redb.Core.Providers;
using redb.Core.Utils;

namespace redb.Tests.Integration.Tests.Unit;

/// <summary>
/// Reflective walkers must treat RedbListItem as a LEAF (production stand, 2026-09-09): the
/// item's lazy Object getter LOADS on read, and the loader-install walk read every property of
/// every business object - including Object - so materializing anything with list-item fields
/// fired a synchronous phantom load per item (~150/s on the stand, with not a single explicit
/// .Object anywhere in the application code).
/// <para>
/// No redb scope is live in these tests: a walk that read Object would load nothing and throw instead (a lazy load runs
/// on the reader's scope, owner decision 2026-09-15), and would leave the item marked loaded under a configured fresh
/// scope - both are caught below.
/// </para>
/// </summary>
public class LazyWalkersListItemLeafTests
{
    private sealed class PropsWithItem
    {
        public string Label { get; set; } = "";
        public RedbListItem? Status { get; set; }
    }

    private static (RedbObject<PropsWithItem> root, RedbListItem item) Make()
    {
        var item = new RedbListItem { Id = 5, IdList = 1, Value = "status", IdObject = 42 };
        var root = new RedbObject<PropsWithItem>
        {
            id = 1,
            scheme_id = 100,
            Props = new PropsWithItem { Label = "root", Status = item },
        };
        return (root, item);
    }

    [Fact]
    public void LoaderInstall_DoesNotWakeTheItemsObject()
    {
        var (root, item) = Make();

        var install = () => LazyReferenceInstaller.Install(root, new NoopLoader());

        install.Should().NotThrow("installing lazy loaders is a wiring walk - it must never read Object");
        item.IsObjectLoaded.Should().BeFalse();
    }

    [Fact]
    public void MarkUnloaded_DoesNotWakeTheItemsObject()
    {
        var (root, item) = Make();

        var mark = () => LazyReferenceInstaller.MarkUnloadedWherePropsAreNull(root);

        mark.Should().NotThrow();
        item.IsObjectLoaded.Should().BeFalse();
    }

    [Fact]
    public void MarkShared_DoesNotWakeTheItemsObject()
    {
        var (root, item) = Make();

        var mark = () => LazyReferenceInstaller.MarkShared(root);

        mark.Should().NotThrow("marking a cached graph shared walks it without reading Object");
        item.IsObjectLoaded.Should().BeFalse();
    }

    [Fact]
    public void MarkShared_SharesTheObjectAnItemAlreadyCarries()
    {
        var nestedItem = new RedbListItem { Id = 6, IdList = 1, Value = "nested", IdObject = 43 };
        var carried = new RedbObject<PropsWithItem>
        {
            id = 42,
            scheme_id = 100,
            Props = new PropsWithItem { Label = "carried", Status = nestedItem },
        };
        var (root, item) = Make();
        item.Object = carried;

        LazyReferenceInstaller.MarkShared(root);

        IsShared(item).Should().BeTrue("precondition: the item itself is marked");
        IsShared(carried).Should().BeTrue("the object an item already carries is shared with the item");
        IsShared(nestedItem).Should().BeTrue("and so is what that object holds");
    }

    /// <summary>The internal shared mark; the core exposes no internals to tests.</summary>
    private static bool IsShared(object instance)
    {
        for (var type = instance.GetType(); type != null; type = type.BaseType)
        {
            var field = type.GetField("_isShared", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            if (field != null) return (bool)field.GetValue(instance)!;
        }
        throw new InvalidOperationException($"{instance.GetType().Name} carries no shared mark");
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
