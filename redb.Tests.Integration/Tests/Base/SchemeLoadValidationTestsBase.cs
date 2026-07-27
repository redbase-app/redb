using redb.Core;
using redb.Core.Exceptions;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// S1 — <c>LoadAsync&lt;TProps&gt;</c> verifies that the loaded object's scheme matches <c>TProps</c>,
/// so an object loaded under the wrong type never yields garbage or poisons the cache.
/// <para>
/// The <b>reaction</b> to a mismatch is configurable via <c>ThrowOnSchemeMismatch</c>: by default
/// <c>LoadAsync</c> returns <c>null</c> (so a soft-deleted object, scheme -10, reads as null — what
/// soft-delete callers expect); with the flag set it throws. See docs/CACHING_FIXES_PLAN.md (S1).
/// </para>
/// </summary>
public abstract class SchemeLoadValidationTestsBase
{
    protected readonly IRedbService Redb;

    protected SchemeLoadValidationTestsBase(IRedbService redb) => Redb = redb;

    private async Task<long> SaveSimpleAsync()
    {
        var obj = TestDataFactory.CreateSimple("S1-probe", 10m);
        return await Redb.SaveAsync(obj);
    }

    /// <summary>Runs <paramref name="body"/> with ThrowOnSchemeMismatch forced on, then restores it.</summary>
    private async Task WithStrictMismatchAsync(Func<Task> body)
    {
        var previous = Redb.Configuration.ThrowOnSchemeMismatch;
        Redb.Configuration.ThrowOnSchemeMismatch = true;
        try { await body(); }
        finally { Redb.Configuration.ThrowOnSchemeMismatch = previous; }
    }

    [Fact]
    public async Task LoadAsync_WrongType_ReturnsNull_ByDefault()
    {
        var id = await SaveSimpleAsync();

        var loaded = await Redb.LoadAsync<EmployeeProps>(id);

        loaded.Should().BeNull("default behaviour is a soft null, not an exception");
    }

    [Fact]
    public async Task LoadAsync_WrongType_Throws_WhenStrict()
    {
        var id = await SaveSimpleAsync();

        await WithStrictMismatchAsync(async () =>
        {
            var act = async () => await Redb.LoadAsync<EmployeeProps>(id);
            var ex = await act.Should().ThrowAsync<RedbSchemeMismatchException>();
            ex.Which.ObjectId.Should().Be(id);
            ex.Which.RequestedType.Should().Be(typeof(EmployeeProps));
        });
    }

    [Fact]
    public async Task LoadAsync_CorrectType_Works()
    {
        var id = await SaveSimpleAsync();

        var loaded = await Redb.LoadAsync<SimpleProps>(id);

        loaded.Should().NotBeNull();
        loaded!.Props.Title.Should().Be("S1-probe");
    }

    [Fact]
    public async Task LoadAsync_WrongType_DoesNotPoisonCache()
    {
        var id = await SaveSimpleAsync();

        // Wrong-type load must be rejected BEFORE anything is cached...
        var bad = await Redb.LoadAsync<EmployeeProps>(id);
        bad.Should().BeNull();

        // ...so the correct-type load still returns clean data (no garbage lingering in the cache).
        var good = await Redb.LoadAsync<SimpleProps>(id);
        good.Should().NotBeNull();
        good!.Props.Title.Should().Be("S1-probe");
    }

    [Fact]
    public async Task LoadAsync_SoftDeletedObject_ReadsAsNull()
    {
        var id = await SaveSimpleAsync();

        // SoftDeleteAsync (not the hard DeleteAsync) moves the object under a trash container and sets
        // its _id_scheme to -10 (@@__deleted). Loading it typed must not return it as a live SimpleProps —
        // -10 never matches a real scheme, so by default the guard returns null (what soft-delete expects).
        var mark = await Redb.SoftDeleteAsync(new[] { id });
        mark.MarkedCount.Should().BeGreaterThan(0);

        var loaded = await Redb.LoadAsync<SimpleProps>(id);
        loaded.Should().BeNull();
    }

    [Fact]
    public async Task LoadAsync_SoftDeletedObject_Throws_WhenStrict()
    {
        var id = await SaveSimpleAsync();
        var mark = await Redb.SoftDeleteAsync(new[] { id });
        mark.MarkedCount.Should().BeGreaterThan(0);

        await WithStrictMismatchAsync(async () =>
        {
            var act = async () => await Redb.LoadAsync<SimpleProps>(id);
            await act.Should().ThrowAsync<RedbSchemeMismatchException>();
        });
    }
}
