using redb.Core;
using redb.Core.Models.Entities;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// A bare (non-generic) <c>RedbObject</c> declared as a Props field (discussion #12, п.6). The
/// scheme sync used to classify it as a business CLASS and reflect over its own properties - the
/// framework's service fields in two casings (<c>id</c>/<c>Id</c>, <c>name</c>/<c>Name</c>...), so
/// MSSQL's CI collation hit UNIQUE <c>IX__structures</c>, PostgreSQL hit the reserved-name trigger
/// (23514), and the user got a driver error naming neither the field nor the cure. A reference
/// field is declared as <c>RedbObject&lt;TProps&gt;</c> - the sync must say exactly that.
/// </summary>
public abstract class BareRedbObjectFieldTestsBase : IAsyncLifetime
{
    protected readonly IRedbService Redb;

    protected BareRedbObjectFieldTestsBase(IRedbService redb) => Redb = redb;

    public Task InitializeAsync() => CleanupAsync();
    public Task DisposeAsync() => CleanupAsync();

    private async Task CleanupAsync()
    {
        // Order matters: a refused sync may leave the scheme with a few structures already
        // inserted (the refusal fires mid-walk), and _structures holds an FK to _schemes.
        const string schemes =
            "SELECT _id FROM _schemes WHERE _name IN ('redb.Tests.Integration.Tests.Base.BareRefProbeProps', 'redb.Tests.Integration.Tests.Base.BareRefListProbeProps')";
        await Redb.Context.ExecuteAsync($"DELETE FROM _objects WHERE _id_scheme IN ({schemes})");
        await Redb.Context.ExecuteAsync($"DELETE FROM _structures WHERE _id_scheme IN ({schemes})");
        await Redb.Context.ExecuteAsync($"DELETE FROM _schemes WHERE _id IN ({schemes})");
    }

    [Fact]
    public async Task BareRedbObjectField_SyncRefusesLoudly()
    {
        var act = async () => await Redb.SyncSchemeAsync<BareRefProbeProps>();

        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*RedbObject<TProps>*",
                "a bare RedbObject field must be refused with the cure in the message, " +
                "not crash on a unique index or a reserved-name trigger");
    }

    [Fact]
    public async Task BareRedbObjectCollection_SyncRefusesLoudly()
    {
        var act = async () => await Redb.SyncSchemeAsync<BareRefListProbeProps>();

        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*RedbObject<TProps>*");
    }
}

// No [RedbScheme]: the fixtures auto-sync every attributed model in the assembly at start-up,
// and these two must fail only inside the pin, not kill the whole collection.
public class BareRefProbeProps
{
    public string? Label { get; set; }
    public RedbObject? Target { get; set; }
}

public class BareRefListProbeProps
{
    public string? Label { get; set; }
    public List<RedbObject>? Targets { get; set; }
}
