using redb.Core;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Sqlite;

/// <summary>
/// DataLoss p.3: a dictionary-structure field whose runtime value is not a plain
/// <c>Dictionary&lt;K,V&gt;</c> was skipped in silence, while DeleteInsert had already deleted the
/// stored values - the field disappeared. It is refused now, before anything is written, and a plain
/// dictionary in the same interface-typed property still round-trips.
/// </summary>
[Collection("Sqlite")]
public class SqliteDictionaryInterfaceRefusalTests : IAsyncLifetime
{
    private readonly IRedbService _redb;

    public SqliteDictionaryInterfaceRefusalTests(SqliteFixture fixture) => _redb = fixture.Redb;

    public async Task InitializeAsync() => await ResetAsync();
    public async Task DisposeAsync() => await ResetAsync();

    private async Task ResetAsync()
    {
        // Order matters: the scheme row is removed too, so the shared SQLite file does not carry an
        // extra probe scheme for the rest of the run (a scheme count assertion elsewhere would trip).
        await _redb.Context.ExecuteAsync(
            "DELETE FROM _objects WHERE _id_scheme IN " +
            "(SELECT _id FROM _schemes WHERE _name = 'DictionaryInterfaceProbe')");
        await _redb.Context.ExecuteAsync(
            "DELETE FROM _structures WHERE _id_scheme IN " +
            "(SELECT _id FROM _schemes WHERE _name = 'DictionaryInterfaceProbe')");
        await _redb.Context.ExecuteAsync("DELETE FROM _schemes WHERE _name = 'DictionaryInterfaceProbe'");
    }

    [Fact]
    public async Task NonDictionaryImplementation_IsRefused_BeforeAnythingIsWritten()
    {
        await _redb.SyncSchemeAsync<DictionaryInterfaceProbeProps>();

        // SortedDictionary implements IDictionary<,>, so the property accepts it, but it is not the
        // Dictionary<,> instance the save path can enumerate into rows.
        var act = async () => await _redb.SaveAsync(new RedbObject<DictionaryInterfaceProbeProps>
        {
            name = "iface-dict-refused",
            Props = new DictionaryInterfaceProbeProps
            {
                Label = "probe",
                Tags = new SortedDictionary<string, string> { ["a"] = "1" },
            },
        });

        var thrown = await act.Should().ThrowAsync<InvalidOperationException>();
        thrown.Which.Message.Should().Contain("Tags", "the message names the field that cannot be stored");

        var objects = await _redb.Context.ExecuteScalarAsync<long>(
            "SELECT COUNT(*) FROM _objects WHERE _name = 'iface-dict-refused'");
        objects.Should().Be(0, "the refusal lands while values are built, before any delete or insert");
    }

    [Fact]
    public async Task DictionaryImplementation_RoundTrips()
    {
        await _redb.SyncSchemeAsync<DictionaryInterfaceProbeProps>();

        var id = await _redb.SaveAsync(new RedbObject<DictionaryInterfaceProbeProps>
        {
            name = "iface-dict-ok",
            Props = new DictionaryInterfaceProbeProps
            {
                Label = "probe",
                Tags = new Dictionary<string, string> { ["a"] = "1", ["b"] = "2" },
            },
        });

        var loaded = await _redb.LoadAsync<DictionaryInterfaceProbeProps>(id, depth: 1);
        loaded!.Props.Tags.Should().NotBeNull();
        loaded.Props.Tags!["a"].Should().Be("1");
        loaded.Props.Tags!["b"].Should().Be("2");
    }
}