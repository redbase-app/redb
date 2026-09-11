using redb.Core;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// The free-form <c>_tags</c> marker (V4): a reserved 450-character slot on <c>_schemes</c> and
/// <c>_structures</c> (mirrored into the metadata cache, indexed on structures) for future and
/// custom extensions. The contract pinned here: <c>[RedbTags]</c> writes it at synchronisation,
/// and a column WITHOUT the attribute is never touched by sync - direct writes survive.
/// </summary>
public abstract class TagsFieldTestsBase : IAsyncLifetime
{
    protected readonly IRedbService Redb;

    protected TagsFieldTestsBase(IRedbService redb) => Redb = redb;

    public async Task InitializeAsync() => await ResetAsync();

    public async Task DisposeAsync() => await ResetAsync();

    private async Task ResetAsync()
    {
        await Redb.Context.ExecuteAsync(
            "DELETE FROM _objects WHERE _id_scheme IN (SELECT _id FROM _schemes WHERE _name = 'TagsProbe')");
        await Redb.Context.ExecuteAsync(
            "DELETE FROM _structures WHERE _id_scheme IN (SELECT _id FROM _schemes WHERE _name = 'TagsProbe')");
        await Redb.Context.ExecuteAsync("DELETE FROM _schemes WHERE _name = 'TagsProbe'");
    }

    [Fact]
    public async Task TagsColumns_AreDelivered_ByInitialize()
    {
        await Redb.InitializeAsync();

        // The queries themselves are the pin: a missing column fails them on every provider.
        await Redb.Context.ExecuteScalarAsync<long?>("SELECT COUNT(*) FROM _schemes WHERE _tags IS NOT NULL");
        await Redb.Context.ExecuteScalarAsync<long?>("SELECT COUNT(*) FROM _structures WHERE _tags IS NOT NULL");
        await Redb.Context.ExecuteScalarAsync<long?>("SELECT COUNT(*) FROM _scheme_metadata_cache WHERE _tags IS NOT NULL");
    }

    [Fact]
    public async Task RedbTags_WritesSchemeAndStructureMarkers_AtSync()
    {
        await Redb.SyncSchemeAsync<TagsProbeProps>();

        var schemeTags = await Redb.Context.ExecuteScalarAsync<string?>(
            "SELECT _tags FROM _schemes WHERE _name = 'TagsProbe'");
        schemeTags.Should().Be("scheme-level,reserved", "[RedbTags] on the Props class writes the scheme marker");

        var markedTags = await Redb.Context.ExecuteScalarAsync<string?>(
            "SELECT _tags FROM _structures WHERE _name = 'Marked' AND _id_scheme = " +
            "(SELECT _id FROM _schemes WHERE _name = 'TagsProbe')");
        markedTags.Should().Be("prop-level", "[RedbTags] on a property writes the structure marker");

        var plainTags = await Redb.Context.ExecuteScalarAsync<string?>(
            "SELECT _tags FROM _structures WHERE _name = 'Plain' AND _id_scheme = " +
            "(SELECT _id FROM _schemes WHERE _name = 'TagsProbe')");
        plainTags.Should().BeNull("a property without the attribute gets no marker");
    }

    [Fact]
    public async Task DirectWrite_SurvivesResync()
    {
        await Redb.SyncSchemeAsync<TagsProbeProps>();

        // An application or extension writes its own marker on an attribute-free structure...
        await Redb.Context.ExecuteAsync(
            "UPDATE _structures SET _tags = 'custom-extension' WHERE _name = 'Plain' AND _id_scheme = " +
            "(SELECT _id FROM _schemes WHERE _name = 'TagsProbe')");

        // ...and synchronisation must leave it alone: no attribute - no write.
        await Redb.SyncSchemeAsync<TagsProbeProps>();

        var plainTags = await Redb.Context.ExecuteScalarAsync<string?>(
            "SELECT _tags FROM _structures WHERE _name = 'Plain' AND _id_scheme = " +
            "(SELECT _id FROM _schemes WHERE _name = 'TagsProbe')");
        plainTags.Should().Be("custom-extension", "sync never wipes a marker it did not write");
    }
}
