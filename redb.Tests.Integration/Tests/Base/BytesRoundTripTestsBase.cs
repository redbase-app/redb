using redb.Core;
using redb.Core.Attributes;
using redb.Core.Models.Entities;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// The byte matrix from docs/BUG_BYTES_FREE_JSON_PROJECTION.md as a permanent suite: the
/// report's repro was a manual run against published 3.7.2 packages, and five of six Free
/// cells were broken (a byte[] property classified as a Byte collection; PG emitting hex
/// instead of base64; MSSQL silently dropping value_bytes on write). Running on all six
/// fixtures turns each cell into a pin against the current tree.
/// </summary>
public abstract class BytesRoundTripTestsBase : IAsyncLifetime
{
    protected readonly IRedbService Redb;

    protected BytesRoundTripTestsBase(IRedbService redb) => Redb = redb;

    public async Task InitializeAsync() => await ResetAsync();
    public async Task DisposeAsync() => await ResetAsync();

    private async Task ResetAsync()
        => await Redb.Context.ExecuteAsync(
            "DELETE FROM _objects WHERE _id_scheme IN (SELECT _id FROM _schemes WHERE _name = 'BytesProbe')");

    [Fact]
    public async Task PropsByteArray_RoundTrips()
    {
        // §3.1 of the report: byte[] in Props was synced as a Byte COLLECTION (a row per byte)
        // and the Free JSON projection emitted an array where System.Text.Json expects a
        // base64 string - loading threw. A megabyte also meant a million _values rows.
        await Redb.SyncSchemeAsync<BytesProbeProps>();
        var payload = new byte[8192];
        new Random(42).NextBytes(payload);

        var id = await Redb.SaveAsync(new RedbObject<BytesProbeProps>
        {
            name = "bytes-props",
            Props = new BytesProbeProps { Label = "p", Blob = payload },
        });

        var loaded = await Redb.LoadAsync<BytesProbeProps>(id, depth: 1);
        loaded!.Props.Blob.Should().NotBeNull();
        loaded.Props.Blob!.SequenceEqual(payload).Should().BeTrue("byte[] Props must round-trip byte-for-byte");

        // The scalar-BLOB layout: one _values row for the property, not a row per byte.
        var rowCount = await Redb.Context.ExecuteScalarAsync<long>(
            "SELECT COUNT(*) FROM _values v JOIN _structures s ON s._id = v._id_structure " +
            $"WHERE v._id_object = {id} AND s._name = 'Blob'");
        rowCount.Should().Be(1, "a byte[] property is a scalar BLOB, not a collection of Byte rows");
    }

    public static TheoryData<int> EdgeLengths => new() { 0, 1, 2, 3, 4, 5, 255, 256, 1023 };

    [Theory]
    [MemberData(nameof(EdgeLengths))]
    public async Task PropsByteArray_EdgeLengths_RoundTrip(int length)
    {
        // Base64 emission has three padding classes (len % 3) and an empty-input case where a
        // wrong spelling yields NULL instead of "" and poisons the whole JSON string. Every
        // provider walks its own emitter here, so the boundary belongs in the shared suite.
        await Redb.SyncSchemeAsync<BytesProbeProps>();
        var payload = new byte[length];
        for (var i = 0; i < length; i++) payload[i] = (byte)(i % 256);

        var id = await Redb.SaveAsync(new RedbObject<BytesProbeProps>
        {
            name = $"bytes-len-{length}",
            Props = new BytesProbeProps { Label = "e", Blob = payload },
        });

        var loaded = await Redb.LoadAsync<BytesProbeProps>(id, depth: 1);
        loaded!.Props.Blob.Should().NotBeNull($"a {length}-byte array must survive the round trip");
        loaded.Props.Blob!.SequenceEqual(payload).Should().BeTrue("bytes must come back identical");
    }

    [Fact]
    public async Task PropsByteArray_AllByteValues_RoundTrip()
    {
        // All 256 values in order: catches any encoder that mangles high bytes or treats the
        // payload as text.
        await Redb.SyncSchemeAsync<BytesProbeProps>();
        var payload = new byte[256];
        for (var i = 0; i < 256; i++) payload[i] = (byte)i;

        var id = await Redb.SaveAsync(new RedbObject<BytesProbeProps>
        {
            name = "bytes-all-values",
            Props = new BytesProbeProps { Label = "a", Blob = payload },
        });

        var loaded = await Redb.LoadAsync<BytesProbeProps>(id, depth: 1);
        loaded!.Props.Blob!.SequenceEqual(payload).Should().BeTrue("every byte value must round-trip");
    }

    [Fact]
    public async Task RootValueBytes_EmptyArray_RoundTrips()
    {
        // The empty root blob: distinct from NULL, and the spelling that returns NULL for it
        // would break the base-field JSON rather than the properties object.
        await Redb.SyncSchemeAsync<BytesProbeProps>();
        var id = await Redb.SaveAsync(new RedbObject<BytesProbeProps>
        {
            name = "bytes-root-empty",
            value_bytes = [],
            Props = new BytesProbeProps { Label = "re" },
        });

        var loaded = await Redb.LoadAsync<BytesProbeProps>(id, depth: 1);
        loaded!.value_bytes.Should().NotBeNull("an empty blob is not a missing blob");
        loaded.value_bytes!.Length.Should().Be(0);
    }

    [Fact]
    public async Task RootValueBytes_PersistsToDbAndRoundTrips()
    {
        // §3.2/§3.3 of the report: PG emitted bytea hex instead of base64 on load; MSSQL
        // dropped value_bytes on write with no exception. The DB-side NULL check is the pin
        // against the silent-loss variant - a cache could mask it on reload.
        await Redb.SyncSchemeAsync<BytesProbeProps>();
        var payload = new byte[] { 1, 2, 3, 4, 5 };

        var id = await Redb.SaveAsync(new RedbObject<BytesProbeProps>
        {
            name = "bytes-root",
            value_bytes = payload,
            Props = new BytesProbeProps { Label = "r" },
        });

        var storedCount = await Redb.Context.ExecuteScalarAsync<long>(
            $"SELECT COUNT(*) FROM _objects WHERE _id = {id} AND _value_bytes IS NOT NULL");
        storedCount.Should().Be(1, "value_bytes must reach the _objects column - silent NULL was the §3.3 defect");

        var loaded = await Redb.LoadAsync<BytesProbeProps>(id, depth: 1);
        loaded!.value_bytes.Should().NotBeNull();
        loaded.value_bytes!.SequenceEqual(payload).Should().BeTrue("root value_bytes must round-trip byte-for-byte");
    }
}

[RedbScheme(Name = "BytesProbe")]
public class BytesProbeProps
{
    public string? Label { get; set; }
    public byte[]? Blob { get; set; }
}
