using redb.Core;
using redb.Core.Exceptions;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// The object key contract (V4, UNIQUE stage 1): <c>_objects._value_unique</c> is an ordinary
/// readable base field the application fills itself — like <c>_value_string</c>, plus the partial
/// unique index over <c>(_id_scheme, _value_unique)</c>. Full radius by decision: the column rides
/// JSON projections, LINQ over base fields, sorting/prefix search — not just a dedicated lookup.
///
/// <para>
/// Comparison semantics are each database's own (closed question of 2026-08-26): no trimming, no
/// case folding by redb. The 440-character limit is enforced in C#, because SQLite checks no
/// VARCHAR lengths and the same key must be accepted or rejected identically everywhere.
/// </para>
/// </summary>
public abstract class ValueUniqueTestsBase : IAsyncLifetime
{
    protected readonly IRedbService Redb;

    protected ValueUniqueTestsBase(IRedbService redb) => Redb = redb;

    private static readonly string[] Schemes = ["UniqueOrder", "UniqueInvoice"];

    public async Task InitializeAsync()
    {
        foreach (var scheme in Schemes)
            await ResetSchemeObjectsAsync(scheme);
    }

    public async Task DisposeAsync()
    {
        foreach (var scheme in Schemes)
            await ResetSchemeObjectsAsync(scheme);
    }

    /// <summary>Objects only: the schemes themselves are shared with the stage-2 suite.</summary>
    private async Task ResetSchemeObjectsAsync(string schemeName)
    {
        await Redb.Context.ExecuteAsync(
            $"DELETE FROM _objects WHERE _id_scheme IN (SELECT _id FROM _schemes WHERE _name = '{schemeName}')");
    }

    private async Task SyncAsync()
    {
        await Redb.SyncSchemeAsync<UniqueOrderProps>();
        await Redb.SyncSchemeAsync<UniqueInvoiceProps>();
    }

    private static RedbObject<UniqueOrderProps> Order(string? key, string note = "n")
        => new() { name = "vu-order", ValueUnique = key, Props = new UniqueOrderProps { Note = note } };

    /// <summary>SQLite's driver names only columns in a unique violation - no key tuple, so the
    /// violated structure/property is not resolved there (the Kind still classifies).</summary>
    protected virtual bool DriverReportsViolatedStructure => true;

    [Fact]
    public async Task UniqueViolation_OnTheObjectKey_IsClassifiedAsObjectKey()
    {
        // BR-8 (Tsak report, 2026-09-02): callers used to discriminate indexes by matching
        // provider-specific constraint-name strings. Kind is the first-class answer.
        await SyncAsync();
        var key = $"VU-KIND-{Guid.NewGuid():N}";
        await Redb.SaveAsync(Order(key));

        var act = async () => await Redb.SaveAsync(Order(key, "loser"));
        (await act.Should().ThrowAsync<RedbUniqueViolationException>())
            .Which.Kind.Should().Be(RedbUniqueViolationKind.ObjectKey);
    }

    [Fact]
    public async Task UniqueViolation_OnARedbUniqueProperty_IsClassifiedAsProperty_AndNeverRetriedByTheUpsert()
    {
        // The other half of BR-8: a [RedbUnique] FIELD collision inside SaveByUniqueAsync used to
        // ride the object-key catch (re-resolve by ValueUnique, rethrow) - Tsak's api-key rotation
        // fell exactly there. Property violations surface directly, classified.
        await SyncAsync();
        var code = $"VU-CODE-{Guid.NewGuid():N}";
        await Redb.SaveAsync(new RedbObject<UniqueOrderProps>
            { name = "vu-a", ValueUnique = $"VU-A-{Guid.NewGuid():N}", Props = new UniqueOrderProps { Code = code } });

        var dup = new RedbObject<UniqueOrderProps>
            { name = "vu-b", ValueUnique = $"VU-B-{Guid.NewGuid():N}", Props = new UniqueOrderProps { Code = code } };
        var act = async () => await Redb.SaveByUniqueAsync(dup);
        var uve = (await act.Should().ThrowAsync<RedbUniqueViolationException>(
                "a property collision is not the object key's business and must surface")).Which;

        uve.Kind.Should().Be(RedbUniqueViolationKind.Property);
        if (DriverReportsViolatedStructure)
        {
            uve.PropertyName.Should().Be("Code");
            uve.SchemeName.Should().NotBeNullOrEmpty();
        }
    }

    // ============================================================
    // === the field behaves like _value_string ===
    // ============================================================

    [Fact]
    public async Task Key_RoundTrips_ThroughSaveAndLoad()
    {
        await SyncAsync();
        var obj = Order("ORD-2026-0001");
        var id = await Redb.SaveAsync(obj);

        var loaded = await Redb.LoadAsync<UniqueOrderProps>(id, depth: 1);
        loaded!.ValueUnique.Should().Be("ORD-2026-0001",
            "the key is a readable base field and rides the JSON projection on every provider");

        loaded.ValueUnique = "ORD-2026-0002";
        await Redb.SaveAsync(loaded);
        (await Redb.LoadAsync<UniqueOrderProps>(id, depth: 1))!.ValueUnique.Should().Be("ORD-2026-0002");
    }

    [Fact]
    public async Task DuplicateKey_InOneScheme_IsRejected_Typed()
    {
        await SyncAsync();
        await Redb.SaveAsync(Order("DUP-1"));

        var act = async () => await Redb.SaveAsync(Order("DUP-1", "second"));
        var ex = (await act.Should().ThrowAsync<RedbUniqueViolationException>()).Which;
        ex.Cause.Should().NotBeNull();
    }

    [Fact]
    public async Task SameKey_InDifferentSchemes_IsAllowed()
    {
        await SyncAsync();
        await Redb.SaveAsync(Order("SHARED-KEY"));
        var invoice = new RedbObject<UniqueInvoiceProps>
        {
            name = "vu-invoice",
            ValueUnique = "SHARED-KEY",
            Props = new UniqueInvoiceProps { Note = "inv" }
        };
        (await Redb.SaveAsync(invoice)).Should().BeGreaterThan(0,
            "_id_scheme is part of the index key: independent domains may share business identifiers");
    }

    [Fact]
    public async Task MultipleNullKeys_AreAllowed()
    {
        await SyncAsync();
        for (var i = 0; i < 3; i++)
            await Redb.SaveAsync(Order(null, $"free-{i}"));

        (await Redb.Context.ExecuteScalarAsync<long?>(
            "SELECT COUNT(*) FROM _objects WHERE _id_scheme = (SELECT _id FROM _schemes WHERE _name = 'UniqueOrder')"))
            .Should().Be(3);
    }

    [Fact]
    public async Task KeyLongerThan440_IsRejectedInCSharp_OnEveryProvider()
    {
        await SyncAsync();
        var tooLong = new string('K', 441);

        var act = async () => await Redb.SaveAsync(Order(tooLong));
        await act.Should().ThrowAsync<RedbUniqueKeyValueException>(
            "SQLite does not check VARCHAR lengths, so without the C# guard the same key would pass " +
            "there and fail on PostgreSQL and MSSQL");

        (await Redb.SaveAsync(Order(new string('K', 440)))).Should().BeGreaterThan(0, "440 exactly fits");
    }
    [Fact]
    public async Task DuplicateInsideCallerTransaction_LeavesTheTransactionAlive()
    {
        // Bug report п.2 (2026-09-02): on PostgreSQL a caught unique violation used to leave the
        // CALLER's transaction aborted (25P02) - every follow-up statement of a catch-and-recover
        // pattern died. The batch now runs under a savepoint inside a foreign transaction.
        await SyncAsync();
        await Redb.SaveAsync(Order("TX-DUP"));

        long recoveredId = 0;
        await Redb.Context.ExecuteAtomicAsync(async () =>
        {
            var act = async () => await Redb.SaveAsync(Order("TX-DUP", "loser"));
            await act.Should().ThrowAsync<RedbUniqueViolationException>();

            // The whole point: the transaction is still usable after the typed error.
            (await Redb.Context.ExecuteScalarAsync<long?>("SELECT 1")).Should().Be(1,
                "the caller's transaction must survive a caught unique violation");
            recoveredId = await Redb.SaveAsync(Order("TX-RECOVERED", "winner"));
            recoveredId.Should().BeGreaterThan(0);
        });

        (await Redb.LoadAsync<UniqueOrderProps>(recoveredId, depth: 1))!.ValueUnique.Should().Be("TX-RECOVERED",
            "work done after the recovery commits with the transaction");
    }
    [Fact]
    public async Task ValueStringLongerThan450_IsRejectedInCSharp_OnEveryProvider()
    {
        // Owner decision 2026-09-02: _value_string is an identifier column - 450 is the MSSQL
        // index-key width, enforced in C# so the text-typed PostgreSQL and SQLite columns hold
        // the same contract. Long text belongs in _note or in a Props field.
        await SyncAsync();
        var act = async () => await Redb.SaveAsync(new RedbObject<UniqueOrderProps>
        {
            name = "vs-long", ValueString = new string('S', 451), Props = new UniqueOrderProps()
        });
        await act.Should().ThrowAsync<RedbValueStringTooLongException>();

        (await Redb.SaveAsync(new RedbObject<UniqueOrderProps>
        {
            name = "vs-fit", ValueString = new string('S', 450), Props = new UniqueOrderProps()
        })).Should().BeGreaterThan(0, "450 exactly fits");
    }

    [Fact]
    public async Task ChangingTheKey_MovesTheObjectHash()
    {
        // Reversal of P5 (full-object-hash plan, 2026-09-11): _hash covers the header, the key
        // included - a key change on another node must invalidate the cached copy.
        await SyncAsync();
        var obj = Order("H-1");
        var id = await Redb.SaveAsync(obj);
        var hashBefore = (await Redb.LoadAsync<UniqueOrderProps>(id, depth: 1))!.hash;

        var loaded = await Redb.LoadAsync<UniqueOrderProps>(id, depth: 1);
        loaded!.ValueUnique = "H-2";
        await Redb.SaveAsync(loaded);

        (await Redb.LoadAsync<UniqueOrderProps>(id, depth: 1))!.hash.Should().NotBe(hashBefore!.Value,
            "the object key is header content: _hash must move with it, or the props cache serves the old key");
    }

    // ============================================================
    // === query surface ===
    // ============================================================

    [Fact]
    public async Task WhereRedb_ByKey_Works_EqualityAndPrefix()
    {
        await SyncAsync();
        var a = await Redb.SaveAsync(Order("ORD-2026-0001"));
        await Redb.SaveAsync(Order("ORD-2026-0002"));
        await Redb.SaveAsync(Order("INV-2026-0001"));

        var exact = await Redb.Query<UniqueOrderProps>()
            .WhereRedb(o => o.ValueUnique == "ORD-2026-0001")
            .ToListAsync();
        exact.Should().ContainSingle().Which.id.Should().Be(a);

        var prefix = await Redb.Query<UniqueOrderProps>()
            .WhereRedb(o => o.ValueUnique!.StartsWith("ORD-2026"))
            .ToListAsync();
        prefix.Should().HaveCount(2, "a readable string key keeps prefix search - the reason it is a string");
    }

    // ============================================================
    // === soft delete releases the key (decision 9) ===
    // ============================================================

    [Fact]
    public async Task SoftDelete_ReleasesObjectKey_RepeatedAndCross()
    {
        await SyncAsync();
        var first = Order("DEL-K");
        await Redb.SaveAsync(first);
        await Redb.SoftDeleteAsync(new[] { first.id });
        (await Redb.Context.ExecuteScalarAsync<string?>($"SELECT _value_unique FROM _objects WHERE _id = {first.id}"))
            .Should().BeNull("decision 9: the trash holds no keys - mark_for_deletion nulls the key as it moves the row");
        (await Redb.Context.ExecuteScalarAsync<long?>($"SELECT _id_scheme FROM _objects WHERE _id = {first.id}"))
            .Should().Be(-10);

        var second = Order("DEL-K");
        await Redb.SaveAsync(second);
        await Redb.SoftDeleteAsync(new[] { second.id }); // the trash is one more point of the same index; without the release the second delete dies

        var order = Order("CROSS-K");
        await Redb.SaveAsync(order);
        var invoice = new RedbObject<UniqueInvoiceProps>
        {
            name = "vu-invoice", ValueUnique = "CROSS-K", Props = new UniqueInvoiceProps()
        };
        await Redb.SaveAsync(invoice);
        await Redb.SoftDeleteAsync(new[] { order.id });
        await Redb.SoftDeleteAsync(new[] { invoice.id });

        (await Redb.SaveAsync(Order("DEL-K"))).Should().BeGreaterThan(0, "a released key is immediately reusable");
    }

    // ============================================================
    // === in-batch key exchange (P7) ===
    // ============================================================

    [Fact]
    public async Task KeySwap_InOneBatchSave_Passes()
    {
        await SyncAsync();
        var a = Order("SW-A");
        var b = Order("SW-B");
        await Redb.SaveAsync(new[] { a, b }.Cast<Core.Models.Contracts.IRedbObject>().ToList());

        a.ValueUnique = "SW-B";
        b.ValueUnique = "SW-A";
        await Redb.SaveAsync(new[] { a, b }.Cast<Core.Models.Contracts.IRedbObject>().ToList());

        (await Redb.LoadAsync<UniqueOrderProps>(a.id, depth: 1))!.ValueUnique.Should().Be("SW-B",
            "P7: the batch releases every key it is about to retake before the row updates run");
        (await Redb.LoadAsync<UniqueOrderProps>(b.id, depth: 1))!.ValueUnique.Should().Be("SW-A");
    }
    [Fact]
    public async Task KeyHandover_InOneBatch_GiverGoesNull_TakerGetsIt()
    {
        await SyncAsync();
        var a = Order("HO-J");
        var b = Order("HO-K");
        await Redb.SaveAsync(new[] { a, b }.Cast<Core.Models.Contracts.IRedbObject>().ToList());

        // A takes the key B gives up, A first in the list: the taker is written before the giver.
        a.ValueUnique = "HO-K";
        b.ValueUnique = null;
        await Redb.SaveAsync(new[] { a, b }.Cast<Core.Models.Contracts.IRedbObject>().ToList());

        (await Redb.LoadAsync<UniqueOrderProps>(a.id, depth: 1))!.ValueUnique.Should().Be("HO-K",
            "P7 releases the keys of every existing object in the batch, not only of those taking one");
        (await Redb.LoadAsync<UniqueOrderProps>(b.id, depth: 1))!.ValueUnique.Should().BeNull();
    }

    // ============================================================
    // === upsert by key (P1) ===
    // ============================================================

    [Fact]
    public async Task SaveByUnique_InsertsThenUpdatesTheSameRow()
    {
        await SyncAsync();

        var first = Order("UP-1", "v1");
        var id1 = await Redb.SaveByUniqueAsync(first);
        id1.Should().BeGreaterThan(0);

        var second = Order("UP-1", "v2");
        var id2 = await Redb.SaveByUniqueAsync(second);
        id2.Should().Be(id1, "the key resolves to the same row; the accept is idempotent");

        var loaded = await Redb.LoadAsync<UniqueOrderProps>(id1, depth: 1);
        loaded!.Props.Note.Should().Be("v2", "last writer's content");

        (await Redb.Context.ExecuteScalarAsync<long?>(
            "SELECT COUNT(*) FROM _objects WHERE _value_unique = 'UP-1' AND _id_scheme = " +
            "(SELECT _id FROM _schemes WHERE _name = 'UniqueOrder')"))
            .Should().Be(1);
    }

    [Fact]
    public async Task SaveByUnique_WithoutKey_IsRefused()
    {
        await SyncAsync();
        var act = async () => await Redb.SaveByUniqueAsync(Order(null));
        await act.Should().ThrowAsync<RedbUniqueKeyDefinitionException>();
    }

    [Fact]
    public async Task ConcurrentSaveByUnique_ConvergesOnOneRow()
    {
        await SyncAsync();

        // One service instance is single-writer (the captive-singleton guard), so a truly parallel
        // pair may serialize or one may be refused before the database sees it - either way the
        // contract is: afterwards exactly one row holds the key.
        var results = await Task.WhenAll(Try("c1"), Try("c2"));
        results.Count(r => r.Id.HasValue).Should().BeGreaterThanOrEqualTo(1);

        (await Redb.Context.ExecuteScalarAsync<long?>(
            "SELECT COUNT(*) FROM _objects WHERE _value_unique = 'RACE-UP' AND _id_scheme = " +
            "(SELECT _id FROM _schemes WHERE _name = 'UniqueOrder')"))
            .Should().Be(1, "however the race interleaves, one key - one object");

        async Task<(long? Id, Exception? Error)> Try(string note)
        {
            try { return (await Redb.SaveByUniqueAsync(Order("RACE-UP", note)), null); }
            catch (Exception ex) { return (null, ex); }
        }
    }
}
