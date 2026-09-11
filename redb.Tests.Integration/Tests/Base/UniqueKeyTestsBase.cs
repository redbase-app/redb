using System.Text;
using redb.Core.Utils;
using redb.Core;
using redb.Core.Exceptions;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// The <c>[RedbUnique]</c> contract (V4, UNIQUE stage 2): a key field's value is unique within its
/// scheme, enforced by the database over the hash of the canonical form
/// (<c>UniqueKeyEncoder</c> → <c>_values._unique</c> → <c>UIX__values__structure_unique</c>), and a
/// violation surfaces as <see cref="RedbUniqueViolationException"/> on every provider.
///
/// <para>
/// The suite owns three schemes (<c>UniqueOrder</c>, <c>UniqueInvoice</c>, <c>UniqueFree</c>) and
/// resets them before and after every test: half of the scenarios deliberately leave duplicates or
/// raw-SQL damage behind, and the shared database accumulates state across runs.
/// </para>
/// </summary>
public abstract class UniqueKeyTestsBase : IAsyncLifetime
{
    protected readonly IRedbService Redb;

    protected UniqueKeyTestsBase(IRedbService redb) => Redb = redb;

    /// <summary>SQL literal for boolean TRUE in raw statements (MSSQL BIT wants 1).</summary>
    protected virtual string BoolTrue => "TRUE";

    /// <summary>
    /// Whether the driver's violation message carries the key tuple, so the exception can name the
    /// property. PostgreSQL and MSSQL do; SQLite names only the columns.
    /// </summary>
    protected virtual bool ReportsKeyTuple => true;

    /// <summary>
    /// Whether two objects may swap their keys inside one batch save. True under DeleteInsert
    /// (all deletes before all inserts); under ChangeTracking the in-place update path may hit the
    /// index mid-way — the documented boundary of §4.6, surfacing as the typed exception.
    /// </summary>
    protected virtual bool KeySwapMustPass => true;

    private static readonly string[] Schemes = ["UniqueOrder", "UniqueInvoice", "UniqueFree", "ByteHolder", "UniqueNested",
        "UniqueDeep", "UniqueSubtree", "UniqueElems",
        "redb.Tests.Integration.Models.UniqueInElementProps",
        "redb.Tests.Integration.Models.ScopedScalarProps"];

    public async Task InitializeAsync()
    {
        foreach (var scheme in Schemes)
            await ResetSchemeAsync(scheme);
    }

    public async Task DisposeAsync()
    {
        foreach (var scheme in Schemes)
            await ResetSchemeAsync(scheme);
    }

    private async Task ResetSchemeAsync(string schemeName)
    {
        await Redb.Context.ExecuteAsync(
            $"DELETE FROM _objects WHERE _id_scheme IN (SELECT _id FROM _schemes WHERE _name = '{schemeName}')");
        await Redb.Context.ExecuteAsync(
            $"DELETE FROM _structures WHERE _id_scheme IN (SELECT _id FROM _schemes WHERE _name = '{schemeName}')");
        await Redb.Context.ExecuteAsync($"DELETE FROM _schemes WHERE _name = '{schemeName}'");
    }

    private async Task SyncAllAsync()
    {
        await Redb.SyncSchemeAsync<UniqueOrderProps>();
        await Redb.SyncSchemeAsync<UniqueInvoiceProps>();
        await Redb.SyncSchemeAsync<UniqueFreeProps>();
    }

    private static RedbObject<UniqueOrderProps> Order(Action<UniqueOrderProps> set)
    {
        var props = new UniqueOrderProps();
        set(props);
        return new RedbObject<UniqueOrderProps> { name = "order", Props = props };
    }

    private async Task<long> StructureIdAsync(string schemeName, string propertyName)
        => (await Redb.Context.ExecuteScalarAsync<long?>(
               $"SELECT _id FROM _structures WHERE _name = '{propertyName}' AND _id_scheme = " +
               $"(SELECT _id FROM _schemes WHERE _name = '{schemeName}')"))!.Value;

    // ============================================================
    // === enforcement ===
    // ============================================================

    [Fact]
    public async Task DuplicateKey_IsRejected_WithTypedException()
    {
        await SyncAllAsync();
        await Redb.SaveAsync(Order(p => { p.Code = "ORD-1"; p.Note = "first"; }));

        var act = async () => await Redb.SaveAsync(Order(p => { p.Code = "ORD-1"; p.Note = "second"; }));

        var ex = (await act.Should().ThrowAsync<RedbUniqueViolationException>(
            "one typed exception on every provider, not three driver errors")).Which;
        ex.Cause.Should().NotBeNull("the driver exception is always attached");
        if (ReportsKeyTuple)
        {
            ex.PropertyName.Should().Be("Code");
            ex.SchemeName.Should().Be("UniqueOrder");
        }
    }

    [Fact]
    public async Task SameKey_InDifferentSchemes_IsAllowed()
    {
        await SyncAllAsync();
        await Redb.SaveAsync(Order(p => p.Code = "SHARED"));
        var invoice = new RedbObject<UniqueInvoiceProps>
        {
            name = "invoice",
            Props = new UniqueInvoiceProps { Code = "SHARED" }
        };
        var id = await Redb.SaveAsync(invoice);
        id.Should().BeGreaterThan(0, "uniqueness is per scheme: _id_structure is part of the index key");
    }

    [Fact]
    public async Task SameValue_InDifferentFieldsOfOneScheme_IsAllowed()
    {
        await SyncAllAsync();
        await Redb.SaveAsync(Order(p => p.Number = 5));
        var id = await Redb.SaveAsync(Order(p => p.Amount = 5m));
        id.Should().BeGreaterThan(0, "each field is its own structure, its own uniqueness namespace");
    }

    [Fact]
    public async Task MultipleNullKeys_AreAllowed()
    {
        await SyncAllAsync();
        for (var i = 0; i < 3; i++)
            await Redb.SaveAsync(Order(p => p.Note = $"empty-{i}"));

        (await Redb.Context.ExecuteScalarAsync<long?>(
            "SELECT COUNT(*) FROM _objects WHERE _id_scheme = (SELECT _id FROM _schemes WHERE _name = 'UniqueOrder')"))
            .Should().Be(3, "NULL never takes part in uniqueness");
    }

    [Fact]
    public async Task RaceOnOneKey_ExactlyOneWins()
    {
        await SyncAllAsync();

        // One service instance is deliberately single-writer (the 3.3.0 captive-singleton guard):
        // a truly concurrent second save may be refused by the SERVICE with
        // InvalidOperationException before the database ever sees it. Both outcomes are legal for
        // the loser - but after the dust settles, exactly one object holds the key, and a retry of
        // the refused save must get the typed violation from the INDEX.
        var results = await Task.WhenAll(TrySave(), TrySave());

        results.Count(r => r is null).Should().BeGreaterThanOrEqualTo(1, "at least one save takes the key");
        results.Where(r => r != null).Should().OnlyContain(
            r => r is RedbUniqueViolationException || r is InvalidOperationException);

        // Whatever the interleaving, the key has exactly one holder now, and the next claim gets
        // the typed rejection.
        var retry = async () => await Redb.SaveAsync(Order(p => p.Code = "RACE"));
        await retry.Should().ThrowAsync<RedbUniqueViolationException>();

        (await Redb.Context.ExecuteScalarAsync<long?>(
            "SELECT COUNT(*) FROM _values WHERE _unique IS NOT NULL AND _id_structure = " +
            "(SELECT _id FROM _structures WHERE _name = 'Code' AND _id_scheme = " +
            "(SELECT _id FROM _schemes WHERE _name = 'UniqueOrder'))"))
            .Should().Be(1, "exactly one row holds the key");

        async Task<Exception?> TrySave()
        {
            try { await Redb.SaveAsync(Order(p => p.Code = "RACE")); return null; }
            catch (Exception ex) { return ex; }
        }
    }

    [Fact]
    public async Task UpdatingObject_KeepsItsOwnKey()
    {
        await SyncAllAsync();
        var obj = Order(p => { p.Code = "SELF"; p.Note = "v1"; });
        var id = await Redb.SaveAsync(obj);

        obj.Props.Note = "v2";
        await Redb.SaveAsync(obj);

        var loaded = await Redb.LoadAsync<UniqueOrderProps>(id, depth: 1);
        loaded!.Props.Note.Should().Be("v2");
        loaded.Props.Code.Should().Be("SELF", "an object never conflicts with itself");
    }

    [Fact]
    public async Task KeyChange_OnUpdate_ReleasesOldValue_AndGuardsNew()
    {
        await SyncAllAsync();
        var obj = Order(p => { p.Code = "K-OLD"; p.Note = "v1"; });
        await Redb.SaveAsync(obj);

        obj.Props.Code = "K-NEW";
        await Redb.SaveAsync(obj);

        // The stored key is the hash of the CURRENT value: the lookup recomputes the hash from
        // the value it is given, so a hit proves the stored key and the encoder agree.
        (await Redb.GetByUniqueAsync<UniqueOrderProps>(p => p.Code, "K-NEW"))!.id.Should().Be(obj.id);
        (await Redb.GetByUniqueAsync<UniqueOrderProps>(p => p.Code, "K-OLD")).Should().BeNull(
            "the old value's key is gone with the value");

        var takeNew = async () => await Redb.SaveAsync(Order(p => p.Code = "K-NEW"));
        await takeNew.Should().ThrowAsync<RedbUniqueViolationException>("the new value is guarded");
        (await Redb.SaveAsync(Order(p => p.Code = "K-OLD"))).Should().BeGreaterThan(0,
            "the old value is free for the next claimant");
    }

    [Fact]
    public async Task NullToValue_OnUpdate_KeysFollowTheValues()
    {
        // The scenario of the 2026-09-10 MSSQL report: objects created with a NULL key get their
        // values later, one update each. Every update must store the hash OF ITS VALUE - if the
        // written key is not a function of the value, the second update collides with the first
        // as a false duplicate.
        await SyncAllAsync();
        var a = Order(p => p.Note = "late-a");
        var b = Order(p => p.Note = "late-b");
        await Redb.SaveAsync(a);
        await Redb.SaveAsync(b);

        a.Props.Code = "LATE-A";
        await Redb.SaveAsync(a);
        b.Props.Code = "LATE-B";
        var actB = async () => await Redb.SaveAsync(b);
        await actB.Should().NotThrowAsync("different values are different keys, never a duplicate");

        (await Redb.GetByUniqueAsync<UniqueOrderProps>(p => p.Code, "LATE-A"))!.id.Should().Be(a.id);
        (await Redb.GetByUniqueAsync<UniqueOrderProps>(p => p.Code, "LATE-B"))!.id.Should().Be(b.id);

        var dup = async () => await Redb.SaveAsync(Order(p => p.Code = "LATE-A"));
        await dup.Should().ThrowAsync<RedbUniqueViolationException>("the late-written keys are live");
    }

    [Fact]
    public async Task NullToValue_OnQueryLoadedObjects_KeysFollowTheValues()
    {
        // The exact shape of a mirror backfill (Identity V4, MSSQL report 2026-09-10): the value
        // lives in _objects._value_string, the [RedbUnique] prop is NULL, the object comes back
        // through the QUERY pipeline (with its change-tracking snapshot), the key is copied in and
        // the object re-saved one at a time. Every save must store the hash of its own value.
        await SyncAllAsync();
        var a = Order(p => p.Note = "mirror-a");
        a.value_string = "MIR-A";
        var b = Order(p => p.Note = "mirror-b");
        b.value_string = "MIR-B";
        await Redb.SaveAsync(a);
        await Redb.SaveAsync(b);

        var candidates = await Redb.Query<UniqueOrderProps>()
            .WhereRedb(o => o.ValueString != null)
            .ToListAsync();
        candidates.Should().HaveCount(2);

        foreach (var obj in candidates.OrderBy(o => o.Id))
        {
            obj.Props.Code = obj.value_string;
            var act = async () => await Redb.SaveAsync(obj);
            await act.Should().NotThrowAsync(
                $"'{obj.value_string}' is this object's own value, not a duplicate of a neighbour");
        }

        (await Redb.GetByUniqueAsync<UniqueOrderProps>(p => p.Code, "MIR-A"))!.id.Should().Be(a.id);
        (await Redb.GetByUniqueAsync<UniqueOrderProps>(p => p.Code, "MIR-B"))!.id.Should().Be(b.id);
    }

    [Fact]
    public async Task NullToValue_OnHashIdenticalObjects_LoadedByOneQuery_KeysFollowTheValues()
    {
        // The 2026-09-10 MSSQL Pro report, second round - the discriminating shape: the objects
        // are BYTE-IDENTICAL in Props (all empty, one content hash for all of them, exactly what
        // legacy backfill candidates look like), they come back through ONE query, and the
        // [RedbUnique] prop is set and saved object by object in one scope. The observed defect:
        // from the second object on, the written key is the FIRST object's hash - a false
        // duplicate - while hash-distinct objects (the other NullToValue pins) pass.
        await SyncAllAsync();
        var mirrors = new[] { "HID-A", "HID-B", "HID-C" };
        foreach (var mirror in mirrors)
        {
            // The SAME Note everywhere: identical Props content, identical content hash.
            var obj = Order(p => p.Note = "hash-identical");
            obj.value_string = mirror;
            await Redb.SaveAsync(obj);
        }

        var candidates = (await Redb.Query<UniqueOrderProps>()
            .WhereRedb(o => o.ValueString != null)
            .ToListAsync())
            .Where(o => o.value_string!.StartsWith("HID-"))
            .OrderBy(o => o.Id)
            .ToList();
        candidates.Should().HaveCount(3);

        foreach (var obj in candidates)
        {
            obj.Props.Code = obj.value_string;
            var act = async () => await Redb.SaveAsync(obj);
            await act.Should().NotThrowAsync(
                $"'{obj.value_string}' is this object's own value, not a duplicate of a hash-identical neighbour");
        }

        foreach (var (mirror, i) in mirrors.Select((m, i) => (m, i)))
            (await Redb.GetByUniqueAsync<UniqueOrderProps>(p => p.Code, mirror))!.id
                .Should().Be(candidates[i].id, "each stored key is the hash of its own object's value");
    }

    [Fact]
    public async Task AfterALegitimateDuplicateFailure_TheNextSaveIsClean()
    {
        // The 2026-09-10 MSSQL report, round 3 (the concurrency one) - distilled to its
        // deterministic core, no concurrency needed: a save that fails on a LEGITIMATE
        // duplicate must not poison the saves that follow in the same scope. The failed
        // save's insert rows (carrying ITS unique hash) must die with the failure - a later
        // save of an unrelated object that gets rejected with the FAILED value's hash while
        // its own Props are intact is exactly the "foreign hash" from the report.
        await SyncAllAsync();

        await Redb.SaveAsync(Order(p => { p.Code = "LEAK-DUP"; p.Note = "holder"; }));

        // An UPDATE transition into the taken value: the insert-change of the diff carries
        // hash(LEAK-DUP) and fails on the index - legitimately.
        var loser = Order(p => p.Note = "loser");
        await Redb.SaveAsync(loser);
        loser.Props.Code = "LEAK-DUP";
        var legit = async () => await Redb.SaveAsync(loser);
        await legit.Should().ThrowAsync<RedbUniqueViolationException>("the value is genuinely taken");

        // An unrelated object with its OWN value, same scope: must save cleanly.
        var clean = Order(p => p.Note = "clean");
        await Redb.SaveAsync(clean);
        clean.Props.Code = "LEAK-CLEAN";
        var act = async () => await Redb.SaveAsync(clean);
        await act.Should().NotThrowAsync(
            "a failed save's pending rows must not leak into the next save of the scope");

        (await Redb.GetByUniqueAsync<UniqueOrderProps>(p => p.Code, "LEAK-CLEAN"))!.id
            .Should().Be(clean.id, "the clean object holds the key of its own value");
    }

    [Fact]
    public async Task NullToValue_OnBatchUpdate_EachKeyFollowsItsValue()
    {
        // Same transition as NullToValue_OnUpdate_KeysFollowTheValues, but the updates ride ONE
        // batch save - the shape of a backfill job. Each row must get the hash of ITS OWN value;
        // a key computed from a neighbour's value surfaces as a false duplicate inside the batch.
        await SyncAllAsync();
        var objects = Enumerable.Range(1, 5)
            .Select(i => Order(p => p.Note = $"late-{i}"))
            .ToList();
        await Redb.SaveAsync(objects.Cast<Core.Models.Contracts.IRedbObject>().ToList());

        for (var i = 0; i < objects.Count; i++)
            objects[i].Props.Code = $"BATCH-{i + 1}";
        var act = async () => await Redb.SaveAsync(objects.Cast<Core.Models.Contracts.IRedbObject>().ToList());
        await act.Should().NotThrowAsync("five different values are five different keys");

        foreach (var obj in objects)
            (await Redb.GetByUniqueAsync<UniqueOrderProps>(p => p.Code, obj.Props.Code))!.id.Should().Be(obj.id,
                "the stored key must be the hash of this object's value");
    }

    [Fact]
    public async Task KeySwap_InOneBatchSave()
    {
        await SyncAllAsync();
        var a = Order(p => p.Code = "SWAP-A");
        var b = Order(p => p.Code = "SWAP-B");
        await Redb.SaveAsync(new[] { a, b }.Cast<Core.Models.Contracts.IRedbObject>().ToList());

        a.Props.Code = "SWAP-B";
        b.Props.Code = "SWAP-A";
        var act = async () => await Redb.SaveAsync(new[] { a, b }.Cast<Core.Models.Contracts.IRedbObject>().ToList());

        if (KeySwapMustPass)
        {
            await act.Should().NotThrowAsync(
                "under DeleteInsert all deletes run before all inserts, so an in-batch swap is legal");
            (await Redb.LoadAsync<UniqueOrderProps>(a.id, depth: 1))!.Props.Code.Should().Be("SWAP-B");
        }
        else
        {
            // §4.6: under ChangeTracking the in-place update can hit the index mid-way. The boundary
            // is a typed error, never silent corruption.
            try
            {
                await act();
                (await Redb.LoadAsync<UniqueOrderProps>(a.id, depth: 1))!.Props.Code.Should().Be("SWAP-B");
            }
            catch (RedbUniqueViolationException)
            {
                // documented boundary - and the batch is one transaction: nothing half-written
                (await Redb.LoadAsync<UniqueOrderProps>(a.id, depth: 1))!.Props.Code.Should().Be("SWAP-A");
                (await Redb.LoadAsync<UniqueOrderProps>(b.id, depth: 1))!.Props.Code.Should().Be("SWAP-B");
            }
        }
    }

    // ============================================================
    // === canonical form, end to end ===
    // ============================================================

    [Fact]
    public async Task CanonicalForm_DecimalTrailingZeros_AreOneKey()
    {
        await SyncAllAsync();
        await Redb.SaveAsync(Order(p => p.Amount = 1.50m));
        var act = async () => await Redb.SaveAsync(Order(p => p.Amount = 1.5m));
        await act.Should().ThrowAsync<RedbUniqueViolationException>("1.50 and 1.5 are one number");
    }

    [Fact]
    public async Task CanonicalForm_SameInstantDifferentOffset_IsOneKey()
    {
        await SyncAllAsync();
        await Redb.SaveAsync(Order(p => p.IssuedAt = new DateTimeOffset(2026, 8, 31, 12, 0, 0, TimeSpan.FromHours(3))));
        var act = async () => await Redb.SaveAsync(Order(p => p.IssuedAt = new DateTimeOffset(2026, 8, 31, 9, 0, 0, TimeSpan.Zero)));
        await act.Should().ThrowAsync<RedbUniqueViolationException>("12:00+03:00 and 09:00Z are one moment");
    }

    [Fact]
    public async Task CanonicalForm_NfcNfd_AreOneKey_CaseIsNot()
    {
        await SyncAllAsync();
        const string nfc = "caf\u00e9";   // e-acute, one code point
        const string nfdCode = "cafe\u0301"; // e + combining acute, two code points
        nfdCode.Should().NotBe(nfc, "the staged literals must really be two spellings");
        await Redb.SaveAsync(Order(p => p.Code = nfc));
        var nfd = async () => await Redb.SaveAsync(Order(p => p.Code = nfdCode));
        await nfd.Should().ThrowAsync<RedbUniqueViolationException>("NFC and NFD spell one string");

        var otherCase = await Redb.SaveAsync(Order(p => p.Code = "CAFÉ"));
        otherCase.Should().BeGreaterThan(0, "no case folding: the application owns the notion of its key");
    }


    [Fact]
    public async Task ByteArrayKey_MegabytePayload_Works()
    {
        await SyncAllAsync();
        var payload = new byte[1 << 20];
        for (var i = 0; i < payload.Length; i++) payload[i] = (byte)(i % 251);

        await Redb.SaveAsync(Order(p => p.Fingerprint = payload));

        var same = (byte[])payload.Clone();
        var dup = async () => await Redb.SaveAsync(Order(p => p.Fingerprint = same));
        await dup.Should().ThrowAsync<RedbUniqueViolationException>(
            "dedup by content is the ordinary byte[] key scenario (plan decision 4)");

        var different = (byte[])payload.Clone();
        different[^1] ^= 0xFF;
        (await Redb.SaveAsync(Order(p => p.Fingerprint = different))).Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task ByteArrayProp_IsStoredAsSingleScalarRow()
    {
        await SyncAllAsync();
        var payload = new byte[] { 1, 2, 3, 250, 251, 252, 0, 255 };
        var id = await Redb.SaveAsync(Order(p => p.Fingerprint = payload));

        var loaded = await Redb.LoadAsync<UniqueOrderProps>(id, depth: 1);
        loaded!.Props.Fingerprint.Should().BeEquivalentTo(payload,
            "byte[] rides the JSON projection as base64 on every builder");

        var structureId = await StructureIdAsync("UniqueOrder", "Fingerprint");
        (await Redb.Context.ExecuteScalarAsync<long?>(
            $"SELECT COUNT(*) FROM _values WHERE _id_structure = {structureId} AND _id_object = {id}"))
            .Should().Be(1, "Б1: one scalar row, not a base row plus a row per byte");
        (await Redb.Context.ExecuteScalarAsync<long?>(
            $"SELECT COUNT(*) FROM _values WHERE _id_structure = {structureId} AND _id_object = {id} AND _ByteArray IS NOT NULL"))
            .Should().Be(1);
    }

    [Fact]
    public async Task LegacyByteArrayLayout_IsConvertedOnSync()
    {
        await SyncAllAsync();
        var payload = new byte[] { 10, 20, 30, 40, 200 };
        var id = await Redb.SaveAsync(Order(p => p.Fingerprint = payload));
        var structureId = await StructureIdAsync("UniqueOrder", "Fingerprint");

        // Stage the pre-V4 layout by hand: structure Array-of-Byte, base row with a hash in _Guid,
        // one element row per byte in _Long. Exactly what an old build left behind.
        var ctx = Redb.Context;
        await StageLegacyByteLayoutAsync(structureId, id, payload);

        // The next synchronisation must convert in place - self-healing, no manual step.
        await Redb.SyncSchemeAsync<UniqueOrderProps>();

        (await ctx.ExecuteScalarAsync<long?>(
            $"SELECT COUNT(*) FROM _values WHERE _id_structure = {structureId} AND _id_object = {id}"))
            .Should().Be(1, "element rows are gone, one scalar row remains");
        var loaded = await Redb.LoadAsync<UniqueOrderProps>(id, depth: 1);
        loaded!.Props.Fingerprint.Should().BeEquivalentTo(payload, "the bytes were reassembled in index order");
    }

    [Fact]
    public async Task ByteArrayNestedInClass_RoundTrips()
    {
        await Redb.SyncSchemeAsync<ByteHolderProps>();
        var data = new byte[] { 9, 8, 7, 6 };
        var id = await Redb.SaveAsync(new RedbObject<ByteHolderProps>
        {
            name = "holder",
            Props = new ByteHolderProps { Root = new byte[] { 1 }, File = new ByteAttachment { Name = "f", Data = data } }
        });

        var loaded = await Redb.LoadAsync<ByteHolderProps>(id, depth: 1);
        loaded!.Props.File.Should().NotBeNull();
        loaded.Props.File!.Name.Should().Be("f");
        loaded.Props.File.Data.Should().BeEquivalentTo(data, "Б1: a byte[] inside a nested class is the same scalar as at the root");
    }

    [Fact]
    public async Task LegacyByteArrayLayout_NestedInClass_IsConvertedWithoutLoss()
    {
        await Redb.SyncSchemeAsync<ByteHolderProps>();
        var root = new byte[] { 1, 2, 3 };
        var data = new byte[] { 9, 8, 7, 6 };
        var id = await Redb.SaveAsync(new RedbObject<ByteHolderProps>
        {
            name = "holder",
            Props = new ByteHolderProps { Root = root, File = new ByteAttachment { Name = "f", Data = data } }
        });
        var rootStructure = await StructureIdAsync("ByteHolder", "Root");
        var dataStructure = await StructureIdAsync("ByteHolder", "Data");

        // Both byte[] in the old shape. The nested one keeps what a class member always has: its
        // base row points at the class row (_array_parent_id) and carries a pseudo index.
        await StageLegacyByteLayoutAsync(rootStructure, id, root);
        await StageLegacyByteLayoutAsync(dataStructure, id, data);

        await Redb.SyncSchemeAsync<ByteHolderProps>();

        var ctx = Redb.Context;
        (await ctx.ExecuteScalarAsync<long?>(
            $"SELECT COUNT(*) FROM _values WHERE _id_structure = {rootStructure} AND _id_object = {id}"))
            .Should().Be(1);
        (await ctx.ExecuteScalarAsync<long?>(
            $"SELECT COUNT(*) FROM _values WHERE _id_structure = {dataStructure} AND _id_object = {id}"))
            .Should().Be(1, "the nested base row is a base row, not an element row of the class");
        var loaded = await Redb.LoadAsync<ByteHolderProps>(id, depth: 1);
        loaded!.Props.Root.Should().BeEquivalentTo(root);
        loaded.Props.File.Should().NotBeNull();
        loaded.Props.File!.Name.Should().Be("f");
        loaded.Props.File.Data.Should().BeEquivalentTo(data,
            "a byte[] nested in a class is told apart by membership, not by a NULL parent");
    }

    [Fact]
    public async Task LegacyByteArrayLayout_AlreadyAssembledRow_IsNotBlankedByARetry()
    {
        await Redb.SyncSchemeAsync<ByteHolderProps>();
        var root = new byte[] { 5, 4, 3, 2, 1 };
        var id = await Redb.SaveAsync(new RedbObject<ByteHolderProps>
        {
            name = "holder",
            Props = new ByteHolderProps { Root = root }
        });
        var structureId = await StructureIdAsync("ByteHolder", "Root");

        // The state a crash between the element delete and the type flip leaves behind, and the
        // state a second node sees after the first one finished: bytes already in _ByteArray, no
        // element rows, the structure still Array-of-Byte. Nothing to assemble; keep the payload.
        var ctx = Redb.Context;
        await ctx.ExecuteAsync(
            $"UPDATE _structures SET _id_type = {redb.Core.Utils.RedbTypeIds.Byte}, " +
            $"_collection_type = {redb.Core.Utils.RedbTypeIds.Array} WHERE _id = {structureId}");

        await Redb.SyncSchemeAsync<ByteHolderProps>();

        var loaded = await Redb.LoadAsync<ByteHolderProps>(id, depth: 1);
        loaded!.Props.Root.Should().BeEquivalentTo(root,
            "a retry over an already converted row must recognise it, not overwrite it with an empty payload");
    }

    /// <summary>
    /// Turns the scalar row of one object into the pre-V4 Array-of-Byte layout: _ByteArray cleared,
    /// one element row per byte (_Long, _array_parent_id = base row, _array_index = position), and
    /// the structure retyped to Byte/Array. The base row keeps its own _array_parent_id, so a byte[]
    /// nested in a class stays attached to its class row exactly as an old build left it.
    /// </summary>
    private async Task StageLegacyByteLayoutAsync(long structureId, long objectId, byte[] payload)
    {
        var ctx = Redb.Context;
        var baseRowId = (await ctx.ExecuteScalarAsync<long?>(
            $"SELECT _id FROM _values WHERE _id_structure = {structureId} AND _id_object = {objectId}"))!.Value;
        await ctx.ExecuteAsync($"UPDATE _values SET _ByteArray = NULL WHERE _id = {baseRowId}");
        for (var i = 0; i < payload.Length; i++)
        {
            var elementId = await ctx.Keys.NextValueIdAsync();
            await ctx.ExecuteAsync(
                $"INSERT INTO _values (_id, _id_structure, _id_object, _Long, _array_parent_id, _array_index) " +
                $"VALUES ({elementId}, {structureId}, {objectId}, {payload[i]}, {baseRowId}, '{i}')");
        }
        await ctx.ExecuteAsync(
            $"UPDATE _structures SET _id_type = {redb.Core.Utils.RedbTypeIds.Byte}, " +
            $"_collection_type = {redb.Core.Utils.RedbTypeIds.Array} WHERE _id = {structureId}");
    }

    // ============================================================
    // === soft delete releases keys (decision 9) ===
    // ============================================================

    [Fact]
    public async Task SoftDelete_ReleasesKey_RepeatedDelete()
    {
        await SyncAllAsync();
        var first = Order(p => p.Code = "DEL-1");
        await Redb.SaveAsync(first);
        await Redb.SoftDeleteAsync(new[] { first.id });
        (await Redb.Context.ExecuteScalarAsync<long?>($"SELECT _id_scheme FROM _objects WHERE _id = {first.id}"))
            .Should().Be(-10, "soft delete moves the object to the trash scheme");
        (await Redb.Context.ExecuteScalarAsync<long?>(
            $"SELECT COUNT(*) FROM _values WHERE _id_object = {first.id} AND _unique IS NOT NULL"))
            .Should().Be(0, "decision 9: the trash holds no keys - mark_for_deletion nulls them in the same statement");

        var second = Order(p => p.Code = "DEL-1");
        await Redb.SaveAsync(second);
        await Redb.SoftDeleteAsync(new[] { second.id });
        (await Redb.Context.ExecuteScalarAsync<long?>($"SELECT _id_scheme FROM _objects WHERE _id = {second.id}"))
            .Should().Be(-10, "without key release the trash scheme is one more uniqueness namespace and the second delete dies");
    }

    [Fact]
    public async Task SoftDelete_ReleasesKey_CrossScheme()
    {
        await SyncAllAsync();
        var order = Order(p => p.Code = "CROSS");
        await Redb.SaveAsync(order);
        var invoice = new RedbObject<UniqueInvoiceProps> { name = "invoice", Props = new UniqueInvoiceProps { Code = "CROSS" } };
        await Redb.SaveAsync(invoice);

        await Redb.SoftDeleteAsync(new[] { order.id });
        await Redb.SoftDeleteAsync(new[] { invoice.id }); // both land in scheme -10; released keys cannot collide there
        (await Redb.Context.ExecuteScalarAsync<long?>(
            $"SELECT COUNT(*) FROM _objects WHERE _id IN ({order.id}, {invoice.id}) AND _id_scheme = -10"))
            .Should().Be(2);

        (await Redb.SaveAsync(Order(p => p.Code = "CROSS"))).Should().BeGreaterThan(0,
            "a key released by deletion is immediately available");
    }

    // ============================================================
    // === recompute (2.8) and the SQL-writer rule (P3) ===
    // ============================================================

    [Fact]
    public async Task SqlWriterDamage_IsReportedAndHealed_ByRecompute()
    {
        await SyncAllAsync();
        var winner = Order(p => p.Code = "P1");
        var winnerId = await Redb.SaveAsync(winner);
        var victim = Order(p => p.Code = $"TMP-{Guid.NewGuid():N}");
        var victimId = await Redb.SaveAsync(victim);

        // A SQL-side writer that does not know the canonical form: value overwritten, key released.
        var structureId = await StructureIdAsync("UniqueOrder", "Code");
        await Redb.Context.ExecuteAsync(
            $"UPDATE _values SET _String = 'P1', _unique = NULL WHERE _id_object = {victimId} AND _id_structure = {structureId}");

        var report = await Redb.RecomputeUniqueAsync<UniqueOrderProps>("Code");

        report.HasDuplicates.Should().BeTrue();
        report.Duplicates.Should().ContainSingle().Which.ObjectIds.Should().BeEquivalentTo([winnerId, victimId]);
        report.LosingRows.Should().Be(1);

        // The winner (lowest value id - saved first) holds the key; the loser is outside the index.
        (await Redb.GetByUniqueAsync<UniqueOrderProps>(p => p.Code, "P1"))!.id.Should().Be(winnerId);

        var third = async () => await Redb.SaveAsync(Order(p => p.Code = "P1"));
        await third.Should().ThrowAsync<RedbUniqueViolationException>(
            "after the recompute the key is enforced for every keyed row");
    }

    [Fact]
    public async Task EncoderVersionBehind_TriggersRecompute_OnSync()
    {
        await SyncAllAsync();
        var id = await Redb.SaveAsync(Order(p => { p.Code = "VER-1"; p.Number = 41; }));

        var codeStructure = await StructureIdAsync("UniqueOrder", "Code");
        var numberStructure = await StructureIdAsync("UniqueOrder", "Number");

        // A version behind the encoder with keys PRESENT but stale: the Code row carries the key of
        // the Number row (another structure, so the index does not object). The "unhashed rows"
        // self-heal sees nothing here - only the version trigger can repair it.
        await Redb.Context.ExecuteAsync(
            $"UPDATE _values SET _unique = (SELECT _unique FROM _values WHERE _id_structure = {numberStructure} AND _id_object = {id}) " +
            $"WHERE _id_structure = {codeStructure} AND _id_object = {id}");
        await Redb.Context.ExecuteAsync($"UPDATE _structures SET _unique_version = 0 WHERE _id = {codeStructure}");

        await Redb.SyncSchemeAsync<UniqueOrderProps>();

        (await Redb.Context.ExecuteScalarAsync<long?>($"SELECT _unique_version FROM _structures WHERE _id = {codeStructure}"))
            .Should().Be(UniqueKeyEncoder.Version, "the structure is stamped with the encoder version after the recompute");
        (await Redb.GetByUniqueAsync<UniqueOrderProps>(p => p.Code, "VER-1"))!.id.Should().Be(id,
            "a version stamp behind the encoder recomputes every key of the structure at synchronisation");
    }

    [Fact]
    public async Task RemovingTheAttribute_ClearsFlagAndKeys()
    {
        await SyncAllAsync();

        // Stage a scheme whose CLR class has NO attribute but whose database state says "key":
        // exactly what a deploy of a class with the attribute removed looks like. The staged key is
        // real: the same value in UniqueOrder.Code has the same canonical form, so its hash is
        // borrowed (keys are unique per structure, the index does not object).
        var free = new RedbObject<UniqueFreeProps> { name = "free", Props = new UniqueFreeProps { Code = "F-1" } };
        var freeId = await Redb.SaveAsync(free);
        var orderId = await Redb.SaveAsync(Order(p => p.Code = "F-1"));
        var structureId = await StructureIdAsync("UniqueFree", "Code");
        var orderStructure = await StructureIdAsync("UniqueOrder", "Code");
        await Redb.Context.ExecuteAsync(
            $"UPDATE _values SET _unique = (SELECT _unique FROM _values WHERE _id_structure = {orderStructure} AND _id_object = {orderId}) " +
            $"WHERE _id_structure = {structureId} AND _id_object = {freeId}");
        await Redb.Context.ExecuteAsync(
            $"UPDATE _structures SET _unique = {BoolTrue}, _unique_version = 1 WHERE _id = {structureId}");
        (await Redb.Context.ExecuteScalarAsync<long?>(
            $"SELECT COUNT(*) FROM _values WHERE _id_structure = {structureId} AND _unique IS NOT NULL"))
            .Should().Be(1, "the staging must plant a real key");

        await Redb.SyncSchemeAsync<UniqueFreeProps>();

        (await Redb.Context.ExecuteScalarAsync<bool?>($"SELECT _unique FROM _structures WHERE _id = {structureId}"))
            .Should().NotBe(true, "no attribute - no flag");
        (await Redb.Context.ExecuteScalarAsync<long?>($"SELECT _unique_version FROM _structures WHERE _id = {structureId}"))
            .Should().BeNull("no attribute - no encoder version");
        (await Redb.Context.ExecuteScalarAsync<long?>(
            $"SELECT COUNT(*) FROM _values WHERE _id_structure = {structureId} AND _unique IS NOT NULL"))
            .Should().Be(0, "a key that no longer guards anything must not keep rejecting values");

        // And duplicates are legal again.
        var duplicateId = await Redb.SaveAsync(new RedbObject<UniqueFreeProps> { name = "free2", Props = new UniqueFreeProps { Code = "F-1" } });
        duplicateId.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task SchemeWithoutKeys_IsUntouched()
    {
        await SyncAllAsync();
        await Redb.SaveAsync(new RedbObject<UniqueFreeProps> { name = "a", Props = new UniqueFreeProps { Code = "DUP" } });
        await Redb.SaveAsync(new RedbObject<UniqueFreeProps> { name = "b", Props = new UniqueFreeProps { Code = "DUP" } });

        (await Redb.Context.ExecuteScalarAsync<long?>(
            "SELECT COUNT(*) FROM _values WHERE _unique IS NOT NULL AND _id_structure IN " +
            "(SELECT _id FROM _structures WHERE _id_scheme = (SELECT _id FROM _schemes WHERE _name = 'UniqueFree'))"))
            .Should().Be(0);
    }

    // ============================================================
    // === lookup API and bulk alignment ===
    // ============================================================

    [Fact]
    public async Task GetByUnique_FindsMisses_AndRejectsNonKey()
    {
        await SyncAllAsync();
        var id = await Redb.SaveAsync(Order(p => { p.Code = "ORD-9"; p.Note = "target"; }));

        var found = await Redb.GetByUniqueAsync<UniqueOrderProps>(p => p.Code, "ORD-9");
        found!.id.Should().Be(id);
        found.Props.Note.Should().Be("target");

        (await Redb.GetByUniqueAsync<UniqueOrderProps>(p => p.Code, "NO-SUCH")).Should().BeNull();
        (await Redb.GetByUniqueAsync<UniqueOrderProps>(p => p.Code, null)).Should().BeNull(
            "NULL never takes part in uniqueness");

        var nonKey = async () => await Redb.GetByUniqueAsync<UniqueOrderProps>(p => p.Note, "x");
        await nonKey.Should().ThrowAsync<RedbUniqueKeyDefinitionException>();
    }

    [Fact]
    public async Task BatchOfHundreds_InsertAndUpdate_KeepColumnsAligned()
    {
        await SyncAllAsync();

        // 200 objects x 3 filled fields crosses every bulk chunk boundary (MSSQL MERGE packs 140
        // rows of 14 columns per statement - the exact place where a miscounted offset writes
        // values into the wrong columns without any exception, §4.7).
        var objects = Enumerable.Range(1, 200)
            .Select(i => Order(p => { p.Code = $"B-{i:D3}"; p.Number = 1000 + i; p.Note = $"n{i}"; }))
            .Cast<Core.Models.Contracts.IRedbObject>()
            .ToList();
        var ids = await Redb.SaveAsync(objects);
        ids.Should().HaveCount(200);

        foreach (var obj in objects.Cast<RedbObject<UniqueOrderProps>>())
            obj.Props.Note = obj.Props.Note + "+";
        await Redb.SaveAsync(objects);

        foreach (var probe in new[] { 0, 99, 199 })
        {
            var loaded = await Redb.LoadAsync<UniqueOrderProps>(ids[probe], depth: 1);
            loaded!.Props.Code.Should().Be($"B-{probe + 1:D3}");
            loaded.Props.Number.Should().Be(1000 + probe + 1);
            loaded.Props.Note.Should().Be($"n{probe + 1}+");
        }

        var dup = async () => await Redb.SaveAsync(Order(p => p.Code = "B-100"));
        await dup.Should().ThrowAsync<RedbUniqueViolationException>("the batch-written keys are live");
    }

    // ============================================================
    // === S2: keys on scalars inside nested classes ===
    // ============================================================

    private static RedbObject<UniqueNestedProps> Nested(string? passport, string? title = null)
        => new()
        {
            name = "nested",
            Props = new UniqueNestedProps
            {
                Title = title ?? "t",
                Identity = passport == null ? null : new NestedIdentity { Passport = passport, Issuer = "MVD" }
            }
        };

    [Fact]
    public async Task NestedKey_SchemeSync_SetsTheFlag()
    {
        await Redb.SyncSchemeAsync<UniqueNestedProps>();

        var flagged = await Redb.Context.ExecuteScalarAsync<long?>(
            $"SELECT COUNT(*) FROM _structures WHERE _name = 'Passport' AND _unique = {BoolTrue} AND _id_parent = " +
            "(SELECT _id FROM _structures WHERE _name = 'Identity' AND _id_scheme = " +
            "(SELECT _id FROM _schemes WHERE _name = 'UniqueNested'))");
        flagged.Should().Be(1, "a [RedbUnique] scalar inside a nested class (collection-free path) is a legal key (S2)");
    }

    [Fact]
    public async Task NestedKey_Duplicate_IsRejected_WithTypedException()
    {
        await Redb.SyncSchemeAsync<UniqueNestedProps>();
        await Redb.SaveAsync(Nested("P-100"));

        var act = async () => await Redb.SaveAsync(Nested("P-100"));

        var ex = (await act.Should().ThrowAsync<RedbUniqueViolationException>(
            "the nested field's rows carry their own structure id, the same index enforces them")).Which;
        if (ReportsKeyTuple)
            ex.PropertyName.Should().Be("Passport");
    }

    [Fact]
    public async Task NestedKey_Nulls_Coexist()
    {
        await Redb.SyncSchemeAsync<UniqueNestedProps>();

        await Redb.SaveAsync(Nested(null, "no identity a"));
        await Redb.SaveAsync(Nested(null, "no identity b"));
        var nullPassportA = Nested("x"); nullPassportA.Props.Identity!.Passport = null;
        var nullPassportB = Nested("x"); nullPassportB.Props.Identity!.Passport = null;
        await Redb.SaveAsync(nullPassportA);
        await Redb.SaveAsync(nullPassportB);

        var count = await Redb.Query<UniqueNestedProps>().CountAsync();
        count.Should().Be(4, "a NULL nested class and a NULL key both claim nothing");
    }

    [Fact]
    public async Task NestedKey_KeyChange_OnUpdate_ReleasesOldValue_AndGuardsNew()
    {
        await Redb.SyncSchemeAsync<UniqueNestedProps>();
        var firstId = await Redb.SaveAsync(Nested("P-OLD"));

        var loaded = await Redb.LoadAsync<UniqueNestedProps>(firstId, depth: 2);
        loaded!.Props.Identity!.Passport = "P-NEW";
        await Redb.SaveAsync(loaded);

        await Redb.SaveAsync(Nested("P-OLD", "successor of the released value"));

        var act = async () => await Redb.SaveAsync(Nested("P-NEW"));
        await act.Should().ThrowAsync<RedbUniqueViolationException>("the moved key guards its new value");
    }

    [Fact]
    public async Task NestedKey_GetByUnique_FindsByPath()
    {
        await Redb.SyncSchemeAsync<UniqueNestedProps>();
        var id = await Redb.SaveAsync(Nested("P-LOOKUP", "the one"));

        var byLambda = await Redb.GetByUniqueAsync<UniqueNestedProps>(p => p.Identity!.Passport, "P-LOOKUP");
        byLambda.Should().NotBeNull("the lambda path walks the nested structure chain");
        byLambda!.Id.Should().Be(id);
        byLambda.Props.Title.Should().Be("the one");

        var byName = await Redb.GetByUniqueAsync<UniqueNestedProps>("Identity.Passport", "P-LOOKUP");
        byName!.Id.Should().Be(id);
    }

    [Fact]
    public async Task NestedKey_SoftDelete_ReleasesKey()
    {
        await Redb.SyncSchemeAsync<UniqueNestedProps>();
        var id = await Redb.SaveAsync(Nested("P-TRASH"));
        await Redb.DeleteAsync(id);

        var successorId = await Redb.SaveAsync(Nested("P-TRASH", "successor"));
        successorId.Should().BeGreaterThan(0, "the trash does not squat on nested keys either");
    }

    [Fact]
    public async Task NestedKey_Recompute_RestoresKeys_AfterSqlWipe()
    {
        await Redb.SyncSchemeAsync<UniqueNestedProps>();
        await Redb.SaveAsync(Nested("P-R1"));
        await Redb.SaveAsync(Nested("P-R2"));

        // A SQL-side writer wiped the keys: rows with a value but no key are exactly the
        // recompute trigger's trace, and the explicit recompute must repair a NESTED structure.
        await Redb.Context.ExecuteAsync(
            "UPDATE _values SET _unique = NULL WHERE _id_structure = " +
            "(SELECT _id FROM _structures WHERE _name = 'Passport' AND _id_parent = " +
            "(SELECT _id FROM _structures WHERE _name = 'Identity' AND _id_scheme = " +
            "(SELECT _id FROM _schemes WHERE _name = 'UniqueNested')))");

        var report = await Redb.RecomputeUniqueAsync<UniqueNestedProps>("Identity.Passport");
        report.HashedRows.Should().Be(2, "both stored passports get their keys back");

        var act = async () => await Redb.SaveAsync(Nested("P-R1"));
        await act.Should().ThrowAsync<RedbUniqueViolationException>("the restored keys are live again");
    }

    [Fact]
    public async Task NestedKey_NullToValue_OnUpdate_ClaimsKey()
    {
        await Redb.SyncSchemeAsync<UniqueNestedProps>();
        var holder = Nested("x");
        holder.Props.Identity!.Passport = null;
        var id = await Redb.SaveAsync(holder);

        var loaded = await Redb.LoadAsync<UniqueNestedProps>(id, depth: 2);
        loaded!.Props.Identity!.Passport = "P-CLAIMED";
        await Redb.SaveAsync(loaded);

        var act = async () => await Redb.SaveAsync(Nested("P-CLAIMED"));
        await act.Should().ThrowAsync<RedbUniqueViolationException>(
            "a key claimed by a null-to-value UPDATE transition must be live");
    }

    [Fact]
    public async Task NestedKey_ClassAppears_OnUpdate_ClaimsKey()
    {
        await Redb.SyncSchemeAsync<UniqueNestedProps>();
        var id = await Redb.SaveAsync(Nested(null, "class appears later"));

        // The whole nested class materialises on UPDATE - its rows go through the insert
        // channel of the save strategy, which must fill the key exactly like a fresh save.
        var loaded = await Redb.LoadAsync<UniqueNestedProps>(id, depth: 2);
        loaded!.Props.Identity = new NestedIdentity { Passport = "P-BORN", Issuer = "MVD" };
        await Redb.SaveAsync(loaded);

        var act = async () => await Redb.SaveAsync(Nested("P-BORN"));
        await act.Should().ThrowAsync<RedbUniqueViolationException>(
            "a key of a nested class that appeared on update must be live");
    }

    [Fact]
    public async Task NestedKey_TwoLevelsDeep_Enforces_AndResolvesPath()
    {
        await Redb.SyncSchemeAsync<UniqueDeepProps>();
        var deep = new RedbObject<UniqueDeepProps>
        {
            name = "deep",
            Props = new UniqueDeepProps { Outer = new DeepOuter { Inner = new DeepInner { Code = "D-1" } } }
        };
        var id = await Redb.SaveAsync(deep);

        var act = async () => await Redb.SaveAsync(new RedbObject<UniqueDeepProps>
        {
            name = "deep-dup",
            Props = new UniqueDeepProps { Outer = new DeepOuter { Inner = new DeepInner { Code = "D-1" } } }
        });
        await act.Should().ThrowAsync<RedbUniqueViolationException>("the machinery is depth-agnostic");

        var found = await Redb.GetByUniqueAsync<UniqueDeepProps>(p => p.Outer!.Inner!.Code, "D-1");
        found!.Id.Should().Be(id);
    }

    [Fact]
    public async Task NestedKey_BatchSave_EnforcesKeys()
    {
        await Redb.SyncSchemeAsync<UniqueNestedProps>();

        var batch = new[] { Nested("B-1"), Nested("B-2"), Nested("B-3") }
            .Cast<Core.Models.Contracts.IRedbObject>().ToList();
        (await Redb.SaveAsync(batch)).Should().HaveCount(3);

        var dupAgainstStored = async () => await Redb.SaveAsync(Nested("B-2"));
        await dupAgainstStored.Should().ThrowAsync<RedbUniqueViolationException>(
            "batch-written nested keys are live");

        var dupInsideBatch = async () => await Redb.SaveAsync(
            new[] { Nested("B-4"), Nested("B-4") }.Cast<Core.Models.Contracts.IRedbObject>().ToList());
        await dupInsideBatch.Should().ThrowAsync<RedbUniqueViolationException>(
            "two claimants of one key inside a single batch must not both land");
    }

    [Fact]
    public async Task NestedKey_GetByUnique_WrongPath_Throws()
    {
        await Redb.SyncSchemeAsync<UniqueNestedProps>();

        var act = async () => await Redb.GetByUniqueAsync<UniqueNestedProps>("Identity.Nope", "x");
        await act.Should().ThrowAsync<RedbUniqueKeyDefinitionException>(
            "a wrong path must name the missing segment, not return null as if the key were free");
    }

    // ============================================================
    // === S1: subtree keys - the whole nested class / collection content ===
    // ============================================================

    private static RedbObject<UniqueSubtreeProps> Subtree(Action<UniqueSubtreeProps> set)
    {
        var props = new UniqueSubtreeProps();
        set(props);
        return new RedbObject<UniqueSubtreeProps> { name = "subtree", Props = props };
    }

    [Fact]
    public async Task SubtreeKey_SchemeSync_SetsTheFlag_OnClassArrayAndDictionary()
    {
        await Redb.SyncSchemeAsync<UniqueSubtreeProps>();

        var flagged = await Redb.Context.ExecuteScalarAsync<long?>(
            $"SELECT COUNT(*) FROM _structures WHERE _unique = {BoolTrue} AND _id_parent IS NULL " +
            "AND _name IN ('Config', 'Slots', 'Limits') AND _id_scheme = " +
            "(SELECT _id FROM _schemes WHERE _name = 'UniqueSubtree')");
        flagged.Should().Be(3, "a root class, array and dictionary are all legal subtree keys (S1)");
    }

    [Fact]
    public async Task SubtreeKey_Class_DuplicateContentRejected()
    {
        await Redb.SyncSchemeAsync<UniqueSubtreeProps>();
        await Redb.SaveAsync(Subtree(p => p.Config = new SubtreeConfig { Region = "eu", Tier = 2 }));
        await Redb.SaveAsync(Subtree(p => p.Config = new SubtreeConfig { Region = "eu", Tier = 3 }));

        var act = async () => await Redb.SaveAsync(Subtree(p => p.Config = new SubtreeConfig { Region = "eu", Tier = 2 }));
        await act.Should().ThrowAsync<RedbUniqueViolationException>(
            "two objects with byte-for-byte equal subtree CONTENT are the duplicate; different content is not");
    }

    [Fact]
    public async Task SubtreeKey_Dictionary_IsOrderInsensitive()
    {
        await Redb.SyncSchemeAsync<UniqueSubtreeProps>();
        await Redb.SaveAsync(Subtree(p => p.Limits = new Dictionary<string, long> { ["a"] = 1, ["b"] = 2 }));

        var act = async () => await Redb.SaveAsync(Subtree(p => p.Limits = new Dictionary<string, long> { ["b"] = 2, ["a"] = 1 }));
        await act.Should().ThrowAsync<RedbUniqueViolationException>(
            "a dictionary is an unordered set of pairs: {a:1,b:2} IS {b:2,a:1}");
    }

    [Fact]
    public async Task SubtreeKey_Array_OrderMatters()
    {
        await Redb.SyncSchemeAsync<UniqueSubtreeProps>();
        await Redb.SaveAsync(Subtree(p => p.Slots = [1, 2, 3]));
        await Redb.SaveAsync(Subtree(p => p.Slots = [3, 2, 1]));

        var act = async () => await Redb.SaveAsync(Subtree(p => p.Slots = [1, 2, 3]));
        await act.Should().ThrowAsync<RedbUniqueViolationException>(
            "a list is ordered: [1,2,3] equals only [1,2,3]");
    }

    [Fact]
    public async Task SubtreeKey_NullSubtrees_Coexist()
    {
        await Redb.SyncSchemeAsync<UniqueSubtreeProps>();
        await Redb.SaveAsync(Subtree(p => p.Note = "empty a"));
        await Redb.SaveAsync(Subtree(p => p.Note = "empty b"));

        (await Redb.Query<UniqueSubtreeProps>().CountAsync()).Should().Be(2,
            "an absent subtree claims no key");
    }

    [Fact]
    public async Task SubtreeKey_GetByUnique_ByObjectValue()
    {
        await Redb.SyncSchemeAsync<UniqueSubtreeProps>();
        var id = await Redb.SaveAsync(Subtree(p => { p.Config = new SubtreeConfig { Region = "apac", Tier = 7 }; p.Note = "the one"; }));

        var found = await Redb.GetByUniqueAsync<UniqueSubtreeProps>(
            p => p.Config, new SubtreeConfig { Region = "apac", Tier = 7 });
        found.Should().NotBeNull("the probe canonicalises the object value the same way the save did");
        found!.Id.Should().Be(id);
        found.Props.Note.Should().Be("the one");
    }

    [Fact]
    public async Task SubtreeKey_ContentEdit_MovesKey()
    {
        await Redb.SyncSchemeAsync<UniqueSubtreeProps>();
        var id = await Redb.SaveAsync(Subtree(p => p.Config = new SubtreeConfig { Region = "us", Tier = 1 }));

        var loaded = await Redb.LoadAsync<UniqueSubtreeProps>(id, depth: 2);
        loaded!.Props.Config!.Tier = 9;
        await Redb.SaveAsync(loaded);

        await Redb.SaveAsync(Subtree(p => p.Config = new SubtreeConfig { Region = "us", Tier = 1 }));

        var act = async () => await Redb.SaveAsync(Subtree(p => p.Config = new SubtreeConfig { Region = "us", Tier = 9 }));
        await act.Should().ThrowAsync<RedbUniqueViolationException>(
            "an edited subtree releases its old content key and guards the new one");
    }

    // ============================================================
    // === S3: element keys on collections - [RedbUnique(Scope = ...)] ===
    // ============================================================

    private static RedbObject<UniqueElementsProps> Elems(Action<UniqueElementsProps> set)
    {
        var props = new UniqueElementsProps();
        set(props);
        return new RedbObject<UniqueElementsProps> { name = "elems", Props = props };
    }

    [Fact]
    public async Task ElemKey_ScopeOnScalar_IsRejectedAtSync()
    {
        var act = async () => await Redb.SyncSchemeAsync<ScopedScalarProps>();
        await act.Should().ThrowAsync<RedbUniqueKeyDefinitionException>(
            "Scope on a scalar is meaningless - the bare attribute already carries the strongest reading");
    }

    [Fact]
    public async Task ElemKey_Collection_DuplicateInsideOneCollection_Rejected()
    {
        await Redb.SyncSchemeAsync<UniqueElementsProps>();

        var act = async () => await Redb.SaveAsync(Elems(p => p.Codes = ["A", "B", "A"]));
        await act.Should().ThrowAsync<RedbUniqueViolationException>(
            "Collection scope means no duplicate elements INSIDE one collection");
    }

    [Fact]
    public async Task ElemKey_Collection_SameValueInTwoObjects_Coexists()
    {
        await Redb.SyncSchemeAsync<UniqueElementsProps>();
        await Redb.SaveAsync(Elems(p => p.Codes = ["A", "B"]));
        var id = await Redb.SaveAsync(Elems(p => p.Codes = ["A", "C"]));

        id.Should().BeGreaterThan(0,
            "Collection scope is per collection: different objects may repeat each other freely");
    }

    [Fact]
    public async Task ElemKey_Scheme_ValueUniqueAcrossObjects()
    {
        await Redb.SyncSchemeAsync<UniqueElementsProps>();
        await Redb.SaveAsync(Elems(p => p.Emails = ["a@x.io", "b@x.io"]));

        var act = async () => await Redb.SaveAsync(Elems(p => p.Emails = ["a@x.io"]));
        await act.Should().ThrowAsync<RedbUniqueViolationException>(
            "Scheme scope: an element value appears once across every object of the scheme");

        (await Redb.SaveAsync(Elems(p => p.Emails = ["c@x.io"]))).Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task ElemKey_Scheme_DuplicateInsideOneCollection_AlsoRejected()
    {
        await Redb.SyncSchemeAsync<UniqueElementsProps>();

        var act = async () => await Redb.SaveAsync(Elems(p => p.Emails = ["dup@x.io", "dup@x.io"]));
        await act.Should().ThrowAsync<RedbUniqueViolationException>(
            "scheme-wide uniqueness subsumes uniqueness inside one collection");
    }

    [Fact]
    public async Task ElemKey_Refs_Collection_NoDuplicateLinks()
    {
        await Redb.SyncSchemeAsync<UniqueElementsProps>();
        await Redb.SyncSchemeAsync<UniqueFreeProps>();
        var target = new RedbObject<UniqueFreeProps> { name = "target", Props = new UniqueFreeProps { Code = "T" } };
        await Redb.SaveAsync(target);

        var holder = Elems(p => p.Note = "links");
        holder.Props.Links = [target, target];

        var act = async () => await Redb.SaveAsync(holder);
        await act.Should().ThrowAsync<RedbUniqueViolationException>(
            "a reference element canonicalises by target id: no duplicate links in one collection");
    }

    [Fact]
    public async Task ElemKey_NullAndEmpty_Coexist()
    {
        await Redb.SyncSchemeAsync<UniqueElementsProps>();
        await Redb.SaveAsync(Elems(p => p.Note = "null lists a"));
        await Redb.SaveAsync(Elems(p => p.Note = "null lists b"));
        await Redb.SaveAsync(Elems(p => p.Codes = []));
        await Redb.SaveAsync(Elems(p => p.Codes = []));

        (await Redb.Query<UniqueElementsProps>().CountAsync()).Should().Be(4,
            "neither an absent nor an empty collection claims element keys");
    }

    [Fact]
    public async Task ElemKey_CT_AddedDuplicateOnUpdate_Rejected()
    {
        await Redb.SyncSchemeAsync<UniqueElementsProps>();
        var id = await Redb.SaveAsync(Elems(p => p.Codes = ["A", "B"]));

        var loaded = await Redb.LoadAsync<UniqueElementsProps>(id, depth: 2);
        loaded!.Props.Codes!.Add("A");

        var act = async () => await Redb.SaveAsync(loaded);
        await act.Should().ThrowAsync<RedbUniqueViolationException>(
            "an element added on UPDATE goes through the insert channel and must be keyed the same way");
    }

    [Fact]
    public async Task ElemKey_GetByUnique_SchemeScope_FindsTheHolder()
    {
        await Redb.SyncSchemeAsync<UniqueElementsProps>();
        var id = await Redb.SaveAsync(Elems(p => { p.Emails = ["find@x.io"]; p.Note = "holder"; }));

        var found = await Redb.GetByUniqueAsync<UniqueElementsProps>(p => p.Emails, "find@x.io");
        found.Should().NotBeNull("a scheme-scoped element key is globally probeable by the element value");
        found!.Id.Should().Be(id);
    }

    [Fact]
    public async Task ElemKey_GetByUnique_CollectionScope_IsRejected()
    {
        await Redb.SyncSchemeAsync<UniqueElementsProps>();

        var act = async () => await Redb.GetByUniqueAsync<UniqueElementsProps>(p => p.Codes, "A");
        await act.Should().ThrowAsync<RedbUniqueKeyDefinitionException>(
            "a collection-scoped key is unique only within its collection - a global by-value probe has no meaning");
    }

    [Fact]
    public async Task ElemKey_Recompute_RestoresKeys_AfterSqlWipe()
    {
        await Redb.SyncSchemeAsync<UniqueElementsProps>();
        await Redb.SaveAsync(Elems(p => p.Emails = ["r1@x.io"]));
        await Redb.SaveAsync(Elems(p => p.Emails = ["r2@x.io"]));

        await Redb.Context.ExecuteAsync(
            "UPDATE _values SET _unique = NULL WHERE _id_structure = " +
            "(SELECT _id FROM _structures WHERE _name = 'Emails' AND _id_scheme = " +
            "(SELECT _id FROM _schemes WHERE _name = 'UniqueElems'))");

        var report = await Redb.RecomputeUniqueAsync<UniqueElementsProps>("Emails");
        report.HashedRows.Should().Be(2, "both stored element values get their keys back");

        var act = async () => await Redb.SaveAsync(Elems(p => p.Emails = ["r1@x.io"]));
        await act.Should().ThrowAsync<RedbUniqueViolationException>("the restored element keys are live");
    }

    [Fact]
    public async Task KeyInsideCollectionElement_IsRejectedAtSync()
    {
        var act = async () => await Redb.SyncSchemeAsync<UniqueInElementProps>();

        await act.Should().ThrowAsync<RedbUniqueKeyDefinitionException>(
            "every element of every object shares one structure, so a key inside a collection " +
            "element would degenerate to one value across the whole database");
    }
}
