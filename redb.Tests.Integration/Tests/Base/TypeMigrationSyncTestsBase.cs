using redb.Core;
using redb.Core.Exceptions;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Type migration as it is reached through <c>SyncSchemeAsync</c> — the path that had no test at all.
///
/// <para>
/// The pre-existing migration tests all call the public <c>MigrateStructureTypeAsync</c> with correct
/// type names, which is why they stayed green while the sync path passed a numeric id into a lookup
/// keyed by name, resolved the old type to the literal "unknown", got an error back from every
/// provider, discarded it, and switched <c>_structures._id_type</c> anyway. Values were left in the
/// old column, read as null, and were physically deleted by the next save under the default
/// DeleteInsert strategy.
/// </para>
///
/// <para>
/// <b>How a type change is staged.</b> Sharing one <c>[RedbScheme(Name = "...")]</c> between two
/// classes is refused by the library, so instead each test writes values through the current class and
/// then rewrites <c>_structures._id_type</c> and the storage column with raw SQL into the shape an
/// earlier version of that class would have left behind. The sync path cannot tell the difference from
/// a real deployment where the property's type changed.
/// </para>
///
/// <para>
/// <b>Why cleanup is mandatory, not tidiness.</b> Half of these tests deliberately leave a scheme the
/// library refuses to sync — that IS the behaviour under test. The fixture's <c>InitializeAsync</c>
/// runs auto-sync across the whole assembly, so one such scheme left behind fails every test in the
/// collection on the next run, not just this suite. Cleanup therefore runs in <c>DisposeAsync</c>,
/// which xUnit calls even when the test fails.
/// </para>
/// </summary>
public abstract class TypeMigrationSyncTestsBase : IAsyncLifetime
{
    protected readonly IRedbService Redb;

    protected TypeMigrationSyncTestsBase(IRedbService redb) => Redb = redb;

    /// <summary>Every scheme this suite touches, dropped before and after each test.</summary>
    private static readonly string[] Schemes =
    [
        "type_migration_supported",
        "type_migration_unsupported",
        "type_migration_samecolumn",
        "type_migration_partial",
        "type_migration_bool_from_text"
    ];

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

    // ============================================================
    // === helpers ===
    // ============================================================

    /// <summary>Objects first (values cascade off them), then structures, then the scheme itself.</summary>
    private async Task ResetSchemeAsync(string schemeName)
    {
        await Redb.Context.ExecuteAsync(
            $"DELETE FROM _objects WHERE _id_scheme IN (SELECT _id FROM _schemes WHERE _name = '{schemeName}')");
        await Redb.Context.ExecuteAsync(
            $"DELETE FROM _structures WHERE _id_scheme IN (SELECT _id FROM _schemes WHERE _name = '{schemeName}')");
        await Redb.Context.ExecuteAsync($"DELETE FROM _schemes WHERE _name = '{schemeName}'");

        // Raw SQL is invisible to the metadata cache; without this the next sync answers from a scheme
        // that no longer exists.
        Redb.Cache.Clear();
    }

    private Task<long?> TypeIdAsync(string typeName) =>
        Redb.Context.ExecuteScalarAsync<long?>($"SELECT _id FROM _types WHERE _name = '{typeName}'");

    private Task<long?> StructureIdAsync(string schemeName, string propertyName) =>
        Redb.Context.ExecuteScalarAsync<long?>(
            $"SELECT _id FROM _structures WHERE _name = '{propertyName}' AND _id_scheme IN " +
            $"(SELECT _id FROM _schemes WHERE _name = '{schemeName}')");

    private Task<long?> StructureTypeIdAsync(long structureId) =>
        Redb.Context.ExecuteScalarAsync<long?>($"SELECT _id_type FROM _structures WHERE _id = {structureId}");

    private Task<long?> CountAsync(string columnName, long structureId) =>
        Redb.Context.ExecuteScalarAsync<long?>(
            $"SELECT CAST(COUNT(*) AS BIGINT) FROM _values WHERE _id_structure = {structureId} " +
            $"AND {columnName} IS NOT NULL");

    /// <summary>Repoints a structure at another REDB type without touching its values.</summary>
    private async Task SetStructureTypeAsync(long structureId, string typeName)
    {
        var typeId = await TypeIdAsync(typeName);
        typeId.Should().NotBeNull($"the seed type '{typeName}' must exist in _types");

        await Redb.Context.ExecuteAsync(
            $"UPDATE _structures SET _id_type = {typeId!.Value} WHERE _id = {structureId}");
        Redb.Cache.Clear();
    }

    // ============================================================
    // === supported conversion: the values must actually move ===
    // ============================================================

    /// <summary>
    /// Boolean to Int on every provider. Red before the fix in the most concrete way possible: the
    /// migration never ran, so _Long stayed empty while _id_type said Int.
    /// </summary>
    [Fact]
    public async Task SupportedConversion_MovesValuesAndUpdatesType()
    {
        const string scheme = "type_migration_supported";
        await Redb.SyncSchemeAsync<TypeMigrationIntProps>();

        var ids = new List<long>();
        foreach (var (flag, tag) in new[] { (1, "a"), (0, "b"), (1, "c") })
        {
            var obj = new RedbObject<TypeMigrationIntProps>
            {
                name = $"probe-{tag}",
                Props = new TypeMigrationIntProps { Flag = flag, Tag = tag }
            };
            ids.Add(await Redb.SaveAsync(obj));
        }

        var structureId = (await StructureIdAsync(scheme, nameof(TypeMigrationIntProps.Flag)))!.Value;

        // Stage the database as an earlier build with `bool Flag` would have left it.
        await Redb.Context.ExecuteAsync(
            $"UPDATE _values SET _Boolean = CASE WHEN _Long <> 0 THEN {BoolTrue} ELSE {BoolFalse} END, _Long = NULL " +
            $"WHERE _id_structure = {structureId}");
        await SetStructureTypeAsync(structureId, "Boolean");

        (await CountAsync("_Boolean", structureId)).Should().Be(3, "staging must put every value in _Boolean");
        (await CountAsync("_Long", structureId)).Should().Be(0);

        await Redb.SyncSchemeAsync<TypeMigrationIntProps>();

        (await CountAsync("_Long", structureId)).Should().Be(3, "the migration must move every value");
        (await CountAsync("_Boolean", structureId)).Should().Be(0, "and must clear the old column");
        (await StructureTypeIdAsync(structureId)).Should().Be(await TypeIdAsync("Int"));

        // The point of the whole exercise: the objects still read back what they were saved with.
        var loadedA = await Redb.LoadAsync<TypeMigrationIntProps>(ids[0]);
        var loadedB = await Redb.LoadAsync<TypeMigrationIntProps>(ids[1]);
        loadedA!.Props.Flag.Should().Be(1);
        loadedA.Props.Tag.Should().Be("a", "a neighbouring property must not be disturbed");
        loadedB!.Props.Flag.Should().Be(0);
    }

    /// <summary>
    /// Int and Long share the <c>_Long</c> column, so there is nothing to move. The type still has to
    /// change, and no value may be touched.
    /// </summary>
    [Fact]
    public async Task SameStorageColumn_ChangesTypeWithoutTouchingValues()
    {
        const string scheme = "type_migration_samecolumn";
        await Redb.SyncSchemeAsync<TypeMigrationLongProps>();

        var obj = new RedbObject<TypeMigrationLongProps>
        {
            name = "probe-same-column",
            Props = new TypeMigrationLongProps { Flag = 42, Tag = "same" }
        };
        var id = await Redb.SaveAsync(obj);

        var structureId = (await StructureIdAsync(scheme, nameof(TypeMigrationLongProps.Flag)))!.Value;
        await SetStructureTypeAsync(structureId, "Int");

        await Redb.SyncSchemeAsync<TypeMigrationLongProps>();

        (await StructureTypeIdAsync(structureId)).Should().Be(await TypeIdAsync("Long"));
        (await CountAsync("_Long", structureId)).Should().Be(1);

        var loaded = await Redb.LoadAsync<TypeMigrationLongProps>(id);
        loaded!.Props.Flag.Should().Be(42);
    }

    // ============================================================
    // === refusal: sync must stop and leave the type alone ===
    // ============================================================

    /// <summary>
    /// Boolean to Guid is in no provider's conversion matrix. Synchronisation must fail, and — the part
    /// that actually protects the data — it must not switch the structure type on the way out.
    /// </summary>
    [Fact]
    public async Task UnsupportedConversion_ThrowsAndLeavesTypeAndValuesIntact()
    {
        const string scheme = "type_migration_unsupported";
        await Redb.SyncSchemeAsync<TypeMigrationGuidProps>();

        var obj = new RedbObject<TypeMigrationGuidProps>
        {
            name = "probe-unsupported",
            Props = new TypeMigrationGuidProps { Flag = Guid.NewGuid(), Tag = "keep" }
        };
        await Redb.SaveAsync(obj);

        var structureId = (await StructureIdAsync(scheme, nameof(TypeMigrationGuidProps.Flag)))!.Value;

        await Redb.Context.ExecuteAsync(
            $"UPDATE _values SET _Boolean = {BoolTrue}, _Guid = NULL WHERE _id_structure = {structureId}");
        await SetStructureTypeAsync(structureId, "Boolean");
        var booleanTypeId = await TypeIdAsync("Boolean");

        var act = async () => await Redb.SyncSchemeAsync<TypeMigrationGuidProps>();

        var ex = (await act.Should().ThrowAsync<RedbTypeMigrationException>()).Which;
        ex.OldTypeName.Should().Be("Boolean");
        ex.NewTypeName.Should().Be("Guid");
        ex.PropertyName.Should().Be(nameof(TypeMigrationGuidProps.Flag));
        ex.StructureId.Should().Be(structureId);
        ex.Message.Should().Contain("migrate_structure_type", "the message has to carry the manual fix");

        (await StructureTypeIdAsync(structureId)).Should().Be(booleanTypeId,
            "a refused migration must not change the type");
        (await CountAsync("_Boolean", structureId)).Should().Be(1, "and must not touch the values");
    }

    /// <summary>
    /// A type with no scalar storage column of its own (Class) makes every provider report an unknown
    /// source type. With values present that is a genuine refusal.
    /// </summary>
    [Fact]
    public async Task TypeWithoutStorageColumn_WithValues_Throws()
    {
        const string scheme = "type_migration_supported";
        await Redb.SyncSchemeAsync<TypeMigrationIntProps>();

        var obj = new RedbObject<TypeMigrationIntProps>
        {
            name = "probe-class-with-values",
            Props = new TypeMigrationIntProps { Flag = 7, Tag = "x" }
        };
        await Redb.SaveAsync(obj);

        var structureId = (await StructureIdAsync(scheme, nameof(TypeMigrationIntProps.Flag)))!.Value;
        await SetStructureTypeAsync(structureId, "Class");
        var classTypeId = await TypeIdAsync("Class");

        var act = async () => await Redb.SyncSchemeAsync<TypeMigrationIntProps>();

        var ex = (await act.Should().ThrowAsync<RedbTypeMigrationException>()).Which;

        // The provider bails out before it counts anything, so it reports zero affected rows while a
        // value is sitting right there. Reporting that zero would say nothing is at stake.
        ex.AffectedRows.Should().Be(1, "the count must fall back to the rows actually stored");
        ex.StrandedRows.Should().Be(1);

        (await StructureTypeIdAsync(structureId)).Should().Be(classTypeId);
        (await CountAsync("_Long", structureId)).Should().Be(1);
    }

    /// <summary>
    /// The same unmigratable pair with no values at all must NOT fail: there is nothing to strand, and
    /// refusing would break type changes on empty schemes for no gain. This is the boundary of the
    /// previous test, and the reason the check counts rows instead of trusting the error string.
    /// </summary>
    [Fact]
    public async Task TypeWithoutStorageColumn_NoValues_Succeeds()
    {
        const string scheme = "type_migration_supported";
        await Redb.SyncSchemeAsync<TypeMigrationIntProps>();

        var structureId = (await StructureIdAsync(scheme, nameof(TypeMigrationIntProps.Flag)))!.Value;
        await SetStructureTypeAsync(structureId, "Class");

        await Redb.SyncSchemeAsync<TypeMigrationIntProps>();

        (await StructureTypeIdAsync(structureId)).Should().Be(await TypeIdAsync("Int"));
    }

    // ============================================================
    // === partial conversion ===
    // ============================================================

    /// <summary>
    /// Text to number where one row is not a number. The readable rows move, the unreadable one stays,
    /// and that half-done state is a failure: synchronisation stops and the type stays put, so the next
    /// start fails the same way instead of quietly presenting the leftovers as missing.
    /// </summary>
    [Fact]
    public async Task PartialConversion_ThrowsAndReportsWhatWasLeftBehind()
    {
        const string scheme = "type_migration_partial";
        await Redb.SyncSchemeAsync<TypeMigrationPartialProps>();

        foreach (var (flag, tag) in new[] { (1L, "one"), (2L, "two"), (3L, "three") })
        {
            var obj = new RedbObject<TypeMigrationPartialProps>
            {
                name = $"probe-partial-{tag}",
                Props = new TypeMigrationPartialProps { Flag = flag, Tag = tag }
            };
            await Redb.SaveAsync(obj);
        }

        var structureId = (await StructureIdAsync(scheme, nameof(TypeMigrationPartialProps.Flag)))!.Value;

        // Stage as text, and make one row unreadable as a number.
        await Redb.Context.ExecuteAsync(
            $"UPDATE _values SET _String = CAST(_Long AS {TextTypeName}), _Long = NULL " +
            $"WHERE _id_structure = {structureId}");
        await Redb.Context.ExecuteAsync(
            $"UPDATE _values SET _String = 'not-a-number' WHERE _id_structure = {structureId} AND _String = '3'");
        await SetStructureTypeAsync(structureId, "String");
        var stringTypeId = await TypeIdAsync("String");

        var act = async () => await Redb.SyncSchemeAsync<TypeMigrationPartialProps>();

        var ex = (await act.Should().ThrowAsync<RedbTypeMigrationException>()).Which;
        ex.StrandedRows.Should().Be(1, "exactly one value could not be read as a number");
        ex.MigratedRows.Should().Be(2);

        (await StructureTypeIdAsync(structureId)).Should().Be(stringTypeId,
            "a partial migration must not switch the type either");
        (await CountAsync("_String", structureId)).Should().Be(1, "the unreadable value stays where it was");
    }

    /// <summary>Boolean literals for the staging SQL: PostgreSQL has a real boolean, the others use 1/0.</summary>
    protected abstract string BoolTrue { get; }

    /// <inheritdoc cref="BoolTrue"/>
    protected abstract string BoolFalse { get; }

    /// <summary>
    /// Text to Boolean used to be the one conversion that DESTROYED what it could not read. The CASE
    /// mapped an unrecognised token to NULL while the same statement cleared the source column, and
    /// because success is measured by rows updated, the loss was reported as a success — no error, no
    /// count, nothing to notice. Every neighbouring text conversion was already guarded; this one was
    /// not, on both PostgreSQL and MSSQL.
    ///
    /// <para>
    /// The value the migration cannot read must survive, exactly like an unparseable number does.
    /// </para>
    /// </summary>
    [Fact]
    public async Task TextToBoolean_KeepsValuesItCannotRead()
    {
        const string scheme = "type_migration_bool_from_text";
        await Redb.SyncSchemeAsync<TypeMigrationBoolProps>();

        foreach (var (flag, tag) in new[] { (true, "yes"), (false, "no"), (true, "odd") })
        {
            var obj = new RedbObject<TypeMigrationBoolProps>
            {
                name = $"probe-bool-{tag}",
                Props = new TypeMigrationBoolProps { Flag = flag, Tag = tag }
            };
            await Redb.SaveAsync(obj);
        }

        var structureId = (await StructureIdAsync(scheme, nameof(TypeMigrationBoolProps.Flag)))!.Value;

        // Stage as text, and make one row a token no provider accepts.
        await Redb.Context.ExecuteAsync(
            $"UPDATE _values SET _String = CASE WHEN _Boolean = {BoolTrue} THEN 'true' ELSE 'false' END, " +
            $"_Boolean = NULL WHERE _id_structure = {structureId}");
        await Redb.Context.ExecuteAsync(
            $"UPDATE _values SET _String = 'maybe' WHERE _id = " +
            $"(SELECT _id FROM _values WHERE _id_structure = {structureId} AND _String = 'false')");
        await SetStructureTypeAsync(structureId, "String");
        var stringTypeId = await TypeIdAsync("String");

        var act = async () => await Redb.SyncSchemeAsync<TypeMigrationBoolProps>();

        var ex = (await act.Should().ThrowAsync<RedbTypeMigrationException>()).Which;
        ex.StrandedRows.Should().Be(1);

        // The assertion the whole test exists for: 'maybe' is still there, not silently erased.
        (await CountAsync("_String", structureId)).Should().Be(1, "an unreadable token must not be destroyed");
        (await StructureTypeIdAsync(structureId)).Should().Be(stringTypeId);
    }

    /// <summary>
    /// Cast target for "number as text" in the staging SQL. Spelled by the provider because the type
    /// name is the one part of a CAST that is not portable.
    /// </summary>
    protected abstract string TextTypeName { get; }
}
