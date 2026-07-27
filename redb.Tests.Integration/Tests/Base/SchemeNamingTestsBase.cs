using redb.Core;
using redb.Core.Exceptions;
using redb.Core.Models.Entities;
using redb.Core.Utils;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Scenarios A–U for explicit scheme names (<c>[RedbScheme(Name = "...")]</c>), scheme alias
/// synchronisation, and the multi-node scheme creation race.
/// See docs/SCHEME_EXPLICIT_NAME_PLAN.md.
/// <para>
/// Every test drops the schemes it touches first, so the suite is repeatable and order-independent —
/// which matters because Free and Pro fixtures share a database on every provider.
/// </para>
/// </summary>
public abstract class SchemeNamingTestsBase
{
    protected readonly IRedbService Redb;

    protected SchemeNamingTestsBase(IRedbService redb) => Redb = redb;

    // ============================================================
    // === helpers ===
    // ============================================================

    /// <summary>Removes schemes by name (structures first — they are FK-bound) and clears the cache.</summary>
    private async Task DropSchemesAsync(params string[] names)
    {
        foreach (var name in names)
        {
            await Redb.Context.ExecuteAsync(
                $"DELETE FROM _structures WHERE _id_scheme IN (SELECT _id FROM _schemes WHERE _name = '{name}')");
            await Redb.Context.ExecuteAsync($"DELETE FROM _schemes WHERE _name = '{name}'");
        }

        // Raw SQL is invisible to the metadata cache; without this a later lookup would answer from a
        // scheme that no longer exists.
        Redb.Cache.Clear();
    }

    private Task<long?> SchemeIdAsync(string name) =>
        Redb.Context.ExecuteScalarAsync<long?>($"SELECT _id FROM _schemes WHERE _name = '{name}'");

    private Task<string?> SchemeAliasAsync(string name) =>
        Redb.Context.ExecuteScalarAsync<string?>($"SELECT _alias FROM _schemes WHERE _name = '{name}'");

    /// <summary>Creates a scheme row directly, bypassing the library — the "other node" in race tests.</summary>
    private async Task<long> InsertSchemeRawAsync(string name, long type = RedbTypeIds.Class)
    {
        var id = await Redb.Context.NextObjectIdAsync();
        await Redb.Context.ExecuteAsync(
            $"INSERT INTO _schemes (_id, _name, _type) VALUES ({id}, '{name}', {type})");
        Redb.Cache.Clear();
        return id;
    }

    private static string FullNameOf<T>() => typeof(T).FullName!;

    // ============================================================
    // === A, B — no explicit name: legacy behaviour is untouched ===
    // ============================================================

    [Fact]
    public async Task A_NoExplicitName_SchemeLivesByFullName()
    {
        var fullName = FullNameOf<NamingByTypeProps>();
        await DropSchemesAsync(fullName, nameof(NamingByTypeProps));

        var scheme = await Redb.EnsureSchemeFromTypeAsync<NamingByTypeProps>();

        scheme.Name.Should().Be(fullName);
        (await SchemeIdAsync(fullName)).Should().NotBeNull();
        (await SchemeIdAsync(nameof(NamingByTypeProps))).Should().BeNull();
    }

    [Fact]
    public async Task B_NoExplicitName_MigratesFromShortName()
    {
        var fullName = FullNameOf<NamingByTypeProps>();
        var shortName = nameof(NamingByTypeProps);
        await DropSchemesAsync(fullName, shortName);

        var legacyId = await InsertSchemeRawAsync(shortName);

        var scheme = await Redb.EnsureSchemeFromTypeAsync<NamingByTypeProps>();

        scheme.Name.Should().Be(fullName);
        scheme.Id.Should().Be(legacyId, "renaming must preserve the id so objects stay attached");
        (await SchemeIdAsync(shortName)).Should().BeNull();
    }

    // ============================================================
    // === C, D, E — the three-step chain ===
    // ============================================================

    [Fact]
    public async Task C_ExplicitName_CreatedOnCleanDatabase()
    {
        await DropSchemesAsync("naming.pinned", FullNameOf<NamingPinnedProps>(), nameof(NamingPinnedProps));

        var scheme = await Redb.EnsureSchemeFromTypeAsync<NamingPinnedProps>();

        scheme.Name.Should().Be("naming.pinned");
        (await SchemeIdAsync(FullNameOf<NamingPinnedProps>())).Should().BeNull("FullName must not be used at all");
    }

    [Fact]
    public async Task D_ExplicitName_RenamesSchemeFoundByFullName()
    {
        var fullName = FullNameOf<NamingPinnedProps>();
        await DropSchemesAsync("naming.pinned", fullName, nameof(NamingPinnedProps));

        var originalId = await InsertSchemeRawAsync(fullName);

        var scheme = await Redb.EnsureSchemeFromTypeAsync<NamingPinnedProps>();

        scheme.Name.Should().Be("naming.pinned");
        scheme.Id.Should().Be(originalId, "the rename is an UPDATE of _name, not a new scheme");
        (await SchemeIdAsync(fullName)).Should().BeNull();
    }

    [Fact]
    public async Task E_ExplicitName_RenamesSchemeFoundByShortName()
    {
        var shortName = nameof(NamingPinnedProps);
        await DropSchemesAsync("naming.pinned", FullNameOf<NamingPinnedProps>(), shortName);

        var originalId = await InsertSchemeRawAsync(shortName);

        var scheme = await Redb.EnsureSchemeFromTypeAsync<NamingPinnedProps>();

        scheme.Name.Should().Be("naming.pinned");
        scheme.Id.Should().Be(originalId, "the third link of the chain must not orphan pre-FullName databases");
        (await SchemeIdAsync(shortName)).Should().BeNull();
    }

    // ============================================================
    // === F, G, H — failing loudly ===
    // ============================================================

    [Fact]
    public async Task F_SplitBrain_TargetAndPreviousNameBothExist_Throws()
    {
        var fullName = FullNameOf<NamingSplitProps>();
        await DropSchemesAsync("naming.split", fullName, nameof(NamingSplitProps));

        // The rename already happened, and then an older binary re-created the old scheme.
        await InsertSchemeRawAsync("naming.split");
        await InsertSchemeRawAsync(fullName);

        var act = async () => await Redb.EnsureSchemeFromTypeAsync<NamingSplitProps>();

        await act.Should().ThrowAsync<RedbSchemeNameTakenException>();

        // Both rows survive: the operator has to merge them, the library must not pick one silently.
        (await SchemeIdAsync("naming.split")).Should().NotBeNull();
        (await SchemeIdAsync(fullName)).Should().NotBeNull();

        await DropSchemesAsync("naming.split", fullName);
    }

    // Scenarios G (two types claiming one name) and H (invalid explicit name) are NOT integration
    // tests: they would require a type with a bad or duplicate name to exist in this assembly, and
    // InitializeAsync auto-syncs every [RedbScheme] type it can see and rethrows — so such a type
    // breaks fixture start-up for every other test here. That is the intended product behaviour.
    // The rules themselves are covered by Tests/Unit/SchemeNameValidatorTests.

    // ============================================================
    // === I, J — the database-level backstop ===
    // ============================================================

    [Theory]
    // Character-set rule — a single GLOB/LIKE test in every dialect.
    [InlineData("Raw Invalid Name")]
    [InlineData("Именование сырьём")]
    // Structural rules.
    [InlineData("raw..double")]
    [InlineData("raw.trailing.")]
    [InlineData("9raw")]
    // Reserved-word rule. This is the one that forced a recursive CTE in SQLite and STRING_SPLIT in
    // MSSql — it is the only case that actually splits the name into parts, so without it the
    // per-part machinery would never run and could be broken without anyone noticing.
    [InlineData("raw.class")]
    [InlineData("raw+static")]
    public async Task I_RawInsertOfInvalidName_RejectedByTrigger(string invalid)
    {
        await DropSchemesAsync(invalid);

        var act = async () => await InsertSchemeRawAsync(invalid);

        await act.Should().ThrowAsync<Exception>("the trigger is the backstop for writes that bypass the library");
        (await SchemeIdAsync(invalid)).Should().BeNull();
    }

    [Fact]
    public async Task J_SystemPrefixedName_BypassesValidation()
    {
        const string systemName = "@@naming_probe";
        await DropSchemesAsync(systemName);

        var id = await InsertSchemeRawAsync(systemName);

        id.Should().BeGreaterThan(0);
        (await SchemeIdAsync(systemName)).Should().Be(id);

        await DropSchemesAsync(systemName);
    }

    // ============================================================
    // === K, L — caches and RTTI survive the rename ===
    // ============================================================

    [Fact]
    public async Task K_AfterRename_LookupsUseTheNewNameOnly()
    {
        var fullName = FullNameOf<NamingPinnedProps>();
        await DropSchemesAsync("naming.pinned", fullName, nameof(NamingPinnedProps));
        await InsertSchemeRawAsync(fullName);

        var scheme = await Redb.EnsureSchemeFromTypeAsync<NamingPinnedProps>();

        (await Redb.GetSchemeByNameAsync("naming.pinned"))!.Id.Should().Be(scheme.Id);
        (await Redb.GetSchemeByNameAsync(fullName)).Should().BeNull("the old name must not resolve to a stale scheme");
        (await Redb.GetSchemeByTypeAsync<NamingPinnedProps>())!.Id.Should().Be(scheme.Id);
        Redb.GetSchemeNameForType<NamingPinnedProps>().Should().Be("naming.pinned");
    }

    [Fact]
    public async Task L_AfterRename_ObjectsRoundTrip()
    {
        await DropSchemesAsync("naming.pinned", FullNameOf<NamingPinnedProps>(), nameof(NamingPinnedProps));
        await Redb.SyncSchemeAsync<NamingPinnedProps>();

        var obj = new RedbObject<NamingPinnedProps>
        {
            name = "naming-roundtrip",
            Props = new NamingPinnedProps { Title = "after rename" }
        };
        obj.id = await Redb.SaveAsync(obj);

        var loaded = await Redb.LoadAsync<NamingPinnedProps>(obj.id);

        loaded.Should().NotBeNull();
        loaded!.Props.Title.Should().Be("after rename");
    }

    // ============================================================
    // === M, N — name plus alias, and idempotency ===
    // ============================================================

    [Fact]
    public async Task M_ExplicitNameAndAlias_BothPersisted()
    {
        await DropSchemesAsync("naming.with_alias", FullNameOf<NamingWithAliasProps>(), nameof(NamingWithAliasProps));

        var scheme = await Redb.EnsureSchemeFromTypeAsync<NamingWithAliasProps>();

        scheme.Name.Should().Be("naming.with_alias");
        (await SchemeAliasAsync("naming.with_alias")).Should().Be("Именование: имя и алиас");
    }

    [Fact]
    public async Task N_RepeatedSync_IsIdempotent()
    {
        await DropSchemesAsync("naming.pinned", FullNameOf<NamingPinnedProps>(), nameof(NamingPinnedProps));

        var first = await Redb.EnsureSchemeFromTypeAsync<NamingPinnedProps>();
        var second = await Redb.EnsureSchemeFromTypeAsync<NamingPinnedProps>();
        var third = await Redb.EnsureSchemeFromTypeAsync<NamingPinnedProps>();

        second.Id.Should().Be(first.Id);
        third.Id.Should().Be(first.Id);
        second.Name.Should().Be("naming.pinned");
    }

    // ============================================================
    // === O, P, R — the alias follows the attribute ===
    // ============================================================

    [Fact]
    public async Task O_AliasChangedInDatabase_IsRestoredFromAttribute()
    {
        var fullName = FullNameOf<NamingAliasProps>();
        await DropSchemesAsync(fullName, nameof(NamingAliasProps));

        var scheme = await Redb.EnsureSchemeFromTypeAsync<NamingAliasProps>();
        (await SchemeAliasAsync(fullName)).Should().Be("Именование: алиас исходный");

        // R: someone edits the alias by hand.
        await Redb.Context.ExecuteAsync($"UPDATE _schemes SET _alias = 'правка руками' WHERE _id = {scheme.Id}");
        Redb.Cache.Clear();

        await Redb.EnsureSchemeFromTypeAsync<NamingAliasProps>();

        (await SchemeAliasAsync(fullName)).Should().Be("Именование: алиас исходный",
            "the attribute is the source of truth, exactly as it already is for structure aliases");
    }

    [Fact]
    public async Task P_TypeWithoutAlias_ResetsAliasToNull()
    {
        await DropSchemesAsync("naming.pinned", FullNameOf<NamingPinnedProps>(), nameof(NamingPinnedProps));

        var scheme = await Redb.EnsureSchemeFromTypeAsync<NamingPinnedProps>();

        await Redb.Context.ExecuteAsync($"UPDATE _schemes SET _alias = 'лишний алиас' WHERE _id = {scheme.Id}");
        Redb.Cache.Clear();

        await Redb.EnsureSchemeFromTypeAsync<NamingPinnedProps>();

        (await SchemeAliasAsync("naming.pinned")).Should().BeNull(
            "a type that declares no alias must clear the column, not leave a ghost");
    }

    [Fact]
    public async Task Q_AliasStableAcrossRepeatedSyncs()
    {
        var fullName = FullNameOf<NamingAliasProps>();
        await DropSchemesAsync(fullName, nameof(NamingAliasProps));

        await Redb.EnsureSchemeFromTypeAsync<NamingAliasProps>();
        await Redb.EnsureSchemeFromTypeAsync<NamingAliasProps>();
        await Redb.EnsureSchemeFromTypeAsync<NamingAliasProps>();

        (await SchemeAliasAsync(fullName)).Should().Be("Именование: алиас исходный");
    }

    // ============================================================
    // === S, T, U — the multi-node creation race ===
    // ============================================================

    [Fact]
    public async Task S_LosingTheCreationRace_AdoptsTheWinnersScheme()
    {
        await DropSchemesAsync("naming.race", FullNameOf<NamingRaceProps>(), nameof(NamingRaceProps));

        // Another node got there first: the row exists, and our lookups have already missed it.
        var winnerId = await InsertSchemeRawAsync("naming.race");

        var scheme = await Redb.EnsureSchemeFromTypeAsync<NamingRaceProps>();

        scheme.Id.Should().Be(winnerId, "the loser must adopt the existing scheme, not fail on UNIQUE(_name)");
        (await Redb.Context.ExecuteScalarAsync<long?>(
            "SELECT COUNT(*) FROM _schemes WHERE _name = 'naming.race'")).Should().Be(1);
    }

    [Fact]
    public async Task T_LosingTheCreationRace_InsideTransaction_TransactionStillCommits()
    {
        await DropSchemesAsync("naming.race", FullNameOf<NamingRaceProps>(), nameof(NamingRaceProps));

        var winnerId = await InsertSchemeRawAsync("naming.race");

        // The point of the test: on PostgreSQL a raised unique violation would poison the transaction
        // and every following statement would fail with 25P02. Suppressing the conflict in SQL instead
        // of catching it is what keeps this commit possible.
        long adoptedId = 0;
        var act = async () => await Redb.Context.ExecuteAtomicAsync(async () =>
        {
            var scheme = await Redb.EnsureSchemeFromTypeAsync<NamingRaceProps>();
            adoptedId = scheme.Id;
        });

        await act.Should().NotThrowAsync();
        adoptedId.Should().Be(winnerId);
    }

    [Fact]
    public async Task U_ObjectSchemePath_HasTheSameProtection()
    {
        const string objectSchemeName = "naming_object_race";
        await DropSchemesAsync(objectSchemeName);

        var winnerId = await InsertSchemeRawAsync(objectSchemeName, RedbTypeIds.Object);

        var scheme = await Redb.EnsureObjectSchemeAsync(objectSchemeName);

        scheme.Id.Should().Be(winnerId, "the untyped path must not be a second door into the same bug");
        (await Redb.Context.ExecuteScalarAsync<long?>(
            $"SELECT COUNT(*) FROM _schemes WHERE _name = '{objectSchemeName}'")).Should().Be(1);

        await DropSchemesAsync(objectSchemeName);
    }
}
