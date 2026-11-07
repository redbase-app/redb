using Microsoft.Extensions.DependencyInjection;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Core.Pro.Migration;
using redb.Core.Pro.Query;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Establishes whether the Pro migration DSL can ADDRESS a nested-class member or an array-element
/// member at all, and records what happens when it is asked to.
/// <para>
/// Review finding PR-32 says the compiled migration SQL carries no <c>_array_index</c> and no
/// <c>_parent_value_id</c>, so a target inside a collection would either fail or write one value to
/// every row of that structure. That finding matters only if such a target is reachable from user
/// code; this suite answers that first, on each provider, without touching any existing test.
/// </para>
/// </summary>
public abstract class MigrationTargetAddressingTestsBase
{
    protected readonly IRedbService Redb;
    private readonly ISqlDialectPro _dialect;

    protected MigrationTargetAddressingTestsBase(IRedbService redb, IServiceProvider services)
    {
        Redb = redb;
        _dialect = services.GetRequiredService<ISqlDialectPro>();
    }

    /// <summary>Known state for each test: the executor is idempotent, so a leftover history row would skip.</summary>
    private async Task<long> ResetAsync()
    {
        var scheme = await Redb.SyncSchemeAsync<MigrationTargetProbeProps>();

        await Redb.Context.ExecuteAsync($"DELETE FROM _migrations WHERE _scheme_id = {scheme.Id}");
        await Redb.Context.ExecuteAsync(
            $"DELETE FROM _values WHERE _id_object IN (SELECT _id FROM _objects WHERE _id_scheme = {scheme.Id})");
        await Redb.Context.ExecuteAsync($"DELETE FROM _objects WHERE _id_scheme = {scheme.Id}");

        return scheme.Id;
    }

    /// <summary>One object carrying BOTH shapes: a nested class member and a one-element array of the same class.</summary>
    private async Task<long> SeedAsync()
    {
        var obj = new RedbObject<MigrationTargetProbeProps>
        {
            name = "migration-target-probe",
            Props = new MigrationTargetProbeProps
            {
                Base = 4,
                Multiplier = 2.5,
                GrandTotal = 0,
                Details = new LineTargetProps { Qty = 3, Price = 7, LineTotal = 0 },
                Items = new List<LineTargetProps> { new() { Qty = 5, Price = 2, LineTotal = 0 } }
            }
        };

        obj.id = await Redb.SaveAsync(obj);
        return obj.id;
    }

    private static string Describe(IEnumerable<MigrationResult> results) =>
        string.Join(" | ", results.Select(r =>
            $"'{r.PropertyName}': Success={r.Success}, Skipped={r.Skipped}, Error={r.Error ?? "<none>"}"));

    [Fact]
    public async Task Target_RootFields_AreAddressedAndWritten()
    {
        await ResetAsync();
        var id = await SeedAsync();

        var results = await Redb.MigrateAsync<MigrationTargetProbeProps, MigrationTargetRootMigration>(_dialect);

        results.Should().OnlyContain(r => r.Success, $"the root-field shape is the control; observed: {Describe(results)}");

        var loaded = await Redb.LoadAsync<MigrationTargetProbeProps>(id);
        loaded!.Props.GrandTotal.Should().BeApproximately(10, 0.0001, "4 * 2.5");
    }

    [Fact]
    public async Task NestedAndArrayValues_ArePersisted_SoTheTargetShapeIsReal()
    {
        await ResetAsync();
        var id = await SeedAsync();

        var loaded = await Redb.LoadAsync<MigrationTargetProbeProps>(id);

        loaded!.Props.Details!.LineTotal.Should().Be(0, "seed value, proves the nested member round-trips");
        loaded.Props.Details.Qty.Should().Be(3);
        loaded.Props.Items.Should().ContainSingle();
        loaded.Props.Items[0].Qty.Should().Be(5);
        loaded.Props.Items[0].Price.Should().Be(2);
    }

    [Fact]
    public async Task Target_NestedClassField_CurrentBehaviour()
    {
        await ResetAsync();
        var id = await SeedAsync();

        var results = await Redb.MigrateAsync<MigrationTargetProbeProps, MigrationTargetNestedMigration>(_dialect);

        // Characterisation of today's behaviour: if the DSL flattens a dotted path to its leaf name,
        // the resolver cannot find a ROOT field called LineTotal and the migration must refuse loudly.
        // A green run here means refusal; a red run prints what actually happened in its message.
        results.Should().OnlyContain(r => !r.Success,
            $"expected a loud refusal for a nested target if paths are flattened; observed: {Describe(results)}");

        var loaded = await Redb.LoadAsync<MigrationTargetProbeProps>(id);
        loaded!.Props.Details!.LineTotal.Should().Be(0, "a refused migration must not have written anything");
    }

    [Fact]
    public async Task Target_ArrayElementField_CurrentBehaviour()
    {
        await ResetAsync();
        var id = await SeedAsync();

        var results = await Redb.MigrateAsync<MigrationTargetProbeProps, MigrationTargetArrayMigration>(_dialect);

        results.Should().OnlyContain(r => !r.Success,
            $"expected a loud refusal for an array-element target if paths are flattened; observed: {Describe(results)}");

        var loaded = await Redb.LoadAsync<MigrationTargetProbeProps>(id);
        loaded!.Props.Items[0].LineTotal.Should().Be(0, "a refused migration must not have written anything");
    }
}
