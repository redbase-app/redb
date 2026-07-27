using Microsoft.Extensions.DependencyInjection;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Core.Pro.Migration;
using redb.Core.Pro.Query;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Pro data-migration suite (ComputedFrom): apply, idempotency, dry-run and history.
/// <para>
/// Written because this feature had no test coverage on any provider, which is why a real defect
/// went unnoticed: on SQLite the <c>_migrations</c> history table was never created (PG/MSSql get it
/// from the concatenated redb_init.sql, SQLite has no such concatenation), so the executor's very
/// first statement — the idempotency probe — failed with "no such table". See
/// docs/SQLITE_PARITY_PLAN.md §1.
/// </para>
/// </summary>
public abstract class MigrationTestsBase
{
    protected readonly IRedbService Redb;
    private readonly ISqlDialectPro _dialect;

    protected MigrationTestsBase(IRedbService redb, IServiceProvider services)
    {
        Redb = redb;
        _dialect = services.GetRequiredService<ISqlDialectPro>();
    }

    /// <summary>
    /// Clears the migration history for this scheme so each test starts from a known state
    /// (the executor is idempotent by design — a leftover row would make it skip).
    /// </summary>
    private async Task<long> ResetAsync()
    {
        var scheme = await Redb.SyncSchemeAsync<MigrationProbeProps>();

        await Redb.Context.ExecuteAsync($"DELETE FROM _migrations WHERE _scheme_id = {scheme.Id}");
        await Redb.Context.ExecuteAsync(
            $"DELETE FROM _values WHERE _id_object IN (SELECT _id FROM _objects WHERE _id_scheme = {scheme.Id})");
        await Redb.Context.ExecuteAsync($"DELETE FROM _objects WHERE _id_scheme = {scheme.Id}");

        return scheme.Id;
    }

    private async Task<long> SeedAsync(long quantity, double price)
    {
        var obj = new RedbObject<MigrationProbeProps>
        {
            name = $"migration-probe-{quantity}x{price}",
            Props = new MigrationProbeProps { Quantity = quantity, Price = price, TotalPrice = 0 }
        };

        obj.id = await Redb.SaveAsync(obj);
        return obj.id;
    }

    private Task<long?> HistoryCountAsync(long schemeId) =>
        Redb.Context.ExecuteScalarAsync<long?>(
            $"SELECT COUNT(*) FROM _migrations WHERE _scheme_id = {schemeId}");

    [Fact]
    public async Task Migrate_ComputedFrom_RecalculatesTheField()
    {
        await ResetAsync();
        var id = await SeedAsync(quantity: 3, price: 10.5);

        var results = await Redb.MigrateAsync<MigrationProbeProps, MigrationProbeMigration>(_dialect);

        results.Should().NotBeEmpty();
        results.Should().OnlyContain(r => r.Success, "a failing migration reports Success=false with Error set");

        var loaded = await Redb.LoadAsync<MigrationProbeProps>(id);
        loaded!.Props.TotalPrice.Should().BeApproximately(31.5, 0.0001);
    }

    [Fact]
    public async Task Migrate_WritesHistoryRow()
    {
        var schemeId = await ResetAsync();
        await SeedAsync(quantity: 2, price: 4);

        (await HistoryCountAsync(schemeId)).Should().Be(0);

        await Redb.MigrateAsync<MigrationProbeProps, MigrationProbeMigration>(_dialect, appliedBy: "tests");

        (await HistoryCountAsync(schemeId)).Should().Be(1, "applying a migration must record it in _migrations");
    }

    [Fact]
    public async Task Migrate_Twice_IsIdempotent()
    {
        var schemeId = await ResetAsync();
        await SeedAsync(quantity: 5, price: 2);

        await Redb.MigrateAsync<MigrationProbeProps, MigrationProbeMigration>(_dialect);
        var second = await Redb.MigrateAsync<MigrationProbeProps, MigrationProbeMigration>(_dialect);

        second.Should().OnlyContain(r => r.Success);
        second.Should().Contain(r => r.Skipped, "the history row must make the second run a no-op");
        (await HistoryCountAsync(schemeId)).Should().Be(1, "a skipped run must not add a second row");
    }

    [Fact]
    public async Task Migrate_DryRun_ChangesNothing()
    {
        var schemeId = await ResetAsync();
        var id = await SeedAsync(quantity: 7, price: 3);

        var results = await Redb.MigrateAsync<MigrationProbeProps, MigrationProbeMigration>(_dialect, dryRun: true);

        results.Should().OnlyContain(r => r.Success);

        var loaded = await Redb.LoadAsync<MigrationProbeProps>(id);
        loaded!.Props.TotalPrice.Should().Be(0, "dry-run must not touch the data");
        (await HistoryCountAsync(schemeId)).Should().Be(0, "dry-run must not record an applied migration");
    }
}
