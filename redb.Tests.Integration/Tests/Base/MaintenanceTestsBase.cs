using redb.Core;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// The maintenance facade (V4, docs/V4/MAINTENANCE_FACADE_PLAN.md): one base implementation,
/// all provider variability in three dialect SQL texts normalized to one alias contract.
/// <c>AnalyzeAsync</c> refreshes planner statistics; <c>GetIndexStatsAsync</c> reads the
/// engine's catalogs. Fields an engine cannot report are null - the pins below assert only
/// what every provider guarantees, plus per-suite capability flags.
/// </summary>
public abstract class MaintenanceTestsBase
{
    protected readonly IRedbService Redb;

    protected MaintenanceTestsBase(IRedbService redb) => Redb = redb;

    /// <summary>Whether the engine reports on-disk index sizes (SQLite only via dbstat).</summary>
    protected virtual bool ReportsSizes => true;

    [Fact]
    public async Task Analyze_Succeeds()
    {
        // The whole point of the facade: one call, no provider-specific incantations. On a
        // test-sized database every engine completes in well under a second.
        await Redb.Maintenance.AnalyzeAsync();
    }

    [Fact]
    public async Task IndexStats_SeeTheRedbSchema()
    {
        var stats = await Redb.Maintenance.GetIndexStatsAsync();

        stats.Should().NotBeEmpty("the redb schema alone carries dozens of indexes");
        stats.Should().OnlyContain(s => s.Table.Length > 0 && s.Name.Length > 0,
            "the alias contract guarantees table and index names on every engine");

        // A schema-delivery cross-check for free: the FK-column indexes shipped by the
        // versioned upgrade must be visible to the stats surface.
        stats.Should().Contain(s => s.Name == "IX__values__ListItem_not_null" && s.Table == "_values");
        stats.Should().Contain(s => s.Name == "IX__values__Object_not_null" && s.Table == "_values");

        if (ReportsSizes)
        {
            stats.Should().Contain(s => s.SizeBytes > 0,
                "engines that report sizes must show at least one non-empty index");
        }
    }
}
