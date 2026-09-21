using redb.Core;
using redb.Core.Models.Entities;
using redb.Core.Providers;
using redb.Tests.Integration.Models;

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

    /// <summary>Whether the engine has schemas at all (SQLite does not).</summary>
    protected virtual bool HasSchemas => true;

    /// <summary>Whether the engine keeps index usage counters (SQLite does not).</summary>
    protected virtual bool ReportsUsageCounters => true;

    /// <summary>Whether the engine records WHEN statistics were last refreshed (SQLite does not).</summary>
    protected virtual bool ReportsAnalyzeTime => true;

    /// <summary>Whether a primary key has an index object of its own (SQLite's INTEGER PRIMARY KEY is the rowid).</summary>
    protected virtual bool ReportsPrimaryKeyIndexes => true;

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

    [Fact]
    public async Task IndexStats_CarryTheSchemaOfTheirTable()
    {
        var stats = await Redb.Maintenance.GetIndexStatsAsync();

        if (HasSchemas)
        {
            // Without it public.orders and archive.orders are one indistinguishable row, and any
            // grouping by table lies (redb.Tsak request, 2026-09-21).
            stats.Should().OnlyContain(s => !string.IsNullOrEmpty(s.Schema),
                "an engine with schemas must say which one the table is in");
        }
        else
        {
            stats.Should().OnlyContain(s => s.Schema == null, "SQLite has no schemas: null, not an invented name");
        }
    }

    [Fact]
    public async Task IndexStats_ListTheColumnsOfEachIndex()
    {
        var stats = await Redb.Maintenance.GetIndexStatsAsync();

        // "Which fields is it on?" is the first question an operator asks, and duplicates and
        // overlaps cannot be computed from names alone.
        stats.Should().OnlyContain(s => s.Columns.Count > 0, "every engine can report index columns");

        var valuesObject = stats.First(s => s.Name == "IX__values__Object_not_null");
        valuesObject.Columns.Should().ContainSingle()
            .Which.Should().BeEquivalentTo("_Object", "the index is on that one column");
    }

    [Fact]
    public async Task EveryIndexOfTheRedbSchema_IsSystemCritical()
    {
        var stats = await Redb.Maintenance.GetIndexStatsAsync();

        // Owner decision 2026-09-21: an index of a redb table is never a drop candidate, whatever
        // its usage counters say - some of them serve uniqueness or a key check and are never
        // scanned. The reason travels along so a dashboard can explain the refusal.
        var redbIndexes = stats.Where(s => s.Table.StartsWith("_")).ToList();
        redbIndexes.Should().NotBeEmpty();
        redbIndexes.Should().OnlyContain(s => s.IsRedbOwned && s.IsSystemCritical);
        redbIndexes.Should().OnlyContain(s => s.CriticalReason != IndexCriticality.None);

        stats.Should().Contain(s => s.Table == "_structures" && s.CriticalReason == IndexCriticality.RedbMetadata);
        stats.Should().Contain(s => s.Table == "_permissions" && s.CriticalReason == IndexCriticality.RedbSecurity);
        stats.Should().Contain(s => s.Table == "_values" && s.CriticalReason == IndexCriticality.RedbData);
        if (ReportsPrimaryKeyIndexes)
            stats.Should().Contain(s => s.Table == "_objects" && s.IsPrimaryKey,
                "the primary key of the object table is reported as one");
    }

    [Fact]
    public async Task StatisticsWindow_SaysSinceWhenTheCountersCount()
    {
        var window = await Redb.Maintenance.GetStatisticsWindowAsync();

        if (ReportsUsageCounters)
        {
            // Zero uses means nothing without the window: the counters may have been reset or the
            // server restarted a minute ago.
            window.CountersSince.Should().NotBeNull();
            window.CountersSince!.Value.Should().BeBefore(DateTimeOffset.UtcNow.AddMinutes(1));
        }
        else
        {
            window.CountersSince.Should().BeNull("SQLite keeps no usage counters, so there is no window");
        }
    }

    [Fact]
    public async Task TableStats_ReportTheRedbTables()
    {
        var tables = await Redb.Maintenance.GetTableStatsAsync();

        tables.Should().NotBeEmpty();
        tables.Should().Contain(t => t.Table == "_objects" && t.IsRedbOwned);
        tables.Should().Contain(t => t.Table == "_values" && t.IsRedbOwned);
        if (HasSchemas)
            tables.Should().OnlyContain(t => !string.IsNullOrEmpty(t.Schema));
    }

    [Fact]
    public async Task AnalyzeTable_RefreshesThatTableAlone()
    {
        // An engine samples nothing on an empty table - SQL Server honestly keeps STATS_DATE null
        // there - so the table must hold a row before the refresh means anything.
        await Redb.SyncSchemeAsync<SimpleProps>();
        await Redb.SaveAsync(new RedbObject<SimpleProps>
        {
            name = "analyze-" + Guid.NewGuid().ToString("N")[..8],
            Props = new SimpleProps { Title = "analyze" }
        });

        // The operator usually knows the table that brought them here, and a whole-database pass
        // runs for minutes on a real database.
        await Redb.Maintenance.AnalyzeTableAsync("_objects");

        var tables = await Redb.Maintenance.GetTableStatsAsync();
        var objects = tables.First(t => t.Table == "_objects");

        if (ReportsAnalyzeTime)
            objects.LastAnalyze.Should().NotBeNull("the engine records when statistics were refreshed");
        else
            objects.HasStatistics.Should().BeTrue("SQLite has no date, only the fact that ANALYZE has run");
    }

    [Fact]
    public async Task AnalyzeTable_OfAnUnknownTable_IsRefused()
    {
        // The name comes from a dashboard and cannot be a query parameter, so it is checked
        // against the catalogs instead of being pasted into SQL.
        var analyzing = async () => await Redb.Maintenance.AnalyzeTableAsync("no_such_table_" + Guid.NewGuid().ToString("N")[..8]);

        await analyzing.Should().ThrowAsync<InvalidOperationException>();
    }
}
