using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace redb.Core.Providers
{
    /// <summary>
    /// Storage maintenance: planner statistics and index health. Born out of the 2026-09-10
    /// incident where stale planner statistics after a bulk seed turned every plan into fiction
    /// (a 30ms tree query ran for a second), and index health was visible only by hand-written
    /// catalog queries per database.
    /// <para>
    /// Everything here is read-only except the two <c>AnalyzeAsync</c> overloads, and both are
    /// explicit calls. Reorganising the storage (VACUUM, REINDEX, resetting counters) is not
    /// offered on purpose (owner decision 2026-09-21): the facade reports what the database is
    /// doing so an operator can hand the findings to a DBA, and a button that locks a production
    /// table must not be one click away in a dashboard.
    /// </para>
    /// </summary>
    public interface IMaintenanceProvider
    {
        /// <summary>
        /// Refreshes the planner statistics of the database (PostgreSQL <c>ANALYZE</c>,
        /// MSSQL <c>sp_updatestats</c>, SQLite <c>PRAGMA analysis_limit; ANALYZE</c>).
        /// An EXPLICIT call by design: on a large database it can run for minutes, so it must
        /// never be a silent side effect of a save. Call it after bulk writes - a seed, an
        /// import, a migration - or every estimate the planner reads will be stale.
        /// </summary>
        Task AnalyzeAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Refreshes the planner statistics of ONE table (PostgreSQL <c>ANALYZE t</c>, MSSQL
        /// <c>UPDATE STATISTICS t</c>, SQLite <c>ANALYZE t</c> - every engine supports it), for
        /// the usual case where the operator knows the table that brought them here and a
        /// whole-database pass would run for minutes.
        /// </summary>
        /// <param name="table">Table name, unquoted, as the catalogs report it.</param>
        /// <param name="schema">Schema of the table; null means the engine's default
        /// (<c>public</c>, <c>dbo</c>) and is the only option on SQLite.</param>
        /// <exception cref="ArgumentException">The name is empty.</exception>
        /// <exception cref="InvalidOperationException">No such table in the database. The name is
        /// checked against the catalogs and quoted by the dialect before it reaches SQL: it comes
        /// from a dashboard, and an identifier cannot be a parameter.</exception>
        Task AnalyzeTableAsync(string table, string? schema = null, CancellationToken cancellationToken = default);

        /// <summary>
        /// Index statistics of the user tables, read from the database catalogs. Fields the
        /// engine cannot report are <c>null</c> - see each property; nothing is guessed.
        /// </summary>
        Task<IReadOnlyList<IndexStatistics>> GetIndexStatsAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Table statistics of the user tables: size, rows, the size of their indexes, and when
        /// the engine last refreshed statistics or reclaimed dead rows. The other half of the
        /// picture an operator needs - <see cref="GetIndexStatsAsync"/> says what the indexes are,
        /// this says what they sit on.
        /// </summary>
        Task<IReadOnlyList<TableStatistics>> GetTableStatsAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Since when the usage counters of <see cref="GetIndexStatsAsync"/> have been counting,
        /// and whether this node is a replica. Without it "this index served no read" says
        /// nothing: the counters may have been reset or the server restarted a minute ago, and on
        /// a PostgreSQL replica they count only this node's own reads.
        /// </summary>
        Task<StatisticsWindow> GetStatisticsWindowAsync(CancellationToken cancellationToken = default);
    }

    /// <summary>
    /// One index as the database catalog sees it. Honestly nullable: each provider fills what
    /// its engine can report (PostgreSQL and MSSQL carry usage counters, SQLite does not;
    /// MSSQL usage counters reset on server restart; <see cref="LastUsed"/> needs PostgreSQL 16+).
    /// </summary>
    public class IndexStatistics
    {
        /// <summary>Schema of the table; null on engines without schemas (SQLite).</summary>
        public string? Schema { get; set; }

        /// <summary>Table the index belongs to.</summary>
        public string Table { get; set; } = string.Empty;

        /// <summary>Index name.</summary>
        public string Name { get; set; } = string.Empty;

        /// <summary>Whether the index is unique; null when the catalog does not say.</summary>
        public bool? IsUnique { get; set; }

        /// <summary>Key columns in index order. Empty only when the engine could not report them.</summary>
        public IReadOnlyList<string> Columns { get; set; } = Array.Empty<string>();

        /// <summary>
        /// Columns carried but not keyed (MSSQL <c>INCLUDE</c>); empty elsewhere. Without this a
        /// covering index is indistinguishable from a composite one.
        /// </summary>
        public IReadOnlyList<string> IncludedColumns { get; set; } = Array.Empty<string>();

        /// <summary>The index backs a primary key.</summary>
        public bool IsPrimaryKey { get; set; }

        /// <summary>The index backs a unique constraint (as opposed to a plain unique index).</summary>
        public bool IsUniqueConstraint { get; set; }

        /// <summary>MSSQL clustered index - the table itself; null on engines without the notion.</summary>
        public bool? IsClustered { get; set; }

        /// <summary>The index serves a foreign key, so dropping it turns key checks into scans.</summary>
        public bool BacksForeignKey { get; set; }

        /// <summary>The index belongs to a table of the redb schema (a name starting with '_').</summary>
        public bool IsRedbOwned { get; set; }

        /// <summary>
        /// The index must not be dropped, whatever its usage counters say (owner decision
        /// 2026-09-21). True for every index of the redb schema and for any index that backs a
        /// primary key, a unique constraint or a foreign key. <see cref="CriticalReason"/> says which.
        /// </summary>
        public bool IsSystemCritical { get; set; }

        /// <summary>Why <see cref="IsSystemCritical"/> is set; <see cref="IndexCriticality.None"/> when it is not.</summary>
        public IndexCriticality CriticalReason { get; set; }

        /// <summary>On-disk size in bytes; null when unavailable (SQLite without dbstat).</summary>
        public long? SizeBytes { get; set; }

        /// <summary>Planner's row estimate for the index; null when unavailable.</summary>
        public long? EstimatedRows { get; set; }

        /// <summary>Point-lookup uses. PostgreSQL reports one combined scan counter here.</summary>
        public long? Seeks { get; set; }

        /// <summary>Range/full scans (MSSQL); null where the engine does not split the counter.</summary>
        public long? Scans { get; set; }

        /// <summary>Maintenance writes into the index (MSSQL); null elsewhere.</summary>
        public long? Updates { get; set; }

        /// <summary>Last time the index served a read; PostgreSQL 16+ and MSSQL only.</summary>
        public DateTimeOffset? LastUsed { get; set; }
    }

    /// <summary>Why an index must survive regardless of how often it is read.</summary>
    public enum IndexCriticality
    {
        /// <summary>Nothing structural: the usage counters are the whole story.</summary>
        None = 0,

        /// <summary>A table of the redb type system: schemes, structures, types, their cache, migrations.</summary>
        RedbMetadata = 1,

        /// <summary>A table redb checks permissions against: users, roles, permissions.</summary>
        RedbSecurity = 2,

        /// <summary>A table holding redb data: objects, values, list items.</summary>
        RedbData = 3,

        /// <summary>Backs a primary key.</summary>
        PrimaryKey = 4,

        /// <summary>Enforces uniqueness.</summary>
        Unique = 5,

        /// <summary>Serves a foreign key.</summary>
        ForeignKey = 6
    }

    /// <summary>
    /// One table as the database catalog sees it. Nullable in the same spirit as
    /// <see cref="IndexStatistics"/>: what the engine does not report stays null.
    /// </summary>
    public class TableStatistics
    {
        /// <summary>Schema of the table; null on engines without schemas (SQLite).</summary>
        public string? Schema { get; set; }

        /// <summary>Table name.</summary>
        public string Table { get; set; } = string.Empty;

        /// <summary>The table belongs to the redb schema (a name starting with '_').</summary>
        public bool IsRedbOwned { get; set; }

        /// <summary>Planner's row estimate; null when unavailable.</summary>
        public long? EstimatedRows { get; set; }

        /// <summary>Size of the table's own data in bytes; null when unavailable.</summary>
        public long? DataSizeBytes { get; set; }

        /// <summary>Total size of the table's indexes in bytes; null when unavailable.</summary>
        public long? IndexesSizeBytes { get; set; }

        /// <summary>Rows dead but not yet reclaimed (PostgreSQL); null elsewhere.</summary>
        public long? DeadRows { get; set; }

        /// <summary>When statistics of this table were last refreshed by an explicit request; null when the engine does not record it.</summary>
        public DateTimeOffset? LastAnalyze { get; set; }

        /// <summary>When the engine refreshed them on its own (PostgreSQL autoanalyze); null elsewhere.</summary>
        public DateTimeOffset? LastAutoAnalyze { get; set; }

        /// <summary>When dead rows were last reclaimed (PostgreSQL vacuum); null elsewhere.</summary>
        public DateTimeOffset? LastVacuum { get; set; }

        /// <summary>
        /// Whether the engine holds any statistics for this table at all. SQLite records no
        /// dates, so this is all it can say: ANALYZE has run at least once, or never.
        /// </summary>
        public bool? HasStatistics { get; set; }
    }

    /// <summary>
    /// The window the usage counters of <see cref="IMaintenanceProvider.GetIndexStatsAsync"/>
    /// cover, and whether this node is a replica.
    /// </summary>
    public class StatisticsWindow
    {
        /// <summary>
        /// Since when the counters have been counting: the last statistics reset (PostgreSQL) or
        /// the start of the instance (MSSQL). Null where the engine keeps no counters (SQLite) or
        /// has never reset them.
        /// </summary>
        public DateTimeOffset? CountersSince { get; set; }

        /// <summary>
        /// The node is a read-only replica (PostgreSQL in recovery). Its counters describe this
        /// node alone, so an index unused here may be busy on the primary. Null where the notion
        /// does not apply.
        /// </summary>
        public bool? IsReplica { get; set; }
    }
}
