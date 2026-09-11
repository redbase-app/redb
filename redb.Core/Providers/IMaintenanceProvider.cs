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
        /// Index statistics of the user tables, read from the database catalogs. Fields the
        /// engine cannot report are <c>null</c> - see each property; nothing is guessed.
        /// </summary>
        Task<IReadOnlyList<IndexStatistics>> GetIndexStatsAsync(CancellationToken cancellationToken = default);
    }

    /// <summary>
    /// One index as the database catalog sees it. Honestly nullable: each provider fills what
    /// its engine can report (PostgreSQL and MSSQL carry usage counters, SQLite does not;
    /// MSSQL usage counters reset on server restart; <see cref="LastUsed"/> needs PostgreSQL 16+).
    /// </summary>
    public class IndexStatistics
    {
        /// <summary>Table the index belongs to.</summary>
        public string Table { get; set; } = string.Empty;

        /// <summary>Index name.</summary>
        public string Name { get; set; } = string.Empty;

        /// <summary>Whether the index is unique; null when the catalog does not say.</summary>
        public bool? IsUnique { get; set; }

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
}
