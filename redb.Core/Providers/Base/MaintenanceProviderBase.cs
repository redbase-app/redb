using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using redb.Core.Data;
using redb.Core.Query;

namespace redb.Core.Providers.Base;

/// <summary>
/// The single implementation of <see cref="IMaintenanceProvider"/>: every provider-specific
/// difference lives in the SQL the dialect returns, normalized to ONE column-alias contract,
/// so the subclasses degenerate to wiring their dialect in (same shape as ValidationProvider).
/// </summary>
public abstract class MaintenanceProviderBase : IMaintenanceProvider
{
    protected IRedbContext Context { get; }
    protected ISqlDialect Sql { get; }
    protected ILogger? Logger { get; }

    /// <summary>SQLite's bounded-ANALYZE limit; other engines ignore it. See configuration.</summary>
    private readonly int _analysisLimit;

    protected MaintenanceProviderBase(
        IRedbContext context,
        ISqlDialect sql,
        int analysisLimit,
        ILogger? logger = null)
    {
        Context = context ?? throw new System.ArgumentNullException(nameof(context));
        Sql = sql ?? throw new System.ArgumentNullException(nameof(sql));
        _analysisLimit = analysisLimit;
        Logger = logger;
    }

    /// <inheritdoc />
    public virtual async Task AnalyzeAsync(CancellationToken cancellationToken = default)
    {
        var sql = Sql.Maintenance_Analyze(_analysisLimit);
        Logger?.LogInformation("REDB maintenance: refreshing planner statistics ({Sql})", sql);
        await Context.ExecuteAsync(sql, System.Array.Empty<object>(), cancellationToken);
    }

    /// <inheritdoc />
    public virtual async Task AnalyzeTableAsync(string table, string? schema = null, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(table))
            throw new ArgumentException("Table name is required.", nameof(table));

        // An identifier cannot travel as a parameter, and this one comes from an operator's
        // screen: it is checked against the catalogs first, and the dialect quotes what survives.
        var exists = await Context.ExecuteScalarAsync<long>(
            Sql.Maintenance_SelectTableExists(),
            new object[] { table, (object?)schema ?? DBNull.Value },
            cancellationToken);
        if (exists <= 0)
            throw new InvalidOperationException(
                $"Table '{(schema == null ? table : schema + "." + table)}' was not found in this database, " +
                "so its planner statistics were not refreshed. Names come from GetTableStatsAsync().");

        var sql = Sql.Maintenance_AnalyzeTable(schema, table, _analysisLimit);
        Logger?.LogInformation("REDB maintenance: refreshing planner statistics of one table ({Sql})", sql);
        await Context.ExecuteAsync(sql, System.Array.Empty<object>(), cancellationToken);
    }

    /// <inheritdoc />
    public virtual async Task<IReadOnlyList<IndexStatistics>> GetIndexStatsAsync(CancellationToken cancellationToken = default)
    {
        // The dialect SQL normalizes each engine's catalogs to one alias contract, so this is
        // the only mapping in existence. The single allowed branch: SQLite without the dbstat
        // virtual table compiled in cannot report sizes - fall back to the size-less form.
        List<IndexStatsRow> rows;
        try
        {
            rows = await Context.QueryAsync<IndexStatsRow>(
                Sql.Maintenance_SelectIndexStats(), System.Array.Empty<object>(), cancellationToken);
        }
        catch (System.Exception ex) when (Sql.Maintenance_SelectIndexStatsNoSize() is { } fallback
                                          && fallback != Sql.Maintenance_SelectIndexStats())
        {
            Logger?.LogDebug(ex,
                "REDB maintenance: sized index-stats query failed (dbstat not compiled in?); " +
                "falling back to the size-less form.");
            rows = await Context.QueryAsync<IndexStatsRow>(
                fallback, System.Array.Empty<object>(), cancellationToken);
        }

        return rows.Select(ToStatistics).ToList();
    }

    /// <inheritdoc />
    public virtual async Task<IReadOnlyList<TableStatistics>> GetTableStatsAsync(CancellationToken cancellationToken = default)
    {
        // The optional parts of a SQLite database decide how much of this question can be answered:
        // dbstat may not be compiled in, and sqlite_stat1 does not exist until the first ANALYZE -
        // a statement naming a missing table does not even prepare. The dialect lists the forms in
        // decreasing capability, and the first one that runs wins; the last failure is rethrown.
        var forms = Sql.Maintenance_SelectTableStatsForms();
        List<TableStatistics>? rows = null;
        for (var i = 0; i < forms.Count; i++)
        {
            try
            {
                rows = await Context.QueryAsync<TableStatistics>(
                    forms[i], System.Array.Empty<object>(), cancellationToken);
                break;
            }
            catch (System.Exception ex) when (i < forms.Count - 1)
            {
                Logger?.LogDebug(ex,
                    "REDB maintenance: table-stats form {Form} of {Count} did not run on this database " +
                    "(an optional catalog is missing); trying the next, less detailed one.",
                    i + 1, forms.Count);
            }
        }

        foreach (var row in rows!)
            row.IsRedbOwned = IsRedbTable(row.Table);
        return rows;
    }

    /// <inheritdoc />
    public virtual async Task<StatisticsWindow> GetStatisticsWindowAsync(CancellationToken cancellationToken = default)
    {
        var rows = await Context.QueryAsync<StatisticsWindow>(
            Sql.Maintenance_SelectStatisticsWindow(), System.Array.Empty<object>(), cancellationToken);
        return rows.FirstOrDefault() ?? new StatisticsWindow();
    }

    // === WHAT MUST SURVIVE ===

    /// <summary>
    /// Tables of the redb type system. Their indexes are read on every scheme sync, every query
    /// compilation and every materialization, and some of them are read so rarely that a usage
    /// counter shows zero - which is exactly how a well-meant cleanup removes them.
    /// </summary>
    private static readonly HashSet<string> MetadataTables = new(StringComparer.OrdinalIgnoreCase)
    {
        "_types", "_schemes", "_structures", "_scheme_metadata_cache", "_dependencies",
        "_functions", "_lists", "_links", "_migrations"
    };

    /// <summary>Tables redb checks permissions against.</summary>
    private static readonly HashSet<string> SecurityTables = new(StringComparer.OrdinalIgnoreCase)
    {
        "_users", "_roles", "_users_roles", "_permissions"
    };

    /// <summary>Tables holding redb data.</summary>
    private static readonly HashSet<string> DataTables = new(StringComparer.OrdinalIgnoreCase)
    {
        "_objects", "_values", "_list_items"
    };

    /// <summary>
    /// A table of the redb schema. By table name, never by index name: the schema carries both
    /// hand-named indexes (IX__values__Object_not_null) and constraint-born ones the engine names
    /// itself, and only the table is a reliable marker.
    /// </summary>
    private static bool IsRedbTable(string table) => table.StartsWith("_", StringComparison.Ordinal);

    private static IndexCriticality ReasonFor(IndexStatsRow row)
    {
        if (MetadataTables.Contains(row.Table)) return IndexCriticality.RedbMetadata;
        if (SecurityTables.Contains(row.Table)) return IndexCriticality.RedbSecurity;
        if (DataTables.Contains(row.Table)) return IndexCriticality.RedbData;
        if (IsRedbTable(row.Table)) return IndexCriticality.RedbData;
        if (row.IsPrimaryKey == true) return IndexCriticality.PrimaryKey;
        if (row.IsUniqueConstraint == true || row.IsUnique == true) return IndexCriticality.Unique;
        if (row.BacksForeignKey == true) return IndexCriticality.ForeignKey;
        return IndexCriticality.None;
    }

    private static IndexStatistics ToStatistics(IndexStatsRow row)
    {
        var reason = ReasonFor(row);
        return new IndexStatistics
        {
            Schema = row.Schema,
            Table = row.Table,
            Name = row.Name,
            IsUnique = row.IsUnique,
            Columns = Split(row.ColumnsCsv),
            IncludedColumns = Split(row.IncludedColumnsCsv),
            IsPrimaryKey = row.IsPrimaryKey == true,
            IsUniqueConstraint = row.IsUniqueConstraint == true,
            IsClustered = row.IsClustered,
            BacksForeignKey = row.BacksForeignKey == true,
            IsRedbOwned = IsRedbTable(row.Table),
            IsSystemCritical = reason != IndexCriticality.None,
            CriticalReason = reason,
            SizeBytes = row.SizeBytes,
            EstimatedRows = row.EstimatedRows,
            Seeks = row.Seeks,
            Scans = row.Scans,
            Updates = row.Updates,
            LastUsed = row.LastUsed
        };
    }

    /// <summary>
    /// Column lists travel as one comma-separated value so the catalogs of three engines keep
    /// fitting one row per index; the list the caller reads is built here, once.
    /// </summary>
    private static IReadOnlyList<string> Split(string? csv)
        => string.IsNullOrEmpty(csv)
            ? Array.Empty<string>()
            : csv.Split(',').Select(p => p.Trim()).Where(p => p.Length > 0).ToArray();

    /// <summary>The row shape the dialect SQL produces; the public model is built from it.</summary>
    private sealed class IndexStatsRow
    {
        public string? Schema { get; set; }
        public string Table { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
        public bool? IsUnique { get; set; }
        public string? ColumnsCsv { get; set; }
        public string? IncludedColumnsCsv { get; set; }
        public bool? IsPrimaryKey { get; set; }
        public bool? IsUniqueConstraint { get; set; }
        public bool? IsClustered { get; set; }
        public bool? BacksForeignKey { get; set; }
        public long? SizeBytes { get; set; }
        public long? EstimatedRows { get; set; }
        public long? Seeks { get; set; }
        public long? Scans { get; set; }
        public long? Updates { get; set; }
        public DateTimeOffset? LastUsed { get; set; }
    }
}
