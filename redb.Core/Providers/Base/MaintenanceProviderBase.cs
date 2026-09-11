using System.Collections.Generic;
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
    public virtual async Task<IReadOnlyList<IndexStatistics>> GetIndexStatsAsync(CancellationToken cancellationToken = default)
    {
        // The dialect SQL normalizes each engine's catalogs to one alias contract, so this is
        // the only mapping in existence. The single allowed branch: SQLite without the dbstat
        // virtual table compiled in cannot report sizes - fall back to the size-less form.
        try
        {
            return await Context.QueryAsync<IndexStatistics>(
                Sql.Maintenance_SelectIndexStats(), System.Array.Empty<object>(), cancellationToken);
        }
        catch (System.Exception ex) when (Sql.Maintenance_SelectIndexStatsNoSize() is { } fallback
                                          && fallback != Sql.Maintenance_SelectIndexStats())
        {
            Logger?.LogDebug(ex,
                "REDB maintenance: sized index-stats query failed (dbstat not compiled in?); " +
                "falling back to the size-less form.");
            return await Context.QueryAsync<IndexStatistics>(
                fallback, System.Array.Empty<object>(), cancellationToken);
        }
    }
}
