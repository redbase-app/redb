using redb.Core.Data;
using redb.Core.Providers;
using redb.Core.Providers.Base;
using redb.MSSql.Sql;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace redb.MSSql.Providers;

/// <summary>
/// MSSQL maintenance provider. All logic lives in the base; this class wires the dialect in
/// (same shape as MssqlValidationProvider) and handles the one server-level read.
/// </summary>
public class MssqlMaintenanceProvider : MaintenanceProviderBase
{
    public MssqlMaintenanceProvider(IRedbContext context, int analysisLimit, ILogger? logger = null)
        : base(context, new MsSqlDialect(), analysisLimit, logger)
    {
    }

    /// <summary>
    /// The instance start time comes from sys.dm_os_sys_info, which needs VIEW SERVER STATE
    /// (VIEW SERVER PERFORMANCE STATE on SQL Server 2022). A database owner without that right
    /// gets a window with <see cref="StatisticsWindow.CountersSince"/> = null: the counters are
    /// still read, only their age is unknown.
    /// </summary>
    public override async Task<StatisticsWindow> GetStatisticsWindowAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            return await base.GetStatisticsWindowAsync(cancellationToken);
        }
        catch (SqlException ex) when (IsMissingServerStateRight(ex))
        {
            Logger?.LogDebug(ex,
                "REDB maintenance: no VIEW SERVER STATE right, the start of the usage counters is unknown.");
            return new StatisticsWindow();
        }
    }

    /// <summary>297: the user does not have permission; 300: a named permission was denied.</summary>
    private static bool IsMissingServerStateRight(SqlException ex)
    {
        foreach (SqlError error in ex.Errors)
            if (error.Number is 297 or 300)
                return true;
        return false;
    }
}
