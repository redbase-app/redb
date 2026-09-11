using redb.Core.Data;
using redb.Core.Providers.Base;
using redb.MSSql.Sql;
using Microsoft.Extensions.Logging;

namespace redb.MSSql.Providers;

/// <summary>
/// MSSQL maintenance provider. All logic lives in the base; this class only wires the dialect
/// in (same shape as MssqlValidationProvider).
/// </summary>
public class MssqlMaintenanceProvider : MaintenanceProviderBase
{
    public MssqlMaintenanceProvider(IRedbContext context, int analysisLimit, ILogger? logger = null)
        : base(context, new MsSqlDialect(), analysisLimit, logger)
    {
    }
}
