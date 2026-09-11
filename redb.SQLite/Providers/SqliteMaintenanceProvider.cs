using redb.Core.Data;
using redb.Core.Providers.Base;
using redb.SQLite.Sql;
using Microsoft.Extensions.Logging;

namespace redb.SQLite.Providers
{
    /// <summary>
    /// SQLite maintenance provider. All logic lives in the base; this class only wires the
    /// dialect in (same shape as SqliteValidationProvider).
    /// </summary>
    public class SqliteMaintenanceProvider : MaintenanceProviderBase
    {
        public SqliteMaintenanceProvider(IRedbContext context, int analysisLimit, ILogger? logger = null)
            : base(context, new SqliteDialect(), analysisLimit, logger)
        {
        }
    }
}
