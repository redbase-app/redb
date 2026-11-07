using redb.Core.Data;
using redb.Core.Providers.Base;
using redb.Postgres.Sql;
using Microsoft.Extensions.Logging;

namespace redb.Postgres.Providers
{
    /// <summary>
    /// PostgreSQL maintenance provider. All logic lives in the base; this class only wires the
    /// dialect in (same shape as PostgresValidationProvider).
    /// </summary>
    public class PostgresMaintenanceProvider : MaintenanceProviderBase
    {
        public PostgresMaintenanceProvider(IRedbContext context, int analysisLimit, ILogger? logger = null)
            : base(context, new PostgreSqlDialect(), analysisLimit, logger)
        {
        }
    }
}
