using redb.Core.Data;
using redb.Core.Providers.Base;
using redb.Postgres.Sql;
using Microsoft.Extensions.Logging;

namespace redb.Postgres.Providers
{
    /// <summary>
    /// PostgreSQL implementation of validation provider.
    /// Inherits common logic from ValidationProviderBase.
    /// </summary>
    public class PostgresValidationProvider : ValidationProviderBase
    {
        /// <summary>
        /// Creates PostgreSQL validation provider.
        /// </summary>
        /// <param name="context">Database context for executing queries</param>
        /// <param name="logger">Optional logger for diagnostics</param>
        public PostgresValidationProvider(IRedbContext context, ILogger? logger = null)
            : base(context, new PostgreSqlDialect(), logger)
        {
        }
    }
}
