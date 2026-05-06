using redb.Core.Data;
using redb.Core.Models.Configuration;
using redb.Core.Providers;
using redb.Core.Providers.Base;
using redb.Postgres.Sql;
using Microsoft.Extensions.Logging;

namespace redb.Postgres.Providers
{
    /// <summary>
    /// PostgreSQL implementation of IListProvider.
    /// Inherits all logic from ListProviderBase, provides PostgreSQL-specific SQL via PostgreSqlDialect.
    /// </summary>
    public class PostgresListProvider : ListProviderBase
    {
        /// <summary>
        /// Creates PostgreSQL list provider with default PostgreSqlDialect.
        /// </summary>
        /// <param name="context">Database context for executing queries</param>
        /// <param name="configuration">Service configuration</param>
        /// <param name="schemeSync">Scheme sync provider (for accessing domain-bound caches)</param>
        /// <param name="logger">Optional logger for diagnostics</param>
        public PostgresListProvider(
            IRedbContext context, 
            RedbServiceConfiguration configuration,
            ISchemeSyncProvider schemeSync,
            ILogger? logger = null)
            : base(context, configuration, new PostgreSqlDialect(), schemeSync, logger)
        {
        }
    }
}
