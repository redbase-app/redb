using redb.Core.Data;
using redb.Core.Models.Configuration;
using redb.Core.Providers;
using redb.Core.Providers.Base;
using redb.SQLite.Sql;
using Microsoft.Extensions.Logging;

namespace redb.SQLite.Providers
{
    /// <summary>
    /// SQLite implementation of IListProvider.
    /// Inherits all logic from ListProviderBase, provides SQLite-specific SQL via SqliteDialect.
    /// </summary>
    public class SqliteListProvider : ListProviderBase
    {
        /// <summary>
        /// Creates SQLite list provider with default SqliteDialect.
        /// </summary>
        /// <param name="context">Database context for executing queries</param>
        /// <param name="configuration">Service configuration</param>
        /// <param name="schemeSync">Scheme sync provider (for accessing domain-bound caches)</param>
        /// <param name="logger">Optional logger for diagnostics</param>
        public SqliteListProvider(
            IRedbContext context, 
            RedbServiceConfiguration configuration,
            ISchemeSyncProvider schemeSync,
            ILogger? logger = null)
            : base(context, configuration, new SqliteDialect(), schemeSync, logger)
        {
        }
    }
}
