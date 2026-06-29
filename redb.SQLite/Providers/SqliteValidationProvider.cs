using redb.Core.Data;
using redb.Core.Providers.Base;
using redb.SQLite.Sql;
using Microsoft.Extensions.Logging;

namespace redb.SQLite.Providers
{
    /// <summary>
    /// SQLite implementation of validation provider.
    /// Inherits common logic from ValidationProviderBase.
    /// </summary>
    public class SqliteValidationProvider : ValidationProviderBase
    {
        /// <summary>
        /// Creates SQLite validation provider.
        /// </summary>
        /// <param name="context">Database context for executing queries</param>
        /// <param name="logger">Optional logger for diagnostics</param>
        public SqliteValidationProvider(IRedbContext context, ILogger? logger = null)
            : base(context, new SqliteDialect(), logger)
        {
        }
    }
}
