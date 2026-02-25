using redb.Core.Data;
using redb.Core.Models.Configuration;
using redb.Core.Providers.Base;
using redb.MSSql.Query;
using Microsoft.Extensions.Logging;
using redb.MSSql.Sql;

namespace redb.MSSql.Providers;

/// <summary>
/// MS SQL Server implementation of scheme synchronization provider.
/// Inherits all business logic from SchemeSyncProviderBase.
/// Only provides MSSQL-specific SQL dialect.
/// 
/// Usage:
/// services.AddScoped&lt;ISchemeSyncProvider, MssqlSchemeSyncProvider&gt;();
/// </summary>
public class MssqlSchemeSyncProvider : SchemeSyncProviderBase
{
    /// <summary>
    /// Creates a new MSSQL scheme sync provider.
    /// </summary>
    /// <param name="context">Database context for executing queries</param>
    /// <param name="configuration">Service configuration</param>
    /// <param name="cacheDomain">Cache domain for isolation</param>
    /// <param name="logger">Optional logger for diagnostics</param>
    public MssqlSchemeSyncProvider(
        IRedbContext context, 
        RedbServiceConfiguration? configuration = null,
        string? cacheDomain = null,
        ILogger? logger = null)
        : base(context, new MsSqlDialect(), configuration, cacheDomain, logger)
    {
    }
}

