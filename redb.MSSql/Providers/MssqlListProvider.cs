using redb.Core.Data;
using redb.Core.Models.Configuration;
using redb.Core.Providers;
using redb.Core.Providers.Base;
using redb.MSSql.Query;
using Microsoft.Extensions.Logging;
using redb.MSSql.Sql;

namespace redb.MSSql.Providers;

/// <summary>
/// MS SQL Server implementation of IListProvider.
/// Inherits all logic from ListProviderBase, provides MSSQL-specific SQL via MsSqlDialect.
/// 
/// Usage:
/// services.AddScoped&lt;IListProvider, MssqlListProvider&gt;();
/// </summary>
public class MssqlListProvider : ListProviderBase
{
    /// <summary>
    /// Creates MSSQL list provider with default MsSqlDialect.
    /// </summary>
    /// <param name="context">Database context for executing queries</param>
    /// <param name="configuration">Service configuration</param>
    /// <param name="schemeSync">Scheme sync provider (for accessing domain-bound caches)</param>
    /// <param name="logger">Optional logger for diagnostics</param>
    public MssqlListProvider(
        IRedbContext context, 
        RedbServiceConfiguration configuration,
        ISchemeSyncProvider schemeSync,
        ILogger? logger = null)
        : base(context, configuration, new MsSqlDialect(), schemeSync, logger)
    {
    }
}

