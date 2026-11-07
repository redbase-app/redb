using redb.Core.Data;
using redb.Core.Providers.Base;
using redb.MSSql.Query;
using Microsoft.Extensions.Logging;
using redb.MSSql.Sql;

namespace redb.MSSql.Providers;

/// <summary>
/// MS SQL Server implementation of validation provider.
/// Inherits all business logic from ValidationProviderBase.
/// Only provides MSSQL-specific SQL dialect.
/// 
/// Usage:
/// services.AddScoped&lt;IValidationProvider, MssqlValidationProvider&gt;();
/// </summary>
public class MssqlValidationProvider : ValidationProviderBase
{
    /// <summary>
    /// Creates a new MSSQL validation provider.
    /// </summary>
    /// <param name="context">Database context for executing queries</param>
    /// <param name="logger">Optional logger for diagnostics</param>
    public MssqlValidationProvider(
        IRedbContext context,
        ILogger? logger = null)
        : base(context, new MsSqlDialect(), logger)
    {
    }
}

