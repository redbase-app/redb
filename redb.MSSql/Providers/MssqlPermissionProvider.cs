using redb.Core.Data;
using redb.Core.Models.Contracts;
using redb.Core.Providers.Base;
using redb.MSSql.Query;
using Microsoft.Extensions.Logging;
using redb.MSSql.Sql;

namespace redb.MSSql.Providers;

/// <summary>
/// MS SQL Server implementation of permission provider.
/// Inherits all business logic from PermissionProviderBase.
/// Only provides MSSQL-specific SQL dialect.
/// 
/// Usage:
/// services.AddScoped&lt;IPermissionProvider, MssqlPermissionProvider&gt;();
/// </summary>
public class MssqlPermissionProvider : PermissionProviderBase
{
    /// <summary>
    /// Creates a new MSSQL permission provider.
    /// </summary>
    /// <param name="context">Database context for executing queries</param>
    /// <param name="securityContext">Security context for authorization</param>
    /// <param name="logger">Optional logger for diagnostics</param>
    public MssqlPermissionProvider(
        IRedbContext context, 
        IRedbSecurityContext securityContext,
        ILogger? logger = null)
        : base(context, securityContext, new MsSqlDialect(), logger)
    {
    }
}

