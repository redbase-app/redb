using redb.Core.Data;
using redb.Core.Models.Contracts;
using redb.Core.Providers.Base;
using redb.MSSql.Query;
using Microsoft.Extensions.Logging;
using redb.MSSql.Sql;

namespace redb.MSSql.Providers;

/// <summary>
/// MS SQL Server implementation of role provider.
/// Inherits all business logic from RoleProviderBase.
/// Only provides MSSQL-specific SQL dialect.
/// 
/// Usage:
/// services.AddScoped&lt;IRoleProvider, MssqlRoleProvider&gt;();
/// </summary>
public class MssqlRoleProvider : RoleProviderBase
{
    /// <summary>
    /// Creates a new MSSQL role provider.
    /// </summary>
    /// <param name="context">Database context for executing queries</param>
    /// <param name="securityContext">Security context for authorization</param>
    /// <param name="logger">Optional logger for diagnostics</param>
    public MssqlRoleProvider(
        IRedbContext context, 
        IRedbSecurityContext securityContext,
        ILogger? logger = null)
        : base(context, securityContext, new MsSqlDialect(), logger)
    {
    }
}

