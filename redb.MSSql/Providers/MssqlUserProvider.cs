using redb.Core.Data;
using redb.Core.Models.Contracts;
using redb.Core.Providers.Base;
using redb.Core.Security;
using redb.MSSql.Query;
using Microsoft.Extensions.Logging;
using redb.MSSql.Sql;

namespace redb.MSSql.Providers;

/// <summary>
/// MS SQL Server implementation of user provider.
/// Inherits all business logic from UserProviderBase.
/// Only provides MSSQL-specific SQL dialect and password hasher.
/// 
/// Usage:
/// services.AddScoped&lt;IUserProvider, MssqlUserProvider&gt;();
/// </summary>
public class MssqlUserProvider : UserProviderBase
{
    /// <summary>
    /// Creates a new MSSQL user provider.
    /// </summary>
    /// <param name="context">Database context for executing queries</param>
    /// <param name="securityContext">Security context for authorization</param>
    /// <param name="logger">Optional logger for diagnostics</param>
    public MssqlUserProvider(
        IRedbContext context, 
        IRedbSecurityContext securityContext,
        ILogger? logger = null)
        : base(context, securityContext, new MsSqlDialect(), new SimplePasswordHasher(), logger)
    {
    }

    /// <summary>
    /// Creates a new MSSQL user provider with custom password hasher.
    /// </summary>
    /// <param name="context">Database context for executing queries</param>
    /// <param name="securityContext">Security context for authorization</param>
    /// <param name="passwordHasher">Custom password hasher implementation</param>
    /// <param name="logger">Optional logger for diagnostics</param>
    public MssqlUserProvider(
        IRedbContext context, 
        IRedbSecurityContext securityContext, 
        IPasswordHasher passwordHasher,
        ILogger? logger = null)
        : base(context, securityContext, new MsSqlDialect(), passwordHasher, logger)
    {
    }
}

