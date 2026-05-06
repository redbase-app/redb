using redb.Core.Data;
using redb.Core.Models.Contracts;
using redb.Core.Providers.Base;
using redb.Core.Security;
using redb.Postgres.Sql;
using Microsoft.Extensions.Logging;

namespace redb.Postgres.Providers;

/// <summary>
/// PostgreSQL implementation of user provider.
/// Inherits all business logic from UserProviderBase.
/// Only provides PostgreSQL-specific SQL dialect and password hasher.
/// 
/// Usage:
/// services.AddScoped&lt;IUserProvider, PostgresUserProvider&gt;();
/// </summary>
public class PostgresUserProvider : UserProviderBase
{
    /// <summary>
    /// Creates a new PostgreSQL user provider.
    /// </summary>
    /// <param name="context">Database context for executing queries</param>
    /// <param name="securityContext">Security context for authorization</param>
    /// <param name="logger">Optional logger for diagnostics</param>
    public PostgresUserProvider(
        IRedbContext context, 
        IRedbSecurityContext securityContext,
        ILogger? logger = null)
        : base(context, securityContext, new PostgreSqlDialect(), new SimplePasswordHasher(), logger)
    {
    }

    /// <summary>
    /// Creates a new PostgreSQL user provider with custom password hasher.
    /// </summary>
    /// <param name="context">Database context for executing queries</param>
    /// <param name="securityContext">Security context for authorization</param>
    /// <param name="passwordHasher">Custom password hasher implementation</param>
    /// <param name="logger">Optional logger for diagnostics</param>
    public PostgresUserProvider(
        IRedbContext context, 
        IRedbSecurityContext securityContext, 
        IPasswordHasher passwordHasher,
        ILogger? logger = null)
        : base(context, securityContext, new PostgreSqlDialect(), passwordHasher, logger)
    {
    }

    // All business logic is inherited from UserProviderBase.
    // PostgreSQL-specific SQL is provided by PostgreSqlDialect.
    // Password hashing is provided by SimplePasswordHasher (or custom implementation).
}
