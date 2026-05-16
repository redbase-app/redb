using redb.Core.Data;
using redb.Core.Models.Contracts;
using redb.Core.Providers.Base;
using redb.Postgres.Sql;
using Microsoft.Extensions.Logging;

namespace redb.Postgres.Providers;

/// <summary>
/// PostgreSQL implementation of role provider.
/// Inherits all business logic from RoleProviderBase.
/// Only provides PostgreSQL-specific SQL dialect.
/// 
/// Usage:
/// services.AddScoped&lt;IRoleProvider, PostgresRoleProvider&gt;();
/// </summary>
public class PostgresRoleProvider : RoleProviderBase
{
    /// <summary>
    /// Creates a new PostgreSQL role provider.
    /// </summary>
    /// <param name="context">Database context for executing queries</param>
    /// <param name="securityContext">Security context for authorization</param>
    /// <param name="logger">Optional logger for diagnostics</param>
    public PostgresRoleProvider(
        IRedbContext context, 
        IRedbSecurityContext securityContext,
        ILogger? logger = null)
        : base(context, securityContext, new PostgreSqlDialect(), logger)
    {
    }

    // All business logic is inherited from RoleProviderBase.
    // PostgreSQL-specific SQL is provided by PostgreSqlDialect.
    
    // Override virtual methods here for PostgreSQL-specific optimizations if needed.
    // For example, using RETURNING clause for insert operations.
}
