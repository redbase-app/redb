using redb.Core.Data;
using redb.Core.Models.Contracts;
using redb.Core.Providers.Base;
using redb.SQLite.Sql;
using Microsoft.Extensions.Logging;

namespace redb.SQLite.Providers;

/// <summary>
/// SQLite implementation of role provider.
/// Inherits all business logic from RoleProviderBase.
/// Only provides SQLite-specific SQL dialect.
/// 
/// Usage:
/// services.AddScoped&lt;IRoleProvider, SqliteRoleProvider&gt;();
/// </summary>
public class SqliteRoleProvider : RoleProviderBase
{
    /// <summary>
    /// Creates a new SQLite role provider.
    /// </summary>
    /// <param name="context">Database context for executing queries</param>
    /// <param name="securityContext">Security context for authorization</param>
    /// <param name="logger">Optional logger for diagnostics</param>
    public SqliteRoleProvider(
        IRedbContext context, 
        IRedbSecurityContext securityContext,
        ILogger? logger = null)
        : base(context, securityContext, new SqliteDialect(), logger)
    {
    }

    // All business logic is inherited from RoleProviderBase.
    // SQLite-specific SQL is provided by SqliteDialect.
    
    // Override virtual methods here for SQLite-specific optimizations if needed.
    // For example, using RETURNING clause for insert operations.
}
