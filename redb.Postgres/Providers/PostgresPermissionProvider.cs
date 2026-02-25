using redb.Core.Data;
using redb.Core.Models.Contracts;
using redb.Core.Providers.Base;
using redb.Postgres.Sql;
using Microsoft.Extensions.Logging;

namespace redb.Postgres.Providers;

/// <summary>
/// PostgreSQL implementation of permission provider.
/// Inherits all business logic from PermissionProviderBase.
/// Only provides PostgreSQL-specific SQL dialect.
/// 
/// IMPORTANT: Uses PostgreSQL function get_user_permissions_for_object()
/// for efficient recursive permission calculation.
/// 
/// Usage:
/// services.AddScoped&lt;IPermissionProvider, PostgresPermissionProvider&gt;();
/// </summary>
public class PostgresPermissionProvider : PermissionProviderBase
{
    /// <summary>
    /// Creates a new PostgreSQL permission provider.
    /// </summary>
    /// <param name="context">Database context for executing queries</param>
    /// <param name="securityContext">Security context for authorization</param>
    /// <param name="logger">Optional logger for diagnostics</param>
    public PostgresPermissionProvider(
        IRedbContext context, 
        IRedbSecurityContext securityContext,
        ILogger? logger = null)
        : base(context, securityContext, new PostgreSqlDialect(), logger)
    {
    }

    // All business logic is inherited from PermissionProviderBase.
    // PostgreSQL-specific SQL is provided by PostgreSqlDialect.
    
    // The critical method Permissions_GetEffectiveForObject() uses
    // PostgreSQL function get_user_permissions_for_object() for efficient
    // recursive permission calculation with inheritance.
}
