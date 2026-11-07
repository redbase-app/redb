using redb.Core.Data;
using redb.Core.Models.Contracts;
using redb.Core.Providers.Base;
using redb.SQLite.Sql;
using Microsoft.Extensions.Logging;

namespace redb.SQLite.Providers;

/// <summary>
/// SQLite implementation of permission provider.
/// Inherits all business logic from PermissionProviderBase.
/// Only provides SQLite-specific SQL dialect.
/// 
/// IMPORTANT: Uses SQLite function get_user_permissions_for_object()
/// for efficient recursive permission calculation.
/// 
/// Usage:
/// services.AddScoped&lt;IPermissionProvider, SqlitePermissionProvider&gt;();
/// </summary>
public class SqlitePermissionProvider : PermissionProviderBase
{
    /// <summary>
    /// Creates a new SQLite permission provider.
    /// </summary>
    /// <param name="context">Database context for executing queries</param>
    /// <param name="securityContext">Security context for authorization</param>
    /// <param name="logger">Optional logger for diagnostics</param>
    public SqlitePermissionProvider(
        IRedbContext context, 
        IRedbSecurityContext securityContext,
        ILogger? logger = null)
        : base(context, securityContext, new SqliteDialect(), logger)
    {
    }

    // All business logic is inherited from PermissionProviderBase.
    // SQLite-specific SQL is provided by SqliteDialect.
    
    // The critical method Permissions_GetEffectiveForObject() uses
    // SQLite function get_user_permissions_for_object() for efficient
    // recursive permission calculation with inheritance.
}
