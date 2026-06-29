using redb.Core.Data;
using redb.Core.Models.Contracts;
using redb.Core.Providers.Base;
using redb.Core.Security;
using redb.SQLite.Sql;
using Microsoft.Extensions.Logging;

namespace redb.SQLite.Providers;

/// <summary>
/// SQLite implementation of user provider.
/// Inherits all business logic from UserProviderBase.
/// Only provides SQLite-specific SQL dialect and password hasher.
/// 
/// Usage:
/// services.AddScoped&lt;IUserProvider, SqliteUserProvider&gt;();
/// </summary>
public class SqliteUserProvider : UserProviderBase
{
    /// <summary>
    /// Creates a new SQLite user provider.
    /// </summary>
    /// <param name="context">Database context for executing queries</param>
    /// <param name="securityContext">Security context for authorization</param>
    /// <param name="logger">Optional logger for diagnostics</param>
    public SqliteUserProvider(
        IRedbContext context, 
        IRedbSecurityContext securityContext,
        ILogger? logger = null)
        : base(context, securityContext, new SqliteDialect(), new SimplePasswordHasher(), logger)
    {
    }

    /// <summary>
    /// Creates a new SQLite user provider with custom password hasher.
    /// </summary>
    /// <param name="context">Database context for executing queries</param>
    /// <param name="securityContext">Security context for authorization</param>
    /// <param name="passwordHasher">Custom password hasher implementation</param>
    /// <param name="logger">Optional logger for diagnostics</param>
    public SqliteUserProvider(
        IRedbContext context, 
        IRedbSecurityContext securityContext, 
        IPasswordHasher passwordHasher,
        ILogger? logger = null)
        : base(context, securityContext, new SqliteDialect(), passwordHasher, logger)
    {
    }

    // All business logic is inherited from UserProviderBase.
    // SQLite-specific SQL is provided by SqliteDialect.
    // Password hashing is provided by SimplePasswordHasher (or custom implementation).
}
