using redb.Core.Data;
using redb.Core.Models.Configuration;
using redb.Core.Models.Contracts;
using redb.Core.Providers;
using redb.Core.Providers.Base;
using redb.Core.Serialization;
using redb.Postgres.Sql;
using Microsoft.Extensions.Logging;

namespace redb.Postgres.Providers;

/// <summary>
/// PostgreSQL implementation of tree provider.
/// Inherits all logic from TreeProviderBase, provides PostgreSQL-specific SQL via PostgreSqlDialect.
/// </summary>
public class PostgresTreeProvider : TreeProviderBase
{
    /// <summary>
    /// Creates PostgreSQL tree provider with default PostgreSqlDialect.
    /// </summary>
    public PostgresTreeProvider(
        IRedbContext context,
        IObjectStorageProvider objectStorage,
        IPermissionProvider permissionProvider,
        IRedbObjectSerializer serializer,
        IRedbSecurityContext securityContext,
        ISchemeSyncProvider schemeSyncProvider,
        RedbServiceConfiguration? configuration = null,
        ILogger? logger = null)
        : base(
            context,
            objectStorage,
            permissionProvider,
            serializer,
            securityContext,
            schemeSyncProvider,
            new PostgreSqlDialect(),
            configuration,
            logger)
    {
    }
}
