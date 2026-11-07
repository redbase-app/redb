using redb.Core.Data;
using redb.Core.Models.Configuration;
using redb.Core.Models.Contracts;
using redb.Core.Providers;
using redb.Core.Providers.Base;
using redb.Core.Serialization;
using redb.SQLite.Sql;
using Microsoft.Extensions.Logging;

namespace redb.SQLite.Providers;

/// <summary>
/// SQLite implementation of tree provider.
/// Inherits all logic from TreeProviderBase, provides SQLite-specific SQL via SqliteDialect.
/// </summary>
public class SqliteTreeProvider : TreeProviderBase
{
    /// <summary>
    /// Creates SQLite tree provider with default SqliteDialect.
    /// </summary>
    public SqliteTreeProvider(
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
            new SqliteDialect(),
            configuration,
            logger)
    {
    }
}
