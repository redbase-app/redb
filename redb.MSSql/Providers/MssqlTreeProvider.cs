using redb.Core.Data;
using redb.Core.Models.Configuration;
using redb.Core.Models.Contracts;
using redb.Core.Providers;
using redb.Core.Providers.Base;
using redb.Core.Serialization;
using redb.MSSql.Query;
using Microsoft.Extensions.Logging;
using redb.MSSql.Sql;

namespace redb.MSSql.Providers;

/// <summary>
/// MS SQL Server implementation of tree provider.
/// Inherits all logic from TreeProviderBase, provides MSSQL-specific SQL via MsSqlDialect.
/// 
/// Usage:
/// services.AddScoped&lt;ITreeProvider, MssqlTreeProvider&gt;();
/// </summary>
public class MssqlTreeProvider : TreeProviderBase
{
    /// <summary>
    /// Creates MSSQL tree provider with default MsSqlDialect.
    /// </summary>
    public MssqlTreeProvider(
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
            new MsSqlDialect(),
            configuration,
            logger)
    {
    }
}

