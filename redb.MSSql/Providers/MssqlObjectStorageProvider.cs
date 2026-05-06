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
/// MS SQL Server implementation of ObjectStorageProvider.
/// Inherits all business logic from ObjectStorageProviderBase, provides MSSQL-specific LazyPropsLoader.
/// 
/// Usage:
/// services.AddScoped&lt;IObjectStorageProvider, MssqlObjectStorageProvider&gt;();
/// </summary>
public class MssqlObjectStorageProvider : ObjectStorageProviderBase
{
    /// <summary>
    /// Creates a new MssqlObjectStorageProvider instance.
    /// </summary>
    public MssqlObjectStorageProvider(
        IRedbContext context,
        IRedbObjectSerializer serializer,
        IPermissionProvider permissionProvider,
        IRedbSecurityContext securityContext,
        ISchemeSyncProvider schemeSync,
        RedbServiceConfiguration configuration,
        IListProvider? listProvider = null,
        ILogger? logger = null)
        : base(context, serializer, permissionProvider, securityContext, 
               schemeSync, configuration, new MsSqlDialect(), listProvider, logger)
    {
    }
    
    /// <summary>
    /// Creates MSSQL-specific LazyPropsLoader for lazy property loading.
    /// </summary>
    protected override ILazyPropsLoader CreateLazyPropsLoader()
    {
        return new LazyPropsLoader(Context, SchemeSyncProvider, Serializer, Configuration, ListProvider, Logger);
    }
}

