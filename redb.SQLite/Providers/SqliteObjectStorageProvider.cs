using redb.Core.Data;
using redb.Core.Models.Configuration;
using redb.Core.Models.Contracts;
using redb.Core.Providers;
using redb.Core.Providers.Base;
using redb.Core.Query;
using redb.Core.Serialization;
using redb.SQLite.Sql;
using Microsoft.Extensions.Logging;

namespace redb.SQLite.Providers
{
    /// <summary>
    /// SQLite implementation of ObjectStorageProvider.
    /// Inherits all business logic from ObjectStorageProviderBase, provides SQLite-specific LazyPropsLoader.
    /// </summary>
    public class SqliteObjectStorageProvider : ObjectStorageProviderBase
    {
        /// <summary>
        /// Creates a new SqliteObjectStorageProvider instance.
        /// </summary>
        public SqliteObjectStorageProvider(
            IRedbContext context,
            IRedbObjectSerializer serializer,
            IPermissionProvider permissionProvider,
            IRedbSecurityContext securityContext,
            ISchemeSyncProvider schemeSync,
            RedbServiceConfiguration configuration,
            IListProvider? listProvider = null,
            ILogger? logger = null)
            : base(context, serializer, permissionProvider, securityContext, 
                   schemeSync, configuration, new SqliteDialect(), listProvider, logger)
        {
        }
        
        /// <summary>
        /// Creates SQLite-specific LazyPropsLoader for lazy property loading.
        /// </summary>
        protected override ILazyPropsLoader CreateLazyPropsLoader()
        {
            return new LazyPropsLoader(Context, SchemeSyncProvider, Serializer, Configuration, ListProvider, Logger);
        }
    }
}

