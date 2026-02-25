using redb.Core.Data;
using redb.Core.Models.Configuration;
using redb.Core.Models.Contracts;
using redb.Core.Providers;
using redb.Core.Providers.Base;
using redb.Core.Query;
using redb.Core.Serialization;
using redb.Postgres.Sql;
using Microsoft.Extensions.Logging;

namespace redb.Postgres.Providers
{
    /// <summary>
    /// PostgreSQL implementation of ObjectStorageProvider.
    /// Inherits all business logic from ObjectStorageProviderBase, provides PostgreSQL-specific LazyPropsLoader.
    /// </summary>
    public class PostgresObjectStorageProvider : ObjectStorageProviderBase
    {
        /// <summary>
        /// Creates a new PostgresObjectStorageProvider instance.
        /// </summary>
        public PostgresObjectStorageProvider(
            IRedbContext context,
            IRedbObjectSerializer serializer,
            IPermissionProvider permissionProvider,
            IRedbSecurityContext securityContext,
            ISchemeSyncProvider schemeSync,
            RedbServiceConfiguration configuration,
            IListProvider? listProvider = null,
            ILogger? logger = null)
            : base(context, serializer, permissionProvider, securityContext, 
                   schemeSync, configuration, new PostgreSqlDialect(), listProvider, logger)
        {
        }
        
        /// <summary>
        /// Creates PostgreSQL-specific LazyPropsLoader for lazy property loading.
        /// </summary>
        protected override ILazyPropsLoader CreateLazyPropsLoader()
        {
            return new LazyPropsLoader(Context, SchemeSyncProvider, Serializer, Configuration, ListProvider, Logger);
        }
    }
}

