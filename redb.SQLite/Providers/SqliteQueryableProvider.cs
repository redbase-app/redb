using redb.Core.Data;
using redb.Core.Models.Configuration;
using redb.Core.Models.Contracts;
using redb.Core.Models.Security;
using redb.Core.Providers;
using redb.Core.Providers.Base;
using redb.Core.Query;
using redb.Core.Query.Base;
using redb.Core.Query.QueryExpressions;
using redb.Core.Serialization;
using redb.SQLite.Query;
using redb.Core.Query.Parsing;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Linq;

namespace redb.SQLite.Providers
{
    /// <summary>
    /// SQLite implementation of IQueryableProvider.
    /// Inherits all logic from QueryableProviderBase, provides SQLite-specific query providers.
    /// </summary>
    public class SqliteQueryableProvider : QueryableProviderBase
    {
        public SqliteQueryableProvider(
            IRedbContext context,
            IRedbObjectSerializer serializer,
            ISchemeSyncProvider schemeSync,
            IRedbSecurityContext securityContext,
            ILazyPropsLoader? lazyPropsLoader = null,
            RedbServiceConfiguration? configuration = null,
            string? cacheDomain = null,
            ILogger? logger = null)
            : base(context, serializer, schemeSync, securityContext, lazyPropsLoader, configuration, cacheDomain, logger)
        {
        }

        protected override IRedbQueryable<TProps> CreateQuery<TProps>(long schemeId, long? userId, bool checkPermissions)
        {
            var queryProvider = new SqliteQueryProvider(Context, Serializer, LazyPropsLoader, Configuration, Logger, SchemeSync);
            return queryProvider.CreateQuery<TProps>(schemeId, userId, checkPermissions);
        }

        protected override IRedbQueryable<TProps> CreateTreeQuery<TProps>(
            long schemeId, long? userId, bool checkPermissions, long? rootObjectId, int? maxDepth)
        {
            var treeQueryProvider = new SqliteTreeQueryProvider(Context, Serializer, LazyPropsLoader, Configuration, Logger, cacheDomain: CacheDomain, schemeSync: SchemeSync);
            return treeQueryProvider.CreateTreeQuery<TProps>(schemeId, userId, checkPermissions, rootObjectId, maxDepth);
        }

        protected override IRedbQueryable<TProps> CreateEmptyTreeQuery<TProps>(long schemeId, long? userId, bool checkPermissions)
        {
            var treeQueryProvider = new SqliteTreeQueryProvider(Context, Serializer, LazyPropsLoader, Configuration, Logger, cacheDomain: CacheDomain, schemeSync: SchemeSync);
            var emptyTreeQuery = treeQueryProvider.CreateTreeQuery<TProps>(schemeId, userId, checkPermissions);
            return emptyTreeQuery.Where(x => false);
        }

        protected override IRedbQueryable<TProps> CreateMultiRootTreeQuery<TProps>(
            long schemeId, long? userId, bool checkPermissions, List<IRedbObject> rootObjects, int? maxDepth)
        {
            var treeQueryProvider = new SqliteTreeQueryProvider(Context, Serializer, LazyPropsLoader, Configuration, Logger, cacheDomain: CacheDomain, schemeSync: SchemeSync);
            var parentIds = rootObjects.Select(obj => obj.Id).ToArray();
            
            var multiRootContext = new TreeQueryContext<TProps>(schemeId, userId, checkPermissions, null, maxDepth)
            {
                ParentIds = parentIds
            };
            
            var filterParser = new FilterExpressionParser();
            var orderingParser = new OrderingExpressionParser();
            
            return new SqliteTreeQueryable<TProps>(treeQueryProvider, multiRootContext, filterParser, orderingParser);
        }

        protected override IRedbQueryable<TProps> CreateMultiRootTreeQueryByIds<TProps>(
            long schemeId, long? userId, bool checkPermissions, IEnumerable<long> rootObjectIds, int? maxDepth)
        {
            var treeQueryProvider = new SqliteTreeQueryProvider(Context, Serializer, LazyPropsLoader, Configuration, Logger, cacheDomain: CacheDomain, schemeSync: SchemeSync);
            var parentIds = rootObjectIds.ToArray();
            
            var multiRootContext = new TreeQueryContext<TProps>(schemeId, userId, checkPermissions, null, maxDepth)
            {
                ParentIds = parentIds
            };
            
            var filterParser = new FilterExpressionParser();
            var orderingParser = new OrderingExpressionParser();
            
            return new SqliteTreeQueryable<TProps>(treeQueryProvider, multiRootContext, filterParser, orderingParser);
        }
    }
}
