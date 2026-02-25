using redb.Core.Data;
using redb.Core.Models.Configuration;
using redb.Core.Models.Contracts;
using redb.Core.Models.Security;
using redb.Core.Query;
using redb.Core.Query.QueryExpressions;
using redb.Core.Serialization;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace redb.Core.Providers.Base
{
    /// <summary>
    /// Base class for IQueryableProvider implementations.
    /// Contains all common logic for creating LINQ queries.
    /// DB-specific implementations provide concrete query providers via factory methods.
    /// </summary>
    public abstract class QueryableProviderBase : IQueryableProvider
    {
        protected readonly IRedbContext Context;
        protected readonly IRedbObjectSerializer Serializer;
        protected readonly ISchemeSyncProvider SchemeSync;
        protected readonly ILogger? Logger;
        protected readonly IRedbSecurityContext SecurityContext;
        protected readonly RedbServiceConfiguration Configuration;
        protected readonly ILazyPropsLoader? LazyPropsLoader;
        protected readonly string CacheDomain;

        protected QueryableProviderBase(
            IRedbContext context,
            IRedbObjectSerializer serializer,
            ISchemeSyncProvider schemeSync,
            IRedbSecurityContext securityContext,
            ILazyPropsLoader? lazyPropsLoader = null,
            RedbServiceConfiguration? configuration = null,
            string? cacheDomain = null,
            ILogger? logger = null)
        {
            Context = context ?? throw new ArgumentNullException(nameof(context));
            Serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            SchemeSync = schemeSync ?? throw new ArgumentNullException(nameof(schemeSync));
            SecurityContext = securityContext ?? throw new ArgumentNullException(nameof(securityContext));
            LazyPropsLoader = lazyPropsLoader;
            Configuration = configuration ?? new RedbServiceConfiguration();
            CacheDomain = cacheDomain ?? Configuration.GetEffectiveCacheDomain();
            Logger = logger;
        }

        // ===== ABSTRACT FACTORY METHODS =====
        
        /// <summary>
        /// Create DB-specific query provider for flat queries.
        /// </summary>
        protected abstract IRedbQueryable<TProps> CreateQuery<TProps>(long schemeId, long? userId, bool checkPermissions) 
            where TProps : class, new();
        
        /// <summary>
        /// Create DB-specific query provider for tree queries.
        /// </summary>
        protected abstract IRedbQueryable<TProps> CreateTreeQuery<TProps>(
            long schemeId, long? userId, bool checkPermissions, long? rootObjectId, int? maxDepth) 
            where TProps : class, new();
        
        /// <summary>
        /// Create empty tree query (for null rootObject case).
        /// </summary>
        protected abstract IRedbQueryable<TProps> CreateEmptyTreeQuery<TProps>(long schemeId, long? userId, bool checkPermissions) 
            where TProps : class, new();
        
        /// <summary>
        /// Create tree query for multiple root objects.
        /// </summary>
        protected abstract IRedbQueryable<TProps> CreateMultiRootTreeQuery<TProps>(
            long schemeId, long? userId, bool checkPermissions, List<IRedbObject> rootObjects, int? maxDepth) 
            where TProps : class, new();
        
        /// <summary>
        /// Create tree query for multiple root object IDs.
        /// </summary>
        protected abstract IRedbQueryable<TProps> CreateMultiRootTreeQueryByIds<TProps>(
            long schemeId, long? userId, bool checkPermissions, IEnumerable<long> rootObjectIds, int? maxDepth) 
            where TProps : class, new();

        // ===== FLAT QUERIES =====

        public IRedbQueryable<TProps> Query<TProps>() where TProps : class, new()
        {
            var scheme = GetSchemeSync<TProps>();
            var effectiveUser = SecurityContext.GetEffectiveUser();
            return CreateQuery<TProps>(scheme.Id, effectiveUser.Id, Configuration.DefaultCheckPermissionsOnQuery);
        }

        public IRedbQueryable<TProps> Query<TProps>(IRedbUser user) where TProps : class, new()
        {
            var scheme = GetSchemeSync<TProps>();
            return CreateQuery<TProps>(scheme.Id, user.Id, Configuration.DefaultCheckPermissionsOnQuery);
        }
        
        /// <summary>
        /// Get scheme synchronously - first from cache, then via Task.Run to avoid SynchronizationContext deadlock.
        /// Cache is warmed up by InitializeAsync() at startup, so Task.Run path is rarely used.
        /// </summary>
        private IRedbScheme GetSchemeSync<TProps>() where TProps : class
        {
            // Try cache first (fast, no async, no DB call)
            var scheme = SchemeSync.GetSchemeFromCache<TProps>();
            if (scheme != null)
                return scheme;
            
            // Fallback: use Task.Run to avoid Blazor/ASP.NET SynchronizationContext deadlock.
            // This runs GetSchemeByTypeAsync on ThreadPool where there's no SynchronizationContext.
            // Note: GetSchemeByTypeAsync will cache the result, so subsequent calls will hit cache.
            Logger?.LogWarning(
                "Scheme '{SchemeName}' not found in cache. Consider calling InitializeAsync() at startup for better performance.",
                typeof(TProps).Name);
            
            scheme = Task.Run(() => SchemeSync.GetSchemeByTypeAsync<TProps>()).GetAwaiter().GetResult();
            
            if (scheme == null)
                throw new InvalidOperationException($"Scheme for type '{typeof(TProps).Name}' not found. Ensure InitializeAsync() was called at startup.");
            
            return scheme;
        }

        // ===== TREE QUERIES =====
        public IRedbQueryable<TProps> TreeQuery<TProps>() where TProps : class, new()
        {
            var scheme = GetSchemeSync<TProps>();

            var effectiveUser = SecurityContext.GetEffectiveUser();
            return CreateTreeQuery<TProps>(scheme.Id, effectiveUser.Id, Configuration.DefaultCheckPermissionsOnQuery, null, null);
        }

        public IRedbQueryable<TProps> TreeQuery<TProps>(IRedbUser user) where TProps : class, new()
        {
            var scheme = GetSchemeSync<TProps>();

            return CreateTreeQuery<TProps>(scheme.Id, user.Id, Configuration.DefaultCheckPermissionsOnQuery, null, null);
        }

        // ===== SYNC TREE QUERIES WITH ROOT OBJECT =====

        public IRedbQueryable<TProps> TreeQuery<TProps>(long rootObjectId, int? maxDepth = null) where TProps : class, new()
        {
            var scheme = GetSchemeSync<TProps>();

            var effectiveUser = SecurityContext.GetEffectiveUser();
            return CreateTreeQuery<TProps>(scheme.Id, effectiveUser.Id, Configuration.DefaultCheckPermissionsOnQuery, rootObjectId, maxDepth);
        }

        public IRedbQueryable<TProps> TreeQuery<TProps>(IRedbObject? rootObject, int? maxDepth = null) where TProps : class, new()
        {
            var scheme = GetSchemeSync<TProps>();

            var effectiveUser = SecurityContext.GetEffectiveUser();
            
            if (rootObject == null)
                return CreateEmptyTreeQuery<TProps>(scheme.Id, effectiveUser.Id, Configuration.DefaultCheckPermissionsOnQuery);

            return CreateTreeQuery<TProps>(scheme.Id, effectiveUser.Id, Configuration.DefaultCheckPermissionsOnQuery, rootObject.Id, maxDepth);
        }

        public IRedbQueryable<TProps> TreeQuery<TProps>(IEnumerable<IRedbObject> rootObjects, int? maxDepth = null) where TProps : class, new()
        {
            var rootList = rootObjects?.ToList() ?? [];
            
            var scheme = GetSchemeSync<TProps>();

            var effectiveUser = SecurityContext.GetEffectiveUser();
            
            if (!rootList.Any())
                return CreateEmptyTreeQuery<TProps>(scheme.Id, effectiveUser.Id, Configuration.DefaultCheckPermissionsOnQuery);

            if (rootList.Count == 1)
                return CreateTreeQuery<TProps>(scheme.Id, effectiveUser.Id, Configuration.DefaultCheckPermissionsOnQuery, rootList[0].Id, maxDepth);

            return CreateMultiRootTreeQuery<TProps>(scheme.Id, effectiveUser.Id, Configuration.DefaultCheckPermissionsOnQuery, rootList, maxDepth);
        }

        public IRedbQueryable<TProps> TreeQuery<TProps>(IEnumerable<long> rootObjectIds, int? maxDepth = null) where TProps : class, new()
        {
            var idsList = rootObjectIds?.ToList() ?? [];
            
            var scheme = GetSchemeSync<TProps>();

            var effectiveUser = SecurityContext.GetEffectiveUser();
            
            if (!idsList.Any())
                return CreateEmptyTreeQuery<TProps>(scheme.Id, effectiveUser.Id, Configuration.DefaultCheckPermissionsOnQuery);

            if (idsList.Count == 1)
                return CreateTreeQuery<TProps>(scheme.Id, effectiveUser.Id, Configuration.DefaultCheckPermissionsOnQuery, idsList[0], maxDepth);

            return CreateMultiRootTreeQueryByIds<TProps>(scheme.Id, effectiveUser.Id, Configuration.DefaultCheckPermissionsOnQuery, idsList, maxDepth);
        }

        public IRedbQueryable<TProps> TreeQuery<TProps>(long rootObjectId, IRedbUser user, int? maxDepth = null) where TProps : class, new()
        {
            var scheme = GetSchemeSync<TProps>();

            return CreateTreeQuery<TProps>(scheme.Id, user.Id, Configuration.DefaultCheckPermissionsOnQuery, rootObjectId, maxDepth);
        }

        public IRedbQueryable<TProps> TreeQuery<TProps>(IRedbObject? rootObject, IRedbUser user, int? maxDepth = null) where TProps : class, new()
        {
            var scheme = GetSchemeSync<TProps>();

            if (rootObject == null)
                return CreateEmptyTreeQuery<TProps>(scheme.Id, user.Id, Configuration.DefaultCheckPermissionsOnQuery);

            return CreateTreeQuery<TProps>(scheme.Id, user.Id, Configuration.DefaultCheckPermissionsOnQuery, rootObject.Id, maxDepth);
        }

        public IRedbQueryable<TProps> TreeQuery<TProps>(IEnumerable<IRedbObject> rootObjects, IRedbUser user, int? maxDepth = null) where TProps : class, new()
        {
            var rootList = rootObjects?.ToList() ?? [];
            
            var scheme = GetSchemeSync<TProps>();

            if (!rootList.Any())
                return CreateEmptyTreeQuery<TProps>(scheme.Id, user.Id, Configuration.DefaultCheckPermissionsOnQuery);

            if (rootList.Count == 1)
                return CreateTreeQuery<TProps>(scheme.Id, user.Id, Configuration.DefaultCheckPermissionsOnQuery, rootList[0].Id, maxDepth);

            return CreateMultiRootTreeQuery<TProps>(scheme.Id, user.Id, Configuration.DefaultCheckPermissionsOnQuery, rootList, maxDepth);
        }

        public IRedbQueryable<TProps> TreeQuery<TProps>(IEnumerable<long> rootObjectIds, IRedbUser user, int? maxDepth = null) where TProps : class, new()
        {
            var idsList = rootObjectIds?.ToList() ?? [];
            
            var scheme = GetSchemeSync<TProps>();

            if (!idsList.Any())
                return CreateEmptyTreeQuery<TProps>(scheme.Id, user.Id, Configuration.DefaultCheckPermissionsOnQuery);

            if (idsList.Count == 1)
                return CreateTreeQuery<TProps>(scheme.Id, user.Id, Configuration.DefaultCheckPermissionsOnQuery, idsList[0], maxDepth);

            return CreateMultiRootTreeQueryByIds<TProps>(scheme.Id, user.Id, Configuration.DefaultCheckPermissionsOnQuery, idsList, maxDepth);
        }
    }
}

