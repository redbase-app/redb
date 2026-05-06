using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using redb.Core.Models.Contracts;
using redb.Core.Models.Security;
using redb.Core.Providers;
using redb.Core.Caching;
using redb.Core.Models.Entities;

namespace redb.Core.Models
{
    /// <summary>
    /// Factory for creating typed RedbObject&lt;TProps&gt; objects
    /// Ensures proper metadata initialization and caching integration
    ///
    /// Usage example:
    /// <code>
    /// 1. Factory initialization (once at startup)
    /// RedbObjectFactory.Initialize(schemeSyncProvider, ownerId: 1, whoChangeId: 1);
    ///
    /// 2. Fast creation without provider
    /// var employee = RedbObjectFactory.CreateFast(new Employee { Name = "Ivan" });
    ///
    /// 3. Creation with automatic scheme determination
    /// var employee = await RedbObjectFactory.CreateAsync(new Employee { Name = "Ivan" });
    ///
    /// 4. Child object creation
    /// var child = await RedbObjectFactory.CreateChildAsync(parent, new Employee { Name = "Subordinate" });
    ///
    /// 5. Batch creation with cache optimization
    /// var employees = await RedbObjectFactory.CreateBatchAsync(
    ///     new Employee { Name = "Ivan" },
    ///     new Employee { Name = "Peter" },
    ///     new Employee { Name = "Sidor" }
    /// );
    /// </code>
    /// </summary>
    public static class RedbObjectFactory
    {
        private static ISchemeSyncProvider? _provider;

        // Remove static IDs - we'll use user context

        /// <summary>
        /// Initialize the factory with a scheme provider
        /// Required for automatic scheme determination
        /// Also sets the provider in RedbObjectBase for global access
        /// </summary>
        public static void Initialize(ISchemeSyncProvider provider)
        {
            _provider = provider ?? throw new ArgumentNullException(nameof(provider));

            // Set provider in base class for global access
            RedbObject.SetSchemeSyncProvider(provider);
        }

        /// <summary>
        /// Check if the factory is initialized
        /// </summary>
        public static bool IsInitialized => _provider != null;

        /// <summary>
        /// Set the scheme provider
        /// </summary>
        public static void SetProvider(ISchemeSyncProvider provider)
        {
            _provider = provider ?? throw new ArgumentNullException(nameof(provider));
            RedbObject.SetSchemeSyncProvider(provider);
        }

        // ===== OBJECT CREATION METHODS =====

        /// <summary>
        /// Create a new object without property initialization
        /// Automatically determines scheme by TProps type
        /// </summary>
        public static async Task<RedbObject<TProps>> CreateAsync<TProps>() where TProps : class, new()
        {
            return await CreateAsync(new TProps());
        }

        /// <summary>
        /// Create a new object with initialized properties
        /// Automatically determines scheme by TProps type
        /// </summary>
        public static async Task<RedbObject<TProps>> CreateAsync<TProps>(TProps properties) where TProps : class, new()
        {
            return await CreateAsync(properties, initializeMetadata: true);
        }

        /// <summary>
        /// Create a new object with initialized properties
        /// </summary>
        /// <param name="properties">Properties of the object being created</param>
        /// <param name="initializeMetadata">Automatically initialize metadata (scheme, owner, dates). Set to false for batch insertion via AddNewObjectsAsync</param>
        public static async Task<RedbObject<TProps>> CreateAsync<TProps>(TProps properties, bool initializeMetadata) where TProps : class, new()
        {
            if (properties == null)
                throw new ArgumentNullException(nameof(properties));

            var obj = new RedbObject<TProps>(properties);

            if (initializeMetadata)
            {
                // Automatic metadata initialization
                await InitializeMetadataAsync(obj);
            }

            return obj;
        }

        /// <summary>
        /// Create a new object as a child of an existing parent
        /// </summary>
        public static async Task<RedbObject<TProps>> CreateChildAsync<TProps>(
            IRedbObject parent,
            TProps properties) where TProps : class, new()
        {
            return await CreateChildAsync(parent, properties, initializeMetadata: true);
        }

        /// <summary>
        /// Create a new object as a child of an existing parent
        /// </summary>
        /// <param name="parent">Parent object</param>
        /// <param name="properties">Properties of the object being created</param>
        /// <param name="initializeMetadata">Automatically initialize metadata (scheme, owner, dates). Set to false for batch insertion via AddNewObjectsAsync</param>
        public static async Task<RedbObject<TProps>> CreateChildAsync<TProps>(
            IRedbObject parent,
            TProps properties,
            bool initializeMetadata) where TProps : class, new()
        {
            if (parent == null)
                throw new ArgumentNullException(nameof(parent));
            if (properties == null)
                throw new ArgumentNullException(nameof(properties));

            var obj = new RedbObject<TProps>(properties)
            {
                parent_id = parent.Id
            };

            if (initializeMetadata)
            {
                await InitializeMetadataAsync(obj);
            }

            return obj;
        }

        /// <summary>
        /// Create copy of existing object with new properties
        /// Preserves all metadata except ID (for creating new object)
        /// </summary>
        public static async Task<RedbObject<TProps>> CreateCopyAsync<TProps>(
            IRedbObject<TProps> source,
            TProps newProperties) where TProps : class, new()
        {
            if (source == null)
                throw new ArgumentNullException(nameof(source));
            if (newProperties == null)
                throw new ArgumentNullException(nameof(newProperties));

            var obj = source.CloneWithProperties(newProperties);

            // ID will be set on save
            // Update only timestamps
            var redbObj = (RedbObject<TProps>)obj;
            redbObj.date_create = DateTimeOffset.Now;
            redbObj.date_modify = DateTimeOffset.Now;

            return redbObj;
        }

        // ===== FAST METHODS WITHOUT PROVIDER =====

        /// <summary>
        /// Create object without automatic scheme initialization (fast)
        /// Scheme will need to be set manually or through provider
        /// </summary>
        public static RedbObject<TProps> CreateFast<TProps>() where TProps : class, new()
        {
            return CreateFast(new TProps());
        }

        /// <summary>
        /// Create object with properties without automatic scheme initialization (fast)
        /// </summary>
        public static RedbObject<TProps> CreateFast<TProps>(TProps properties) where TProps : class, new()
        {
            var obj = new RedbObject<TProps>(properties);

            // Basic initialization without accessing provider
            var now = DateTimeOffset.Now;
            var securityContext = AmbientSecurityContext.GetOrCreateDefault();
            var effectiveUser = securityContext.GetEffectiveUser();

            obj.date_create = now;
            obj.date_modify = now;
            obj.owner_id = effectiveUser.Id;
            obj.who_change_id = effectiveUser.Id;

            return obj;
        }

        /// <summary>
        /// Create object with full manual initialization of all fields
        /// </summary>
        public static RedbObject<TProps> CreateWithMetadata<TProps>(
            TProps properties,
            long schemeId,
            string? name = null,
            long? parentId = null,
            long? ownerId = null,
            long? whoChangeId = null) where TProps : class, new()
        {
            var obj = new RedbObject<TProps>(properties);

            var now = DateTimeOffset.Now;
            var securityContext = AmbientSecurityContext.GetOrCreateDefault();
            var effectiveUser = securityContext.GetEffectiveUser();

            obj.scheme_id = schemeId;
            obj.name = name ?? $"Object_{typeof(TProps).Name}";
            obj.parent_id = parentId;
            obj.owner_id = ownerId ?? effectiveUser.Id;
            obj.who_change_id = whoChangeId ?? effectiveUser.Id;
            obj.date_create = now;
            obj.date_modify = now;

            return obj;
        }

        // ===== PRIVATE METHODS =====

        /// <summary>
        /// Initialize object metadata automatically
        /// </summary>
        private static async Task InitializeMetadataAsync<TProps>(RedbObject<TProps> obj) where TProps : class, new()
        {
            var now = DateTimeOffset.Now;
            var securityContext = AmbientSecurityContext.GetOrCreateDefault();
            var effectiveUser = securityContext.GetEffectiveUser();

            // Timestamps
            obj.date_create = now;
            obj.date_modify = now;

            // Use current user from security context
            obj.owner_id = effectiveUser.Id;
            obj.who_change_id = effectiveUser.Id;

            // Default object name
            if (string.IsNullOrEmpty(obj.name))
            {
                obj.name = $"New{typeof(TProps).Name}";
            }

            // Automatic scheme determination (if provider is available)
            if (_provider != null)
            {
                try
                {
                    var scheme = await _provider.GetSchemeByTypeAsync<TProps>();
                    if (scheme != null)
                    {
                        obj.scheme_id = scheme.Id;
                    }
                    else
                    {
                        // Try to create scheme automatically
                        var newScheme = await _provider.EnsureSchemeFromTypeAsync<TProps>();
                        obj.scheme_id = newScheme.Id;
                    }
                }
                catch
                {
                    // If scheme determination failed, leave as 0
                    // Scheme will be determined on save
                    obj.scheme_id = 0;
                }
            }
            else
            {
                // Without provider, scheme will be determined later
                obj.scheme_id = 0;
            }
        }

        // ===== INFORMATION METHODS =====

        /// <summary>
        /// Get current factory settings
        /// </summary>
        public static (bool IsInitialized, long CurrentUserId, string CurrentUserName) GetSettings()
        {
            var securityContext = AmbientSecurityContext.GetOrCreateDefault();
            var effectiveUser = securityContext.GetEffectiveUser();

            return (IsInitialized, effectiveUser.Id, effectiveUser.Name ?? "Unknown user");
        }

        // ===== CACHING INTEGRATION =====

        /// <summary>
        /// Create object with metadata preload to cache
        /// Useful for batch creation of objects of the same type
        /// </summary>
        public static async Task<RedbObject<TProps>> CreateWithWarmupAsync<TProps>(TProps properties) where TProps : class, new()
        {
            // Preload metadata to cache
            if (_provider is ISchemeCacheProvider cacheProvider)
            {
                await cacheProvider.WarmupCacheAsync<TProps>();
            }

            return await CreateAsync(properties);
        }

        /// <summary>
        /// Batch creation of objects with cache preloading
        /// Optimized for creating multiple objects of the same type
        /// </summary>
        public static async Task<List<RedbObject<TProps>>> CreateBatchAsync<TProps>(IEnumerable<TProps> properties) where TProps : class, new()
        {
            return await CreateBatchAsync(properties, initializeMetadata: true);
        }

        /// <summary>
        /// Batch creation of objects with cache preloading
        /// Optimized for creating multiple objects of the same type
        /// </summary>
        /// <param name="properties">Collection of properties for objects being created</param>
        /// <param name="initializeMetadata">Automatically initialize metadata (scheme, owner, dates). Set to false for batch insertion via AddNewObjectsAsync</param>
        public static async Task<List<RedbObject<TProps>>> CreateBatchAsync<TProps>(IEnumerable<TProps> properties, bool initializeMetadata) where TProps : class, new()
        {
            var propsList = properties?.ToList() ?? new List<TProps>();
            if (propsList.Count == 0)
                return new List<RedbObject<TProps>>();

            // Preload cache once for all objects
            if (initializeMetadata && _provider is ISchemeCacheProvider cacheProvider)
            {
                await cacheProvider.WarmupCacheAsync<TProps>();
            }

            var results = new List<RedbObject<TProps>>(propsList.Count);

            foreach (var prop in propsList)
            {
                results.Add(await CreateAsync(prop, initializeMetadata));
            }

            return results;
        }


        /// <summary>
        /// Batch creation of child objects with cache preloading
        /// All created objects will be children of the specified parent
        /// Optimized for creating multiple child objects of the same type
        /// </summary>
        public static async Task<List<RedbObject<TProps>>> CreateBatchChildAsync<TProps>(
            IRedbObject parent,
            IEnumerable<TProps> properties) where TProps : class, new()
        {
            return await CreateBatchChildAsync(parent, properties, initializeMetadata: true);
        }

        /// <summary>
        /// Batch creation of child objects with cache preloading
        /// All created objects will be children of the specified parent
        /// Optimized for creating multiple child objects of the same type
        /// </summary>
        /// <param name="parent">Parent object</param>
        /// <param name="properties">Collection of properties for objects being created</param>
        /// <param name="initializeMetadata">Automatically initialize metadata (scheme, owner, dates). Set to false for batch insertion via AddNewObjectsAsync</param>
        public static async Task<List<RedbObject<TProps>>> CreateBatchChildAsync<TProps>(
            IRedbObject parent,
            IEnumerable<TProps> properties,
            bool initializeMetadata) where TProps : class, new()
        {
            if (parent == null)
                throw new ArgumentNullException(nameof(parent));
            
            var propsList = properties?.ToList() ?? new List<TProps>();
            if (propsList.Count == 0)
                return new List<RedbObject<TProps>>();

            // Preload cache once for all objects
            if (initializeMetadata && _provider is ISchemeCacheProvider cacheProvider)
            {
                await cacheProvider.WarmupCacheAsync<TProps>();
            }

            var results = new List<RedbObject<TProps>>(propsList.Count);

            foreach (var prop in propsList)
            {
                results.Add(await CreateChildAsync(parent, prop, initializeMetadata));
            }

            return results;
        }

        // ===== NON-GENERIC OBJECT CREATION =====
        
        /// <summary>
        /// Create non-generic RedbObject with basic metadata.
        /// Scheme will be auto-determined on save (Object scheme).
        /// </summary>
        public static RedbObject CreateObject(string? name = null)
        {
            var obj = new RedbObject();
            
            var now = DateTimeOffset.Now;
            var securityContext = AmbientSecurityContext.GetOrCreateDefault();
            var effectiveUser = securityContext.GetEffectiveUser();
            
            obj.name = name ?? "NewObject";
            obj.date_create = now;
            obj.date_modify = now;
            obj.owner_id = effectiveUser.Id;
            obj.who_change_id = effectiveUser.Id;
            
            return obj;
        }
    }
}
