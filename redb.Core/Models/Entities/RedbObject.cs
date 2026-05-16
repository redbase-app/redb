using redb.Core.Models.Contracts;
using redb.Core.Models.Configuration;
using redb.Core.Utils;
using redb.Core.Caching;
using redb.Core.Providers;
using System;
using System.Reflection;
using System.Threading.Tasks;
using System.Text.Json.Serialization;

namespace redb.Core.Models.Entities
{
    /// <summary>
    /// Generic wrapper for JSON from get_object_json with typed interface.
    /// Field names match JSON/DB (snake_case) to work without attributes and settings.
    /// Inherits from base RedbObject for API unification.
    /// Implements typed interface IRedbObject&lt;TProps&gt; for type safety.
    /// 
    /// Object saving uses ChangeTracking strategy by default -
    /// compares with DB and updates only changed properties.
    /// </summary>
    /// <typeparam name="TProps">Type of object properties.</typeparam>
    public class RedbObject<TProps> : RedbObject, IRedbObject<TProps> where TProps : class, new()
    {
        /// <summary>
        /// Default constructor for deserialization.
        /// </summary>
        public RedbObject()
        {
        }

        /// <summary>
        /// Constructor with properties.
        /// </summary>
        /// <param name="props">Initial properties object.</param>
        public RedbObject(TProps props)
        {
            _properties = props;
            _propsLoaded = true;
        }

        /// <summary>
        /// Explicit cast to TProps.
        /// </summary>
        public static explicit operator TProps(RedbObject<TProps> obj) => obj.Props;


        /// <summary>
        /// Implicit cast to TProps.
        /// </summary>
        // public static implicit operator TProps(RedbObject<TProps> obj) => obj.Props;


        // SINGLE SOURCE OF DATA - always current (serialized to JSON)
        private TProps? _properties = null;

        // === LAZY LOADING FIELDS ===

        /// <summary>
        /// Props loader (set during lazy loading) - public for Provider access.
        /// </summary>
        [JsonIgnore]
        public ILazyPropsLoader? _lazyLoader = null;

        /// <summary>
        /// Props loaded flag (public for Provider access).
        /// </summary>
        public bool _propsLoaded = false;

        /// <summary>
        /// Synchronization object for lazy loading.
        /// </summary>
        private readonly object _lazyLoadLock = new object();

        /// <summary>
        /// Object properties with lazy loading support.
        /// </summary>
        [JsonPropertyName("properties")]
        public TProps Props
        {
            get
            {
                // Lazy loading on first access
                lock (_lazyLoadLock)
                {
                    if (!_propsLoaded && _lazyLoader != null && id > 0)
                    {
                        try
                        {
                            // Synchronous loading from DB
                            _properties = _lazyLoader.LoadProps<TProps>(id, scheme_id);
                            _propsLoaded = true;
                            _lazyLoader = null;
                        }
                        catch (Exception ex)
                        {
                            throw new InvalidOperationException(
                                $"Error during lazy loading Props for object {id}: {ex.Message}", ex);
                        }
                    }
                }

                return _properties!;
            }
            set
            {
                _properties = value;
                _propsLoaded = true;
                _lazyLoader = null;
            }
        }

        /// <summary>
        /// Get Props without triggering lazy loading (for internal use in cache).
        /// </summary>
        public TProps? GetPropsDirectly()
        {
            return _properties;
        }

        /// <summary>
        /// Explicit async Props preloading (for eager loading scenarios).
        /// </summary>
        public async Task LoadPropsAsync()
        {
            if (!_propsLoaded && _lazyLoader != null && id > 0)
            {
                _properties = await _lazyLoader.LoadPropsAsync<TProps>(id, scheme_id);
                _propsLoaded = true;
                _lazyLoader = null;
            }
        }

        /// <summary>
        /// Recompute MD5 hash from Props values and write to hash field.
        /// </summary>
        public override void RecomputeHash()
        {
            hash = RedbHash.ComputeFor(this);
        }

        /// <summary>
        /// Get MD5 hash from Props values without changing hash field.
        /// </summary>
        public override Guid ComputeHash() => RedbHash.ComputeFor(this) ?? Guid.Empty;

        // ===== TYPED CACHE AND METADATA METHODS =====

        /// <summary>
        /// Get scheme for type TProps (using cache and provider).
        /// Tries cache first, then provider, and only then throws exception.
        /// </summary>
        public async Task<IRedbScheme> GetSchemeForTypeAsync()
        {
            var typeName = typeof(TProps).Name;

            // 1. Check cache via provider
            var provider = GetSchemeSyncProvider();
            if (provider != null)
            {
                var cachedScheme = provider.Cache.GetScheme(typeName);
                if (cachedScheme != null)
                    return cachedScheme;
            }

            // 2. If not in cache and provider exists - try to load
            if (provider != null)
            {
                var scheme = await provider.GetSchemeByTypeAsync<TProps>();
                if (scheme != null)
                    return scheme;

                // Try to create scheme automatically
                try
                {
                    var newScheme = await provider.EnsureSchemeFromTypeAsync<TProps>();
                    return newScheme;
                }
                catch
                {
                    // If failed to create - proceed to exceptions
                }
            }

            // 3. If nothing worked - exceptions with hints
            if (provider == null)
            {
                throw new InvalidOperationException(
                    "Scheme provider not initialized. Call RedbObjectFactory.Initialize() or " +
                    "RedbObject.SetSchemeSyncProvider() to set provider.");
            }

            throw new InvalidOperationException(
                $"Scheme for type '{typeName}' not found and cannot be created automatically. " +
                $"Use provider.EnsureSchemeFromTypeAsync<{typeName}>() to create scheme manually.");
        }

        /// <summary>
        /// Get scheme structures for type TProps.
        /// </summary>
        public async Task<IReadOnlyCollection<IRedbStructure>> GetStructuresForTypeAsync()
        {
            var scheme = await GetSchemeForTypeAsync();
            return scheme.Structures;
        }

        /// <summary>
        /// Get structure by field name for type TProps.
        /// </summary>
        public new async Task<IRedbStructure?> GetStructureByNameAsync(string fieldName)
        {
            var scheme = await GetSchemeForTypeAsync();
            return scheme.GetStructureByName(fieldName);
        }

        /// <summary>
        /// Recompute hash based on current TProps properties.
        /// </summary>
        public void RecomputeHashForType()
        {
            RecomputeHash();
        }

        /// <summary>
        /// Get new hash based on current properties without changing object.
        /// </summary>
        public Guid ComputeHashForType()
        {
            return ComputeHash();
        }

        /// <summary>
        /// Check if current hash matches TProps properties.
        /// </summary>
        public bool IsHashValidForType()
        {
            if (!hash.HasValue)
                return false;

            var computedHash = ComputeHashForType();
            return hash.Value == computedHash;
        }

        /// <summary>
        /// Create object copy with same metadata but new properties.
        /// </summary>
        public IRedbObject<TProps> CloneWithProperties(TProps newProperties)
        {
            return new RedbObject<TProps>(newProperties)
            {
                // Copy all metadata except ID (to create new object)
                parent_id = this.parent_id,
                scheme_id = this.scheme_id,
                owner_id = this.owner_id,
                who_change_id = this.who_change_id,
                date_create = DateTimeOffset.Now,
                date_modify = DateTimeOffset.Now,
                date_begin = this.date_begin,
                date_complete = this.date_complete,
                key = this.key,
                value_long = this.value_long,
                value_string = this.value_string,
                value_guid = this.value_guid,
                value_bool = this.value_bool,
                value_double = this.value_double,
                value_numeric = this.value_numeric,
                value_datetime = this.value_datetime,
                value_bytes = this.value_bytes,
                name = this.name,
                note = this.note
            };
        }

        /// <summary>
        /// Clear metadata cache for type TProps.
        /// </summary>
        public void InvalidateCacheForType()
        {
            var provider = GetSchemeSyncProvider();
            if (provider is ISchemeCacheProvider cacheProvider)
            {
                cacheProvider.InvalidateSchemeCache<TProps>();
            }
        }

        /// <summary>
        /// Preload metadata cache for type TProps.
        /// </summary>
        public async Task WarmupCacheForTypeAsync()
        {
            var provider = GetSchemeSyncProvider();
            if (provider is ISchemeCacheProvider cacheProvider)
            {
                await cacheProvider.WarmupCacheAsync<TProps>();
            }
        }

        /// <summary>
        /// Override with recursive Props processing.
        /// Resets ID and ParentId for current object and all nested IRedbObject.
        /// </summary>
        /// <param name="recursive">If true, recursively processes all nested IRedbObject in Props.</param>
        public override void ResetIds(bool recursive = false)
        {
            // Reset base fields
            id = 0;
            parent_id = null;

            // Recursive Props processing
            if (recursive && Props != null)
            {
                ProcessNestedObjectsForReset(Props);
            }
        }

        /// <summary>
        /// Recursively processes Props to reset nested object IDs.
        /// </summary>
        private void ProcessNestedObjectsForReset(object obj)
        {
            if (obj == null) return;

            var objType = obj.GetType();
            var objProperties = objType.GetProperties(BindingFlags.Public | BindingFlags.Instance);

            foreach (var property in objProperties)
            {
                try
                {
                    var value = property.GetValue(obj);
                    if (value == null) continue;

                    // Process single IRedbObject
                    if (value is IRedbObject redbObj)
                    {
                        redbObj.ResetIds(true);
                        continue;
                    }

                    // Process IRedbObject collections
                    if (value is System.Collections.IEnumerable enumerable && value is not string)
                    {
                        foreach (var item in enumerable)
                        {
                            if (item is IRedbObject redbItem)
                            {
                                redbItem.ResetIds(true);
                            }
                        }
                    }
                }
                catch
                {
                    // Ignore property access errors
                    continue;
                }
            }
        }
    }
}
