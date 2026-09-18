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
                        var loader = _lazyLoader;
                        try
                        {
                            // Synchronous loading from DB, on the reader's scope
                            var loaded = loader.LoadProps<TProps>(id, scheme_id);
                            // A shared instance (props cache) keeps what a transaction reads while that transaction has
                            // written nothing - committed state - and forgets it if the transaction rolls back; once the
                            // transaction has written, it keeps nothing (owner decisions 2026-09-15, 2026-09-17). The
                            // reader still gets it.
                            var ambient = loader as AmbientLazyPropsLoader;
                            if (_isShared && ambient != null && !ambient.ReaderKeepsLoads)
                                return loaded!;
                            _properties = loaded;
                            _propsLoaded = true;
                            _lazyLoader = null;
                            if (_isShared)
                            {
                                LazyReferenceInstaller.MarkShared(loaded);
                                ambient?.OnReaderTransactionCompleted(committed => { if (!committed) Forget(loaded, loader); });
                            }
                        }
                        catch (Exception ex) when (ex is not Exceptions.RedbSynchronousLazyLoadException and not Exceptions.RedbLazyLoadScopeEndedException)
                        {
                            throw new InvalidOperationException(
                                $"Error during lazy loading Props for object {id}: {ex.Message}", ex);
                        }
                    }
                    // A reference nothing can load - an id, no properties, no loader - answers null: plain
                    // System.Text.Json serialization of a graph with hand-made references reads this getter, and an
                    // exception here would fail every such API response (review after 4.0.0, LazyStubSerializationRepro).
                    // Saving such a reference is refused instead (RedbUnloadedReferenceException).
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

        /// <inheritdoc/>
        [JsonIgnore]
        public override bool IsPropsLoaded => _propsLoaded;

        /// <summary>
        /// Get Props without triggering lazy loading (for internal use in cache).
        /// </summary>
        public TProps? GetPropsDirectly()
        {
            return _properties;
        }

        // What a rolled-back transaction loaded into a shared instance is dropped: the next read loads again.
        private void Forget(TProps? loaded, ILazyPropsLoader loader)
        {
            lock (_lazyLoadLock)
            {
                if (!_propsLoaded || !ReferenceEquals(_properties, loaded)) return;
                _properties = null;
                _propsLoaded = false;
                _lazyLoader = loader;
            }
        }

        /// <summary>
        /// Explicit async Props preloading (for eager loading scenarios).
        /// </summary>
        public async Task LoadPropsAsync()
        {
            if (!_propsLoaded && _lazyLoader != null && id > 0)
            {
                var loader = _lazyLoader;
                var loaded = await loader.LoadPropsAsync<TProps>(id, scheme_id);
                // The rule of the Props getter: kept while the reader's transaction has written nothing, forgotten on its
                // rollback; what a shared instance keeps is shared too (owner decisions 2026-09-15, 2026-09-17).
                var ambient = loader as AmbientLazyPropsLoader;
                if (_isShared && ambient != null && !ambient.ReaderKeepsLoads)
                    return;
                _properties = loaded;
                _propsLoaded = true;
                _lazyLoader = null;
                if (_isShared)
                {
                    LazyReferenceInstaller.MarkShared(loaded);
                    ambient?.OnReaderTransactionCompleted(committed => { if (!committed) Forget(loaded, loader); });
                }
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
        /// Create object copy with same metadata but new properties. The copy is a NEW object:
        /// it takes no <c>id</c>, no <c>hash</c> (recomputed from the new properties on save) and
        /// no <c>value_unique</c> - the object key is identity, not content, and a copy must
        /// claim its own or none (copying it would trip <c>UIX__objects__scheme_unique</c> on the
        /// first save).
        /// </summary>
        public IRedbObject<TProps> CloneWithProperties(TProps newProperties)
        {
            return new RedbObject<TProps>(newProperties)
            {
                // Copy all metadata except ID (to create new object). Deliberately NOT copied:
                // value_unique (the key is identity - one object per key per scheme) and hash
                // (the new Props get their own on save).
                parent_id = this.parent_id,
                scheme_id = this.scheme_id,
                owner_id = this.owner_id,
                who_change_id = this.who_change_id,
                date_create = DateTimeOffset.UtcNow,
                date_modify = DateTimeOffset.UtcNow,
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
