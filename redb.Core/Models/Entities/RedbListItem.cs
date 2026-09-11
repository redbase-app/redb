using redb.Core.Attributes;
using redb.Core.Models.Contracts;
using System;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace redb.Core.Models.Entities
{
    /// <summary>
    /// REDB list item entity with direct data storage.
    /// Maps to _list_items table in PostgreSQL.
    /// </summary>
    public class RedbListItem : IRedbListItem
    {
        /// <summary>
        /// Unique item identifier.
        /// </summary>
        [JsonPropertyName("id")]
        public long Id { get; set; }
        
        /// <summary>
        /// List identifier this item belongs to.
        /// </summary>
        [JsonPropertyName("id_list")]
        public long IdList { get; set; }
        
        /// <summary>
        /// Item value.
        /// </summary>
        [JsonPropertyName("value")]
        public string Value { get; set; } = string.Empty;
        
        /// <summary>
        /// Linked object identifier (optional).
        /// </summary>
        [JsonPropertyName("id_object")]
        public long? IdObject { get; set; }
        
        /// <summary>
        /// Item alias (display name).
        /// </summary>
        [JsonPropertyName("alias")]
        public string? Alias { get; set; }
        
        // === Object loaders (for lazy loading) ===

        // Process-wide fallback for items that no provider materialized (deserialized by hand,
        // constructed in user code). It must never capture a scoped service: the service that
        // installs it borrows a fresh scope per call (see RedbServiceBase). Before V4 every
        // RedbService constructor installed a delegate over its OWN scoped context here, so the
        // static pointed at whichever scope was created last, usually one already disposed; a
        // lazy load through it re-opened a physical connection nobody could ever return to the
        // pool (prod, 2026-09: ~30 idle PostgreSQL sessions a day until restart).
        private static Func<long, Task<IRedbObject?>>? _globalObjectLoader;

        // Synchronous twin of the fallback above, for the thread-pool-free path of the sync
        // getter: the whole load runs on the calling thread down to ADO.NET, so a saturated
        // thread pool cannot slow or deadlock a touch of Object.
        private static Func<long, IRedbObject?>? _globalSyncObjectLoader;

        // Per-item loader, attached by the provider that handed the item out. It resolves the
        // object through that provider's scope while the scope lives and through a fresh scope
        // once it is gone, so the item never touches a dead context.
        private Func<long, Task<IRedbObject?>>? _objectLoader;

        // Synchronous twin of the per-item loader; the sync getter prefers it.
        private Func<long, IRedbObject?>? _syncObjectLoader;

        /// <summary>
        /// Set the process-wide fallback loader used by items that carry no loader of their own.
        /// The delegate must not hold a scoped service: it is called from arbitrary threads for
        /// the rest of the process lifetime.
        /// </summary>
        public static void SetGlobalObjectLoader(Func<long, Task<IRedbObject?>> loader)
        {
            _globalObjectLoader = loader ?? throw new ArgumentNullException(nameof(loader));
        }

        /// <summary>
        /// Set the process-wide synchronous fallback loader (see <see cref="SetGlobalObjectLoader"/>
        /// for the delegate rules). The sync getter of <see cref="Object"/> prefers it: the load
        /// runs on the calling thread down to ADO.NET, free of the thread pool.
        /// </summary>
        public static void SetGlobalSyncObjectLoader(Func<long, IRedbObject?> loader)
        {
            _globalSyncObjectLoader = loader ?? throw new ArgumentNullException(nameof(loader));
        }

        /// <summary>
        /// Check if the process-wide fallback loader is available.
        /// </summary>
        public static bool IsObjectLoaderAvailable => _globalObjectLoader != null;

        /// <summary>Whether the linked object has been resolved (set, preloaded or lazily loaded).</summary>
        [JsonIgnore]
        public bool IsObjectLoaded => _objectLoaded;

        /// <summary>
        /// Asynchronous form of <see cref="Object"/>: resolves the linked object without blocking
        /// a thread. Prefer this (or the hand-out preload) on hot paths - the sync getter parks
        /// the calling thread for the duration of the load.
        /// </summary>
        public async Task<IRedbObject?> GetObjectAsync(CancellationToken cancellationToken = default)
        {
            if (_objectLoaded) return _object;
            if (!IdObject.HasValue) return _object;
            // Specificity first (see the sync getter): item-async, item-sync, global-async,
            // global-sync - on the async API the async form wins within a specificity level.
            IRedbObject? loaded;
            if (_objectLoader is { } itemLoader)
                loaded = await itemLoader(IdObject.Value).ConfigureAwait(false);
            else if (_syncObjectLoader is { } itemSync)
                loaded = itemSync(IdObject.Value); // inline: a fast blocking DB call, no thread parked on a Task
            else if (_globalObjectLoader is { } globalLoader)
                loaded = await globalLoader(IdObject.Value).ConfigureAwait(false);
            else if (_globalSyncObjectLoader is { } globalSync)
                loaded = globalSync(IdObject.Value);
            else
                return _object;
            lock (_lazyLoadLock)
            {
                if (!_objectLoaded)
                {
                    _object = loaded;
                    _objectLoaded = true;
                }
            }
            return _object;
        }

        /// <summary>
        /// Attaches the loader this item resolves <see cref="Object"/> through. Called by the list
        /// provider (and the props materializers) for every item they hand out; the last provider
        /// to hand the item out wins. A later call replaces the loader but never the object already
        /// loaded.
        /// </summary>
        public void AttachObjectLoader(Func<long, Task<IRedbObject?>> loader)
        {
            _objectLoader = loader ?? throw new ArgumentNullException(nameof(loader));
        }

        /// <summary>
        /// Attaches the synchronous twin of the loader (same rules as
        /// <see cref="AttachObjectLoader"/>); the sync getter of <see cref="Object"/> prefers it.
        /// </summary>
        public void AttachSyncObjectLoader(Func<long, IRedbObject?> loader)
        {
            _syncObjectLoader = loader ?? throw new ArgumentNullException(nameof(loader));
        }

        /// <summary>True when this item carries a loader of its own (not the process-wide fallback).</summary>
        [JsonIgnore]
        public bool HasObjectLoader => _objectLoader != null;

        // === Lazy loading fields ===

        private IRedbObject? _object = null;
        private bool _objectLoaded = false;
        private readonly object _lazyLoadLock = new();

        /// <summary>
        /// Linked object with lazy loading. Reading it may hit the database; it is deliberately
        /// invisible to System.Text.Json so that serializing an item (an HTTP response, an audit
        /// record) never turns into a load through some other scope's connection. Serialize
        /// <see cref="IdObject"/> and load the object explicitly where the JSON needs it.
        /// </summary>
        [JsonIgnore]
        [RedbIgnore]
        public IRedbObject? Object
        {
            get
            {
                if (_objectLoaded) return _object;
                // Specificity beats transport: a loader attached BY the provider that handed the
                // item out (it knows the right scope) wins over the process-wide fallback, and
                // within one specificity level the sync form wins (thread-pool-free). So:
                // item-sync, item-async, global-sync, global-async.
                var syncLoader = _syncObjectLoader ?? (_objectLoader == null ? _globalSyncObjectLoader : null);
                var loader = _objectLoader ?? _globalObjectLoader;
                if (!IdObject.HasValue || (syncLoader == null && loader == null)) return _object;

                // The freeze anatomy this shape avoids (tsum production, 2026-09-09, reproduced
                // locally): items are shared through the static list cache, so a lock held ACROSS
                // the load parked every toucher of the item process-wide behind the first one;
                // and Task.Run needed a second pool thread per touch on top of the one blocked in
                // GetResult - on a busy pool the whole process seized without a single exception
                // (even timeout continuations had no thread to run on). So: load OUTSIDE any
                // lock and WITHOUT Task.Run - and preferably through the SYNC loader, which runs
                // the whole load on the calling thread down to ADO.NET (zero thread-pool
                // dependence). The blocking-over-async fallback keeps one blocked thread per
                // touch, never two; concurrent touchers proceed independently (a racing
                // duplicate load is harmless: the first published result wins).
                IRedbObject? loaded;
                try
                {
                    loaded = syncLoader != null
                        ? syncLoader(IdObject.Value)
                        : loader!(IdObject.Value).ConfigureAwait(false).GetAwaiter().GetResult();
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException(
                        $"Error lazy loading Object for ListItem {Id}: {ex.Message}", ex);
                }

                lock (_lazyLoadLock)
                {
                    if (!_objectLoaded)
                    {
                        _object = loaded;
                        _objectLoaded = true;
                    }
                }
                return _object;
            }
            set
            {
                _object = value;
                _objectLoaded = true;
                IdObject = value?.Id > 0 ? value.Id : null;
            }
        }

        /// <summary>
        /// Check if item is object reference.
        /// </summary>
        [JsonIgnore]
        public bool IsObjectReference => IdObject.HasValue;

        // === Constructors ===
        
        /// <summary>
        /// Default constructor for deserialization and mapping.
        /// </summary>
        public RedbListItem()
        {
        }
        
        /// <summary>
        /// Constructor for creating item linked to list.
        /// </summary>
        public RedbListItem(IRedbList list, string? value, string? alias = null, long? idObject = null)
        {
            if (list == null) throw new ArgumentNullException(nameof(list));
            
            IdList = list.Id;
            Value = value;
            Alias = alias;
            IdObject = idObject;
        }
        
        /// <summary>
        /// Constructor for creating item with linked object.
        /// </summary>
        public RedbListItem(IRedbList list, string? value, string? alias, IRedbObject linkedObject)
        {
            if (list == null) throw new ArgumentNullException(nameof(list));
            if (linkedObject == null) throw new ArgumentNullException(nameof(linkedObject));
            
            IdList = list.Id;
            Value = value;
            Alias = alias;
            IdObject = linkedObject.Id > 0 ? linkedObject.Id : null;
            _object = linkedObject;
            _objectLoaded = true;
        }

        // === Static factory methods ===
        
        /// <summary>
        /// Create ListItem for specific list.
        /// </summary>
        public static RedbListItem ForList(IRedbList list, string? value, string? alias = null, long? idObject = null)
        {
            return new RedbListItem(list, value, alias, idObject);
        }
        
        /// <summary>
        /// Create ListItem with linked object.
        /// </summary>
        public static RedbListItem ForList(IRedbList list, string? value, string? alias, IRedbObject linkedObject)
        {
            return new RedbListItem(list, value, alias, linkedObject);
        }

        /// <summary>
        /// Get display value.
        /// </summary>
        public string GetDisplayValue()
        {
            if (!string.IsNullOrEmpty(Alias))
                return Alias;
                
            if (!string.IsNullOrEmpty(Value))
                return Value;
            
            if (IdObject.HasValue)
                return $"Object #{IdObject}";
                
            return $"Item #{Id}";
        }

        public override string ToString()
        {
            var displayValue = GetDisplayValue();
            var aliasStr = !string.IsNullOrEmpty(Alias) && Alias != displayValue ? $" ({Alias})" : "";
            return $"ListItem {Id}: {displayValue}{aliasStr} [List: {IdList}]";
        }
    }
}
