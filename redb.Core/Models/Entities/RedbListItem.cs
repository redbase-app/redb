using redb.Core.Attributes;
using redb.Core.Exceptions;
using redb.Core.Models.Contracts;
using System;
using System.Text.Json.Serialization;
using System.Threading;
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

        // === Linked object: loaded on the reader's scope ===
        // Owner decision 2026-09-15 (plan docs/V4/PROPS_CACHE_PROD_AND_TAILS_PLAN.md §4.1): a data object owns no connection.
        // The item carries no loader delegate - it remembers only the database it came from - and Object loads on the live
        // redb scope of whoever reads it. The delegates it carried before (a per-item loader over the handing-out scope, a
        // process-wide fallback) opened a fresh scope and pooled connection per read wherever the right one was missing.

        // The database (cache domain) the item was materialized from; null for an item built in user code.
        internal string? _cacheDomain;

        // Set by the list cache and the props cache: the instance is shared by every scope.
        internal bool _isShared;

        // The service that materialized the item, held weakly; asked when no scope is current for the reader, while it
        // lives. A shared item has none.
        internal WeakReference<RedbServiceBase>? _origin;

        // The scope current for the reader, else the materializing scope while it lives (owner decision 2026-09-15).
        private RedbServiceBase? Reader() => RedbAmbientScope.Resolve(_cacheDomain) ?? RedbAmbientScope.LiveOrigin(_origin);

        private IRedbObject? _object = null;
        private bool _objectLoaded = false;
        private readonly object _lazyLoadLock = new();

        /// <summary>Whether the linked object has been resolved (set, preloaded or lazily loaded).</summary>
        [JsonIgnore]
        public bool IsObjectLoaded => _objectLoaded;

        /// <summary>The object this item already carries, without loading; null when none is loaded.</summary>
        internal IRedbObject? LoadedObject => _objectLoaded ? _object : null;

        /// <summary>
        /// Asynchronous form of <see cref="Object"/>: resolves the linked object on the reader's scope without blocking a
        /// thread. Prefer this - or <c>redb.LoadLinkedObjectsAsync(items)</c> for many items, one query - on hot paths.
        /// </summary>
        public async Task<IRedbObject?> GetObjectAsync(CancellationToken cancellationToken = default)
        {
            if (_objectLoaded) return _object;
            if (!IdObject.HasValue) return _object;

            var objectId = IdObject.Value;
            var reader = Reader();
            // A captive reader (a service of the root provider, outside a transaction) lends its scope factory, never its one connection.
            if (reader is { LendsScope: true } captive)
                return Publish(await RedbDomainRegistry.InScopeOfAsync(captive,
                    service => service.LoadLinkedObjectAsync(objectId, cancellationToken)).ConfigureAwait(false), readerContext: null);
            if (reader == null)
                return Publish(await RedbDomainRegistry.InFreshScopeAsync(_cacheDomain,
                    service => service.LoadLinkedObjectAsync(objectId, cancellationToken), Refusal).ConfigureAwait(false),
                    readerContext: null);

            var loaded = await reader.LoadLinkedObjectAsync(objectId, cancellationToken).ConfigureAwait(false);
            return Publish(loaded, reader.Context);
        }

        /// <summary>
        /// Linked object, loaded lazily on the live redb scope of whoever reads it. Reading it may hit the database; it is
        /// deliberately invisible to System.Text.Json so that serializing an item (an HTTP response, an audit record) never
        /// turns into a load. With no live scope reading here the getter refuses with
        /// <see cref="RedbLazyLoadScopeEndedException"/> (see <c>RedbServiceConfiguration.LazyLoadWithoutScope</c>).
        /// </summary>
        [JsonIgnore]
        [RedbIgnore]
        public IRedbObject? Object
        {
            get
            {
                if (_objectLoaded) return _object;
                if (!IdObject.HasValue) return _object;

                // The freeze anatomy this shape avoids (tsum production, 2026-09-09, reproduced locally): items are shared
                // through the static list cache, so a lock held ACROSS the load parked every toucher of the item
                // process-wide behind the first one; and Task.Run needed a second pool thread per touch. So: load OUTSIDE
                // any lock and on the calling thread, down to ADO.NET; concurrent touchers proceed independently (a racing
                // duplicate load is harmless: the first published result wins).
                var objectId = IdObject.Value;
                var reader = Reader();
                if (reader is { LendsScope: true } captive)
                    return Publish(RedbDomainRegistry.InScopeOf(captive, service => service.LoadLinkedObjectSync(objectId)), readerContext: null);
                if (reader == null)
                    return Publish(RedbDomainRegistry.InFreshScope(_cacheDomain,
                        service => service.LoadLinkedObjectSync(objectId), Refusal), readerContext: null);

                var loaded = reader.LoadLinkedObjectSync(objectId);
                return Publish(loaded, reader.Context);
            }
            set
            {
                _object = value;
                _objectLoaded = true;
                IdObject = value?.Id > 0 ? value.Id : null;
            }
        }

        private Exception Refusal() => RedbLazyLoadScopeEndedException.ForListItem(Id, IdObject ?? 0);

        /// <summary>
        /// Publishes a loaded object on the item and returns what readers get. A shared instance does not keep what one
        /// transaction saw - the write behind it may roll back (owner decision 2026-09-15); the reader still gets it.
        /// </summary>
        internal IRedbObject? Publish(IRedbObject? loaded, Data.IRedbContext? readerContext)
        {
            // A shared item keeps what a transaction loads while that transaction has written nothing - committed state -
            // and forgets it if the transaction rolls back; once the transaction has written, it keeps nothing (owner
            // decisions 2026-09-15, 2026-09-17). The reader still gets it.
            if (_isShared && readerContext != null && !Data.TransactionWrites.ReadsCommitted(readerContext)) return loaded;
            // What a shared item keeps is shared too: its references and items load on the reader's scope only, never on
            // the scope that happened to load it. Marked before publishing, outside the lock; a racing duplicate that
            // loses is marked for nothing.
            if (_isShared && loaded != null)
                global::redb.Core.Utils.LazyReferenceInstaller.MarkShared(loaded);
            var kept = false;
            lock (_lazyLoadLock)
            {
                if (!_objectLoaded)
                {
                    _object = loaded;
                    _objectLoaded = true;
                    kept = true;
                }
            }
            if (kept && _isShared && readerContext != null)
                Data.TransactionHooks.OnCompleted(readerContext, committed => { if (!committed) Forget(loaded); });
            return _object;
        }

        // What a rolled-back transaction loaded into a shared item is dropped: the next read loads again.
        private void Forget(IRedbObject? loaded)
        {
            lock (_lazyLoadLock)
            {
                if (!_objectLoaded || !ReferenceEquals(_object, loaded)) return;
                _object = null;
                _objectLoaded = false;
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
