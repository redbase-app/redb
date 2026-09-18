using System;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using redb.Core.Models.Contracts;
using redb.Core.Providers;
using redb.Core.Caching;
using redb.Core.Utils;

namespace redb.Core.Models.Entities
{
    /// <summary>
    /// Base class for all Redb objects with access to metadata.
    /// Contains all fields from _objects table and methods for working with cached metadata.
    /// Can be used directly for Object schemes (without Props) or as base for RedbObject{TProps}.
    /// </summary>
    /// <remarks>
    /// The debugger shows the short display string below (the debugger prefers it over
    /// <see cref="ToString"/> and walks up the hierarchy, so every derived object gets it);
    /// <see cref="ToString"/> returns the object's canonical JSON (discussion #12, item 2).
    /// </remarks>
    [System.Diagnostics.DebuggerDisplay("{name,nq} (id={id}, scheme={scheme_id})")]
    public class RedbObject : IRedbObject
    {
        // ===== GLOBAL PROVIDER FOR METADATA ACCESS =====
        private static ISchemeSyncProvider? _globalProvider;

        /// <summary>
        /// Set global scheme provider for all objects
        /// Allows objects to access their metadata
        /// </summary>
        public static void SetSchemeSyncProvider(ISchemeSyncProvider provider)
        {
            _globalProvider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        /// <summary>
        /// Get global scheme provider
        /// </summary>
        protected static ISchemeSyncProvider? GetSchemeSyncProvider() => _globalProvider;

        /// <summary>
        /// Check if scheme provider is available
        /// </summary>
        public static bool IsProviderAvailable => _globalProvider != null;

        // ===== ROOT FIELDS (_objects) =====
        public long id { get; set; }
        public long? parent_id { get; set; }
        public long scheme_id { get; set; }
        public long owner_id { get; set; }
        public long who_change_id { get; set; }
        public DateTimeOffset date_create { get; set; }
        public DateTimeOffset date_modify { get; set; }
        public DateTimeOffset? date_begin { get; set; }
        public DateTimeOffset? date_complete { get; set; }
        public long? key { get; set; }
        public long? value_long { get; set; }
        /// <summary>
        /// Identifier / external-key value (RedbPrimitive&lt;string&gt;). Contract limit 450 characters
        /// (the MSSQL index-key width, enforced in C# on every provider; owner decision 2026-09-02) -
        /// long text belongs in <see cref="note"/> or in a Props field.
        /// </summary>
        public string? value_string { get; set; }
        public Guid? value_guid { get; set; }
        public bool? value_bool { get; set; }
        public double? value_double { get; set; }
        public decimal? value_numeric { get; set; }
        public DateTimeOffset? value_datetime { get; set; }
        public byte[]? value_bytes { get; set; }
        public string? value_unique { get; set; }   // V4: unique key within the scheme (UNIQUE stage 1)
        public string? name { get; set; }
        public string? note { get; set; }
        
        public Guid? hash { get; set; }

        /// <summary>
        /// Whether the object's properties are present in memory. A non-generic object has none,
        /// so it is always "loaded"; <c>RedbObject&lt;TProps&gt;</c> overrides this with its lazy-loading
        /// state. The save path uses it to tell a <b>reference</b> (an object with an id whose
        /// properties were never loaded: a stub from the depth boundary of a load, or
        /// <c>new RedbObject&lt;T&gt; { id = x }</c> written by hand) from a nested object to persist.
        /// A reference is written as its id only and is never re-saved by a parent.
        /// </summary>
        [JsonIgnore]
        public virtual bool IsPropsLoaded => true;

        /// <summary>
        /// Set by the props cache: this instance is shared by every scope. A lazy load under it runs on the reader's scope
        /// and, inside the reader's transaction, keeps nothing (owner decision 2026-09-15).
        /// </summary>
        internal bool _isShared;

        /// <summary>
        /// Recompute MD5 hash and store in hash field.
        /// For non-generic RedbObject: hash from base value_* fields.
        /// For RedbObject{TProps}: overridden to hash from Props.
        /// </summary>
        public virtual void RecomputeHash()
        {
            hash = ComputeHash();
        }

        /// <summary>
        /// Compute MD5 hash without modifying the hash field.
        /// For non-generic RedbObject: hash from base value_* fields.
        /// For RedbObject{TProps}: overridden to hash from Props.
        /// </summary>
        public virtual Guid ComputeHash()
        {
            return RedbHash.ComputeForBaseFields(this);
        }

        // ===== ENRICHED IRedbObject IMPLEMENTATION =====
        // (excluded from JSON serialization)

        public void ResetId(long id)
        {
            this.id = id;
        }

        // Main identifiers
        [JsonIgnore]
        public long Id { get => id; set => id = value; }
        [JsonIgnore]
        public long SchemeId { get => scheme_id; set => scheme_id = value; }
        [JsonIgnore]
        public string Name { get => name ?? $"Object_{id}"; set => name = value; }

        // Tree structure
        [JsonIgnore]
        public long? ParentId { get => parent_id; set => parent_id = value; }
        [JsonIgnore]
        public bool HasParent => parent_id.HasValue;
        [JsonIgnore]
        public bool IsRoot => !parent_id.HasValue;

        // Timestamps
        [JsonIgnore]
        public DateTimeOffset DateCreate { get => date_create; set => date_create = value; }
        [JsonIgnore]
        public DateTimeOffset DateModify { get => date_modify; set => date_modify = value; }
        [JsonIgnore]
        public DateTimeOffset? DateBegin { get => date_begin; set => date_begin = value; }
        [JsonIgnore]
        public DateTimeOffset? DateComplete { get => date_complete; set => date_complete = value; }

        // Ownership and audit
        [JsonIgnore]
        public long OwnerId { get => owner_id; set => owner_id = value; }
        [JsonIgnore]
        public long WhoChangeId { get => who_change_id; set => who_change_id = value; }

        // Additional identifiers
        [JsonIgnore]
        public long? Key { get => key; set => key = value; }
        
        // Primitive values stored directly in _objects table (for primitive schemas)
        [JsonIgnore]
        public long? ValueLong { get => value_long; set => value_long = value; }
        [JsonIgnore]
        public string? ValueString { get => value_string; set => value_string = value; }
        [JsonIgnore]
        public Guid? ValueGuid { get => value_guid; set => value_guid = value; }
        [JsonIgnore]
        public bool? ValueBool { get => value_bool; set => value_bool = value; }
        [JsonIgnore]
        public double? ValueDouble { get => value_double; set => value_double = value; }
        [JsonIgnore]
        public decimal? ValueNumeric { get => value_numeric; set => value_numeric = value; }
        [JsonIgnore]
        public DateTimeOffset? ValueDatetime { get => value_datetime; set => value_datetime = value; }
        [JsonIgnore]
        public byte[]? ValueBytes { get => value_bytes; set => value_bytes = value; }
        [JsonIgnore]
        public string? ValueUnique { get => value_unique; set => value_unique = value; }

        // Object state
        [JsonIgnore]
        public string? Note { get => note; set => note = value; }
        [JsonIgnore]
        public Guid? Hash { get => hash; set => hash = value; }

        // ===== METADATA ACCESS METHODS =====

        /// <summary>
        /// Get object scheme by scheme_id (using cache)
        /// </summary>
        public async Task<IRedbScheme?> GetSchemeAsync()
        {
            if (_globalProvider == null)
                return null;

            return await _globalProvider.GetSchemeByIdAsync(scheme_id);
        }

        /// <summary>
        /// Get object scheme structures (using cache)
        /// </summary>
        public async Task<IReadOnlyCollection<IRedbStructure>?> GetStructuresAsync()
        {
            var scheme = await GetSchemeAsync();
            return scheme?.Structures;
        }

        /// <summary>
        /// Get structure by field name (using cache)
        /// </summary>
        public async Task<IRedbStructure?> GetStructureByNameAsync(string fieldName)
        {
            var scheme = await GetSchemeAsync();
            return scheme?.GetStructureByName(fieldName);
        }

        /// <summary>
        /// Invalidate cache of this object's scheme
        /// </summary>
        public void InvalidateSchemeCache()
        {
            if (_globalProvider is ISchemeCacheProvider cacheProvider)
            {
                cacheProvider.InvalidateSchemeCache(scheme_id);
            }
        }

        /// <summary>
        /// Reset object ID and optionally ParentId (IRedbObject.ResetId implementation)
        /// </summary>
        /// <param name="withParent">If true, also resets ParentId to null (default true)</param>
        public void ResetId(bool withParent = true)
        {
            id = 0;
            if (withParent)
            {
                parent_id = null;
            }
        }
        
        /// <summary>
        /// Reset object ID and ParentId (base implementation of IRedbObject.ResetIds)
        /// Recursive processing is overridden in RedbObject&lt;TProps&gt;
        /// </summary>
        /// <param name="recursive">If true, should recursively reset IDs in all nested IRedbObject</param>
        public virtual void ResetIds(bool recursive = false)
        {
            id = 0;
            parent_id = null;

            // Recursive logic is overridden in descendants with access to Props
        }

        /// <summary>
        /// The object's canonical JSON - the same contract <c>get_object_json(id)</c> returns and
        /// the same serializer that parses it, so a loaded object stringifies to exactly what the
        /// database holds. A lazy stub never triggers loading: the stub write converter emits base
        /// fields only (<c>"properties": null</c>). Mind the price in logs - a large Props graph
        /// (a blob above all) serializes whole; the debugger is unaffected, it shows the short
        /// DebuggerDisplay line instead.
        /// </summary>
        public override string ToString()
        {
            try
            {
                // Serialize AS the canonical RedbObject<T> view, whatever the runtime type: the
                // stub write converter matches exactly RedbObject<> (review find) - serializing a
                // TreeRedbObject<T> by its own type would bypass it into reflection, which walks
                // Parent/Children (a cycle) and the Props GETTER (a lazy load). The canonical view
                // is also what get_object_json returns - tree navigation is not part of it.
                for (var t = GetType(); t != null; t = t.BaseType)
                {
                    if (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(RedbObject<>))
                        return System.Text.Json.JsonSerializer.Serialize(
                            this, t, Serialization.SystemTextJsonRedbSerializer.Options);
                }

                // No RedbObject<T> shape in the hierarchy (the non-generic RedbObject of an Object
                // scheme): reflection would hit surprise getters, so write the short form by hand.
                return $"{{\"id\":{id},\"scheme_id\":{scheme_id},\"name\":{System.Text.Json.JsonSerializer.Serialize(name)},\"hash\":{System.Text.Json.JsonSerializer.Serialize(hash)}}}";
            }
            catch (Exception ex)
            {
                // ToString must not throw (debuggers and log formatters call it blindly), and the
                // failure must not vanish either - it rides inside the fallback JSON.
                return $"{{\"id\":{id},\"scheme_id\":{scheme_id},\"toStringError\":\"{ex.GetType().Name}: {System.Text.Json.JsonEncodedText.Encode(ex.Message)}\"}}";
            }
        }
    }
}
