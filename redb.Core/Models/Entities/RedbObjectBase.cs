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
        public string? value_string { get; set; }
        public Guid? value_guid { get; set; }
        public bool? value_bool { get; set; }
        public double? value_double { get; set; }
        public decimal? value_numeric { get; set; }
        public DateTimeOffset? value_datetime { get; set; }
        public byte[]? value_bytes { get; set; }
        public string? name { get; set; }
        public string? note { get; set; }
        
        public Guid? hash { get; set; }

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
    }
}
