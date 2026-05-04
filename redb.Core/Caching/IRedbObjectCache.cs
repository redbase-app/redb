using System;
using System.Collections.Generic;
using redb.Core.Models.Entities;

namespace redb.Core.Caching
{
    /// <summary>
    /// Interface for caching WHOLE RedbObject objects (not just Props!)
    /// Cache key = objectId, Hash is used for validity validation
    /// Cache entire object with base fields + Props for:
    /// - 0 DB queries on Cache HIT (without SELECT base fields)
    /// - Reuse of nested RedbObject through references (memory savings)
    /// </summary>
    public interface IRedbObjectCache
    {
        /// <summary>
        /// Get WHOLE RedbObject from cache by objectId with hash validation
        /// </summary>
        /// <param name="objectId">Object ID</param>
        /// <param name="currentHash">Current object hash for validation</param>
        /// <returns>RedbObject if found and hash matches, otherwise null</returns>
        RedbObject<TProps>? Get<TProps>(long objectId, Guid currentHash) where TProps : class, new();
        
        /// <summary>
        /// Get WHOLE RedbObject from cache by objectId WITHOUT hash validation
        /// Used when SkipHashValidationOnCacheCheck = true (monolithic applications)
        /// </summary>
        /// <param name="objectId">Object ID</param>
        /// <returns>RedbObject if found in cache, otherwise null (without actuality check)</returns>
        RedbObject<TProps>? GetWithoutHashValidation<TProps>(long objectId) where TProps : class, new();
        
        /// <summary>
        /// Save WHOLE RedbObject to cache
        /// </summary>
        /// <param name="obj">RedbObject for caching (with base fields + Props)</param>
        void Set<TProps>(RedbObject<TProps> obj) where TProps : class, new();
        
        /// <summary>
        /// BULK: determine which objects need to be loaded from DB (set difference)
        /// Returns cached WHOLE objects (not just Props)
        /// </summary>
        /// <param name="objects">List of (objectId, hash) from DB</param>
        /// <param name="fromCache">OUT: RedbObject instances taken from cache</param>
        /// <returns>HashSet of objectId that need to be loaded from DB</returns>
        HashSet<long> FilterNeedToLoad<TProps>(
            List<(long objectId, Guid hash)> objects,
            out Dictionary<long, RedbObject<TProps>> fromCache) where TProps : class, new();
        
        /// <summary>
        /// Remove object from cache
        /// </summary>
        /// <param name="objectId">Object ID</param>
        void Remove(long objectId);
        
        /// <summary>
        /// Clear entire cache
        /// </summary>
        void Clear();
        
        /// <summary>
        /// Get cache statistics
        /// </summary>
        PropsCacheStatistics GetStats();
        
        // === LEGACY METHODS (COMMENTED OUT - use Get/Set for full RedbObject) ===
        
        // /// <summary>
        // /// [LEGACY] Get only Props (for backward compatibility)
        // /// </summary>
        // [Obsolete("Use Get<TProps>(objectId, hash) to get full RedbObject instead of only Props")]
        // TProps? GetProps<TProps>(long objectId, Guid currentHash) where TProps : class, new();
        
        // /// <summary>
        // /// [LEGACY] Save only Props (for backward compatibility)
        // /// </summary>
        // [Obsolete("Use Set<TProps>(redbObject) to save full RedbObject instead of only Props")]
        // void SetProps<TProps>(long objectId, Guid hash, TProps props) where TProps : class, new();
    }
}

