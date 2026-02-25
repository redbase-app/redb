using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Microsoft.Extensions.Logging;
using redb.Core.Models.Entities;

namespace redb.Core.Caching
{
    /// <summary>
    /// In-memory cache implementation for WHOLE RedbObject (not just Props!).
    /// Cache key = objectId, Hash is used for automatic invalidation.
    /// We cache entire object for 0 DB queries on Cache HIT and nested object reuse through references.
    /// </summary>
    public class MemoryRedbObjectCache : IRedbObjectCache
    {
        private readonly Dictionary<long, CacheEntry> _cache = new();
        private readonly ReaderWriterLockSlim _lock = new();
        private readonly int _maxSize;
        private readonly TimeSpan _ttl;
        private readonly ILogger? _logger;
        
        // Fields for user quota accounting
        private readonly Func<long>? _getUserIdFunc;
        private readonly Func<long, System.Threading.Tasks.Task<int?>>? _getQuotaFunc;
        private readonly Dictionary<long, HashSet<long>> _userOwnedObjects = new();
        
        private long _hitCount;
        private long _missCount;
        
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="maxSize">Maximum number of objects in cache (global limit)</param>
        /// <param name="ttl">Cache entry time-to-live</param>
        /// <param name="getUserIdFunc">Function to get current userId (for quotas)</param>
        /// <param name="getQuotaFunc">Function to get user quota (returns null for sys)</param>
        /// <param name="logger">Optional logger for cache diagnostics</param>
        public MemoryRedbObjectCache(
            int maxSize = 10000, 
            TimeSpan? ttl = null,
            Func<long>? getUserIdFunc = null,
            Func<long, System.Threading.Tasks.Task<int?>>? getQuotaFunc = null,
            ILogger? logger = null)
        {
            _maxSize = maxSize;
            _ttl = ttl ?? TimeSpan.FromMinutes(30);
            _getUserIdFunc = getUserIdFunc;
            _getQuotaFunc = getQuotaFunc;
            _logger = logger;
        }
        
        /// <summary>
        /// Get WHOLE RedbObject from cache with hash validation.
        /// </summary>
        public RedbObject<TProps>? Get<TProps>(long objectId, Guid currentHash) where TProps : class, new()
        {
            _lock.EnterUpgradeableReadLock();
            try
            {
                if (!_cache.TryGetValue(objectId, out var entry))
                {
                    Interlocked.Increment(ref _missCount);
                    return null;  // Not in cache
                }
                
                // TTL check
                if (DateTime.UtcNow - entry.CreatedAt > _ttl)
                {
                    Interlocked.Increment(ref _missCount);
                    return null;  // Expired by time
                }
                
                // Hash check
                if (entry.Hash != currentHash)
                {
                    Interlocked.Increment(ref _missCount);
                    return null;  // Hash changed → data outdated
                }
                
                // All checks passed - Cache HIT
                Interlocked.Increment(ref _hitCount);
                
                // Update access metadata
                _lock.EnterWriteLock();
                try
                {
                    entry.LastAccessAt = DateTime.UtcNow;
                    entry.AccessCount++;
                    
                    // Add current user as co-owner
                    if (_getUserIdFunc != null)
                    {
                        var userId = _getUserIdFunc();
                        entry.OwnerUserIds.Add(userId);
                        
                        // Register in user accounting
                        if (!_userOwnedObjects.ContainsKey(userId))
                        {
                            _userOwnedObjects[userId] = new HashSet<long>();
                        }
                        _userOwnedObjects[userId].Add(objectId);
                    }
                }
                finally
                {
                    _lock.ExitWriteLock();
                }
                
                return entry.RedbObject as RedbObject<TProps>;
            }
            finally
            {
                _lock.ExitUpgradeableReadLock();
            }
        }
        
        /// <summary>
        /// Get WHOLE RedbObject from cache WITHOUT hash validation.
        /// Used for monolithic applications (SkipHashValidationOnCacheCheck = true).
        /// </summary>
        public RedbObject<TProps>? GetWithoutHashValidation<TProps>(long objectId) where TProps : class, new()
        {
            _lock.EnterUpgradeableReadLock();
            try
            {
                if (!_cache.TryGetValue(objectId, out var entry))
                {
                    Interlocked.Increment(ref _missCount);
                    return null;  // Not in cache
                }
                
                // TTL check
                if (DateTime.UtcNow - entry.CreatedAt > _ttl)
                {
                    Interlocked.Increment(ref _missCount);
                    return null;  // Expired by time
                }
                
                // ✅ Skip hash check - trust cache
                
                // Cache HIT
                Interlocked.Increment(ref _hitCount);
                
                // Update access metadata
                _lock.EnterWriteLock();
                try
                {
                    entry.LastAccessAt = DateTime.UtcNow;
                    entry.AccessCount++;
                    
                    // Add current user as co-owner
                    if (_getUserIdFunc != null)
                    {
                        var userId = _getUserIdFunc();
                        entry.OwnerUserIds.Add(userId);
                        
                        // Register in user accounting
                        if (!_userOwnedObjects.ContainsKey(userId))
                        {
                            _userOwnedObjects[userId] = new HashSet<long>();
                        }
                        _userOwnedObjects[userId].Add(objectId);
                    }
                }
                finally
                {
                    _lock.ExitWriteLock();
                }
                
                return entry.RedbObject as RedbObject<TProps>;
            }
            finally
            {
                _lock.ExitUpgradeableReadLock();
            }
        }
        
        /// <summary>
        /// Save WHOLE RedbObject to cache.
        /// </summary>
        // 🛡️ Protection from recursion: scheme_id for UserConfigurationProps
        private static long? _userConfigSchemeId = null;
        private static readonly object _schemeIdLock = new();
        
        public void Set<TProps>(RedbObject<TProps> obj) where TProps : class, new()
        {
            if (!obj.hash.HasValue) return;  // Cannot cache without hash
            
            // 🛡️ Determine scheme_id for UserConfigurationProps (once)
            if (_userConfigSchemeId == null && typeof(TProps).Name == "UserConfigurationProps")
            {
                lock (_schemeIdLock)
            {
                    _userConfigSchemeId ??= obj.scheme_id;
                }
            }
            
            // FIX DEADLOCK: Get userId and quota BEFORE acquiring lock
            // BUT: skip for UserConfigurationProps to avoid infinite recursion!
                long userId = _getUserIdFunc?.Invoke() ?? 0;
                int? quota = null;
            Dictionary<long, int?>? allQuotas = null;
            
            // DO NOT call getQuotaFunc for UserConfigurationProps — this will cause recursion!
            bool isUserConfig = _userConfigSchemeId.HasValue && obj.scheme_id == _userConfigSchemeId.Value;
            
            if (_getQuotaFunc != null && !isUserConfig)
            {
                try
                {
                    quota = _getQuotaFunc(userId).GetAwaiter().GetResult();
                    
                    // Preload quotas for EvictGlobalObject (need all users)
                    allQuotas = PreloadQuotasForEviction();
                }
                catch
                {
                    // If quota couldn't be obtained - work without quotas
                    quota = null;
                }
            }
            
            _lock.EnterWriteLock();
            try
            {
                // Check: is object already in cache?
                if (_cache.TryGetValue(obj.id, out var existing))
                {
                    // Update existing object
                    existing.Hash = obj.hash.Value;
                    existing.RedbObject = obj;
                    existing.LastAccessAt = DateTime.UtcNow;
                    existing.OwnerUserIds.Add(userId);  // Add user as owner
                    return;
                }
                
                // Object is new - check user quota (if quota != null, i.e. not sys)
                if (quota.HasValue && _userOwnedObjects.ContainsKey(userId))
                {
                    var userObjects = _userOwnedObjects[userId];
                    if (userObjects.Count >= quota.Value)
                    {
                        // User quota exceeded - try to evict
                        bool evicted = EvictUserObject(userId);
                        
                        if (!evicted)
                        {
                            // Eviction failed (all objects protected by multiple owners)
                            _logger?.LogDebug(
                                "Quota exceeded for user {UserId}: {Count}/{Quota}. Object {ObjectId} NOT cached (all objects protected)",
                                userId, userObjects.Count, quota.Value, obj.id);
                            return;
                        }
                    }
                }
                
                // Check global limit
                if (_cache.Count >= _maxSize)
                {
                    // Global limit exceeded - smart eviction (use preloaded quotas)
                    bool evicted = EvictGlobalObjectWithQuotas(allQuotas);
                    
                    if (!evicted)
                    {
                        // Global cache full and eviction failed
                        _logger?.LogDebug(
                            "Global cache full ({Count}/{MaxSize}). Object {ObjectId} NOT cached",
                            _cache.Count, _maxSize, obj.id);
                        return;
                    }
                }
                
                // Add new object
                _cache[obj.id] = new CacheEntry
                {
                    ObjectId = obj.id,
                    Hash = obj.hash.Value,
                    RedbObject = obj,
                    CreatedAt = DateTime.UtcNow,
                    LastAccessAt = DateTime.UtcNow,
                    OwnerUserIds = new HashSet<long> { userId },
                    AccessCount = 0
                };
                
                // Register ownership
                if (!_userOwnedObjects.ContainsKey(userId))
                {
                    _userOwnedObjects[userId] = new HashSet<long>();
                }
                _userOwnedObjects[userId].Add(obj.id);
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }
        
        /// <summary>
        /// Preload quotas for all users (called BEFORE lock acquisition).
        /// </summary>
        private Dictionary<long, int?>? PreloadQuotasForEviction()
        {
            if (_getQuotaFunc == null) return null;
            
            // Read userId list under ReadLock (fast)
            List<long> userIds;
            _lock.EnterReadLock();
            try
            {
                userIds = _userOwnedObjects.Keys.ToList();
            }
            finally
            {
                _lock.ExitReadLock();
            }
            
            // Load quotas OUTSIDE lock (may call async operations)
            var quotas = new Dictionary<long, int?>();
            foreach (var uid in userIds)
            {
                try
                {
                    quotas[uid] = _getQuotaFunc(uid).GetAwaiter().GetResult();
                }
                catch
                {
                    quotas[uid] = null;
                }
            }
            
            return quotas;
        }
        
        /// <summary>
        /// BULK: determine which objects need to be loaded from DB.
        /// Returns cached WHOLE RedbObject instances.
        /// </summary>
        public HashSet<long> FilterNeedToLoad<TProps>(
            List<(long objectId, Guid hash)> objects,
            out Dictionary<long, RedbObject<TProps>> fromCache) where TProps : class, new()
        {
            var cacheDict = new Dictionary<long, RedbObject<TProps>>();
            
            _lock.EnterReadLock();
            try
            {
                // First collect what's in cache
                foreach (var obj in objects)
                {
                    var cached = GetInternal<TProps>(obj.objectId, obj.hash);
                    if (cached != null)
                    {
                        cacheDict[obj.objectId] = cached;
                    }
                }
                
                // LINQ: set difference
                var allIds = objects.Select(o => o.objectId).ToHashSet();
                var inCache = cacheDict.Keys.ToHashSet();
                var needToLoad = allIds.Except(inCache).ToHashSet();
                
                fromCache = cacheDict;
                return needToLoad;
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }
        
        // === EVICTION METHODS (for quotas) ===
        
        /// <summary>
        /// Evict user object (used when quota exceeded).
        /// Evicts only objects where user is the sole owner.
        /// </summary>
        /// <returns>true if eviction occurred, false if no objects to evict</returns>
        private bool EvictUserObject(long userId)
        {
            if (!_userOwnedObjects.TryGetValue(userId, out var userObjects) || userObjects.Count == 0)
                return false;
                
            // Find objects where user is the sole owner
            var candidatesForEviction = userObjects
                .Where(objId => _cache.ContainsKey(objId) && _cache[objId].OwnerUserIds.Count == 1)
                .OrderBy(objId => _cache[objId].LastAccessAt)  // LRU - least recently used
                .ToList();
                
            if (candidatesForEviction.Any())
            {
                var toEvict = candidatesForEviction.First();
                
                // Remove from cache
                _cache.Remove(toEvict);
                
                // Remove from user accounting
                userObjects.Remove(toEvict);
                
                return true;  // Eviction successful
            }
            
            return false;  // No objects to evict (all protected)
        }
        
        /// <summary>
        /// Global eviction with preloaded quotas (used when global limit exceeded).
        /// Fair Share strategy: evicts user with highest quota usage percentage.
        /// ✅ FIX DEADLOCK: Quotas passed as parameter (loaded BEFORE lock acquisition).
        /// </summary>
        /// <param name="preloadedQuotas">Preloaded quotas (may be null)</param>
        /// <returns>true if eviction occurred, false if eviction impossible</returns>
        private bool EvictGlobalObjectWithQuotas(Dictionary<long, int?>? preloadedQuotas)
        {
            // 1. Find users exceeding quota (>100%)
            var usersOverQuota = new List<(long userId, int overBy)>();
            
            if (preloadedQuotas != null)
            {
                foreach (var kvp in _userOwnedObjects)
                {
                    var userId = kvp.Key;
                    var objectCount = kvp.Value.Count;
                    
                    if (preloadedQuotas.TryGetValue(userId, out var quota) && quota.HasValue && objectCount > quota.Value)
                    {
                        usersOverQuota.Add((userId, objectCount - quota.Value));
                    }
                }
            }
            
            // 2. If there are users exceeding quota - evict the one who exceeded most
            if (usersOverQuota.Any())
            {
                var userToEvict = usersOverQuota.OrderByDescending(u => u.overBy).First().userId;
                return EvictUserObject(userToEvict);
            }
            
            // 3. No one exceeded quota - use Fair Share (percentage ratio)
            var userGreediness = new List<(long userId, double greediness)>();
            
            if (preloadedQuotas != null)
            {
                foreach (var kvp in _userOwnedObjects)
                {
                    var userId = kvp.Key;
                    var objectCount = kvp.Value.Count;
                    
                    if (objectCount == 0) continue;  // Skip users without objects
                    
                    if (preloadedQuotas.TryGetValue(userId, out var quota))
                    {
                    if (quota.HasValue && quota.Value > 0)
                    {
                        // Quota usage percentage (quota = user weight)
                        var greediness = (double)objectCount / quota.Value;
                        userGreediness.Add((userId, greediness));
                    }
                    else if (!quota.HasValue)
                    {
                        // sys (quota=NULL) - minimum greediness (protected)
                        userGreediness.Add((userId, 0.0));
                        }
                    }
                }
            }
            
            // 4. Evict user with maximum quota usage percentage
            if (userGreediness.Any())
            {
                var greedyUser = userGreediness
                    .OrderByDescending(u => u.greediness)
                    .ThenByDescending(u => _userOwnedObjects[u.userId].Count)  // At equal greediness - who has more objects
                    .First().userId;
                
                return EvictUserObject(greedyUser);  // He will evict the oldest (LRU)
            }
            
            // 5. Fallback: simple FIFO if no quotas
            if (_cache.Any())
            {
                var oldestEntry = _cache.Values.OrderBy(e => e.CreatedAt).First();
                
                // Remove from cache
                _cache.Remove(oldestEntry.ObjectId);
                
                // Remove from accounting of all owners
                foreach (var ownerId in oldestEntry.OwnerUserIds)
                {
                    if (_userOwnedObjects.TryGetValue(ownerId, out var userObjects))
                    {
                        userObjects.Remove(oldestEntry.ObjectId);
                    }
                }
                
                return true;
            }
            
            return false;  // Cache is empty
        }
        
        // === LEGACY METHODS for backward compatibility ===
        
        // === LEGACY METHODS (COMMENTED OUT - use Get/Set for full RedbObject) ===
        
        // /// <summary>
        // /// [LEGACY] Get only Props from cached object
        // /// </summary>
        // public TProps? GetProps<TProps>(long objectId, Guid currentHash) where TProps : class, new()
        // {
        //     var obj = Get<TProps>(objectId, currentHash);
        //     return obj?.Props;
        // }
        
        // /// <summary>
        // /// [LEGACY] Save only Props (creates minimal RedbObject)
        // /// </summary>
        // public void SetProps<TProps>(long objectId, Guid hash, TProps props) where TProps : class, new()
        // {
        //     // Create minimal RedbObject for backward compatibility
        //     var obj = new RedbObject<TProps>
        //     {
        //         id = objectId,
        //         hash = hash,
        //         Props = props
        //     };
        //     Set(obj);
        // }
        
        public void Remove(long objectId)
        {
            _lock.EnterWriteLock();
            try
            {
                // Remove object from cache
                if (_cache.TryGetValue(objectId, out var entry))
                {
                    // Remove from accounting of all owners
                    foreach (var ownerId in entry.OwnerUserIds)
                    {
                        if (_userOwnedObjects.TryGetValue(ownerId, out var userObjects))
                        {
                            userObjects.Remove(objectId);
                        }
                    }
                    
                    _cache.Remove(objectId);
                }
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }
        
        public void Clear()
        {
            _lock.EnterWriteLock();
            try
            {
                _cache.Clear();
                _userOwnedObjects.Clear();  // Clear user accounting!
                _hitCount = 0;
                _missCount = 0;
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }
        
        public PropsCacheStatistics GetStats()
        {
            _lock.EnterReadLock();
            try
            {
                return new PropsCacheStatistics
                {
                    TotalEntries = _cache.Count,
                    HitCount = _hitCount,
                    MissCount = _missCount
                };
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }
        
        /// <summary>
        /// Get detailed user statistics.
        /// ✅ FIX DEADLOCK: Quotas loaded BEFORE lock acquisition.
        /// </summary>
        public Dictionary<long, UserCacheStats> GetUserStatistics()
        {
            // Step 1: Get user list under ReadLock (fast)
            List<(long userId, int objectCount)> userData;
            _lock.EnterReadLock();
            try
            {
                userData = _userOwnedObjects
                    .Select(kvp => (userId: kvp.Key, objectCount: kvp.Value.Count))
                    .ToList();
            }
            finally
            {
                _lock.ExitReadLock();
            }
            
            // Step 2: Load quotas OUTSIDE lock (may call async operations)
            var quotas = new Dictionary<long, int?>();
                    if (_getQuotaFunc != null)
                    {
                foreach (var (userId, _) in userData)
                {
                    try
                    {
                        quotas[userId] = _getQuotaFunc(userId).GetAwaiter().GetResult();
                    }
                    catch
                    {
                        quotas[userId] = null;
                    }
                }
            }
            
            // Step 3: Collect statistics
            var stats = new Dictionary<long, UserCacheStats>();
            foreach (var (userId, objectCount) in userData)
            {
                quotas.TryGetValue(userId, out var quota);
                    
                    stats[userId] = new UserCacheStats
                    {
                        UserId = userId,
                    ObjectCount = objectCount,
                        Quota = quota,
                        UsagePercent = quota.HasValue && quota.Value > 0 
                        ? (double)objectCount / quota.Value * 100 
                            : 0
                    };
                }
                
                return stats;
        }
        
        /// <summary>
        /// Internal Get method with statistics tracking (for FilterNeedToLoad).
        /// Already called inside ReadLock, so no lock needed.
        /// </summary>
        private RedbObject<TProps>? GetInternal<TProps>(long objectId, Guid currentHash) where TProps : class, new()
        {
            if (!_cache.TryGetValue(objectId, out var entry))
            {
                Interlocked.Increment(ref _missCount);
                return null;
            }
            
            if (DateTime.UtcNow - entry.CreatedAt > _ttl)
            {
                Interlocked.Increment(ref _missCount);
                return null;
            }
            
            if (entry.Hash != currentHash)
            {
                Interlocked.Increment(ref _missCount);
                return null;
            }
            
            var result = entry.RedbObject as RedbObject<TProps>;
            if (result == null)
            {
                Interlocked.Increment(ref _missCount);
                return null;
            }
            
            Interlocked.Increment(ref _hitCount);
            return result;
        }
        
        /// <summary>
        /// Cache entry - now stores WHOLE RedbObject.
        /// </summary>
        private class CacheEntry
        {
            public long ObjectId { get; set; }
            public Guid Hash { get; set; }
            public object RedbObject { get; set; } = null!;  // ✅ Now RedbObject, not Props!
            public DateTime CreatedAt { get; set; }
            
            // Fields for ownership and user quota accounting
            public DateTime LastAccessAt { get; set; }  // For LRU strategy
            public HashSet<long> OwnerUserIds { get; set; } = new();  // Who uses the object
            public int AccessCount { get; set; }  // For LFU strategy
        }
    }
    
    /// <summary>
    /// User cache statistics.
    /// </summary>
    public class UserCacheStats
    {
        /// <summary>
        /// User ID.
        /// </summary>
        public long UserId { get; set; }
        
        /// <summary>
        /// Number of user objects in cache.
        /// </summary>
        public int ObjectCount { get; set; }
        
        /// <summary>
        /// User quota (null = unlimited, for sys).
        /// </summary>
        public int? Quota { get; set; }
        
        /// <summary>
        /// Quota usage percentage (0-100+).
        /// </summary>
        public double UsagePercent { get; set; }
    }
}

