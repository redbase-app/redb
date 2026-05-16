using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace redb.Core.Data
{
    /// <summary>
    /// Domain-isolated key cache data.
    /// </summary>
    internal class KeyCacheDomain
    {
        public readonly ConcurrentQueue<long> Keys = new();
        public readonly SemaphoreSlim Lock = new(1, 1);
        public volatile bool IsRefilling = false;
    }

    /// <summary>
    /// Base class for key generation with domain-isolated caching.
    /// Each domain (typically one per database connection) has isolated cache.
    /// Instance is bound to specific domain, static data is shared.
    /// </summary>
    public abstract class RedbKeyGeneratorBase : IKeyGenerator
    {
        // === STATIC: shared data across all instances, isolated by domain ===
        
        private static readonly ConcurrentDictionary<string, KeyCacheDomain> _domains = new();
        private static int _cacheSize = 10000;
        private const double REFILL_THRESHOLD = 0.1; // 10% of cache size

        // === INSTANCE: domain binding ===
        
        private readonly string _domain;
        
        /// <summary>
        /// Cache domain this instance is bound to.
        /// </summary>
        public string Domain => _domain;
        
        protected RedbKeyGeneratorBase(string? domain = null)
        {
            _domain = domain ?? "default";
        }
        
        private KeyCacheDomain GetCache() => _domains.GetOrAdd(_domain, _ => new KeyCacheDomain());

        // === ABSTRACT METHODS (DB-specific) ===
        
        /// <summary>
        /// Generate batch of keys from database sequence.
        /// Only batch generation - no single key methods!
        /// </summary>
        protected abstract Task<List<long>> GenerateKeysAsync(int count);

        // === PUBLIC API ===
        
        /// <summary>
        /// Get next object ID (uses shared static cache).
        /// </summary>
        public async Task<long> NextObjectIdAsync()
        {
            return await GetNextKeyAsync();
        }
        
        /// <summary>
        /// Get next value ID (uses shared static cache).
        /// </summary>
        public async Task<long> NextValueIdAsync()
        {
            return await GetNextKeyAsync();
        }
        
        /// <summary>
        /// Get batch of object IDs.
        /// </summary>
        public async Task<long[]> NextObjectIdBatchAsync(int count)
        {
            return await GetNextKeyBatchAsync(count);
        }
        
        /// <summary>
        /// Get batch of value IDs.
        /// </summary>
        public async Task<long[]> NextValueIdBatchAsync(int count)
        {
            return await GetNextKeyBatchAsync(count);
        }
        
        // === CORE CACHE LOGIC ===
        
        private async Task<long> GetNextKeyAsync()
        {
            var cache = GetCache();
            
            // Try to get from cache
            if (cache.Keys.TryDequeue(out long key))
            {
                // Check if background refill needed (< threshold)
                int currentCount = cache.Keys.Count;
                int threshold = (int)(_cacheSize * REFILL_THRESHOLD);
                
                if (currentCount <= threshold && !cache.IsRefilling)
                {
                    // Fire-and-forget: background thread with separate connection
                    _ = Task.Run(async () => await RefillCacheBackgroundAsync());
                }
                
                return key;
            }
            
            // Cache empty - WAIT until fully refilled
            await RefillCacheBlockingAsync();
            
            // Must have keys now
            cache.Keys.TryDequeue(out key);
            return key;
        }
        
        private async Task<long[]> GetNextKeyBatchAsync(int count)
        {
            if (count <= 0) return Array.Empty<long>();
            
            var cache = GetCache();
            var result = new long[count];
            int collected = 0;
            
            while (collected < count)
            {
                // Try to get from cache
                if (cache.Keys.TryDequeue(out long key))
                {
                    result[collected++] = key;
                    
                    // Check if background refill needed
                    int currentCount = cache.Keys.Count;
                    int threshold = (int)(_cacheSize * REFILL_THRESHOLD);
                    
                    if (currentCount <= threshold && !cache.IsRefilling)
                    {
                        _ = Task.Run(async () => await RefillCacheBackgroundAsync());
                    }
                }
                else
                {
                    // Cache empty - wait for refill
                    await RefillCacheBlockingAsync();
                }
            }
            
            return result;
        }

        // === CACHE REFILL ===
        
        /// <summary>
        /// Background refill - runs in separate thread with separate DB connection.
        /// Called when cache is below threshold but not empty.
        /// </summary>
        private async Task RefillCacheBackgroundAsync()
        {
            var cache = GetCache();
            
            // Non-blocking check
            if (cache.IsRefilling)
                return;
            
            if (!await cache.Lock.WaitAsync(0))
                return;
            
            try
            {
                cache.IsRefilling = true;
                
                int currentCount = cache.Keys.Count;
                int keysToGenerate = _cacheSize - currentCount;
                
                if (keysToGenerate <= 0)
                    return;
                
                // Separate DB connection in background thread
                var keys = await GenerateKeysAsync(keysToGenerate);
                
                foreach (var newKey in keys)
                {
                    cache.Keys.Enqueue(newKey);
                }
            }
            finally
            {
                cache.IsRefilling = false;
                cache.Lock.Release();
            }
        }
        
        /// <summary>
        /// Blocking refill - waits until cache is fully filled.
        /// Called when cache is EMPTY - must wait for keys.
        /// </summary>
        private async Task RefillCacheBlockingAsync()
        {
            var cache = GetCache();
            
            // Wait for semaphore (blocks until we can refill)
            await cache.Lock.WaitAsync();
            
            try
            {
                // Double-check if another thread filled while we waited
                if (cache.Keys.Count > 0)
                    return;
                
                cache.IsRefilling = true;
                
                // Generate full batch
                var keys = await GenerateKeysAsync(_cacheSize);
                
                foreach (var newKey in keys)
                {
                    cache.Keys.Enqueue(newKey);
                }
            }
            finally
            {
                cache.IsRefilling = false;
                cache.Lock.Release();
            }
        }

        // === CONFIGURATION ===
        
        /// <summary>
        /// Set cache size (default 10000).
        /// </summary>
        public static void SetCacheSize(int size)
        {
            _cacheSize = size;
        }
        
        /// <summary>
        /// Clear cache for this domain.
        /// </summary>
        public void ClearCache()
        {
            var cache = GetCache();
            while (cache.Keys.TryDequeue(out _)) { }
        }
        
        /// <summary>
        /// Clear cache for all domains (for testing/reset).
        /// </summary>
        public static void ClearAllCaches()
        {
            foreach (var domain in _domains.Values)
            {
                while (domain.Keys.TryDequeue(out _)) { }
            }
        }
    }
}

