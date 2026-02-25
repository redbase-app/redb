using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace redb.Core.Data
{
    /// <summary>
    /// Base class for key generation with caching.
    /// Caching logic is database-agnostic, only SQL queries differ.
    /// Static cache shared across all instances for thread-safety.
    /// </summary>
    public abstract class RedbKeyGeneratorBase : IKeyGenerator
    {
        // === STATIC KEY CACHE (shared across all instances) ===
        
        private static readonly ConcurrentQueue<long> _keyCache = new();
        private static readonly SemaphoreSlim _cacheLock = new(1, 1);
        private static volatile bool _isRefilling = false;
        private static int _cacheSize = 10000;
        private const double REFILL_THRESHOLD = 0.1; // 10% of cache size

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
            // Try to get from cache
            if (_keyCache.TryDequeue(out long key))
            {
                // Check if background refill needed (< threshold)
                int currentCount = _keyCache.Count;
                int threshold = (int)(_cacheSize * REFILL_THRESHOLD);
                
                if (currentCount <= threshold && !_isRefilling)
                {
                    // Fire-and-forget: background thread with separate connection
                    _ = Task.Run(async () => await RefillCacheBackgroundAsync());
                }
                
                return key;
            }
            
            // Cache empty - WAIT until fully refilled
            await RefillCacheBlockingAsync();
            
            // Must have keys now
            _keyCache.TryDequeue(out key);
            return key;
        }
        
        private async Task<long[]> GetNextKeyBatchAsync(int count)
        {
            if (count <= 0) return Array.Empty<long>();
            
            var result = new long[count];
            int collected = 0;
            
            while (collected < count)
            {
                // Try to get from cache
                if (_keyCache.TryDequeue(out long key))
                {
                    result[collected++] = key;
                    
                    // Check if background refill needed
                    int currentCount = _keyCache.Count;
                    int threshold = (int)(_cacheSize * REFILL_THRESHOLD);
                    
                    if (currentCount <= threshold && !_isRefilling)
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
            // Non-blocking check
            if (_isRefilling)
                return;
            
            if (!await _cacheLock.WaitAsync(0))
                return;
            
            try
            {
                _isRefilling = true;
                
                int currentCount = _keyCache.Count;
                int keysToGenerate = _cacheSize - currentCount;
                
                if (keysToGenerate <= 0)
                    return;
                
                // Separate DB connection in background thread
                var keys = await GenerateKeysAsync(keysToGenerate);
                
                foreach (var newKey in keys)
                {
                    _keyCache.Enqueue(newKey);
                }
            }
            finally
            {
                _isRefilling = false;
                _cacheLock.Release();
            }
        }
        
        /// <summary>
        /// Blocking refill - waits until cache is fully filled.
        /// Called when cache is EMPTY - must wait for keys.
        /// </summary>
        private async Task RefillCacheBlockingAsync()
        {
            // Wait for semaphore (blocks until we can refill)
            await _cacheLock.WaitAsync();
            
            try
            {
                // Double-check if another thread filled while we waited
                if (_keyCache.Count > 0)
                    return;
                
                _isRefilling = true;
                
                // Generate full batch
                var keys = await GenerateKeysAsync(_cacheSize);
                
                foreach (var newKey in keys)
                {
                    _keyCache.Enqueue(newKey);
                }
            }
            finally
            {
                _isRefilling = false;
                _cacheLock.Release();
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
        /// Clear cache (for testing/reset).
        /// </summary>
        public static void ClearCache()
        {
            while (_keyCache.TryDequeue(out _)) { }
        }
    }
}

