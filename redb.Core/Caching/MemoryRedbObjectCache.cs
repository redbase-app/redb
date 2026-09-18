using System;
using System.Collections.Generic;
using System.Diagnostics;
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
    /// <remarks>
    /// Production under load (2026-09-15): hit validation - the object hash and the walk of its loaded graph - ran
    /// under one process-wide lock, and an insert into a full cache sorted every entry under the write lock, so
    /// readers queued behind each other with their connections open. The lock now guards the dictionary and the
    /// least-recently-used list only; validation runs outside it, and a full cache evicts a tenth of its limit at once.
    /// </remarks>
    public class MemoryRedbObjectCache : IRedbObjectCache
    {
        private readonly object _sync = new();
        private readonly Dictionary<long, LinkedListNode<CacheEntry>> _cache = new();
        // Most recently used first.
        private readonly LinkedList<CacheEntry> _lru = new();
        private readonly int _maxSize;
        private readonly int _evictionBatch;
        private readonly TimeSpan _ttl;
        private readonly ILogger? _logger;

        private long _hitCount;
        private long _missCount;

        // Diagnostics: every warning is written at most once per window.
        private static readonly TimeSpan DiagnosticWindow = TimeSpan.FromSeconds(10);
        private static readonly TimeSpan SlowValidation = TimeSpan.FromMilliseconds(50);
        private const int UnservedLookupThreshold = 100;
        private readonly Utils.RateWindow _evictionRate;
        private readonly Utils.RateWindow _slowValidationRate = new(DiagnosticWindow, 1);
        // The served/unserved window is lock-free like RateWindow: a hit ran under a second process-wide lock for
        // this bookkeeping alone. A race at a window boundary may lose a few counts; it is diagnostics.
        private long _servedWindowStartMs = Environment.TickCount64;
        private int _servedWindowLookups;
        private int _servedWindowHits;
        private int _servedWindowWarned;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="maxSize">Maximum number of objects in cache; a full cache evicts the least recently used tenth</param>
        /// <param name="ttl">Cache entry time-to-live, counted from the last Set of the object</param>
        /// <param name="logger">Logger for the cache warnings: working set above the limit, slow hit validation, entries never served</param>
        public MemoryRedbObjectCache(int maxSize = 10000, TimeSpan? ttl = null, ILogger? logger = null)
        {
            if (maxSize <= 0)
                throw new ArgumentOutOfRangeException(nameof(maxSize), "Must be greater than 0");
            _maxSize = maxSize;
            _evictionBatch = Math.Max(1, maxSize / 10);
            _ttl = ttl ?? TimeSpan.FromMinutes(30);
            _logger = logger;
            // A fifth of the limit evicted within one window: the objects in use do not fit the cache.
            _evictionRate = new Utils.RateWindow(DiagnosticWindow, Math.Max(1, maxSize / 5));
        }

        /// <summary>
        /// Get WHOLE RedbObject from cache with hash validation.
        /// </summary>
        public RedbObject<TProps>? Get<TProps>(long objectId, Guid currentHash) where TProps : class, new()
            => GetValidated<TProps>(objectId, currentHash);

        /// <summary>
        /// Get WHOLE RedbObject from cache WITHOUT hash validation.
        /// Used for monolithic applications (SkipHashValidationOnCacheCheck = true).
        /// </summary>
        public RedbObject<TProps>? GetWithoutHashValidation<TProps>(long objectId) where TProps : class, new()
            => GetValidated<TProps>(objectId, null);

        private RedbObject<TProps>? GetValidated<TProps>(long objectId, Guid? currentHash) where TProps : class, new()
        {
            Candidate candidate;
            bool found;
            lock (_sync)
                found = FindCurrentLocked(objectId, currentHash, out candidate);

            var served = found ? Validate<TProps>(candidate) : null;
            Interlocked.Increment(ref served != null ? ref _hitCount : ref _missCount);
            return served;
        }

        /// <summary>
        /// Save WHOLE RedbObject to cache.
        /// </summary>
        public void Set<TProps>(RedbObject<TProps> obj) where TProps : class, new()
        {
            if (!obj.hash.HasValue) return;  // Cannot cache without hash

            var evicted = 0;
            lock (_sync)
            {
                var now = DateTime.UtcNow;
                if (_cache.TryGetValue(obj.id, out var node))
                {
                    // A re-Set brings the object's current state and starts a new lifetime. Keeping the first
                    // CreatedAt made an object that a periodic job re-caches on every pass miss for ever once
                    // one TTL had passed.
                    node.Value.Hash = obj.hash.Value;
                    node.Value.RedbObject = obj;
                    node.Value.CreatedAt = now;
                    MoveToFrontLocked(node);
                    return;
                }

                if (_cache.Count >= _maxSize)
                    evicted = EvictLeastRecentlyUsedLocked(_evictionBatch);

                var entry = new CacheEntry { ObjectId = obj.id, Hash = obj.hash.Value, RedbObject = obj, CreatedAt = now };
                _cache[obj.id] = _lru.AddFirst(entry);
            }

            if (evicted > 0)
                ReportEvictions(evicted);
        }

        /// <summary>
        /// BULK: determine which objects need to be loaded from DB.
        /// Returns cached WHOLE RedbObject instances.
        /// </summary>
        public HashSet<long> FilterNeedToLoad<TProps>(
            List<(long objectId, Guid hash)> objects,
            out Dictionary<long, RedbObject<TProps>> fromCache) where TProps : class, new()
        {
            var needToLoad = new HashSet<long>();
            var candidates = new List<Candidate>(objects.Count);
            lock (_sync)
            {
                foreach (var (objectId, hash) in objects)
                {
                    if (FindCurrentLocked(objectId, hash, out var candidate))
                        candidates.Add(candidate);
                    else
                    {
                        needToLoad.Add(objectId);
                        Interlocked.Increment(ref _missCount);
                    }
                }
            }

            // Validated outside the lock, the same way as a single hit.
            fromCache = new Dictionary<long, RedbObject<TProps>>();
            foreach (var candidate in candidates)
            {
                var served = Validate<TProps>(candidate);
                if (served != null)
                {
                    fromCache[candidate.ObjectId] = served;
                    Interlocked.Increment(ref _hitCount);
                }
                else
                {
                    needToLoad.Add(candidate.ObjectId);
                    Interlocked.Increment(ref _missCount);
                }
            }

            return needToLoad;
        }

        public void Remove(long objectId)
        {
            lock (_sync)
            {
                if (_cache.TryGetValue(objectId, out var node))
                    RemoveLocked(node);
            }
        }

        public void Clear()
        {
            lock (_sync)
            {
                _cache.Clear();
                _lru.Clear();
            }
            Interlocked.Exchange(ref _hitCount, 0);
            Interlocked.Exchange(ref _missCount, 0);
        }

        public PropsCacheStatistics GetStats()
        {
            int entries;
            lock (_sync)
                entries = _cache.Count;

            return new PropsCacheStatistics
            {
                TotalEntries = entries,
                HitCount = Interlocked.Read(ref _hitCount),
                MissCount = Interlocked.Read(ref _missCount)
            };
        }

        /// <summary>
        /// The entry of <paramref name="objectId"/> if it is alive and, when <paramref name="currentHash"/> is given,
        /// stored under that hash. An expired entry is dropped. Caller holds <see cref="_sync"/>.
        /// </summary>
        private bool FindCurrentLocked(long objectId, Guid? currentHash, out Candidate candidate)
        {
            candidate = default;
            if (!_cache.TryGetValue(objectId, out var node))
                return false;

            var entry = node.Value;
            if (DateTime.UtcNow - entry.CreatedAt > _ttl)
            {
                RemoveLocked(node);  // Expired by time - free the slot
                return false;
            }

            // Hash check (against the DB hash the caller passed in) → a changed hash means outdated data.
            if (currentHash.HasValue && entry.Hash != currentHash.Value)
                return false;

            candidate = new Candidate(objectId, entry.RedbObject, entry.Hash);
            return true;
        }

        /// <summary>
        /// Checks a found entry against the live object, outside the cache lock: the object hash and the walk of
        /// the loaded graph read every stored property and must not hold other readers back.
        /// </summary>
        private RedbObject<TProps>? Validate<TProps>(Candidate candidate) where TProps : class, new()
        {
            // Same objectId, different TProps: the caller reloads instead of getting a mistyped object.
            if (candidate.RedbObject is not RedbObject<TProps> typed)
                return null;

            // Never wake a stub: validating an unloaded instance would be a lazy load inside the cache probe - and a
            // deadlock with the load that probes. Nothing was stored for it to serve.
            if (!typed.IsPropsLoaded)
                return null;

            var started = Stopwatch.GetTimestamp();

            // Dirty-snapshot check. The cache holds a REFERENCE to a shared object. If it was mutated
            // in place after Set (e.g. OpenIddict's SetStatus BEFORE Save), the stored hash is stale — it
            // reflects the state at Set, not the current Props. Recompute the live hash; if it drifted,
            // the cached object no longer matches its stored hash → MISS, so the caller reloads the
            // committed state from the DB instead of getting a stale ("already redeemed") snapshot.
            // V4 (review): the root's hash is id:hash of its references - an unsaved edit INSIDE
            // a loaded nested object does not move it. Ask the loaded part of the graph as well;
            // stubs are neither touched nor woken (raw Props access only).
            var served = typed.ComputeHash() == candidate.Hash
                && !Utils.LoadedGraphInspector.HasDirtyLoadedReference(typed.GetPropsDirectly());

            ReportValidation(candidate.ObjectId, Stopwatch.GetElapsedTime(started), served);
            if (!served)
                return null;

            lock (_sync)
            {
                if (_cache.TryGetValue(candidate.ObjectId, out var node))
                    MoveToFrontLocked(node);
            }
            return typed;
        }

        private void MoveToFrontLocked(LinkedListNode<CacheEntry> node)
        {
            if (ReferenceEquals(_lru.First, node))
                return;
            _lru.Remove(node);
            _lru.AddFirst(node);
        }

        private void RemoveLocked(LinkedListNode<CacheEntry> node)
        {
            _lru.Remove(node);
            _cache.Remove(node.Value.ObjectId);
        }

        private int EvictLeastRecentlyUsedLocked(int count)
        {
            var evicted = 0;
            while (evicted < count && _lru.Last is { } last)
            {
                RemoveLocked(last);
                evicted++;
            }
            return evicted;
        }

        private void ReportEvictions(int evicted)
        {
            var inWindow = _evictionRate.Add(evicted);
            if (inWindow > 0)
                _logger?.LogWarning(
                    "Props cache is full: {Evicted} least recently used objects evicted within {Seconds}s " +
                    "(PropsCacheMaxSize = {MaxSize}). The objects in use do not fit the cache, so loads keep missing " +
                    "and caching them again; raise PropsCacheMaxSize above the working set.",
                    inWindow, (int)DiagnosticWindow.TotalSeconds, _maxSize);
        }

        private void ReportValidation(long objectId, TimeSpan elapsed, bool served)
        {
            if (elapsed >= SlowValidation && _slowValidationRate.Add() > 0)
                _logger?.LogWarning(
                    "Props cache hit validation of object {ObjectId} took {ElapsedMs} ms: the object hash and the check " +
                    "of its loaded graph for unsaved edits read every stored property, so a getter that does work " +
                    "(a lazy load, I/O) makes each cache hit of such an object that slow.",
                    objectId, (long)elapsed.TotalMilliseconds);

            var now = Environment.TickCount64;
            var start = Interlocked.Read(ref _servedWindowStartMs);
            if (now - start > (long)DiagnosticWindow.TotalMilliseconds
                && Interlocked.CompareExchange(ref _servedWindowStartMs, now, start) == start)
            {
                Interlocked.Exchange(ref _servedWindowLookups, 0);
                Interlocked.Exchange(ref _servedWindowHits, 0);
                Interlocked.Exchange(ref _servedWindowWarned, 0);
            }

            var lookups = Interlocked.Increment(ref _servedWindowLookups);
            if (served)
                Interlocked.Increment(ref _servedWindowHits);
            var unserved = 0;
            if (lookups >= UnservedLookupThreshold && Volatile.Read(ref _servedWindowHits) == 0
                && Interlocked.CompareExchange(ref _servedWindowWarned, 1, 0) == 0)
                unserved = lookups;

            if (unserved > 0)
                _logger?.LogWarning(
                    "Props cache: {Lookups} lookups within {Seconds}s found a live cached entry and served none - the " +
                    "cached objects no longer hash to what was stored. Typical causes: a read model that does not " +
                    "reproduce the saved graph (a narrower class, a renamed or added property), or objects changed in " +
                    "place after loading. The cache holds memory and gives no hits here.",
                    unserved, (int)DiagnosticWindow.TotalSeconds);
        }

        private readonly struct Candidate
        {
            public Candidate(long objectId, object redbObject, Guid hash)
            {
                ObjectId = objectId;
                RedbObject = redbObject;
                Hash = hash;
            }

            public long ObjectId { get; }
            public object RedbObject { get; }
            public Guid Hash { get; }
        }

        /// <summary>
        /// Cache entry - stores WHOLE RedbObject.
        /// </summary>
        private sealed class CacheEntry
        {
            public long ObjectId { get; init; }
            public Guid Hash { get; set; }
            public object RedbObject { get; set; } = null!;
            public DateTime CreatedAt { get; set; }
        }
    }
}
