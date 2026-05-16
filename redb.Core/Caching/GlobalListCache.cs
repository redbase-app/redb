using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;

namespace redb.Core.Caching
{
    /// <summary>
    /// Cache entry with TTL.
    /// </summary>
    internal class ListCacheEntry<T>
    {
        public T Value { get; set; } = default!;
        public DateTime ExpiresAt { get; set; }
        public bool IsExpired => DateTime.UtcNow > ExpiresAt;
    }
    
    /// <summary>
    /// Domain-isolated cache data for lists.
    /// </summary>
    internal class ListCacheDomain
    {
        public bool Enabled = true;
        public TimeSpan Ttl = TimeSpan.FromMinutes(5);
        
        // Lists cache
        public readonly ConcurrentDictionary<long, ListCacheEntry<RedbList>> ListsById = new();
        public readonly ConcurrentDictionary<string, ListCacheEntry<RedbList>> ListsByName = new();
        
        // Items cache
        public readonly ConcurrentDictionary<long, ListCacheEntry<List<RedbListItem>>> ItemsByListId = new();
        public readonly ConcurrentDictionary<long, ListCacheEntry<RedbListItem>> ItemsById = new();
        
        public void Clear()
        {
            ListsById.Clear();
            ListsByName.Clear();
            ItemsByListId.Clear();
            ItemsById.Clear();
        }
    }

    /// <summary>
    /// Domain-isolated cache for lists and their items.
    /// Hybrid model: TTL for eventual consistency + local invalidation on changes.
    /// Instance is bound to specific domain, static data is shared.
    /// </summary>
    public sealed class GlobalListCache
    {
        private static readonly ConcurrentDictionary<string, ListCacheDomain> _domains = new();
        
        private readonly string _domain;
        
        /// <summary>
        /// Domain identifier for this cache instance.
        /// </summary>
        public string Domain => _domain;
        
        /// <summary>
        /// Create cache instance for specific domain.
        /// </summary>
        public GlobalListCache(string? domain = null, bool? enabled = null)
        {
            _domain = domain ?? "default";
            var cache = GetCache();
            if (enabled.HasValue)
                cache.Enabled = enabled.Value;
        }
        
        private ListCacheDomain GetCache() => _domains.GetOrAdd(_domain, _ => new ListCacheDomain());
        
        /// <summary>
        /// Enable or disable cache for this domain.
        /// </summary>
        public void SetEnabled(bool enabled)
        {
            var cache = GetCache();
            cache.Enabled = enabled;
            if (!enabled)
                cache.Clear();
        }
        
        /// <summary>
        /// Check if cache is enabled.
        /// </summary>
        public bool IsEnabled => GetCache().Enabled;
        
        /// <summary>
        /// Set TTL for the cache.
        /// </summary>
        public void SetTtl(TimeSpan ttl)
        {
            if (ttl <= TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(ttl), "TTL must be greater than 0");
            GetCache().Ttl = ttl;
        }
        
        // ===== LISTS =====
        
        /// <summary>
        /// Get list by ID.
        /// </summary>
        public RedbList? GetList(long id)
        {
            var cache = GetCache();
            if (!cache.Enabled) return null;
            
            if (cache.ListsById.TryGetValue(id, out var entry))
            {
                if (!entry.IsExpired)
                    return entry.Value;
                cache.ListsById.TryRemove(id, out _);
            }
            return null;
        }
        
        /// <summary>
        /// Get list by name.
        /// </summary>
        public RedbList? GetListByName(string name)
        {
            var cache = GetCache();
            if (!cache.Enabled) return null;
            
            if (cache.ListsByName.TryGetValue(name, out var entry))
            {
                if (!entry.IsExpired)
                    return entry.Value;
                cache.ListsByName.TryRemove(name, out _);
            }
            return null;
        }
        
        /// <summary>
        /// Cache list.
        /// </summary>
        public void CacheList(RedbList list)
        {
            var cache = GetCache();
            if (!cache.Enabled) return;
            
            var entry = new ListCacheEntry<RedbList>
            {
                Value = list,
                ExpiresAt = DateTime.UtcNow.Add(cache.Ttl)
            };
            
            cache.ListsById[list.Id] = entry;
            cache.ListsByName[list.Name] = entry;
        }
        
        // ===== ITEMS =====
        
        /// <summary>
        /// Get list items by list ID.
        /// </summary>
        public List<RedbListItem>? GetListItems(long listId)
        {
            var cache = GetCache();
            if (!cache.Enabled) return null;
            
            if (cache.ItemsByListId.TryGetValue(listId, out var entry))
            {
                if (!entry.IsExpired)
                    return entry.Value;
                cache.ItemsByListId.TryRemove(listId, out _);
            }
            return null;
        }
        
        /// <summary>
        /// Get list item by ID.
        /// </summary>
        public RedbListItem? GetListItem(long itemId)
        {
            var cache = GetCache();
            if (!cache.Enabled) return null;
            
            if (cache.ItemsById.TryGetValue(itemId, out var entry))
            {
                if (!entry.IsExpired)
                    return entry.Value;
                cache.ItemsById.TryRemove(itemId, out _);
            }
            return null;
        }
        
        /// <summary>
        /// Cache list items.
        /// </summary>
        public void CacheListItems(long listId, List<RedbListItem> items)
        {
            var cache = GetCache();
            if (!cache.Enabled) return;
            
            var itemsEntry = new ListCacheEntry<List<RedbListItem>>
            {
                Value = items,
                ExpiresAt = DateTime.UtcNow.Add(cache.Ttl)
            };
            
            cache.ItemsByListId[listId] = itemsEntry;
            
            foreach (var item in items)
            {
                var itemEntry = new ListCacheEntry<RedbListItem>
                {
                    Value = item,
                    ExpiresAt = DateTime.UtcNow.Add(cache.Ttl)
                };
                cache.ItemsById[item.Id] = itemEntry;
            }
        }
        
        // ===== INVALIDATION =====
        
        /// <summary>
        /// Invalidate list and its items.
        /// </summary>
        public void InvalidateList(long listId)
        {
            var cache = GetCache();
            if (cache.ListsById.TryRemove(listId, out var listEntry))
            {
                cache.ListsByName.TryRemove(listEntry.Value.Name, out _);
            }
            InvalidateListItems(listId);
        }
        
        /// <summary>
        /// Invalidate list items.
        /// </summary>
        public void InvalidateListItems(long listId)
        {
            var cache = GetCache();
            if (cache.ItemsByListId.TryRemove(listId, out var itemsEntry))
            {
                foreach (var item in itemsEntry.Value)
                {
                    cache.ItemsById.TryRemove(item.Id, out _);
                }
            }
        }
        
        /// <summary>
        /// Invalidate single list item.
        /// </summary>
        public void InvalidateListItem(long itemId)
        {
            GetCache().ItemsById.TryRemove(itemId, out _);
        }
        
        /// <summary>
        /// Clear cache for this domain.
        /// </summary>
        public void Clear()
        {
            GetCache().Clear();
        }
        
        /// <summary>
        /// Get cache size statistics.
        /// </summary>
        public (int Lists, int Items) GetCacheSize()
        {
            var cache = GetCache();
            return (cache.ListsById.Count, cache.ItemsById.Count);
        }
    }
}
