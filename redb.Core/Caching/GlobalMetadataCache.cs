using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using redb.Core.Attributes;
using redb.Core.Models.Contracts;
using redb.Core.Models.Configuration;
using redb.Core.Providers;

namespace redb.Core.Caching;

/// <summary>
/// Type information for caching (DB types, not CLR types).
/// </summary>
public class RedbTypeInfo
{
    public long Id { get; set; }
    public string Name { get; set; } = "";
    public string? DbType { get; set; }
    public string? Type1 { get; set; }
}

/// <summary>
/// Domain-isolated cache data.
/// </summary>
internal class DomainCache
{
    // Cache enabled flag (per-domain)
    public bool Enabled = true;
    
    // Scheme caching
    public readonly ConcurrentDictionary<string, IRedbScheme> SchemeByName = new();
    public readonly ConcurrentDictionary<long, IRedbScheme> SchemeById = new();
    
    // DB Type caching (redb types table)
    public readonly ConcurrentDictionary<string, long> TypeCache = new();
    public readonly ConcurrentDictionary<long, RedbTypeInfo> TypeById = new();
    
    // Per-domain CLR type projection. scheme_id is a per-database fact, so this must stay isolated
    // per connection/domain. The database-independent schemeName↔Type mapping lives process-globally
    // in ClrSchemeTypeIndex (one scan serves all domains; self-healing on assembly load).
    public readonly ConcurrentDictionary<long, Type> SchemeIdToClrType = new();
    public readonly ConcurrentDictionary<Type, long> ClrTypeToSchemeId = new();
    public bool ClrTypeRegistryInitialized = false;   // per-domain warm-up done (optimization only; resolution is lazy)
    
    // Statistics
    public long SchemeHits;
    public long SchemeMisses;
    public long TypeHits;
    public long TypeMisses;
    
    /// <summary>
    /// Clear scheme cache only (does NOT clear CLR Type Registry).
    /// Use this when disabling cache - system will work slower but correctly.
    /// </summary>
    public void Clear()
    {
        // Clear only scheme/type cache, NOT CLR Type Registry
        SchemeByName.Clear();
        SchemeById.Clear();
        TypeCache.Clear();
        TypeById.Clear();
        // DO NOT clear CLR Type Registry - it's required for polymorphic deserialization
        // SchemeIdToClrType, ClrTypeToSchemeId stay intact (name↔Type lives in ClrSchemeTypeIndex)
        Interlocked.Exchange(ref SchemeHits, 0);
        Interlocked.Exchange(ref SchemeMisses, 0);
        Interlocked.Exchange(ref TypeHits, 0);
        Interlocked.Exchange(ref TypeMisses, 0);
    }
    
    /// <summary>
    /// Clear everything including CLR Type Registry (full reset).
    /// Use only for complete reinitialization or testing.
    /// </summary>
    public void ClearAll()
    {
        SchemeByName.Clear();
        SchemeById.Clear();
        TypeCache.Clear();
        TypeById.Clear();
        SchemeIdToClrType.Clear();
        ClrTypeToSchemeId.Clear();
        ClrTypeRegistryInitialized = false;
        Interlocked.Exchange(ref SchemeHits, 0);
        Interlocked.Exchange(ref SchemeMisses, 0);
        Interlocked.Exchange(ref TypeHits, 0);
        Interlocked.Exchange(ref TypeMisses, 0);
    }
    
    public void ResetStatistics()
    {
        Interlocked.Exchange(ref SchemeHits, 0);
        Interlocked.Exchange(ref SchemeMisses, 0);
        Interlocked.Exchange(ref TypeHits, 0);
        Interlocked.Exchange(ref TypeMisses, 0);
    }
}

/// <summary>
/// Metadata cache with domain isolation.
/// Combines scheme caching and CLR type registry (former AutomaticTypeRegistry).
/// Instance is bound to specific domain, static data is shared.
/// Each domain (typically one per database connection) has isolated cache.
/// </summary>
public class GlobalMetadataCache
{
    // ===== STATIC: shared data across all instances =====
    private static readonly ConcurrentDictionary<string, DomainCache> _domains = new();
    private static readonly object _lock = new();
    
    // ===== INSTANCE: domain binding =====
    private readonly string _domain;
    
    /// <summary>
    /// Cache domain this instance is bound to.
    /// </summary>
    public string Domain => _domain;
    
    /// <summary>
    /// Creates cache instance bound to specific domain.
    /// </summary>
    public GlobalMetadataCache(string? domain = null)
    {
        _domain = domain ?? "default";
    }
    
    /// <summary>
    /// Creates cache instance with domain from configuration.
    /// </summary>
    public GlobalMetadataCache(RedbServiceConfiguration configuration)
    {
        _domain = configuration?.GetEffectiveCacheDomain() ?? "default";
    }
    
    private DomainCache GetCache() => _domains.GetOrAdd(_domain, _ => new DomainCache());
    
    // ===== SCHEME CACHING =====
    
    /// <summary>
    /// Get scheme by name from cache.
    /// </summary>
    public IRedbScheme? GetScheme(string schemeName)
    {
        if (!GetCache().Enabled) return null;
        
        var cache = GetCache();
        if (cache.SchemeByName.TryGetValue(schemeName, out var scheme))
        {
            Interlocked.Increment(ref cache.SchemeHits);
            return scheme;
        }
        
        Interlocked.Increment(ref cache.SchemeMisses);
        return null;
    }
    
    /// <summary>
    /// Get scheme by ID from cache.
    /// </summary>
    public IRedbScheme? GetScheme(long schemeId)
    {
        if (!GetCache().Enabled) return null;
        
        var cache = GetCache();
        if (cache.SchemeById.TryGetValue(schemeId, out var scheme))
        {
            Interlocked.Increment(ref cache.SchemeHits);
            return scheme;
        }
        
        Interlocked.Increment(ref cache.SchemeMisses);
        return null;
    }
    
    /// <summary>
    /// Cache scheme.
    /// </summary>
    public void CacheScheme(IRedbScheme scheme)
    {
        if (!GetCache().Enabled || scheme == null) return;
        
        var cache = GetCache();
        cache.SchemeByName[scheme.Name] = scheme;
        cache.SchemeById[scheme.Id] = scheme;
    }
    
    /// <summary>
    /// Invalidate scheme by ID.
    /// </summary>
    public void InvalidateScheme(long schemeId)
    {
        var cache = GetCache();
        if (cache.SchemeById.TryGetValue(schemeId, out var scheme))
        {
            cache.SchemeByName.TryRemove(scheme.Name, out _);
            cache.SchemeById.TryRemove(schemeId, out _);
            // Drop the per-domain scheme_id→Type projection (it can be lazily re-derived). The global
            // name↔Type (ClrSchemeTypeIndex) is a code fact and is NOT touched by per-domain invalidation.
            cache.SchemeIdToClrType.TryRemove(schemeId, out _);
        }
    }
    
    /// <summary>
    /// Invalidate scheme by name.
    /// </summary>
    public void InvalidateScheme(string schemeName)
    {
        var cache = GetCache();
        if (cache.SchemeByName.TryGetValue(schemeName, out var scheme))
        {
            cache.SchemeByName.TryRemove(schemeName, out _);
            cache.SchemeById.TryRemove(scheme.Id, out _);
            // Drop the per-domain scheme_id→Type projection (lazily re-derivable). Global name↔Type
            // (ClrSchemeTypeIndex) is a code fact and is NOT touched by per-domain invalidation.
            cache.SchemeIdToClrType.TryRemove(scheme.Id, out _);
        }
    }
    
    // ===== DB TYPE CACHING =====
    
    /// <summary>
    /// Get type ID from cache.
    /// </summary>
    public long? GetTypeId(string typeName)
    {
        if (!GetCache().Enabled) return null;
        
        var cache = GetCache();
        if (cache.TypeCache.TryGetValue(typeName, out var typeId))
        {
            Interlocked.Increment(ref cache.TypeHits);
            return typeId;
        }
        
        Interlocked.Increment(ref cache.TypeMisses);
        return null;
    }
    
    /// <summary>
    /// Cache type ID.
    /// </summary>
    public void CacheType(string typeName, long typeId)
    {
        if (!GetCache().Enabled) return;
        GetCache().TypeCache[typeName] = typeId;
    }
    
    /// <summary>
    /// Get type info by ID.
    /// </summary>
    public RedbTypeInfo? GetTypeById(long typeId)
    {
        if (!GetCache().Enabled) return null;
        
        var cache = GetCache();
        if (cache.TypeById.TryGetValue(typeId, out var typeInfo))
        {
            Interlocked.Increment(ref cache.TypeHits);
            return typeInfo;
        }
        
        Interlocked.Increment(ref cache.TypeMisses);
        return null;
    }
    
    /// <summary>
    /// Bulk cache types by ID.
    /// </summary>
    public void CacheTypesById(IEnumerable<RedbTypeInfo> types)
    {
        if (!GetCache().Enabled) return;
        
        var cache = GetCache();
        foreach (var t in types)
        {
            cache.TypeById[t.Id] = t;
        }
    }
    
    /// <summary>
    /// Check if types by ID cache has data.
    /// </summary>
    public bool HasTypesByIdCache => !GetCache().TypeById.IsEmpty;
    
    // ===== CLR TYPE REGISTRY (former AutomaticTypeRegistry) =====
    
    /// <summary>
    /// Check if CLR type registry is initialized for this domain.
    /// </summary>
    public bool IsClrTypeRegistryInitialized => GetCache().ClrTypeRegistryInitialized;
    
    /// <summary>
    /// Best-effort warm-up of CLR type resolution for this domain:
    /// (1) refreshes the process-global schemeName↔Type index (<see cref="ClrSchemeTypeIndex"/>), and
    /// (2) pre-populates this domain's scheme_id→Type projection for schemes already present in the DB.
    /// This is an OPTIMIZATION only — it is never a precondition for resolution, which is lazy and
    /// self-healing (see <see cref="GetClrType(long)"/> / <see cref="ResolveClrTypeAsync"/>). The
    /// per-domain warm pass runs once; the global index stays self-healing regardless.
    /// </summary>
    /// <param name="schemeProvider">Provider for scheme metadata</param>
    /// <param name="logger">Optional logger for diagnostics</param>
    public async Task InitializeClrTypeRegistryAsync(ISchemeSyncProvider schemeProvider, ILogger? logger = null)
    {
        // Global, database-independent name↔Type layer — cheap no-op when no new assemblies loaded.
        ClrSchemeTypeIndex.EnsureFresh();

        var cache = GetCache();
        lock (_lock)
        {
            if (cache.ClrTypeRegistryInitialized)
                return;
        }

        // Pre-pop the per-domain scheme_id→Type for types whose scheme already exists in THIS domain's DB.
        foreach (var type in ClrSchemeTypeIndex.EnumerateSchemeTypes())
        {
            try
            {
                var attr = type.GetCustomAttribute<RedbSchemeAttribute>();
                if (attr == null) continue;

                var schemeName = attr.GetSchemeName(type);
                ClrSchemeTypeIndex.Register(schemeName, type);
                if (!string.IsNullOrEmpty(attr.Alias)) ClrSchemeTypeIndex.Register(attr.Alias!, type);

                var scheme = await schemeProvider.GetSchemeByNameAsync(schemeName);
                if (scheme != null)
                {
                    cache.SchemeIdToClrType[scheme.Id] = type;
                    cache.ClrTypeToSchemeId[type] = scheme.Id;
                }
            }
            catch (Exception ex)
            {
                logger?.LogDebug(ex, "Skipping type {Type} during CLR registry warm-up", type.FullName);
            }
        }

        lock (_lock)
        {
            cache.ClrTypeRegistryInitialized = true;
        }

        logger?.LogInformation(
            "CLR type registry warmed for domain '{Domain}': {NameCount} global names, {SchemeIdCount} scheme_id mappings",
            _domain, ClrSchemeTypeIndex.Count, cache.SchemeIdToClrType.Count);
    }

    /// <summary>
    /// Get C# type by scheme ID (this domain). Fast path is the per-domain cache; on a miss it lazily
    /// self-heals by deriving the type from this domain's cached scheme name + the global
    /// <see cref="ClrSchemeTypeIndex"/>, then backfills. Returns null if the scheme is not cached in this
    /// domain or genuinely has no CLR type. For the cold case (scheme not cached here) use
    /// <see cref="ResolveClrTypeAsync"/>, which can load the scheme by id.
    /// </summary>
    public Type? GetClrType(long schemeId)
    {
        var cache = GetCache();
        if (cache.SchemeIdToClrType.TryGetValue(schemeId, out var type))
            return type;

        // Lazy derive: scheme_id → (this domain's cached scheme name) → (global name↔Type).
        if (cache.SchemeById.TryGetValue(schemeId, out var scheme))
        {
            var t = ClrSchemeTypeIndex.Resolve(scheme.Name);
            if (t != null)
            {
                cache.SchemeIdToClrType[schemeId] = t;
                cache.ClrTypeToSchemeId[t] = schemeId;
                return t;
            }
        }
        return null;
    }

    /// <summary>
    /// Get C# type by scheme name — served from the process-global <see cref="ClrSchemeTypeIndex"/>
    /// (database-independent, self-healing on assembly load).
    /// </summary>
    public Type? GetClrType(string schemeName) => ClrSchemeTypeIndex.Resolve(schemeName);

    /// <summary>
    /// Resolve scheme_id → Type with a cold fallback. Tries the sync path first (per-domain cache +
    /// cached scheme); if the scheme is not cached in this domain, loads it by id and resolves via the
    /// global index, caching both for next time. Covers the cross-domain case (scheme synced under a
    /// different connection-hash domain, or created by another cluster node) with no per-call-site logic.
    /// Returns null only when the scheme genuinely has no CLR type (a legitimately non-generic scheme).
    /// </summary>
    public async Task<Type?> ResolveClrTypeAsync(long schemeId, ISchemeSyncProvider schemeProvider)
    {
        var sync = GetClrType(schemeId);
        if (sync != null) return sync;

        var scheme = await schemeProvider.GetSchemeByIdAsync(schemeId);
        if (scheme == null) return null;
        CacheScheme(scheme);   // populate this domain's scheme cache so future sync lookups hit

        var type = ClrSchemeTypeIndex.Resolve(scheme.Name);
        if (type != null)
        {
            var cache = GetCache();
            cache.SchemeIdToClrType[schemeId] = type;
            cache.ClrTypeToSchemeId[type] = schemeId;
        }
        return type;
    }

    /// <summary>
    /// Register CLR type mapping authoritatively: the database-independent name↔Type goes to the global
    /// <see cref="ClrSchemeTypeIndex"/>, the per-database scheme_id↔Type goes to this domain.
    /// </summary>
    public void RegisterClrType(string schemeName, long schemeId, Type type)
    {
        ClrSchemeTypeIndex.Register(schemeName, type);   // global (shared across domains/connections)
        var cache = GetCache();
        cache.SchemeIdToClrType[schemeId] = type;        // per-domain (scheme_id is per-database)
        cache.ClrTypeToSchemeId[type] = schemeId;
    }

    /// <summary>
    /// Get scheme_id by C# type.
    /// </summary>
    public long? GetSchemeIdByClrType(Type type)
    {
        return GetCache().ClrTypeToSchemeId.TryGetValue(type, out var schemeId) ? schemeId : null;
    }

    /// <summary>
    /// Get CLR type registry statistics.
    /// </summary>
    public (int SchemeNames, int SchemeIds) GetClrTypeStatistics()
    {
        return (ClrSchemeTypeIndex.Count, GetCache().SchemeIdToClrType.Count);
    }
    
    // ===== STATISTICS & DIAGNOSTICS =====
    
    /// <summary>
    /// Get cache statistics for this domain.
    /// </summary>
    public CacheStatistics GetStatistics()
    {
        var cache = GetCache();
        return new CacheStatistics
        {
            SchemeHits = (int)cache.SchemeHits,
            SchemeMisses = (int)cache.SchemeMisses,
            TypeHits = (int)cache.TypeHits,
            TypeMisses = (int)cache.TypeMisses,
            StructureHits = 0,
            StructureMisses = 0
        };
    }
    
    /// <summary>
    /// Clear cache for this domain.
    /// </summary>
    public void Clear()
    {
        lock (_lock)
        {
            GetCache().Clear();
        }
    }
    
    /// <summary>
    /// Get diagnostic information.
    /// </summary>
    public string GetDiagnosticInfo()
    {
        var cache = GetCache();
        var stats = GetStatistics();
        return $"Domain: {_domain}, " +
               $"Schemes: {cache.SchemeByName.Count}, " +
               $"DbTypes: {cache.TypeCache.Count}, " +
               $"ClrTypes: {cache.SchemeIdToClrType.Count}, " +
               $"Hit Rate: {stats.OverallHitRatio:P1}, " +
               $"Total Domains: {_domains.Count}";
    }
    
    // ===== STATIC METHODS: global operations =====
    
    // ===== INSTANCE METHODS FOR CACHE CONTROL =====
    
    /// <summary>
    /// Is caching enabled for this domain.
    /// </summary>
    public bool IsEnabled => GetCache().Enabled;
    
    /// <summary>
    /// Enable/disable caching for this domain.
    /// </summary>
    public void SetEnabled(bool enabled)
    {
        var cache = GetCache();
        cache.Enabled = enabled;
        if (!enabled) cache.Clear();
    }
    
    /// <summary>
    /// Initialize cache settings from configuration.
    /// </summary>
    public void Initialize(RedbServiceConfiguration configuration)
    {
        var cache = GetCache();
        cache.Enabled = configuration.EnableMetadataCache;
        if (!cache.Enabled) cache.Clear();
    }
    
    /// <summary>
    /// Reset statistics for this domain.
    /// </summary>
    public void ResetStatistics()
    {
        GetCache().ResetStatistics();
    }
    
    // ===== STATIC UTILITY METHODS =====
    
    /// <summary>
    /// Clear all domain caches.
    /// </summary>
    public static void ClearAll()
    {
        lock (_lock)
        {
            foreach (var cache in _domains.Values)
            {
                cache.Clear();
            }
            _domains.Clear();
        }
    }
    
    /// <summary>
    /// Clear cache for specific domain.
    /// </summary>
    public static void ClearDomain(string domain)
    {
        lock (_lock)
        {
            if (_domains.TryGetValue(domain, out var cache))
            {
                cache.Clear();
            }
        }
    }
    
    /// <summary>
    /// Get list of all registered domains.
    /// </summary>
    public static IReadOnlyCollection<string> GetAllDomains()
    {
        return _domains.Keys.ToList();
    }
    
    /// <summary>
    /// Reset statistics for all domains (static utility).
    /// </summary>
    public static void ResetAllStatistics()
    {
        lock (_lock)
        {
            foreach (var cache in _domains.Values)
            {
                Interlocked.Exchange(ref cache.SchemeHits, 0);
                Interlocked.Exchange(ref cache.SchemeMisses, 0);
                Interlocked.Exchange(ref cache.TypeHits, 0);
                Interlocked.Exchange(ref cache.TypeMisses, 0);
            }
        }
    }
}
