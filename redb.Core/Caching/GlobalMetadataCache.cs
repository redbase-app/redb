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
    
    // CLR Type registry (for polymorphic deserialization)
    public readonly ConcurrentDictionary<string, Type> SchemeNameToClrType = new();
    public readonly ConcurrentDictionary<long, Type> SchemeIdToClrType = new();
    public readonly ConcurrentDictionary<Type, long> ClrTypeToSchemeId = new();
    public bool ClrTypeRegistryInitialized = false;
    
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
        // SchemeNameToClrType, SchemeIdToClrType, ClrTypeToSchemeId stay intact
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
        SchemeNameToClrType.Clear();
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
            // Also invalidate CLR type mapping
            cache.SchemeIdToClrType.TryRemove(schemeId, out _);
            cache.SchemeNameToClrType.TryRemove(scheme.Name, out _);
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
            // Also invalidate CLR type mapping
            cache.SchemeIdToClrType.TryRemove(scheme.Id, out _);
            cache.SchemeNameToClrType.TryRemove(schemeName, out _);
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
    /// Initialize CLR type registry by scanning assemblies with RedbSchemeAttribute.
    /// </summary>
    /// <param name="schemeProvider">Provider for scheme metadata</param>
    /// <param name="logger">Optional logger for diagnostics</param>
    public async Task InitializeClrTypeRegistryAsync(ISchemeSyncProvider schemeProvider, ILogger? logger = null)
    {
        var cache = GetCache();
        
        lock (_lock)
        {
            if (cache.ClrTypeRegistryInitialized)
                return;
        }

        var assemblies = AppDomain.CurrentDomain.GetAssemblies();
        
        foreach (var assembly in assemblies)
        {
            try
            {
                var typesWithAttribute = assembly.GetTypes()
                    .Where(t => t.GetCustomAttribute<RedbSchemeAttribute>() != null)
                    .ToArray();
                    
                foreach (var type in typesWithAttribute)
                {
                    var attr = type.GetCustomAttribute<RedbSchemeAttribute>()!;
                    var schemeName = attr.GetSchemeName(type);
                    
                    // Register by scheme name
                    cache.SchemeNameToClrType[schemeName] = type;
                    
                    // Also register by alias
                    if (!string.IsNullOrEmpty(attr.Alias))
                    {
                        cache.SchemeNameToClrType[attr.Alias] = type;
                    }
                    
                    // Get scheme_id from DB and register
                    var scheme = await schemeProvider.GetSchemeByNameAsync(schemeName);
                    if (scheme != null)
                    {
                        cache.SchemeIdToClrType[scheme.Id] = type;
                        cache.ClrTypeToSchemeId[type] = scheme.Id;
                    }
                    else
                    {
                        logger?.LogWarning(
                            "Scheme '{SchemeName}' not found in database for type '{TypeName}'",
                            schemeName, type.Name);
                    }
                }
            }
            catch (ReflectionTypeLoadException ex)
            {
                logger?.LogDebug(ex, "Skipping assembly {Assembly} due to type loading issues", assembly.FullName);
            }
            catch (Exception ex)
            {
                logger?.LogDebug(ex, "Skipping assembly {Assembly} due to loading error", assembly.FullName);
            }
        }

        lock (_lock)
        {
            cache.ClrTypeRegistryInitialized = true;
        }
        
        logger?.LogInformation(
            "CLR type registry initialized for domain '{Domain}': {TypeCount} types, {SchemeIdCount} scheme_id mappings",
            _domain, cache.SchemeNameToClrType.Count, cache.SchemeIdToClrType.Count);
    }
    
    /// <summary>
    /// Get C# type by scheme ID.
    /// </summary>
    public Type? GetClrType(long schemeId)
    {
        return GetCache().SchemeIdToClrType.TryGetValue(schemeId, out var type) ? type : null;
    }
    
    /// <summary>
    /// Get C# type by scheme name.
    /// </summary>
    public Type? GetClrType(string schemeName)
    {
        return GetCache().SchemeNameToClrType.TryGetValue(schemeName, out var type) ? type : null;
    }
    
    /// <summary>
    /// Register CLR type mapping manually.
    /// </summary>
    public void RegisterClrType(string schemeName, long schemeId, Type type)
    {
        var cache = GetCache();
        cache.SchemeNameToClrType[schemeName] = type;
        cache.SchemeIdToClrType[schemeId] = type;
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
        var cache = GetCache();
        return (cache.SchemeNameToClrType.Count, cache.SchemeIdToClrType.Count);
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
