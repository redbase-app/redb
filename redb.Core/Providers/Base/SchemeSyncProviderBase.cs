using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using redb.Core.Attributes;
using redb.Core.Caching;
using redb.Core.Data;
using redb.Core.Models.Configuration;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Core.Query;
using redb.Core.Utils;
using Microsoft.Extensions.Logging;

namespace redb.Core.Providers.Base;

/// <summary>
/// Base class for scheme synchronization provider.
/// Contains all reflection logic and type mapping which is database-agnostic.
/// SQL queries are delegated to ISqlDialect.
/// </summary>
public abstract class SchemeSyncProviderBase : ISchemeSyncProvider, ISchemeCacheProvider
{
    protected readonly IRedbContext Context;
    protected readonly RedbServiceConfiguration Configuration;
    protected readonly ISqlDialect Sql;
    protected readonly ILogger? Logger;
    
    /// <summary>
    /// Cache domain identifier for isolating caches between different database connections.
    /// </summary>
    protected readonly string CacheDomain;
    
    /// <summary>
    /// Domain-bound metadata cache for this provider.
    /// </summary>
    public GlobalMetadataCache Cache { get; }
    
    /// <summary>
    /// Domain-bound list cache for this provider.
    /// </summary>
    public GlobalListCache ListCache { get; }
    
    /// <summary>
    /// Domain-bound props/object cache for this provider.
    /// </summary>
    public GlobalPropsCache PropsCache { get; }
    
    // Structure tree cache for fast hierarchy access
    protected static readonly ConcurrentDictionary<long, List<StructureTreeNode>> StructureTreeCache = new();
    protected static readonly ConcurrentDictionary<(long, long?), List<StructureTreeNode>> SubtreeCache = new();
    
    // C# type to REDB type mapping cache
    private static Dictionary<Type, string>? _csharpToRedbTypeCache;

    protected SchemeSyncProviderBase(
        IRedbContext context,
        ISqlDialect sql,
        RedbServiceConfiguration? configuration = null,
        string? cacheDomain = null,
        ILogger? logger = null)
    {
        Context = context ?? throw new ArgumentNullException(nameof(context));
        Sql = sql ?? throw new ArgumentNullException(nameof(sql));
        Configuration = configuration ?? new RedbServiceConfiguration();
        CacheDomain = cacheDomain ?? Configuration.GetEffectiveCacheDomain();
        
        // Initialize domain-bound caches
        Cache = new GlobalMetadataCache(CacheDomain);
        Cache.Initialize(Configuration);
        
        ListCache = new GlobalListCache(CacheDomain, Configuration.EnableListCache);
        ListCache.SetTtl(Configuration.ListCacheTtl);
        
        PropsCache = new GlobalPropsCache(CacheDomain);
        
        Logger = logger;
    }

    // ============================================================
    // === MAIN SYNC METHODS ===
    // ============================================================

    public async Task<IRedbScheme> EnsureSchemeFromTypeAsync<TProps>() where TProps : class
    {
        return await EnsureSchemeFromTypeInternalAsync(typeof(TProps), GetSchemeAliasForType<TProps>());
    }

    /// <summary>
    /// Ensures scheme exists for the given type. 
    /// Uses FullName (with namespace) as scheme name.
    /// Performs automatic migration from short name to full name if legacy scheme found.
    /// </summary>
    private async Task<IRedbScheme> EnsureSchemeFromTypeInternalAsync(Type type, string? alias = null)
    {
        var fullName = type.FullName ?? type.Name;
        var shortName = type.Name;
        
        // 1. Try to find by full name first
        var existingScheme = await Context.QueryFirstOrDefaultAsync<RedbScheme>(
            Sql.Schemes_SelectByName(), fullName);

        if (existingScheme != null)
            return existingScheme;

        // 2. Fallback: try to find by short name (legacy scheme)
        var legacyScheme = await Context.QueryFirstOrDefaultAsync<RedbScheme>(
            Sql.Schemes_SelectByName(), shortName);
        
        if (legacyScheme != null)
        {
            // Migrate: update short name to full name
            var hasTransaction = Context.CurrentTransaction != null;
            Logger?.LogInformation(
                "Migrating scheme '{OldName}' to full name '{NewName}' (ID: {SchemeId}, InTransaction: {InTx})",
                shortName, fullName, legacyScheme.Id, hasTransaction);
            
            var rowsAffected = await Context.ExecuteAsync(Sql.Schemes_UpdateName(), fullName, legacyScheme.Id);
            if (rowsAffected == 0)
            {
                Logger?.LogWarning(
                    "Migration UPDATE affected 0 rows for scheme ID {SchemeId}. SQL: {Sql}",
                    legacyScheme.Id, Sql.Schemes_UpdateName());
            }
            
            legacyScheme.Name = fullName;
            
            // Invalidate cache for both old and new names
            Cache.InvalidateScheme(legacyScheme.Id);
            Cache.InvalidateScheme(shortName);
            
            return legacyScheme;
        }

        // 3. Create new scheme with full name
        var newId = await Context.NextObjectIdAsync();
        var newScheme = new RedbScheme
        {
            Id = newId,
            Name = fullName,
            Alias = alias,
            Type = RedbTypeIds.Class
        };

        await Context.ExecuteAsync(Sql.Schemes_Insert(), newScheme.Id, newScheme.Name, newScheme.Alias, newScheme.Type);
        return newScheme;
    }

    public async Task<List<IRedbStructure>> SyncStructuresFromTypeAsync<TProps>(IRedbScheme scheme, bool strictDeleteExtra = true) where TProps : class
    {
        var existingStructures = await Context.QueryAsync<RedbStructure>(Sql.Structures_SelectByScheme(), scheme.Id);
        var structuresToKeep = new List<long>();

        await SyncStructuresRecursively(typeof(TProps), scheme.Id, null, existingStructures, structuresToKeep);

        int deletedCount = 0;
        if (strictDeleteExtra)
        {
            var idsToDelete = existingStructures
                .Where(s => !structuresToKeep.Contains(s.Id))
                .Select(s => s.Id)
                .ToList();

            if (idsToDelete.Count > 0)
            {
                deletedCount = await Context.ExecuteAsync(Sql.Structures_DeleteByIds(idsToDelete));
            }
        }

        if (deletedCount > 0 || structuresToKeep.Count > 0)
        {
            await Context.ExecuteAsync(Sql.Schemes_SyncMetadataCache(), scheme.Id);
        }
        
        var allStructures = await Context.QueryAsync<RedbStructure>(Sql.Structures_SelectBySchemeShort(), scheme.Id);
        if (allStructures.Any())
        {
            var schemeEntity = await Context.QueryFirstOrDefaultAsync<RedbScheme>(Sql.Schemes_SelectById(), scheme.Id);
            if (schemeEntity != null)
            {
                var newSchemeHash = SchemeHashCalculator.ComputeSchemeStructureHash(allStructures);
                
                if (schemeEntity.StructureHash != newSchemeHash)
                {
                    await Context.ExecuteAsync(Sql.Schemes_UpdateHash(), newSchemeHash, scheme.Id);
                    InvalidateStructureTreeCache(scheme.Id);
                    Cache.InvalidateScheme(scheme.Id);
                }
            }
        }
        
        var updatedStructures = await Context.QueryAsync<RedbStructure>(Sql.Structures_SelectByScheme(), scheme.Id);
        return updatedStructures.Cast<IRedbStructure>().ToList();
    }

    public async Task<IRedbScheme> SyncSchemeAsync<TProps>() where TProps : class
    {
        var attr = GetRedbSchemeAttribute<TProps>();
        var alias = attr?.Alias;
        
        var scheme = await EnsureSchemeFromTypeInternalAsync(typeof(TProps), alias);
        await SyncStructuresFromTypeAsync<TProps>(scheme, strictDeleteExtra: true);
        return scheme;
    }

    // ============================================================
    // === RECURSIVE STRUCTURE SYNC (platform-agnostic) ===
    // ============================================================

    private async Task SyncStructuresRecursively(
        Type type, long schemeId, long? parentId, 
        List<RedbStructure> existingStructures, List<long> structuresToKeep, 
        HashSet<Type>? visitedTypes = null)
    {
        visitedTypes ??= [];
        if (visitedTypes.Contains(type)) return;
        visitedTypes.Add(type);

        var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => !ShouldIgnoreProperty(p))
            .ToArray();
        var nullabilityContext = new NullabilityInfoContext();

        foreach (var property in properties)
        {
            var nullabilityInfo = nullabilityContext.Create(property);
            var isArray = IsArrayType(property.PropertyType);
            var isDictionary = IsDictionaryType(property.PropertyType);
            
            Type baseType;
            Type? keyType = null;
            
            if (isDictionary)
            {
                var (dictKeyType, dictValueType) = GetDictionaryKeyValueTypes(property.PropertyType);
                baseType = dictValueType;
                keyType = dictKeyType;
            }
            else if (isArray)
            {
                baseType = GetArrayElementType(property.PropertyType);
            }
            else
            {
                baseType = property.PropertyType;
            }
            
            var isRequired = nullabilityInfo.WriteState != NullabilityState.Nullable && 
                            Nullable.GetUnderlyingType(baseType) == null;
            
            var typeId = await GetTypeIdForTypeAsync(baseType);
            var structureName = property.Name;
            var typeName = await MapCSharpTypeToRedbTypeAsync(baseType);
            
            long? keyTypeId = null;
            if (isDictionary && keyType != null)
            {
                keyTypeId = RedbKeySerializer.GetKeyTypeId(keyType);
            }

            var aliasAttr = property.GetCustomAttribute<RedbAliasAttribute>();
            var structureAlias = aliasAttr?.Alias;

            var existingStructure = existingStructures
                .FirstOrDefault(s => s.Name == structureName && s.IdParent == parentId);

            if (existingStructure != null)
            {
                await UpdateExistingStructure(existingStructure, typeId, typeName, isDictionary, isArray, keyTypeId, structureAlias, isRequired);
                structuresToKeep.Add(existingStructure.Id);
            }
            else
            {
                var newStructure = await CreateNewStructure(schemeId, parentId, structureName, structureAlias, typeId, isRequired, isDictionary, isArray, keyTypeId, properties.ToList().IndexOf(property));
                existingStructures.Add(newStructure);
                structuresToKeep.Add(newStructure.Id);
            }

            if (IsBusinessClass(baseType))
            {
                var currentStructureId = existingStructure?.Id ?? 
                    existingStructures.Last(s => s.Name == structureName && s.IdParent == parentId).Id;
                
                await SyncStructuresRecursively(baseType, schemeId, currentStructureId, existingStructures, structuresToKeep, visitedTypes);
            }
        }
        
        visitedTypes.Remove(type);
    }

    private async Task UpdateExistingStructure(
        RedbStructure structure, long typeId, string typeName,
        bool isDictionary, bool isArray, long? keyTypeId, string? alias, bool isRequired)
    {
        if (structure.IdType != typeId)
        {
            await MigrateStructureTypeInternalAsync(structure.Id, structure.IdType, typeName);
            await Context.ExecuteAsync(Sql.Structures_UpdateType(), typeId, structure.Id);
            structure.IdType = typeId;
        }
        
        var newCollectionType = isDictionary ? RedbTypeIds.Dictionary 
            : (isArray ? RedbTypeIds.Array : (long?)null);
        if (structure.CollectionType != newCollectionType)
        {
            await Context.ExecuteAsync(Sql.Structures_UpdateCollectionType(), (object?)newCollectionType ?? DBNull.Value, structure.Id);
            structure.CollectionType = newCollectionType;
        }
        
        if (structure.KeyType != keyTypeId)
        {
            await Context.ExecuteAsync(Sql.Structures_UpdateKeyType(), (object?)keyTypeId ?? DBNull.Value, structure.Id);
            structure.KeyType = keyTypeId;
        }
        
        if (structure.Alias != alias)
        {
            await Context.ExecuteAsync(Sql.Structures_UpdateAlias(), (object?)alias ?? DBNull.Value, structure.Id);
            structure.Alias = alias;
        }
        
        if (structure.AllowNotNull != isRequired)
        {
            await Context.ExecuteAsync(Sql.Structures_UpdateAllowNotNull(), isRequired, structure.Id);
            structure.AllowNotNull = isRequired;
        }
    }

    private async Task<RedbStructure> CreateNewStructure(
        long schemeId, long? parentId, string name, string? alias, long typeId,
        bool isRequired, bool isDictionary, bool isArray, long? keyTypeId, int order)
    {
        var newId = await Context.NextObjectIdAsync();
        var structure = new RedbStructure
        {
            Id = newId,
            IdScheme = schemeId,
            IdParent = parentId,
            Name = name,
            Alias = alias,
            IdType = typeId,
            AllowNotNull = isRequired,
            CollectionType = isDictionary ? RedbTypeIds.Dictionary : (isArray ? RedbTypeIds.Array : null),
            KeyType = keyTypeId,
            Order = order
        };

        await Context.ExecuteAsync(Sql.Structures_Insert(),
            structure.Id, structure.IdScheme, structure.IdParent, structure.Name,
            structure.Alias, structure.IdType, structure.AllowNotNull,
            structure.CollectionType, structure.KeyType, structure.Order);

        return structure;
    }

    /// <summary>
    /// Migrate structure type (internal version with oldTypeId).
    /// </summary>
    protected virtual async Task MigrateStructureTypeInternalAsync(long structureId, long oldTypeId, string newTypeName)
    {
        var oldType = await Context.QueryFirstOrDefaultAsync<RedbType>(Sql.Types_SelectByName(), oldTypeId.ToString());
        var oldTypeName = oldType?.Name ?? "unknown";
        await MigrateStructureTypeAsync(structureId, oldTypeName, newTypeName, false);
    }

    /// <inheritdoc />
    public virtual async Task<TypeMigrationResult> MigrateStructureTypeAsync(long structureId, string oldTypeName, string newTypeName, bool dryRun = false)
    {
        var result = await Context.QueryFirstOrDefaultAsync<TypeMigrationResult>(
            Sql.Schemes_MigrateStructureType(), structureId, oldTypeName, newTypeName, dryRun);
        return result ?? new TypeMigrationResult();
    }

    // ============================================================
    // === TYPE MAPPING (platform-agnostic) ===
    // ============================================================

    private async Task<long> GetTypeIdForTypeAsync(Type type)
    {
        var underlyingType = Nullable.GetUnderlyingType(type) ?? type;
        var typeName = await MapCSharpTypeToRedbTypeAsync(underlyingType);
        
        var cachedId = Cache.GetTypeId(typeName);
        if (cachedId.HasValue)
            return cachedId.Value;
        
        var typeEntity = await Context.QueryFirstOrDefaultAsync<RedbType>(Sql.Types_SelectByName(), typeName);
        
        if (typeEntity == null)
            throw new InvalidOperationException($"Type '{typeName}' not found in _types table. Check DB schema.");
        
        Cache.CacheType(typeName, typeEntity.Id);
        return typeEntity.Id;
    }

    private async Task<string> MapCSharpTypeToRedbTypeAsync(Type csharpType)
    {
        if (_csharpToRedbTypeCache == null)
            await InitializeCSharpToRedbTypeMappingAsync();

        if (csharpType.IsGenericType && csharpType.GetGenericTypeDefinition() == typeof(RedbObject<>))
            return "Object";

        if (csharpType == typeof(IRedbListItem) || csharpType == typeof(RedbListItem))
            return "ListItem";
        
        var underlyingType = Nullable.GetUnderlyingType(csharpType);
        if (underlyingType != null && (underlyingType == typeof(IRedbListItem) || underlyingType == typeof(RedbListItem)))
            return "ListItem";

        if (IsBusinessClass(csharpType))
            return "Class";

        if (_csharpToRedbTypeCache!.TryGetValue(csharpType, out var exactMatch))
            return exactMatch;

        return "String";
    }

    private async Task InitializeCSharpToRedbTypeMappingAsync()
    {
        var allTypes = await Context.QueryAsync<RedbType>(Sql.Types_SelectAll());
        _csharpToRedbTypeCache = new Dictionary<Type, string>();

        // Sort by ID to ensure base types (String, Long, etc.) are processed first
        // Base types have more negative IDs (e.g., String=-9223372036854775700)
        // Derived types like MimeType, FilePath have less negative IDs
        foreach (var dbType in allTypes.OrderBy(t => t.Id))
        {
            var dotNetTypeName = dbType.Type1;
            if (string.IsNullOrEmpty(dotNetTypeName))
                continue;

            var csharpType = MapStringToType(dotNetTypeName);
            // Don't overwrite base type mapping with derived types (e.g., String -> MimeType)
            if (csharpType != null && !_csharpToRedbTypeCache.ContainsKey(csharpType))
            {
                _csharpToRedbTypeCache[csharpType] = dbType.Name;
            }
        }
        
        if (!_csharpToRedbTypeCache.ContainsKey(typeof(DateTime)))
        {
            var dateTimeType = allTypes.FirstOrDefault(t => t.Name == "DateTime");
            if (dateTimeType != null)
            {
                _csharpToRedbTypeCache[typeof(DateTime)] = "DateTime";
            }
        }
        
        var numericType = allTypes.FirstOrDefault(t => t.Name == "Numeric");
        if (numericType != null)
        {
            _csharpToRedbTypeCache[typeof(decimal)] = "Numeric";
        }
    }

    private static Type? MapStringToType(string typeName) => typeName switch
    {
        "string" => typeof(string),
        "int" => typeof(int),
        "long" => typeof(long),
        "short" => typeof(short),
        "byte" => typeof(byte),
        "double" => typeof(double),
        "float" => typeof(float),
        "decimal" => typeof(decimal),
        "Numeric" => typeof(decimal),
        "boolean" => typeof(bool),
        "DateTime" => typeof(DateTimeOffset),
        "DateTimeOffset" => typeof(DateTimeOffset),
        "Guid" => typeof(Guid),
        "byte[]" => typeof(byte[]),
        "char" => typeof(char),
        "TimeSpan" => typeof(TimeSpan),
#if NET6_0_OR_GREATER
        "DateOnly" => typeof(DateOnly),
        "TimeOnly" => typeof(TimeOnly),
#endif
        "RedbObjectRow" => typeof(RedbObject<>),
        "_RListItem" => null,
        "Enum" => typeof(Enum),
        _ => null
    };

    // ============================================================
    // === TYPE HELPERS (platform-agnostic) ===
    // ============================================================

    private static bool ShouldIgnoreProperty(PropertyInfo property)
    {
        // Only RedbIgnore affects DB schema. JsonIgnore is for JSON serialization (frontend).
        return property.GetCustomAttribute<RedbIgnoreAttribute>() != null;
    }

    private static bool IsArrayType(Type type)
    {
        if (IsDictionaryType(type))
            return false;
            
        return type.IsArray || 
               (type.IsGenericType && 
                (type.GetGenericTypeDefinition() == typeof(List<>) ||
                 type.GetGenericTypeDefinition() == typeof(IList<>) ||
                 type.GetGenericTypeDefinition() == typeof(ICollection<>) ||
                 type.GetGenericTypeDefinition() == typeof(IEnumerable<>)));
    }
    
    private static bool IsDictionaryType(Type type)
    {
        if (!type.IsGenericType)
            return false;
            
        var genericDef = type.GetGenericTypeDefinition();
        return genericDef == typeof(Dictionary<,>) ||
               genericDef == typeof(IDictionary<,>);
    }
    
    private static (Type KeyType, Type ValueType) GetDictionaryKeyValueTypes(Type dictionaryType)
    {
        if (!dictionaryType.IsGenericType)
            throw new ArgumentException($"Type {dictionaryType} is not a generic Dictionary", nameof(dictionaryType));
            
        var args = dictionaryType.GetGenericArguments();
        if (args.Length != 2)
            throw new ArgumentException($"Type {dictionaryType} does not have 2 generic arguments", nameof(dictionaryType));
            
        return (args[0], args[1]);
    }

    private static Type GetArrayElementType(Type arrayType)
    {
        if (arrayType.IsArray)
            return arrayType.GetElementType()!;
        
        if (arrayType.IsGenericType)
            return arrayType.GetGenericArguments()[0];
        
        return arrayType;
    }

    private static bool IsBusinessClass(Type csharpType)
    {
        if (csharpType.IsPrimitive || csharpType == typeof(string) || csharpType == typeof(decimal))
            return false;

        if (csharpType == typeof(DateTime) || csharpType == typeof(DateTimeOffset) || 
            csharpType == typeof(DateOnly) || csharpType == typeof(TimeOnly) || 
            csharpType == typeof(Guid) || csharpType == typeof(TimeSpan) || csharpType == typeof(byte[]))
            return false;

        if (Nullable.GetUnderlyingType(csharpType) != null)
            return false;

        if (csharpType.IsArray || IsArrayType(csharpType))
            return false;

        if (csharpType.IsGenericType && csharpType.GetGenericTypeDefinition() == typeof(RedbObject<>))
            return false;

        if (csharpType == typeof(IRedbListItem) || csharpType == typeof(RedbListItem))
            return false;

        if (csharpType.IsEnum)
            return false;

        if (csharpType.Namespace?.StartsWith("System") == true)
            return false;

        return csharpType.IsClass;
    }

    private static RedbSchemeAttribute? GetRedbSchemeAttribute<TProps>() where TProps : class
        => typeof(TProps).GetCustomAttribute<RedbSchemeAttribute>();

    private static RedbSchemeAttribute? GetRedbSchemeAttribute(Type type)
        => type.GetCustomAttribute<RedbSchemeAttribute>();

    // ============================================================
    // === SCHEME LOOKUP METHODS ===
    // ============================================================

    public async Task<IRedbScheme?> GetSchemeByIdAsync(long schemeId)
    {
        var cachedScheme = Cache.GetScheme(schemeId);
        if (cachedScheme != null)
        {
            var hashInDb = await Context.ExecuteScalarAsync<Guid?>(Sql.Schemes_SelectHashById(), schemeId);
            
            if (cachedScheme.StructureHash == hashInDb)
                return cachedScheme;
            
            Cache.InvalidateScheme(schemeId);
            InvalidateStructureTreeCache(schemeId);
        }
        
        var scheme = await Context.QueryFirstOrDefaultAsync<RedbScheme>(Sql.Schemes_SelectById(), schemeId);
        if (scheme == null)
            return null;
        
        var structures = await Context.QueryAsync<RedbStructure>(Sql.Structures_SelectBySchemeCacheable(), schemeId);
        scheme.SetStructures(structures);
        
        if (scheme.StructureHash == null && structures.Any())
        {
            var newHash = SchemeHashCalculator.ComputeSchemeStructureHash(structures);
            await Context.ExecuteAsync(Sql.Schemes_UpdateHash(), newHash, schemeId);
            scheme.StructureHash = newHash;
        }
        
        Cache.CacheScheme(scheme);
        return scheme;
    }
    
    public async Task<IRedbScheme?> GetSchemeByNameAsync(string schemeName)
    {
        var cachedScheme = Cache.GetScheme(schemeName);
        if (cachedScheme != null)
            return cachedScheme;
        
        var scheme = await Context.QueryFirstOrDefaultAsync<RedbScheme>(Sql.Schemes_SelectByName(), schemeName);
        if (scheme == null)
            return null;
        
        var structures = await Context.QueryAsync<RedbStructure>(Sql.Structures_SelectBySchemeCacheable(), scheme.Id);
        scheme.SetStructures(structures);
        
        Cache.CacheScheme(scheme);
        return scheme;
    }

    public async Task<IRedbScheme?> GetSchemeByTypeAsync<TProps>() where TProps : class
    {
        var schemeName = typeof(TProps).FullName ?? typeof(TProps).Name;
        
        // Check cache first
        var cached = Cache.GetScheme(schemeName);
        if (cached != null)
            return cached;
        
        // Load from DB and cache
        var scheme = await Context.QueryFirstOrDefaultAsync<RedbScheme>(Sql.Schemes_SelectByName(), schemeName);
        if (scheme != null)
            Cache.CacheScheme(scheme);
        
        return scheme;
    }

    public async Task<IRedbScheme?> GetSchemeByTypeAsync(Type type)
    {
        var schemeName = type.FullName ?? type.Name;
        
        // Check cache first
        var cached = Cache.GetScheme(schemeName);
        if (cached != null)
            return cached;
        
        // Load from DB and cache
        var scheme = await Context.QueryFirstOrDefaultAsync<RedbScheme>(Sql.Schemes_SelectByName(), schemeName);
        if (scheme != null)
            Cache.CacheScheme(scheme);
            
        return scheme;
    }
    
    /// <summary>
    /// Get scheme from cache synchronously (no DB call).
    /// Returns null if not in cache.
    /// </summary>
    public IRedbScheme? GetSchemeFromCache<TProps>() where TProps : class
    {
        return Cache.GetScheme(typeof(TProps).FullName ?? typeof(TProps).Name);
    }
    
    /// <summary>
    /// Get scheme from cache synchronously (no DB call).
    /// Returns null if not in cache.
    /// </summary>
    public IRedbScheme? GetSchemeFromCache(string schemeName)
    {
        return Cache.GetScheme(schemeName);
    }

    public async Task<IRedbScheme> LoadSchemeByTypeAsync<TProps>() where TProps : class
    {
        var scheme = await GetSchemeByTypeAsync<TProps>();
        return scheme ?? throw new ArgumentException($"Scheme for type '{typeof(TProps).Name}' not found");
    }

    public async Task<IRedbScheme> LoadSchemeByTypeAsync(Type type)
    {
        var scheme = await GetSchemeByTypeAsync(type);
        return scheme ?? throw new ArgumentException($"Scheme for type '{type.Name}' not found");
    }

    public async Task<List<IRedbScheme>> GetSchemesAsync()
    {
        var schemes = await Context.QueryAsync<RedbScheme>(Sql.Schemes_SelectAll());
        return schemes.Cast<IRedbScheme>().ToList();
    }
    
    public Task<List<IRedbStructure>> GetStructuresAsync(IRedbScheme scheme)
        => Task.FromResult(scheme.Structures.ToList());

    public async Task<List<IRedbStructure>> GetStructuresByTypeAsync<TProps>() where TProps : class
    {
        var scheme = await GetSchemeByTypeAsync<TProps>();
        if (scheme == null)
            return [];

        var structures = await Context.QueryAsync<RedbStructure>(Sql.Structures_SelectBySchemeShort(), scheme.Id);
        return structures.Cast<IRedbStructure>().ToList();
    }

    public async Task<List<IRedbStructure>> GetStructuresByTypeAsync(Type type)
    {
        var scheme = await GetSchemeByTypeAsync(type);
        if (scheme == null)
            return [];

        var structures = await Context.QueryAsync<RedbStructure>(Sql.Structures_SelectBySchemeShort(), scheme.Id);
        return structures.Cast<IRedbStructure>().ToList();
    }

    // ============================================================
    // === SCHEME EXISTS METHODS ===
    // ============================================================

    public async Task<bool> SchemeExistsForTypeAsync<TProps>() where TProps : class
    {
        var schemeName = typeof(TProps).FullName ?? typeof(TProps).Name;
        var result = await Context.ExecuteScalarAsync<long?>(Sql.Schemes_ExistsByName(), schemeName);
        return result.HasValue;
    }

    public async Task<bool> SchemeExistsForTypeAsync(Type type)
    {
        var schemeName = type.FullName ?? type.Name;
        var result = await Context.ExecuteScalarAsync<long?>(Sql.Schemes_ExistsByName(), schemeName);
        return result.HasValue;
    }

    public async Task<bool> SchemeExistsByNameAsync(string schemeName)
    {
        var result = await Context.ExecuteScalarAsync<long?>(Sql.Schemes_ExistsByName(), schemeName);
        return result.HasValue;
    }

    // ============================================================
    // === NAME/ALIAS HELPERS ===
    // ============================================================

    public string GetSchemeNameForType<TProps>() where TProps : class => typeof(TProps).FullName ?? typeof(TProps).Name;
    public string GetSchemeNameForType(Type type) => type.FullName ?? type.Name;

    public string? GetSchemeAliasForType<TProps>() where TProps : class
        => GetRedbSchemeAttribute<TProps>()?.Alias;

    public string? GetSchemeAliasForType(Type type)
        => GetRedbSchemeAttribute(type)?.Alias;

    // ============================================================
    // === OBJECT SCHEME (NON-GENERIC) ===
    // ============================================================

    public async Task<IRedbScheme> EnsureObjectSchemeAsync(string name)
    {
        var cached = Cache.GetScheme(name);
        if (cached != null)
            return cached;
        
        var existing = await Context.QueryFirstOrDefaultAsync<RedbScheme>(
            Sql.Schemes_SelectObjectByName(), name, RedbTypeIds.Object);
        
        if (existing != null)
        {
            Cache.CacheScheme(existing);
            return existing;
        }
        
        var newId = await Context.NextObjectIdAsync();
        var newScheme = new RedbScheme
        {
            Id = newId,
            Name = name,
            Type = RedbTypeIds.Object
        };
        
        await Context.ExecuteAsync(Sql.Schemes_InsertObject(), newScheme.Id, newScheme.Name, newScheme.Type);
        Cache.CacheScheme(newScheme);
        
        return newScheme;
    }
    
    public async Task<IRedbScheme?> GetObjectSchemeAsync(string name)
    {
        var cached = Cache.GetScheme(name);
        if (cached != null)
            return cached;
        
        var scheme = await Context.QueryFirstOrDefaultAsync<RedbScheme>(
            Sql.Schemes_SelectObjectByName(), name, RedbTypeIds.Object);
        
        if (scheme == null)
            return null;
        
        Cache.CacheScheme(scheme);
        return scheme;
    }

    // ============================================================
    // === ISchemeCacheProvider IMPLEMENTATION ===
    // ============================================================
    
    public void SetCacheEnabled(bool enabled) => Cache.SetEnabled(enabled);
    public bool IsCacheEnabled => Cache.IsEnabled;
    public void InvalidateCache() => Cache.Clear();
    
    public void InvalidateSchemeCache<TProps>() where TProps : class
        => Cache.InvalidateScheme(typeof(TProps).Name);
    
    public void InvalidateSchemeCache(long schemeId)
        => Cache.InvalidateScheme(schemeId);
    
    public void InvalidateSchemeCache(string schemeName)
        => Cache.InvalidateScheme(schemeName);
    
    public CacheStatistics GetCacheStatistics() => Cache.GetStatistics();
    public void ResetCacheStatistics() => Cache.ResetStatistics();
    
    public async Task WarmupCacheAsync<TProps>() where TProps : class
    {
        await GetSchemeByTypeAsync<TProps>(); // Warmup by loading scheme
    }
    
    public async Task WarmupCacheAsync(Type[] types)
    {
        foreach (var type in types) await GetSchemeByTypeAsync(type); // Warmup by loading schemes
    }
    
    public async Task WarmupAllSchemesAsync()
    {
        var allSchemes = await Context.QueryAsync<RedbScheme>(Sql.Schemes_SelectAll());
        var allStructures = await Context.QueryAsync<RedbStructure>(Sql.Structures_SelectBySchemeCacheable());
        
        var structuresByScheme = allStructures.GroupBy(s => s.IdScheme).ToDictionary(g => g.Key, g => g.ToList());
        
        foreach (var scheme in allSchemes)
        {
            if (structuresByScheme.TryGetValue(scheme.Id, out var structures))
            {
                scheme.SetStructures(structures);
            }
            else
            {
                scheme.SetStructures(Array.Empty<RedbStructure>());
            }
            
            if (scheme.StructureHash == null && scheme.Structures.Any())
            {
                scheme.StructureHash = SchemeHashCalculator.ComputeSchemeStructureHash(scheme.StructuresInternal);
                await Context.ExecuteAsync(Sql.Schemes_UpdateHash(), scheme.StructureHash, scheme.Id);
            }
        }
        
        foreach (var scheme in allSchemes)
        {
            Cache.CacheScheme(scheme);
        }
    }
    
    public CacheDiagnosticInfo GetCacheDiagnosticInfo()
    {
        var diagnosticText = Cache.GetDiagnosticInfo();
        return new CacheDiagnosticInfo
        {
            Issues = [diagnosticText],
            Recommendations = []
        };
    }
    
    public long EstimateMemoryUsage()
    {
        var stats = Cache.GetStatistics();
        var schemeCount = stats.SchemeHits + stats.SchemeMisses;
        var typeCount = stats.TypeHits + stats.TypeMisses;
        return schemeCount * 2048 + typeCount * 100;
    }

    // ============================================================
    // === STRUCTURE TREE METHODS ===
    // ============================================================

    public async Task<List<StructureTreeNode>> GetStructureTreeAsync(long schemeId)
    {
        if (StructureTreeCache.TryGetValue(schemeId, out var cachedTree))
            return cachedTree;
        
        var scheme = await GetSchemeByIdAsync(schemeId);
        if (scheme == null)
            return [];
        
        var tree = StructureTreeBuilder.BuildFromFlat(scheme.Structures.ToList());
        StructureTreeCache.TryAdd(schemeId, tree);
        
        return tree;
    }
    
    public async Task<List<StructureTreeNode>> GetSubtreeAsync(long schemeId, long? parentStructureId)
    {
        var cacheKey = (schemeId, parentStructureId);
        
        if (SubtreeCache.TryGetValue(cacheKey, out var cachedSubtree))
            return cachedSubtree;
        
        var fullTree = await GetStructureTreeAsync(schemeId);
        
        List<StructureTreeNode> subtree;
        if (parentStructureId == null)
        {
            subtree = fullTree.Where(n => n.IsRoot).ToList();
        }
        else
        {
            var allNodes = StructureTreeBuilder.FlattenTree(fullTree);
            var parentNode = allNodes.FirstOrDefault(n => n.Structure.Id == parentStructureId);
            subtree = parentNode?.Children ?? [];
        }
        
        SubtreeCache.TryAdd(cacheKey, subtree);
        return subtree;
    }
    
    public async Task<List<IRedbStructure>> GetChildrenStructuresAsync(long schemeId, long parentStructureId)
    {
        var subtree = await GetSubtreeAsync(schemeId, parentStructureId);
        return subtree.Select(n => n.Structure).ToList();
    }
    
    public async Task<StructureTreeNode?> FindStructureNodeAsync(long schemeId, long structureId)
    {
        var tree = await GetStructureTreeAsync(schemeId);
        var allNodes = StructureTreeBuilder.FlattenTree(tree);
        return allNodes.FirstOrDefault(n => n.Structure.Id == structureId);
    }
    
    public async Task<StructureTreeNode?> FindStructureByPathAsync(long schemeId, string path)
    {
        var tree = await GetStructureTreeAsync(schemeId);
        return StructureTreeBuilder.FindNodeByPath(tree, path);
    }
    
    public async Task<string> GetStructureTreeJsonAsync(long schemeId)
    {
        var result = await Context.ExecuteJsonAsync(Sql.Schemes_GetStructureTree(), schemeId);
        return result ?? "[]";
    }
    
    public async Task<TreeDiagnosticReport> ValidateStructureTreeAsync<TProps>(long schemeId) where TProps : class
    {
        var tree = await GetStructureTreeAsync(schemeId);
        return StructureTreeBuilder.DiagnoseTree(tree, typeof(TProps));
    }
    
    public void InvalidateStructureTreeCache(long schemeId)
    {
        StructureTreeCache.TryRemove(schemeId, out _);
        
        var keysToRemove = SubtreeCache.Keys.Where(k => k.Item1 == schemeId).ToList();
        foreach (var key in keysToRemove)
        {
            SubtreeCache.TryRemove(key, out _);
        }
    }
    
    public (int TreesCount, int SubtreesCount, long MemoryEstimate) GetStructureTreeCacheStats()
    {
        var treesCount = StructureTreeCache.Count;
        var subtreesCount = SubtreeCache.Count;
        var memoryEstimate = treesCount * 1024 + subtreesCount * 200;
        
        return (treesCount, subtreesCount, memoryEstimate);
    }
    
    public async Task<bool> HasChildrenStructuresAsync(long schemeId, long structureId)
    {
        var children = await GetSubtreeAsync(schemeId, structureId);
        return children.Count > 0;
    }
}

