using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using redb.Core.Attributes;
using redb.Core.Caching;
using redb.Core.Data;
using redb.Core.Exceptions;
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
    
    // C# type to REDB type mapping cache. Non-concurrent Dictionary, so it is built in a
    // local and published atomically; the lock serializes builders. Parallel scheme syncs
    // (e.g. Postgres + MSSql fixtures initializing at once) would otherwise read the cache
    // while another thread is still populating it and corrupt its internal state.
    private static Dictionary<Type, string>? _csharpToRedbTypeCache;
    private static readonly SemaphoreSlim _csharpToRedbTypeCacheLock = new(1, 1);

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
        var scheme = await EnsureSchemeFromTypeInternalAsync(typeof(TProps), GetSchemeAliasForType<TProps>());
        // Same authoritative binding as SyncSchemeAsync, so the idempotent "ensure" path (which may
        // early-return an already-existing scheme) also makes the type polymorphically loadable.
        Cache.RegisterClrType(scheme.Name, scheme.Id, typeof(TProps));
        return scheme;
    }

    /// <summary>
    /// Ensures a scheme exists for the given type and carries its name to the target.
    /// <para>
    /// The target name is the explicit <c>[RedbScheme(Name = "...")]</c> when declared, otherwise
    /// FullName. The scheme is looked up along a three-step chain — target name, FullName, short type
    /// name — and the first match is renamed in place. Renaming is a single-row UPDATE of
    /// <c>_schemes._name</c>: the id is preserved, so objects, structures and values are untouched.
    /// </para>
    /// <para>
    /// With no explicit name the target equals FullName, the first two steps collapse into one, and the
    /// behaviour is byte-for-byte the legacy one: FullName, then migrate from the short name, then create.
    /// </para>
    /// </summary>
    private async Task<IRedbScheme> EnsureSchemeFromTypeInternalAsync(Type type, string? alias = null)
    {
        var fullName = type.FullName ?? type.Name;
        var shortName = type.Name;
        var explicitName = GetExplicitSchemeName(type);
        var targetName = explicitName ?? fullName;

        if (explicitName != null)
        {
            // Fail before touching the database, naming the type — a raw constraint violation from the
            // driver would say nothing about which class is at fault.
            SchemeNameValidator.Validate(explicitName, type);
            ClrSchemeTypeIndex.ThrowIfNameConflict(explicitName, type);
        }

        // 1. Try the target name first.
        var existingScheme = await Context.QueryFirstOrDefaultAsync<RedbScheme>(
            Sql.Schemes_SelectByName(), targetName);

        if (existingScheme != null)
        {
            // The rename already happened, so nothing should still sit under a previous name. If
            // something does, the type maps to two schemes at once — an older binary re-created the
            // old one after the rename — and objects are quietly being split between them. Fail loudly;
            // silently picking one would hide real data loss.
            //
            // Only checked for explicitly named types: without an explicit name this is the untouched
            // legacy path, and tightening it would break projects that never opted in.
            if (explicitName != null)
            {
                foreach (var previousName in new[] { fullName, shortName })
                {
                    if (previousName == targetName)
                        continue;

                    var strayId = await Context.ExecuteScalarAsync<long?>(Sql.Schemes_ExistsByName(), previousName);
                    if (strayId.HasValue)
                    {
                        throw new RedbSchemeNameTakenException(
                            type, targetName, existingScheme.Id, previousName, strayId.Value);
                    }
                }
            }

            await SyncSchemeAliasAsync(existingScheme, alias);
            return existingScheme;
        }

        // 2-3. Fallback: find the scheme under a previous name and rename it in place.
        //      FullName covers "type used to live by its CLR name"; the short name covers schemes
        //      created by redb versions that predate FullName naming. Dropping the short-name step
        //      would silently orphan those old databases.
        foreach (var previousName in explicitName != null
                     ? new[] { fullName, shortName }
                     : new[] { shortName })
        {
            if (previousName == targetName)
                continue;

            var legacyScheme = await Context.QueryFirstOrDefaultAsync<RedbScheme>(
                Sql.Schemes_SelectByName(), previousName);

            if (legacyScheme == null)
                continue;

            var hasTransaction = Context.CurrentTransaction != null;
            Logger?.LogInformation(
                "Renaming scheme '{OldName}' to '{NewName}' (ID: {SchemeId}, InTransaction: {InTx})",
                previousName, targetName, legacyScheme.Id, hasTransaction);

            var rowsAffected = await Context.ExecuteAsync(Sql.Schemes_UpdateName(), targetName, legacyScheme.Id);
            if (rowsAffected == 0)
            {
                Logger?.LogWarning(
                    "Rename UPDATE affected 0 rows for scheme ID {SchemeId}. SQL: {Sql}",
                    legacyScheme.Id, Sql.Schemes_UpdateName());
            }

            legacyScheme.Name = targetName;

            // Three keys: the id, the name it was found under, and the target name (in case a stale
            // entry sits there). The process-global name -> Type index is refreshed immediately rather
            // than waiting for the next assembly rescan.
            Cache.InvalidateScheme(legacyScheme.Id);
            Cache.InvalidateScheme(previousName);
            Cache.InvalidateScheme(targetName);
            ClrSchemeTypeIndex.Register(targetName, type);

            await SyncSchemeAliasAsync(legacyScheme, alias);
            return legacyScheme;
        }

        // 4. Nothing to rename — create the scheme.
        var newId = await Context.NextObjectIdAsync();
        var newScheme = new RedbScheme
        {
            Id = newId,
            Name = targetName,
            Alias = alias,
            Type = RedbTypeIds.Class
        };

        // Conflict-safe INSERT: several nodes starting at once all miss the lookups above and all
        // reach this line. A plain INSERT would let all but one fail on UNIQUE(_name) — and inside a
        // transaction on PostgreSQL that error also poisons the transaction, so catching it and
        // re-reading is not an option. The dialect suppresses the conflict instead, and the loser
        // simply reads back the winner's row.
        var inserted = await Context.ExecuteAsync(
            Sql.Schemes_InsertIfAbsent(), newScheme.Id, newScheme.Name, newScheme.Alias, newScheme.Type);

        if (inserted == 0)
        {
            var winner = await Context.QueryFirstOrDefaultAsync<RedbScheme>(
                Sql.Schemes_SelectByName(), targetName);

            if (winner == null)
            {
                throw new InvalidOperationException(
                    $"Scheme '{targetName}' was not inserted (conflict) but cannot be read back. " +
                    "The scheme was likely removed concurrently.");
            }

            Logger?.LogInformation(
                "Lost the creation race for scheme '{SchemeName}'; using the existing scheme (ID: {SchemeId})",
                targetName, winner.Id);

            await SyncSchemeAliasAsync(winner, alias);
            return winner;
        }

        if (explicitName != null)
            ClrSchemeTypeIndex.Register(targetName, type);

        return newScheme;
    }

    /// <summary>
    /// Brings <c>_schemes._alias</c> in line with the attribute. The attribute is the source of truth,
    /// mirroring how structure aliases already behave — a value edited by hand in the database is
    /// overwritten, and removing the attribute resets the alias to NULL.
    /// </summary>
    private async Task SyncSchemeAliasAsync(RedbScheme scheme, string? alias)
    {
        if (scheme.Alias == alias)
            return;

        await Context.ExecuteAsync(Sql.Schemes_UpdateAlias(), (object?)alias ?? DBNull.Value, scheme.Id);
        scheme.Alias = alias;

        Cache.InvalidateScheme(scheme.Id);
        Cache.InvalidateScheme(scheme.Name);
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
                if (Logger != null)
                {
                    var namesToDelete = existingStructures
                        .Where(s => idsToDelete.Contains(s.Id))
                        .Select(s => $"{s.Id}:{s.Name}")
                        .ToList();

                    Logger.LogWarning(
                        "REDB schema sync: removing {Count} structure(s) from scheme '{SchemeName}' (id={SchemeId}): [{Items}]. " +
                        "This is a destructive DDL operation. On PostgreSQL the FK `_values._id_structure -> _structures._id` " +
                        "is ON DELETE CASCADE; on MSSQL the equivalent effect is produced by the INSTEAD OF DELETE trigger " +
                        "TR__structures__cascade_values. In both backends every `_values` row referencing these structures " +
                        "will be silently deleted. " +
                        "Set RedbServiceConfiguration.DefaultStrictDeleteExtra=false (or pass strictDeleteExtra:false) " +
                        "to skip destructive sync \u2014 recommended for rolling/blue-green deployments.",
                        idsToDelete.Count, scheme.Name, scheme.Id, string.Join(", ", namesToDelete));
                }

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
        // Honor the configured policy. Default value of DefaultStrictDeleteExtra is true,
        // so behavior is unchanged for users on the default config. Users who explicitly
        // set the flag to false (or pick the Development/HighPerformance/Migration presets)
        // now actually get the non-destructive sync they asked for \u2014 required for safe
        // rolling/blue-green deployments where old and new app versions share a database.
        var structures = await SyncStructuresFromTypeAsync<TProps>(
            scheme, strictDeleteExtra: Configuration.DefaultStrictDeleteExtra);

        // Authoritatively bind scheme_id → TProps for polymorphic loads. This is the canonical write
        // of the per-domain projection: Type and the freshly-synced scheme_id co-exist here, so the
        // registry cannot drift from the DB regardless of assembly/ALC timing or which node synced.
        // (Global, db-independent name↔Type lands in ClrSchemeTypeIndex.)
        Cache.RegisterClrType(scheme.Name, scheme.Id, typeof(TProps));

        // Reload scheme from DB to get current state (including updated hash),
        // attach structures, and cache for subsequent queries
        var freshScheme = await Context.QueryFirstOrDefaultAsync<RedbScheme>(Sql.Schemes_SelectById(), scheme.Id);
        if (freshScheme != null)
        {
            freshScheme.SetStructures(structures.Cast<RedbStructure>().ToList());
            Cache.CacheScheme(freshScheme);
            return freshScheme;
        }
        
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
            // Order matters and is load-bearing: the migration either moves the values or throws, and
            // only a completed migration is allowed to reach Structures_UpdateType. Switching the type
            // on a failed migration is what turned a bad type change into silently missing values.
            await MigrateStructureTypeInternalAsync(
                structure.Id, structure.IdType, typeName, structure.Name, structure.IdScheme);
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
    /// Moves a structure's stored values to the column the new type reads from, during scheme sync.
    /// Throws <see cref="RedbTypeMigrationException"/> when it cannot, so the caller never switches
    /// <c>_id_type</c> on values that stayed behind.
    ///
    /// <para>
    /// <paramref name="propertyName"/> and <paramref name="schemeId"/> only enrich the error message and
    /// are optional so that the pre-existing signature keeps compiling for anyone who overrode it.
    /// </para>
    /// </summary>
    protected virtual async Task MigrateStructureTypeInternalAsync(
        long structureId, long oldTypeId, string newTypeName,
        string? propertyName = null, long? schemeId = null)
    {
        // Resolve by ID. Passing the id into a lookup keyed by NAME is what used to happen here: it
        // matched nothing, fell back to the literal "unknown", and every provider then reported an
        // unknown source type — an error nobody read, because the result was discarded.
        var oldTypeName = await ResolveTypeNameByIdAsync(oldTypeId);

        var result = await MigrateStructureTypeAsync(structureId, oldTypeName, newTypeName, false);

        // Providers report failure as data, not as an exception: an `errors` string when the migration
        // never ran, a non-zero error count when it ran and left rows behind. Both are failures here.
        var failed = !string.IsNullOrWhiteSpace(result.Errors) || result.ErrorCount > 0;
        if (!failed)
            return;

        // A structure with no stored values has nothing to strand. This is the ordinary case for a type
        // with no scalar storage column of its own (a nested class), where every provider reports an
        // unknown source type and there is genuinely nothing to move. Refusing here would break type
        // changes on empty schemes for no gain.
        var storedRows = await CountStructureValuesAsync(structureId);
        if (storedRows == 0)
            return;

        // A migration that never started reports zero affected rows even though the values are sitting
        // right there — it bailed out before counting anything. Reporting that zero would tell the
        // reader nothing is at stake, which is the opposite of the truth, so fall back to the rows we
        // just counted ourselves.
        var affected = result.AffectedRows > 0 ? result.AffectedRows : (int)storedRows;

        throw new RedbTypeMigrationException(
            structureId,
            propertyName ?? $"structure {structureId}",
            schemeId ?? 0,
            schemeId.HasValue ? await TryResolveSchemeNameAsync(schemeId.Value) : null,
            oldTypeName,
            newTypeName,
            affected,
            result.SuccessCount,
            affected - result.SuccessCount,
            result.Errors);
    }

    /// <summary>
    /// REDB type name for a type id, from the by-id metadata cache and falling back to one query for
    /// the whole (tiny, seed-only) <c>_types</c> table. No new dialect method: the id-keyed cache and
    /// <c>Types_SelectAll</c> already exist.
    /// </summary>
    private async Task<string> ResolveTypeNameByIdAsync(long typeId)
    {
        var cached = Cache.GetTypeById(typeId);
        if (cached != null)
            return cached.Name;

        var allTypes = (await Context.QueryAsync<RedbTypeInfo>(Sql.Types_SelectAll())).ToList();
        Cache.CacheTypesById(allTypes);

        var found = allTypes.FirstOrDefault(t => t.Id == typeId);
        if (found == null)
            throw new InvalidOperationException(
                $"Type id={typeId} is referenced by a structure but is missing from the _types table. " +
                "The database schema is inconsistent.");

        return found.Name;
    }

    /// <summary>
    /// Rows in <c>_values</c> for a structure, regardless of which typed column holds them. Composed
    /// here rather than added to ISqlDialect: it is one portable statement and the parameter form is
    /// the only provider-specific part. COUNT(*) is cast because its natural width differs by engine.
    /// </summary>
    private async Task<long> CountStructureValuesAsync(long structureId)
    {
        var sql = $"SELECT CAST(COUNT(*) AS BIGINT) FROM _values WHERE _id_structure = {Sql.FormatParameter(1)}";
        return await Context.ExecuteScalarAsync<long?>(sql, structureId) ?? 0;
    }

    /// <summary>Best-effort scheme name for an error message; never the reason a call fails.</summary>
    private async Task<string?> TryResolveSchemeNameAsync(long schemeId)
    {
        try
        {
            var scheme = await Context.QueryFirstOrDefaultAsync<RedbScheme>(Sql.Schemes_SelectById(), schemeId);
            return scheme?.Name;
        }
        catch
        {
            return null;
        }
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
        await _csharpToRedbTypeCacheLock.WaitAsync();
        try
        {
            // Double-check: another thread may have finished building the cache while we waited.
            if (_csharpToRedbTypeCache != null)
                return;

            var allTypes = await Context.QueryAsync<RedbType>(Sql.Types_SelectAll());

            // Build into a LOCAL dictionary and publish it only once fully populated, so
            // concurrent readers in MapCSharpTypeToRedbTypeAsync never observe a half-filled
            // (and mid-mutation) instance.
            var cache = new Dictionary<Type, string>();

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
                if (csharpType != null && !cache.ContainsKey(csharpType))
                {
                    cache[csharpType] = dbType.Name;
                }
            }

            if (!cache.ContainsKey(typeof(DateTime)))
            {
                var dateTimeType = allTypes.FirstOrDefault(t => t.Name == "DateTime");
                if (dateTimeType != null)
                {
                    cache[typeof(DateTime)] = "DateTime";
                }
            }

            var numericType = allTypes.FirstOrDefault(t => t.Name == "Numeric");
            if (numericType != null)
            {
                cache[typeof(decimal)] = "Numeric";
            }

            _csharpToRedbTypeCache = cache; // atomic publish — readers see null or a complete map
        }
        finally
        {
            _csharpToRedbTypeCacheLock.Release();
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
        var schemeName = GetSchemeNameForType<TProps>();

        // Check cache first
        var cached = Cache.GetScheme(schemeName);
        if (cached != null)
            return cached;
        
        // Load from DB with structures and cache
        var scheme = await Context.QueryFirstOrDefaultAsync<RedbScheme>(Sql.Schemes_SelectByName(), schemeName);
        if (scheme != null)
        {
            var structures = await Context.QueryAsync<RedbStructure>(Sql.Structures_SelectBySchemeCacheable(), scheme.Id);
            scheme.SetStructures(structures);
            Cache.CacheScheme(scheme);
        }
        
        return scheme;
    }

    public async Task<IRedbScheme?> GetSchemeByTypeAsync(Type type)
    {
        var schemeName = GetSchemeNameForType(type);

        // Check cache first
        var cached = Cache.GetScheme(schemeName);
        if (cached != null)
            return cached;
        
        // Load from DB with structures and cache
        var scheme = await Context.QueryFirstOrDefaultAsync<RedbScheme>(Sql.Schemes_SelectByName(), schemeName);
        if (scheme != null)
        {
            var structures = await Context.QueryAsync<RedbStructure>(Sql.Structures_SelectBySchemeCacheable(), scheme.Id);
            scheme.SetStructures(structures);
            Cache.CacheScheme(scheme);
        }
            
        return scheme;
    }
    
    /// <summary>
    /// Get scheme from cache synchronously (no DB call).
    /// Returns null if not in cache.
    /// </summary>
    public IRedbScheme? GetSchemeFromCache<TProps>() where TProps : class
    {
        // Mirrors the three-step lookup chain used when syncing: the effective name first (an explicit
        // [RedbScheme(Name = "...")] or FullName), then FullName for the window where the type declares
        // an explicit name but the cache still holds the pre-rename entry, then the short type name for
        // backward compatibility with manually registered schemes.
        var t = typeof(TProps);
        return Cache.GetScheme(GetSchemeNameForType(t))
            ?? Cache.GetScheme(t.FullName ?? t.Name)
            ?? Cache.GetScheme(t.Name);
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
        var schemeName = GetSchemeNameForType<TProps>();
        var result = await Context.ExecuteScalarAsync<long?>(Sql.Schemes_ExistsByName(), schemeName);
        return result.HasValue;
    }

    public async Task<bool> SchemeExistsForTypeAsync(Type type)
    {
        var schemeName = GetSchemeNameForType(type);
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

    /// <summary>
    /// THE single place that turns a CLR type into a scheme name. Returns the explicit
    /// <see cref="RedbSchemeAttribute.Name"/> when the type declares one, otherwise FullName.
    /// <para>
    /// Every lookup, existence check and creation path in this class must go through here. A path
    /// that computes <c>type.FullName</c> on its own does not fail loudly — it silently misses the
    /// lookup and ends up creating a second scheme for the same type.
    /// </para>
    /// </summary>
    public string GetSchemeNameForType<TProps>() where TProps : class => GetSchemeNameForType(typeof(TProps));

    /// <inheritdoc cref="GetSchemeNameForType{TProps}()"/>
    public string GetSchemeNameForType(Type type)
        => GetRedbSchemeAttribute(type)?.GetSchemeName(type) ?? type.FullName ?? type.Name;

    /// <summary>
    /// The explicit name declared by the type, or null when the type lives by FullName.
    /// Distinguishes "renaming requested" from "default naming" for the sync chain.
    /// </summary>
    private static string? GetExplicitSchemeName(Type type)
    {
        var name = GetRedbSchemeAttribute(type)?.Name;
        return string.IsNullOrWhiteSpace(name) ? null : name;
    }

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

        // Same creation race as the typed path — several nodes can reach this line for the same name.
        // The dialect suppresses the unique-name conflict so the transaction survives, and the loser
        // reads back the winner's row.
        var inserted = await Context.ExecuteAsync(
            Sql.Schemes_InsertObjectIfAbsent(), newScheme.Id, newScheme.Name, newScheme.Type);

        if (inserted == 0)
        {
            var winner = await Context.QueryFirstOrDefaultAsync<RedbScheme>(
                Sql.Schemes_SelectObjectByName(), name, RedbTypeIds.Object);

            if (winner == null)
            {
                throw new InvalidOperationException(
                    $"Object scheme '{name}' was not inserted (conflict) but cannot be read back. " +
                    "A non-Object scheme may already hold that name.");
            }

            Logger?.LogInformation(
                "Lost the creation race for object scheme '{SchemeName}'; using the existing scheme (ID: {SchemeId})",
                name, winner.Id);

            Cache.CacheScheme(winner);
            return winner;
        }

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

