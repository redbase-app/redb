using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core.Caching;
using redb.Core.Data;
using redb.Core.Providers;
using redb.Core.Query;
using redb.Core.Serialization;
using redb.Core.Utils;
using redb.Core.Models.Enums;
using redb.Core.Models.Permissions;
using redb.Core.Models.Contracts;
using redb.Core.Models.Security;
using redb.Core.Models.Entities;
using redb.Core.Models.Configuration;
using redb.Core.Attributes;
using redb.Core.Models;
using redb.Core.Services;
using System.Reflection;
using System.Threading;
using System.Runtime.Loader;

namespace redb.Core;

/// <summary>
/// Base abstract class for RedbService implementations.
/// Contains all common logic for delegating to providers.
/// Provider-specific implementations (Postgres, MSSQL) inherit from this class.
/// </summary>
public abstract class RedbServiceBase : IRedbService
{
    private readonly IRedbContext _context;
    private readonly ISchemeSyncProvider _schemeSync;
    protected IObjectStorageProvider _objectStorage;
    protected ITreeProvider _treeProvider;
    private readonly IPermissionProvider _permissionProvider;
    protected IQueryableProvider _queryProvider;
    private readonly IValidationProvider _validationProvider;
    private readonly IRedbSecurityContext _securityContext;
    private readonly IUserProvider _userProvider;
    private readonly IRoleProvider _roleProvider;
    private readonly IListProvider _listProvider;
    private readonly IServiceProvider _serviceProvider;
    private RedbServiceConfiguration _configuration;
    private readonly IRedbObjectSerializer _serializer;
    private readonly ILogger? _logger;
    
    /// <summary>
    /// Cache domain identifier for isolating caches between different database connections.
    /// </summary>
    protected readonly string _cacheDomain;

    /// <summary>
    /// Database context for direct data access.
    /// </summary>
    public IRedbContext Context => _context;
    
    public IRedbSecurityContext SecurityContext => _securityContext;
    public IUserProvider UserProvider => _userProvider;
    public IRoleProvider RoleProvider => _roleProvider;
    public IListProvider ListProvider => _listProvider;
    public RedbServiceConfiguration Configuration => _configuration;
    
    /// <summary>
    /// Cache domain identifier for this service instance.
    /// </summary>
    public string CacheDomain => _cacheDomain;

    // === ABSTRACT METHODS FOR PROVIDER-SPECIFIC IMPLEMENTATIONS ===
    
    /// <summary>
    /// Database provider name (e.g., "PostgreSQL", "MSSql").
    /// </summary>
    protected abstract string DatabaseTypeName { get; }
    
    /// <summary>
    /// SQL query to get database version.
    /// </summary>
    protected abstract string GetVersionSql { get; }
    
    /// <summary>
    /// SQL query to get database size in bytes.
    /// </summary>
    protected abstract string GetDatabaseSizeSql { get; }
    
    /// <summary>
    /// Error message when IRedbContext is not registered.
    /// </summary>
    protected abstract string ContextNotRegisteredError { get; }
    
    /// <summary>
    /// Create scheme sync provider.
    /// </summary>
    protected abstract ISchemeSyncProvider CreateSchemeSyncProvider(
        IRedbContext context, RedbServiceConfiguration config, string cacheDomain, ILogger? logger);
    
    /// <summary>
    /// Create permission provider.
    /// </summary>
    protected abstract IPermissionProvider CreatePermissionProvider(
        IRedbContext context, IRedbSecurityContext securityContext, ILogger? logger);
    
    /// <summary>
    /// Create user provider.
    /// </summary>
    protected abstract IUserProvider CreateUserProvider(
        IRedbContext context, IRedbSecurityContext securityContext, ILogger? logger);
    
    /// <summary>
    /// Create role provider.
    /// </summary>
    protected abstract IRoleProvider CreateRoleProvider(
        IRedbContext context, IRedbSecurityContext securityContext, ILogger? logger);
    
    /// <summary>
    /// Create list provider.
    /// </summary>
    protected abstract IListProvider CreateListProvider(
        IRedbContext context, RedbServiceConfiguration config, ISchemeSyncProvider schemeSync, ILogger? logger);
    
    /// <summary>
    /// Create object storage provider.
    /// </summary>
    protected abstract IObjectStorageProvider CreateObjectStorageProvider(
        IRedbContext context, IRedbObjectSerializer serializer, IPermissionProvider permissionProvider,
        IRedbSecurityContext securityContext, ISchemeSyncProvider schemeSync,
        RedbServiceConfiguration config, IListProvider listProvider, ILogger? logger);
    
    /// <summary>
    /// Create tree provider.
    /// </summary>
    protected abstract ITreeProvider CreateTreeProvider(
        IRedbContext context, IObjectStorageProvider objectStorage, IPermissionProvider permissionProvider,
        IRedbObjectSerializer serializer, IRedbSecurityContext securityContext,
        ISchemeSyncProvider schemeSync, RedbServiceConfiguration config, ILogger? logger);
    
    /// <summary>
    /// Create lazy props loader.
    /// </summary>
    protected abstract ILazyPropsLoader CreateLazyPropsLoader(
        IRedbContext context, ISchemeSyncProvider schemeSync, IRedbObjectSerializer serializer,
        RedbServiceConfiguration config, string cacheDomain, IListProvider listProvider, ILogger? logger);
    
    /// <summary>
    /// Create queryable provider.
    /// </summary>
    protected abstract IQueryableProvider CreateQueryableProvider(
        IRedbContext context, IRedbObjectSerializer serializer, ISchemeSyncProvider schemeSync,
        IRedbSecurityContext securityContext, ILazyPropsLoader lazyPropsLoader,
        RedbServiceConfiguration config, string cacheDomain, ILogger? logger);
    
    /// <summary>
    /// Create validation provider.
    /// </summary>
    protected abstract IValidationProvider CreateValidationProvider(
        IRedbContext context, ILogger? logger);
    
    /// <summary>
    /// SQL dialect for database-specific queries.
    /// </summary>
    protected abstract ISqlDialect SqlDialect { get; }

    /// <summary>
    /// Initialize RedbServiceBase with dependency injection.
    /// </summary>
    protected RedbServiceBase(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider;
        _context = serviceProvider.GetService<IRedbContext>() ?? 
            throw new InvalidOperationException(ContextNotRegisteredError);
        _serializer = serviceProvider.GetService<IRedbObjectSerializer>() ?? new SystemTextJsonRedbSerializer();
        _logger = serviceProvider.GetService<ILogger<RedbServiceBase>>() as ILogger;
        _configuration = serviceProvider.GetService<RedbServiceConfiguration>() ?? new RedbServiceConfiguration();
        _securityContext = serviceProvider.GetService<IRedbSecurityContext>() ?? 
                          AmbientSecurityContext.GetOrCreateDefault();

        // Compute cache domain for this service instance
        _cacheDomain = _configuration.GetEffectiveCacheDomain();
        
        // Create providers using abstract factory methods (passing cacheDomain)
        _schemeSync = CreateSchemeSyncProvider(_context, _configuration, _cacheDomain, _logger);
        _permissionProvider = CreatePermissionProvider(_context, _securityContext, _logger);
        _userProvider = CreateUserProvider(_context, _securityContext, _logger);
        _roleProvider = CreateRoleProvider(_context, _securityContext, _logger);
        _listProvider = CreateListProvider(_context, _configuration, _schemeSync, _logger);
        
        _objectStorage = CreateObjectStorageProvider(_context, _serializer, _permissionProvider, 
            _securityContext, _schemeSync, _configuration, _listProvider, _logger);
        _treeProvider = CreateTreeProvider(_context, _objectStorage, _permissionProvider, 
            _serializer, _securityContext, _schemeSync, _configuration, _logger);
        
        // Setup global object loader for RedbListItem
        SetupGlobalObjectLoader();
        
        var lazyPropsLoader = CreateLazyPropsLoader(_context, _schemeSync, _serializer, 
            _configuration, _cacheDomain, _listProvider, _logger);
        
        _queryProvider = CreateQueryableProvider(_context, _serializer, _schemeSync, 
            _securityContext, lazyPropsLoader, _configuration, _cacheDomain, _logger);
        _validationProvider = CreateValidationProvider(_context, _logger);
    }
    
    /// <summary>
    /// Setup global object loader for RedbListItem.
    /// </summary>
    private void SetupGlobalObjectLoader()
    {
        RedbListItem.SetGlobalObjectLoader(async (objectId) =>
        {
            try
            {
                var schemeId = await _context.ExecuteScalarAsync<long>(
                    "SELECT _id_scheme FROM _objects WHERE _id = $1", objectId);
                
                if (schemeId == 0)
                    return null;
                
                var propsType = _schemeSync.Cache.GetClrType(schemeId);
                
                if (propsType != null)
                {
                    var loadMethod = typeof(IObjectStorageProvider)
                        .GetMethod("LoadAsync", new[] { typeof(long), typeof(int), typeof(bool?) })!
                        .MakeGenericMethod(propsType);
                    
                    var task = (Task)loadMethod.Invoke(_objectStorage, new object[] { objectId, 10, (bool?)false })!;
                    await task;
                    
                    return (IRedbObject?)task.GetType().GetProperty("Result")?.GetValue(task);
                }
                else
                {
                    var json = await _context.ExecuteJsonAsync(
                        GetObjectJsonSql(), objectId, 10);
                    
                    return string.IsNullOrEmpty(json) ? null : _serializer.DeserializeDynamic(json, typeof(object));
                }
            }
            catch
            {
                throw;
            }
        });
    }
    
    /// <summary>
    /// Get SQL for loading object as JSON. Override in derived classes for DB-specific syntax.
    /// </summary>
    protected virtual string GetObjectJsonSql() => "SELECT get_object_json($1, $2)::text";

    /// <summary>
    /// Get tree provider for extended scenarios.
    /// </summary>
    public ITreeProvider GetTreeProvider() => _treeProvider;

    // === DATABASE METADATA ===
    public string dbVersion => _context.ExecuteScalarAsync<string>(GetVersionSql).Result ?? "unknown";
    public string dbType => DatabaseTypeName;
    public string dbMigration => "ADO.NET";
    public long? dbSize => _context.ExecuteScalarAsync<long>(GetDatabaseSizeSql).Result;

    // === ISchemeSyncProvider DELEGATION ===

    public Task<IRedbScheme> EnsureSchemeFromTypeAsync<TProps>() where TProps : class
        => _schemeSync.EnsureSchemeFromTypeAsync<TProps>();
    
    public Task<List<IRedbStructure>> SyncStructuresFromTypeAsync<TProps>(IRedbScheme scheme, bool strictDeleteExtra = true) where TProps : class
        => _schemeSync.SyncStructuresFromTypeAsync<TProps>(scheme, strictDeleteExtra);
    
    public Task<IRedbScheme> SyncSchemeAsync<TProps>() where TProps : class
        => _schemeSync.SyncSchemeAsync<TProps>();

    public Task<IRedbScheme?> GetSchemeByTypeAsync<TProps>() where TProps : class
        => _schemeSync.GetSchemeByTypeAsync<TProps>();

    public Task<IRedbScheme?> GetSchemeByTypeAsync(Type type)
        => _schemeSync.GetSchemeByTypeAsync(type);
    
    public IRedbScheme? GetSchemeFromCache<TProps>() where TProps : class
        => _schemeSync.GetSchemeFromCache<TProps>();
    
    public IRedbScheme? GetSchemeFromCache(string schemeName)
        => _schemeSync.GetSchemeFromCache(schemeName);

    public Task<IRedbScheme> LoadSchemeByTypeAsync<TProps>() where TProps : class
        => _schemeSync.LoadSchemeByTypeAsync<TProps>();

    public Task<IRedbScheme> LoadSchemeByTypeAsync(Type type)
        => _schemeSync.LoadSchemeByTypeAsync(type);

    public Task<List<IRedbStructure>> GetStructuresByTypeAsync<TProps>() where TProps : class
        => _schemeSync.GetStructuresByTypeAsync<TProps>();

    public Task<List<IRedbStructure>> GetStructuresByTypeAsync(Type type)
        => _schemeSync.GetStructuresByTypeAsync(type);

    public Task<bool> SchemeExistsForTypeAsync<TProps>() where TProps : class
        => _schemeSync.SchemeExistsForTypeAsync<TProps>();

    public Task<bool> SchemeExistsForTypeAsync(Type type)
        => _schemeSync.SchemeExistsForTypeAsync(type);

    public Task<bool> SchemeExistsByNameAsync(string schemeName)
        => _schemeSync.SchemeExistsByNameAsync(schemeName);

    public string GetSchemeNameForType<TProps>() where TProps : class
        => _schemeSync.GetSchemeNameForType<TProps>();

    public string GetSchemeNameForType(Type type)
        => _schemeSync.GetSchemeNameForType(type);

    public string? GetSchemeAliasForType<TProps>() where TProps : class
        => _schemeSync.GetSchemeAliasForType<TProps>();

    public string? GetSchemeAliasForType(Type type)
        => _schemeSync.GetSchemeAliasForType(type);
    
    public Task<IRedbScheme?> GetSchemeByIdAsync(long schemeId)
        => _schemeSync.GetSchemeByIdAsync(schemeId);
    
    public Task<IRedbScheme?> GetSchemeByNameAsync(string schemeName)
        => _schemeSync.GetSchemeByNameAsync(schemeName);
    
    public Task<IRedbScheme> EnsureObjectSchemeAsync(string name)
        => _schemeSync.EnsureObjectSchemeAsync(name);
    
    public Task<IRedbScheme?> GetObjectSchemeAsync(string name)
        => _schemeSync.GetObjectSchemeAsync(name);
    
    public Task<List<IRedbScheme>> GetSchemesAsync()
        => _schemeSync.GetSchemesAsync();
    
    public GlobalMetadataCache Cache => _schemeSync.Cache;
    public GlobalListCache ListCache => _schemeSync.ListCache;
    public GlobalPropsCache PropsCache => _schemeSync.PropsCache;
    
    public Task<List<IRedbStructure>> GetStructuresAsync(IRedbScheme scheme)
        => _schemeSync.GetStructuresAsync(scheme);
    
    public Task<TypeMigrationResult> MigrateStructureTypeAsync(long structureId, string oldTypeName, string newTypeName, bool dryRun = false)
        => _schemeSync.MigrateStructureTypeAsync(structureId, oldTypeName, newTypeName, dryRun);
    
    public Task<List<StructureTreeNode>> GetStructureTreeAsync(long schemeId)
        => _schemeSync.GetStructureTreeAsync(schemeId);
    
    public Task<List<StructureTreeNode>> GetSubtreeAsync(long schemeId, long? parentStructureId)
        => _schemeSync.GetSubtreeAsync(schemeId, parentStructureId);
    
    public void InvalidateStructureTreeCache(long schemeId)
        => _schemeSync.InvalidateStructureTreeCache(schemeId);
    
    public (int TreesCount, int SubtreesCount, long MemoryEstimate) GetStructureTreeCacheStats()
        => _schemeSync.GetStructureTreeCacheStats();

    // === IObjectStorageProvider DELEGATION ===

    public Task<RedbObject<TProps>?> LoadAsync<TProps>(long objectId, int depth = 10, bool? lazyLoadProps = null) where TProps : class, new()
        => _objectStorage.LoadAsync<TProps>(objectId, depth, lazyLoadProps);

    public Task<RedbObject<TProps>?> LoadAsync<TProps>(IRedbObject obj, int depth = 10, bool? lazyLoadProps = null) where TProps : class, new()
        => _objectStorage.LoadAsync<TProps>(obj, depth, lazyLoadProps);

    public Task<RedbObject<TProps>?> LoadAsync<TProps>(IRedbObject obj, IRedbUser user, int depth = 10, bool? lazyLoadProps = null) where TProps : class, new()
        => _objectStorage.LoadAsync<TProps>(obj, user, depth, lazyLoadProps);

    public Task<long> SaveAsync(IRedbObject obj)
        => _objectStorage.SaveAsync(obj);

    public Task<long> SaveAsync<TProps>(IRedbObject<TProps> obj) where TProps : class, new()
        => _objectStorage.SaveAsync(obj);

    public Task<bool> DeleteAsync(IRedbObject obj)
        => _objectStorage.DeleteAsync(obj);

    public Task<RedbObject<TProps>?> LoadAsync<TProps>(long objectId, IRedbUser user, int depth = 10, bool? lazyLoadProps = null) where TProps : class, new()
        => _objectStorage.LoadAsync<TProps>(objectId, user, depth, lazyLoadProps);

    public Task<long> SaveAsync(IRedbObject obj, IRedbUser user)
        => _objectStorage.SaveAsync(obj, user);

    public Task<long> SaveAsync<TProps>(IRedbObject<TProps> obj, IRedbUser user) where TProps : class, new()
        => _objectStorage.SaveAsync(obj, user);

    public Task<bool> DeleteAsync(IRedbObject obj, IRedbUser user)
        => _objectStorage.DeleteAsync(obj, user);

    public Task<bool> DeleteAsync(long objectId)
        => _objectStorage.DeleteAsync(objectId);

    public Task<bool> DeleteAsync(long objectId, IRedbUser user)
        => _objectStorage.DeleteAsync(objectId, user);

    public Task<List<long>> AddNewObjectsAsync<TProps>(IEnumerable<IRedbObject<TProps>> objects) where TProps : class, new()
        => _objectStorage.AddNewObjectsAsync(objects);

    public Task<List<long>> AddNewObjectsAsync<TProps>(IEnumerable<IRedbObject<TProps>> objects, IRedbUser user) where TProps : class, new()
        => _objectStorage.AddNewObjectsAsync(objects, user);

    public Task<int> DeleteAsync(IEnumerable<long> objectIds)
        => _objectStorage.DeleteAsync(objectIds);

    public Task<int> DeleteAsync(IEnumerable<long> objectIds, IRedbUser user)
        => _objectStorage.DeleteAsync(objectIds, user);

    public Task<int> DeleteAsync(IEnumerable<IRedbObject> objects)
        => _objectStorage.DeleteAsync(objects);

    public Task<int> DeleteAsync(IEnumerable<IRedbObject> objects, IRedbUser user)
        => _objectStorage.DeleteAsync(objects, user);
    
    public Task<DeletionMark> SoftDeleteAsync(IEnumerable<long> objectIds, long? trashParentId = null)
        => _objectStorage.SoftDeleteAsync(objectIds, trashParentId);
    
    public Task<DeletionMark> SoftDeleteAsync(IEnumerable<long> objectIds, IRedbUser user, long? trashParentId = null)
        => _objectStorage.SoftDeleteAsync(objectIds, user, trashParentId);
    
    public Task<DeletionMark> SoftDeleteAsync(IEnumerable<IRedbObject> objects, long? trashParentId = null)
        => _objectStorage.SoftDeleteAsync(objects, trashParentId);
    
    public Task<DeletionMark> SoftDeleteAsync(IEnumerable<IRedbObject> objects, IRedbUser user, long? trashParentId = null)
        => _objectStorage.SoftDeleteAsync(objects, user, trashParentId);
    
    public Task DeleteWithPurgeAsync(
        IEnumerable<long> objectIds, 
        int batchSize = 10,
        IProgress<PurgeProgress>? progress = null,
        CancellationToken cancellationToken = default,
        long? trashParentId = null)
        => _objectStorage.DeleteWithPurgeAsync(objectIds, batchSize, progress, cancellationToken, trashParentId);
    
    public Task PurgeTrashAsync(
        long trashId,
        int totalCount,
        int batchSize = 10,
        IProgress<PurgeProgress>? progress = null,
        CancellationToken cancellationToken = default)
        => _objectStorage.PurgeTrashAsync(trashId, totalCount, batchSize, progress, cancellationToken);
    
    public Task<PurgeProgress?> GetDeletionProgressAsync(long trashId)
        => _objectStorage.GetDeletionProgressAsync(trashId);
    
    public Task<List<PurgeProgress>> GetUserActiveDeletionsAsync(long userId)
        => _objectStorage.GetUserActiveDeletionsAsync(userId);
    
    public Task<List<OrphanedTask>> GetOrphanedDeletionTasksAsync(int timeoutMinutes = 30)
        => _objectStorage.GetOrphanedDeletionTasksAsync(timeoutMinutes);
    
    public Task<bool> TryClaimOrphanedTaskAsync(long trashId, int timeoutMinutes = 30)
        => _objectStorage.TryClaimOrphanedTaskAsync(trashId, timeoutMinutes);

    public Task<List<IRedbObject>> LoadAsync(IEnumerable<long> objectIds, int depth = 10, bool? lazyLoadProps = null)
        => _objectStorage.LoadAsync(objectIds, depth, lazyLoadProps);

    public Task<List<IRedbObject>> LoadAsync(IEnumerable<long> objectIds, IRedbUser user, int depth = 10, bool? lazyLoadProps = null)
        => _objectStorage.LoadAsync(objectIds, user, depth, lazyLoadProps);

    public Task<List<long>> SaveAsync(IEnumerable<IRedbObject> objects)
        => _objectStorage.SaveAsync(objects);

    public Task<List<long>> SaveAsync(IEnumerable<IRedbObject> objects, IRedbUser user)
        => _objectStorage.SaveAsync(objects, user);

    // LoadWithParentsAsync - Load with parent chain to root
    Task<TreeRedbObject<TProps>?> IObjectStorageProvider.LoadWithParentsAsync<TProps>(long objectId, int depth, bool? lazyLoadProps)
        => _objectStorage.LoadWithParentsAsync<TProps>(objectId, depth, lazyLoadProps);

    Task<TreeRedbObject<TProps>?> IObjectStorageProvider.LoadWithParentsAsync<TProps>(IRedbObject obj, int depth, bool? lazyLoadProps)
        => _objectStorage.LoadWithParentsAsync<TProps>(obj, depth, lazyLoadProps);

    Task<TreeRedbObject<TProps>?> IObjectStorageProvider.LoadWithParentsAsync<TProps>(long objectId, IRedbUser user, int depth, bool? lazyLoadProps)
        => _objectStorage.LoadWithParentsAsync<TProps>(objectId, user, depth, lazyLoadProps);

    Task<TreeRedbObject<TProps>?> IObjectStorageProvider.LoadWithParentsAsync<TProps>(IRedbObject obj, IRedbUser user, int depth, bool? lazyLoadProps)
        => _objectStorage.LoadWithParentsAsync<TProps>(obj, user, depth, lazyLoadProps);

    Task<List<TreeRedbObject<TProps>>> IObjectStorageProvider.LoadWithParentsAsync<TProps>(IEnumerable<long> objectIds, int depth, bool? lazyLoadProps)
        => _objectStorage.LoadWithParentsAsync<TProps>(objectIds, depth, lazyLoadProps);

    Task<List<TreeRedbObject<TProps>>> IObjectStorageProvider.LoadWithParentsAsync<TProps>(IEnumerable<long> objectIds, IRedbUser user, int depth, bool? lazyLoadProps)
        => _objectStorage.LoadWithParentsAsync<TProps>(objectIds, user, depth, lazyLoadProps);

    Task<List<ITreeRedbObject>> IObjectStorageProvider.LoadWithParentsAsync(IEnumerable<long> objectIds, int depth, bool? lazyLoadProps)
        => _objectStorage.LoadWithParentsAsync(objectIds, depth, lazyLoadProps);

    Task<List<ITreeRedbObject>> IObjectStorageProvider.LoadWithParentsAsync(IEnumerable<long> objectIds, IRedbUser user, int depth, bool? lazyLoadProps)
        => _objectStorage.LoadWithParentsAsync(objectIds, user, depth, lazyLoadProps);

    // === ITreeProvider DELEGATION ===

    public Task<int> DeleteSubtreeAsync(IRedbObject parentObj)
        => _treeProvider.DeleteSubtreeAsync(parentObj);

    public Task<int> DeleteSubtreeAsync(IRedbObject parentObj, IRedbUser user)
        => _treeProvider.DeleteSubtreeAsync(parentObj, user);

    public Task<int> DeleteSubtreeAsync(RedbObject parentObj)
        => _treeProvider.DeleteSubtreeAsync(parentObj);

    public Task<int> DeleteSubtreeAsync(RedbObject parentObj, IRedbUser user)
        => _treeProvider.DeleteSubtreeAsync(parentObj, user);

    public Task<TreeRedbObject<TProps>> LoadTreeAsync<TProps>(long rootObjectId, int? maxDepth = null) where TProps : class, new()
        => _treeProvider.LoadTreeAsync<TProps>(rootObjectId, maxDepth);

    public Task<TreeRedbObject<TProps>> LoadTreeAsync<TProps>(IRedbObject rootObj, int? maxDepth = null) where TProps : class, new()
        => _treeProvider.LoadTreeAsync<TProps>(rootObj, maxDepth);

    public Task<IEnumerable<TreeRedbObject<TProps>>> GetChildrenAsync<TProps>(IRedbObject parentObj) where TProps : class, new()
        => _treeProvider.GetChildrenAsync<TProps>(parentObj);

    public Task<IEnumerable<TreeRedbObject<TProps>>> GetPathToRootAsync<TProps>(IRedbObject obj) where TProps : class, new()
        => _treeProvider.GetPathToRootAsync<TProps>(obj);

    public Task<IEnumerable<TreeRedbObject<TProps>>> GetDescendantsAsync<TProps>(IRedbObject parentObj, int? maxDepth = null) where TProps : class, new()
        => _treeProvider.GetDescendantsAsync<TProps>(parentObj, maxDepth);

    public Task MoveObjectAsync(IRedbObject obj, IRedbObject? newParentObj)
        => _treeProvider.MoveObjectAsync(obj, newParentObj);

    public Task<long> CreateChildAsync<TProps>(TreeRedbObject<TProps> obj, IRedbObject parentObj) where TProps : class, new()
        => _treeProvider.CreateChildAsync<TProps>(obj, parentObj);

    public Task<TreeRedbObject<TProps>> LoadTreeAsync<TProps>(long rootObjectId, IRedbUser user, int? maxDepth = null) where TProps : class, new()
        => _treeProvider.LoadTreeAsync<TProps>(rootObjectId, user, maxDepth);

    public Task<TreeRedbObject<TProps>> LoadTreeAsync<TProps>(IRedbObject rootObj, IRedbUser user, int? maxDepth = null) where TProps : class, new()
        => _treeProvider.LoadTreeAsync<TProps>(rootObj, user, maxDepth);

    public Task<IEnumerable<TreeRedbObject<TProps>>> GetChildrenAsync<TProps>(IRedbObject parentObj, IRedbUser user) where TProps : class, new()
        => _treeProvider.GetChildrenAsync<TProps>(parentObj, user);

    public Task<IEnumerable<TreeRedbObject<TProps>>> GetPathToRootAsync<TProps>(IRedbObject obj, IRedbUser user) where TProps : class, new()
        => _treeProvider.GetPathToRootAsync<TProps>(obj, user);

    public Task<IEnumerable<TreeRedbObject<TProps>>> GetDescendantsAsync<TProps>(IRedbObject parentObj, IRedbUser user, int? maxDepth = null) where TProps : class, new()
        => _treeProvider.GetDescendantsAsync<TProps>(parentObj, user, maxDepth);

    public Task MoveObjectAsync(IRedbObject obj, IRedbObject? newParentObj, IRedbUser user)
        => _treeProvider.MoveObjectAsync(obj, newParentObj, user);

    public Task<long> CreateChildAsync<TProps>(TreeRedbObject<TProps> obj, IRedbObject parentObj, IRedbUser user) where TProps : class, new()
        => _treeProvider.CreateChildAsync<TProps>(obj, parentObj, user);
    
    public Task<ITreeRedbObject> LoadPolymorphicTreeAsync(IRedbObject rootObj, int? maxDepth = null)
        => _treeProvider.LoadPolymorphicTreeAsync(rootObj, maxDepth);
        
    public Task<IEnumerable<ITreeRedbObject>> GetPolymorphicChildrenAsync(IRedbObject parentObj)
        => _treeProvider.GetPolymorphicChildrenAsync(parentObj);
        
    public Task<IEnumerable<ITreeRedbObject>> GetPolymorphicPathToRootAsync(IRedbObject obj)
        => _treeProvider.GetPolymorphicPathToRootAsync(obj);
        
    public Task<IEnumerable<ITreeRedbObject>> GetPolymorphicDescendantsAsync(IRedbObject parentObj, int? maxDepth = null)
        => _treeProvider.GetPolymorphicDescendantsAsync(parentObj, maxDepth);
    
    public Task<ITreeRedbObject> LoadPolymorphicTreeAsync(IRedbObject rootObj, IRedbUser user, int? maxDepth = null)
        => _treeProvider.LoadPolymorphicTreeAsync(rootObj, user, maxDepth);
        
    public Task<IEnumerable<ITreeRedbObject>> GetPolymorphicChildrenAsync(IRedbObject parentObj, IRedbUser user)
        => _treeProvider.GetPolymorphicChildrenAsync(parentObj, user);
        
    public Task<IEnumerable<ITreeRedbObject>> GetPolymorphicPathToRootAsync(IRedbObject obj, IRedbUser user)
        => _treeProvider.GetPolymorphicPathToRootAsync(obj, user);
        
    public Task<IEnumerable<ITreeRedbObject>> GetPolymorphicDescendantsAsync(IRedbObject parentObj, IRedbUser user, int? maxDepth = null)
        => _treeProvider.GetPolymorphicDescendantsAsync(parentObj, user, maxDepth);
        
    public Task InitializeTypeRegistryAsync()
        => _treeProvider.InitializeTypeRegistryAsync();

    // === IPermissionProvider DELEGATION ===
    
    public IQueryable<long> GetReadableObjectIds()
        => _permissionProvider.GetReadableObjectIds();
    
    public Task<bool> CanUserEditObject(IRedbObject obj)
        => _permissionProvider.CanUserEditObject(obj);
    
    public Task<bool> CanUserSelectObject(IRedbObject obj)
        => _permissionProvider.CanUserSelectObject(obj);

    public Task<bool> CanUserInsertScheme(IRedbScheme scheme)
        => _permissionProvider.CanUserInsertScheme(scheme);

    public Task<bool> CanUserInsertScheme(IRedbScheme scheme, IRedbUser user)
        => _permissionProvider.CanUserInsertScheme(scheme, user);

    public Task<bool> CanUserDeleteObject(IRedbObject obj)
        => _permissionProvider.CanUserDeleteObject(obj);
    
    public IQueryable<long> GetReadableObjectIds(IRedbUser user)
        => _permissionProvider.GetReadableObjectIds(user);
    
    public Task<bool> CanUserEditObject(IRedbObject obj, IRedbUser user)
        => _permissionProvider.CanUserEditObject(obj, user);
    
    public Task<bool> CanUserSelectObject(IRedbObject obj, IRedbUser user)
        => _permissionProvider.CanUserSelectObject(obj, user);
    
    public Task<bool> CanUserDeleteObject(IRedbObject obj, IRedbUser user)
        => _permissionProvider.CanUserDeleteObject(obj, user);
    
    public Task<bool> CanUserEditObject(RedbObject obj)
        => _permissionProvider.CanUserEditObject(obj);
    
    public Task<bool> CanUserSelectObject(RedbObject obj)
        => _permissionProvider.CanUserSelectObject(obj);
    
    public Task<bool> CanUserDeleteObject(RedbObject obj)
        => _permissionProvider.CanUserDeleteObject(obj);
    
    public Task<bool> CanUserEditObject(RedbObject obj, IRedbUser user)
        => _permissionProvider.CanUserEditObject(obj, user);
    
    public Task<bool> CanUserSelectObject(RedbObject obj, IRedbUser user)
        => _permissionProvider.CanUserSelectObject(obj, user);
    
    public Task<bool> CanUserDeleteObject(RedbObject obj, IRedbUser user)
        => _permissionProvider.CanUserDeleteObject(obj, user);

    public Task<bool> CanUserInsertScheme(RedbObject obj, IRedbUser user)
        => _permissionProvider.CanUserInsertScheme(obj, user);

    public Task<IRedbPermission> CreatePermissionAsync(PermissionRequest request, IRedbUser? currentUser = null)
        => _permissionProvider.CreatePermissionAsync(request, currentUser);

    public Task<IRedbPermission> UpdatePermissionAsync(IRedbPermission permission, PermissionRequest request, IRedbUser? currentUser = null)
        => _permissionProvider.UpdatePermissionAsync(permission, request, currentUser);

    public Task<bool> DeletePermissionAsync(IRedbPermission permission, IRedbUser? currentUser = null)
        => _permissionProvider.DeletePermissionAsync(permission, currentUser);

    public Task<List<IRedbPermission>> GetPermissionsByUserAsync(IRedbUser user)
        => _permissionProvider.GetPermissionsByUserAsync(user);

    public Task<List<IRedbPermission>> GetPermissionsByRoleAsync(IRedbRole role)
        => _permissionProvider.GetPermissionsByRoleAsync(role);

    public Task<List<IRedbPermission>> GetPermissionsByObjectAsync(IRedbObject obj)
        => _permissionProvider.GetPermissionsByObjectAsync(obj);

    public Task<IRedbPermission?> GetPermissionByIdAsync(long permissionId)
        => _permissionProvider.GetPermissionByIdAsync(permissionId);

    public Task<bool> CanUserEditObject(long objectId, long userId)
        => _permissionProvider.CanUserEditObject(objectId, userId);

    public Task<bool> CanUserSelectObject(long objectId, long userId)
        => _permissionProvider.CanUserSelectObject(objectId, userId);

    public Task<bool> CanUserInsertScheme(long schemeId, long userId)
        => _permissionProvider.CanUserInsertScheme(schemeId, userId);

    public Task<bool> CanUserDeleteObject(long objectId, long userId)
        => _permissionProvider.CanUserDeleteObject(objectId, userId);

    public Task<bool> GrantPermissionAsync(IRedbUser user, IRedbObject obj, PermissionAction actions, IRedbUser? currentUser = null)
        => _permissionProvider.GrantPermissionAsync(user, obj, actions, currentUser);

    public Task<bool> GrantPermissionAsync(IRedbRole role, IRedbObject obj, PermissionAction actions, IRedbUser? currentUser = null)
        => _permissionProvider.GrantPermissionAsync(role, obj, actions, currentUser);

    public Task<bool> RevokePermissionAsync(IRedbUser user, IRedbObject obj, IRedbUser? currentUser = null)
        => _permissionProvider.RevokePermissionAsync(user, obj, currentUser);

    public Task<bool> RevokePermissionAsync(IRedbRole role, IRedbObject obj, IRedbUser? currentUser = null)
        => _permissionProvider.RevokePermissionAsync(role, obj, currentUser);

    public Task<int> RevokeAllUserPermissionsAsync(IRedbUser user, IRedbUser? currentUser = null)
        => _permissionProvider.RevokeAllUserPermissionsAsync(user, currentUser);

    public Task<int> RevokeAllRolePermissionsAsync(IRedbRole role, IRedbUser? currentUser = null)
        => _permissionProvider.RevokeAllRolePermissionsAsync(role, currentUser);

    public Task<EffectivePermissionResult> GetEffectivePermissionsAsync(IRedbUser user, IRedbObject obj)
        => _permissionProvider.GetEffectivePermissionsAsync(user, obj);

    public Task<Dictionary<IRedbObject, EffectivePermissionResult>> GetEffectivePermissionsBatchAsync(IRedbUser user, IRedbObject[] objects)
        => _permissionProvider.GetEffectivePermissionsBatchAsync(user, objects);

    public Task<List<EffectivePermissionResult>> GetAllEffectivePermissionsAsync(IRedbUser user)
        => _permissionProvider.GetAllEffectivePermissionsAsync(user);

    public Task<int> GetPermissionCountAsync()
        => _permissionProvider.GetPermissionCountAsync();

    public Task<int> GetUserPermissionCountAsync(IRedbUser user)
        => _permissionProvider.GetUserPermissionCountAsync(user);

    public Task<int> GetRolePermissionCountAsync(IRedbRole role)
        => _permissionProvider.GetRolePermissionCountAsync(role);

    // === IQueryableProvider DELEGATION ===

    public IRedbQueryable<TProps> Query<TProps>() where TProps : class, new()
        => _queryProvider.Query<TProps>();
    
    public IRedbQueryable<TProps> Query<TProps>(IRedbUser user) where TProps : class, new()
        => _queryProvider.Query<TProps>(user);

    public IRedbQueryable<TProps> TreeQuery<TProps>() where TProps : class, new()
        => _queryProvider.TreeQuery<TProps>();
    
    public IRedbQueryable<TProps> TreeQuery<TProps>(IRedbUser user) where TProps : class, new()
        => _queryProvider.TreeQuery<TProps>(user);

    public IRedbQueryable<TProps> TreeQuery<TProps>(long rootObjectId, int? maxDepth = null) where TProps : class, new()
        => _queryProvider.TreeQuery<TProps>(rootObjectId, maxDepth);

    public IRedbQueryable<TProps> TreeQuery<TProps>(IRedbObject? rootObject, int? maxDepth = null) where TProps : class, new()
        => _queryProvider.TreeQuery<TProps>(rootObject, maxDepth);

    public IRedbQueryable<TProps> TreeQuery<TProps>(IEnumerable<IRedbObject> rootObjects, int? maxDepth = null) where TProps : class, new()
        => _queryProvider.TreeQuery<TProps>(rootObjects, maxDepth);

    public IRedbQueryable<TProps> TreeQuery<TProps>(IEnumerable<long> rootObjectIds, int? maxDepth = null) where TProps : class, new()
        => _queryProvider.TreeQuery<TProps>(rootObjectIds, maxDepth);

    public IRedbQueryable<TProps> TreeQuery<TProps>(long rootObjectId, IRedbUser user, int? maxDepth = null) where TProps : class, new()
        => _queryProvider.TreeQuery<TProps>(rootObjectId, user, maxDepth);

    public IRedbQueryable<TProps> TreeQuery<TProps>(IRedbObject? rootObject, IRedbUser user, int? maxDepth = null) where TProps : class, new()
        => _queryProvider.TreeQuery<TProps>(rootObject, user, maxDepth);

    public IRedbQueryable<TProps> TreeQuery<TProps>(IEnumerable<IRedbObject> rootObjects, IRedbUser user, int? maxDepth = null) where TProps : class, new()
        => _queryProvider.TreeQuery<TProps>(rootObjects, user, maxDepth);

    public IRedbQueryable<TProps> TreeQuery<TProps>(IEnumerable<long> rootObjectIds, IRedbUser user, int? maxDepth = null) where TProps : class, new()
        => _queryProvider.TreeQuery<TProps>(rootObjectIds, user, maxDepth);

    // === IValidationProvider DELEGATION ===

    public Task<List<SupportedType>> GetSupportedTypesAsync()
        => _validationProvider.GetSupportedTypesAsync();

    public Task<ValidationIssue?> ValidateTypeAsync(Type csharpType, string propertyName)
        => _validationProvider.ValidateTypeAsync(csharpType, propertyName);

    public Task<SchemaValidationResult> ValidateSchemaAsync<TProps>(string schemeName, bool strictDeleteExtra = true) where TProps : class
        => _validationProvider.ValidateSchemaAsync<TProps>(schemeName, strictDeleteExtra);

    public ValidationIssue? ValidatePropertyConstraints(Type propertyType, string propertyName, bool isRequired, bool isArray)
        => _validationProvider.ValidatePropertyConstraints(propertyType, propertyName, isRequired, isArray);
    
    public Task<SchemaValidationResult> ValidateSchemaAsync<TProps>(IRedbScheme scheme, bool strictDeleteExtra = true) where TProps : class
        => _validationProvider.ValidateSchemaAsync<TProps>(scheme, strictDeleteExtra);
    
    public Task<SchemaChangeReport> AnalyzeSchemaChangesAsync<TProps>(IRedbScheme scheme) where TProps : class
        => _validationProvider.AnalyzeSchemaChangesAsync<TProps>(scheme);

    // === SECURITY CONTEXT MANAGEMENT ===

    public void SetCurrentUser(IRedbUser user)
        => _securityContext.SetCurrentUser(user);

    public IDisposable CreateSystemContext()
        => _securityContext.CreateSystemContext();

    public long GetEffectiveUserId()
        => _securityContext.GetEffectiveUserId();

    // === CONFIGURATION MANAGEMENT ===

    public void UpdateConfiguration(Action<RedbServiceConfiguration> configure)
    {
        configure(_configuration);
    }

    public void UpdateConfiguration(Action<RedbServiceConfigurationBuilder> configureBuilder)
    {
        var builder = new RedbServiceConfigurationBuilder(_configuration);
        configureBuilder(builder);
        _configuration = builder.Build();
    }

    // === INITIALIZATION ===
    
    /// <summary>
    /// Initialize REDB system at application startup.
    /// </summary>
    public async Task InitializeAsync(params Assembly[] assemblies)
    {
        // 1. Set type resolver for serializer (for polymorphic deserialization)
        SystemTextJsonRedbSerializer.SetTypeResolver(schemeId => _schemeSync.Cache.GetClrType(schemeId));
        
        // 2. Sync all schemes with RedbSchemeAttribute
        await AutoSyncSchemesAsync(assemblies);

        // 3. Sync UserConfigurationProps scheme
        await SyncSchemeAsync<Models.Configuration.UserConfigurationProps>();

        // 4. Initialize default user configuration
        var configInitializer = new Configuration.DefaultUserConfigurationInitializer(this);
        await configInitializer.InitializeAsync();

        // 5. Initialize object factory
        RedbObjectFactory.Initialize(this);
        
        // 6. Set global provider for RedbObject
        RedbObject.SetSchemeSyncProvider(_schemeSync);

        // 7. Warmup metadata cache
        await WarmupMetadataCacheAsync();
        
        // 8. Initialize GlobalPropsCache
        InitializePropsCache();

        // 9. Initialize type registry for polymorphic operations
        await _treeProvider.InitializeTypeRegistryAsync();
    }

    // === DATABASE SCHEMA MANAGEMENT ===

    /// <summary>
    /// Checks whether the specified table exists in the database.
    /// </summary>
    protected abstract Task<bool> TableExistsAsync(string tableName);

    /// <summary>
    /// Reads the embedded combined SQL initialization script (redb_init.sql).
    /// </summary>
    protected abstract string ReadEmbeddedSql();

    /// <summary>
    /// Executes the full schema initialization SQL script against the database.
    /// Provider-specific: e.g. MSSQL splits by GO batch separators.
    /// </summary>
    protected abstract Task ExecuteSchemaScriptAsync(string sql);

    /// <inheritdoc />
    public virtual async Task EnsureDatabaseAsync()
    {
        // Check if the core table '_schemes' already exists
        if (await TableExistsAsync("_schemes"))
        {
            _logger?.LogInformation("REDB schema already exists, skipping creation.");
            return;
        }

        _logger?.LogInformation("REDB schema not found. Creating database schema...");

        var sql = ReadEmbeddedSql();
        await ExecuteSchemaScriptAsync(sql);

        _logger?.LogInformation("REDB database schema created successfully.");
    }

    /// <inheritdoc />
    public string GetSchemaScript() => ReadEmbeddedSql();

    /// <inheritdoc />
    public async Task InitializeAsync(bool ensureCreated, params Assembly[] assemblies)
    {
        if (ensureCreated)
            await EnsureDatabaseAsync();

        await InitializeAsync(assemblies);
    }
    
    /// <summary>
    /// Warmup metadata cache. Override in derived classes for DB-specific SQL.
    /// </summary>
    protected virtual async Task WarmupMetadataCacheAsync()
    {
        if (!_configuration.WarmupMetadataCacheOnInit) return;
        
        try
        {
            var sw = System.Diagnostics.Stopwatch.StartNew();
            var warmupSql = SqlDialect.Warmup_AllMetadataCaches();
            var result = await _context.QueryAsync<WarmupCacheResult>(warmupSql);
            sw.Stop();
            
            if (result.Any())
            {
                _logger?.LogInformation(
                    "Metadata cache warmed up: {SchemeCount} schemes, {StructureCount} structures in {ElapsedMs} ms",
                    result.Count, result.Sum(r => r.structures_count), sw.ElapsedMilliseconds);
            }
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Failed to warmup metadata cache during initialization");
        }
    }
    
    /// <summary>
    /// Initialize GlobalPropsCache.
    /// </summary>
    private void InitializePropsCache()
    {
        if (!_configuration.EnablePropsCache) return;
        
        var userConfigService = _serviceProvider.GetService(typeof(Configuration.IUserConfigurationService)) 
            as Configuration.IUserConfigurationService;
        
        var cache = new Caching.MemoryRedbObjectCache(
            maxSize: _configuration.PropsCacheMaxSize,
            ttl: _configuration.PropsCacheTtl,
            getUserIdFunc: () => GetEffectiveUserId(),
            getQuotaFunc: userConfigService != null 
                ? async (userId) => 
                {
                    var config = await userConfigService.GetEffectiveConfigurationAsync(userId);
                    return config.PropsCacheSize;
                }
                : null);
        
        _schemeSync.PropsCache.Initialize(cache);
    }
    
    /// <summary>
    /// Auto-sync all schemes with RedbSchemeAttribute.
    /// </summary>
    private async Task AutoSyncSchemesAsync(params Assembly[] assemblies)
    {
        IEnumerable<Assembly> assembliesToScan = assemblies.Length > 0
            ? assemblies
            : GetAllLoadedAssemblies();

        var typesToSync = assembliesToScan
            .SelectMany(GetTypesWithRedbSchemeAttribute)
            .ToList();

        if (typesToSync.Count == 0) return;

        foreach (var type in typesToSync)
        {
            await SyncSchemeForTypeAsync(type);
        }
    }
    
    private static IEnumerable<Assembly> GetAllLoadedAssemblies()
    {
#if NET5_0_OR_GREATER
        return AssemblyLoadContext.Default.Assemblies;
#else
        return AppDomain.CurrentDomain.GetAssemblies();
#endif
    }
    
    private static IEnumerable<Type> GetTypesWithRedbSchemeAttribute(Assembly assembly)
    {
        try
        {
            return assembly.GetTypes()
                .Where(t => t.GetCustomAttribute<RedbSchemeAttribute>() != null);
        }
        catch (ReflectionTypeLoadException ex)
        {
            return ex.Types
                .Where(t => t != null && t.GetCustomAttribute<RedbSchemeAttribute>() != null)!;
        }
        catch
        {
            return Enumerable.Empty<Type>();
        }
    }
    
    private async Task SyncSchemeForTypeAsync(Type type)
    {
        try
        {
            var method = typeof(ISchemeSyncProvider)
                .GetMethod(nameof(ISchemeSyncProvider.SyncSchemeAsync))
                ?.MakeGenericMethod(type);

            if (method != null)
            {
                var task = method.Invoke(this, null);
                if (task is Task asyncTask)
                {
                    await asyncTask;
                }
            }
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Failed to sync scheme for type '{TypeName}'", type.FullName);
            throw;
        }
    }
}

/// <summary>
/// Result from warmup_all_metadata_caches() SQL function.
/// </summary>
internal class WarmupCacheResult
{
    public long scheme_id { get; set; }
    public long structures_count { get; set; }
    public string? scheme_name { get; set; }
}

