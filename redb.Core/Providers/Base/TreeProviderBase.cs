using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using redb.Core.Data;
using redb.Core.Models;
using redb.Core.Models.Configuration;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Core.Models.Security;
using redb.Core.Query;
using redb.Core.Serialization;
using redb.Core.Utils;
using redb.Core.Caching;
using Microsoft.Extensions.Logging;

namespace redb.Core.Providers.Base;

/// <summary>
/// Base class for tree provider with all platform-agnostic logic.
/// SQL queries are delegated to ISqlDialect.
/// </summary>
public abstract class TreeProviderBase : ITreeProvider
{
    protected readonly IRedbContext Context;
    protected readonly IObjectStorageProvider ObjectStorage;
    protected readonly IPermissionProvider PermissionProvider;
    protected readonly IRedbObjectSerializer Serializer;
    protected readonly IRedbSecurityContext SecurityContext;
    protected readonly RedbServiceConfiguration Configuration;
    protected readonly ISchemeSyncProvider SchemeSyncProvider;
    protected readonly ISqlDialect Sql;
    protected GlobalMetadataCache Cache => SchemeSyncProvider.Cache;
    protected readonly ILogger? Logger;

    protected TreeProviderBase(
        IRedbContext context,
        IObjectStorageProvider objectStorage,
        IPermissionProvider permissionProvider,
        IRedbObjectSerializer serializer,
        IRedbSecurityContext securityContext,
        ISchemeSyncProvider schemeSyncProvider,
        ISqlDialect sql,
        RedbServiceConfiguration? configuration = null,
        ILogger? logger = null)
    {
        Context = context ?? throw new ArgumentNullException(nameof(context));
        ObjectStorage = objectStorage ?? throw new ArgumentNullException(nameof(objectStorage));
        PermissionProvider = permissionProvider ?? throw new ArgumentNullException(nameof(permissionProvider));
        Serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        SecurityContext = securityContext ?? throw new ArgumentNullException(nameof(securityContext));
        SchemeSyncProvider = schemeSyncProvider ?? throw new ArgumentNullException(nameof(schemeSyncProvider));
        Sql = sql ?? throw new ArgumentNullException(nameof(sql));
        Configuration = configuration ?? new RedbServiceConfiguration();
        Logger = logger;
    }

    // ============================================================
    // === INTERFACE IMPLEMENTATION ===
    // ============================================================

    public async Task InitializeTypeRegistryAsync()
    {
        if (!Cache.IsClrTypeRegistryInitialized)
        {
            await Cache.InitializeClrTypeRegistryAsync(SchemeSyncProvider);
        }
    }

    // ===== BASE METHODS (use _securityContext and configuration) =====
    
    /// <summary>
    /// Load tree by root object ID (uses _securityContext).
    /// </summary>
    public async Task<TreeRedbObject<TProps>> LoadTreeAsync<TProps>(long rootObjectId, int? maxDepth = null) where TProps : class, new()
    {
        var effectiveUser = SecurityContext.GetEffectiveUser();
        var actualMaxDepth = maxDepth ?? Configuration.DefaultMaxTreeDepth;
        return await LoadTreeWithUserAsync<TProps>(rootObjectId, actualMaxDepth, effectiveUser.Id, Configuration.DefaultCheckPermissionsOnLoad);
    }
    
    public async Task<TreeRedbObject<TProps>> LoadTreeAsync<TProps>(IRedbObject rootObj, int? maxDepth = null) where TProps : class, new()
    {
        var effectiveUser = SecurityContext.GetEffectiveUser();
        var actualMaxDepth = maxDepth ?? Configuration.DefaultMaxTreeDepth;
        return await LoadTreeWithUserAsync<TProps>(rootObj.Id, actualMaxDepth, effectiveUser.Id, Configuration.DefaultCheckPermissionsOnLoad);
    }
    
    public async Task<IEnumerable<TreeRedbObject<TProps>>> GetChildrenAsync<TProps>(IRedbObject parentObj) where TProps : class, new()
    {
        var effectiveUser = SecurityContext.GetEffectiveUser();
        return await GetChildrenWithUserAsync<TProps>(parentObj.Id, effectiveUser.Id, Configuration.DefaultCheckPermissionsOnLoad);
    }
    
    public async Task<IEnumerable<TreeRedbObject<TProps>>> GetPathToRootAsync<TProps>(IRedbObject obj) where TProps : class, new()
    {
        var effectiveUser = SecurityContext.GetEffectiveUser();
        return await GetPathToRootWithUserAsync<TProps>(obj.Id, effectiveUser.Id, Configuration.DefaultCheckPermissionsOnLoad);
    }
    
    public async Task<IEnumerable<TreeRedbObject<TProps>>> GetDescendantsAsync<TProps>(IRedbObject parentObj, int? maxDepth = null) where TProps : class, new()
    {
        var effectiveUser = SecurityContext.GetEffectiveUser();
        var actualMaxDepth = maxDepth ?? Configuration.DefaultMaxTreeDepth;
        return await GetDescendantsWithUserAsync<TProps>(parentObj.Id, actualMaxDepth, effectiveUser.Id, Configuration.DefaultCheckPermissionsOnLoad);
    }
    
    public async Task MoveObjectAsync(IRedbObject obj, IRedbObject? newParentObj)
    {
        var effectiveUser = SecurityContext.GetEffectiveUser();
        await MoveObjectWithUserAsync(obj.Id, newParentObj?.Id, effectiveUser.Id, Configuration.DefaultCheckPermissionsOnSave);
    }
    
    public async Task<long> CreateChildAsync<TProps>(TreeRedbObject<TProps> obj, IRedbObject parentObj) where TProps : class, new()
    {
        var effectiveUser = SecurityContext.GetEffectiveUser();
        return await CreateChildWithUserAsync(obj, parentObj.Id, effectiveUser.Id, Configuration.DefaultCheckPermissionsOnSave);
    }

    public async Task<int> DeleteSubtreeAsync(IRedbObject parentObj)
    {
        var effectiveUser = SecurityContext.GetEffectiveUser();
        return await DeleteSubtreeWithUserAsync(parentObj.Id, effectiveUser);
    }

    // ===== OVERLOADS WITH EXPLICIT USER =====
    
    /// <summary>
    /// Load tree by root object ID with explicit user.
    /// </summary>
    public async Task<TreeRedbObject<TProps>> LoadTreeAsync<TProps>(long rootObjectId, IRedbUser user, int? maxDepth = null) where TProps : class, new()
    {
        var actualMaxDepth = maxDepth ?? Configuration.DefaultMaxTreeDepth;
        return await LoadTreeWithUserAsync<TProps>(rootObjectId, actualMaxDepth, user.Id, Configuration.DefaultCheckPermissionsOnLoad);
    }
    
    public async Task<TreeRedbObject<TProps>> LoadTreeAsync<TProps>(IRedbObject rootObj, IRedbUser user, int? maxDepth = null) where TProps : class, new()
    {
        var actualMaxDepth = maxDepth ?? Configuration.DefaultMaxTreeDepth;
        return await LoadTreeWithUserAsync<TProps>(rootObj.Id, actualMaxDepth, user.Id, Configuration.DefaultCheckPermissionsOnLoad);
    }
    
    public async Task<IEnumerable<TreeRedbObject<TProps>>> GetChildrenAsync<TProps>(IRedbObject parentObj, IRedbUser user) where TProps : class, new()
        => await GetChildrenWithUserAsync<TProps>(parentObj.Id, user.Id, Configuration.DefaultCheckPermissionsOnLoad);
    
    public async Task<IEnumerable<TreeRedbObject<TProps>>> GetPathToRootAsync<TProps>(IRedbObject obj, IRedbUser user) where TProps : class, new()
        => await GetPathToRootWithUserAsync<TProps>(obj.Id, user.Id, Configuration.DefaultCheckPermissionsOnLoad);
    
    public async Task<IEnumerable<TreeRedbObject<TProps>>> GetDescendantsAsync<TProps>(IRedbObject parentObj, IRedbUser user, int? maxDepth = null) where TProps : class, new()
    {
        var actualMaxDepth = maxDepth ?? Configuration.DefaultMaxTreeDepth;
        return await GetDescendantsWithUserAsync<TProps>(parentObj.Id, actualMaxDepth, user.Id, Configuration.DefaultCheckPermissionsOnLoad);
    }
    
    public async Task MoveObjectAsync(IRedbObject obj, IRedbObject? newParentObj, IRedbUser user)
        => await MoveObjectWithUserAsync(obj.Id, newParentObj?.Id, user.Id, Configuration.DefaultCheckPermissionsOnSave);
    
    public async Task<long> CreateChildAsync<TProps>(TreeRedbObject<TProps> obj, IRedbObject parentObj, IRedbUser user) where TProps : class, new()
        => await CreateChildWithUserAsync(obj, parentObj.Id, user.Id, Configuration.DefaultCheckPermissionsOnSave);

    public async Task<int> DeleteSubtreeAsync(IRedbObject parentObj, IRedbUser user)
        => await DeleteSubtreeWithUserAsync(parentObj.Id, user);

    // ===== POLYMORPHIC METHODS =====
    
    public async Task<ITreeRedbObject> LoadPolymorphicTreeAsync(IRedbObject rootObj, int? maxDepth = null)
    {
        var effectiveUser = SecurityContext.GetEffectiveUser();
        var actualMaxDepth = maxDepth ?? Configuration.DefaultMaxTreeDepth;
        return await LoadPolymorphicTreeWithUserAsync(rootObj.Id, actualMaxDepth, effectiveUser.Id, Configuration.DefaultCheckPermissionsOnLoad);
    }
    
    public async Task<IEnumerable<ITreeRedbObject>> GetPolymorphicChildrenAsync(IRedbObject parentObj)
    {
        var effectiveUser = SecurityContext.GetEffectiveUser();
        return await GetPolymorphicChildrenWithUserAsync(parentObj.Id, effectiveUser.Id, Configuration.DefaultCheckPermissionsOnLoad);
    }
    
    public async Task<IEnumerable<ITreeRedbObject>> GetPolymorphicPathToRootAsync(IRedbObject obj)
    {
        var effectiveUser = SecurityContext.GetEffectiveUser();
        return await GetPolymorphicPathToRootWithUserAsync(obj.Id, effectiveUser.Id, Configuration.DefaultCheckPermissionsOnLoad);
    }
    
    public async Task<IEnumerable<ITreeRedbObject>> GetPolymorphicDescendantsAsync(IRedbObject parentObj, int? maxDepth = null)
    {
        var effectiveUser = SecurityContext.GetEffectiveUser();
        var actualMaxDepth = maxDepth ?? Configuration.DefaultMaxTreeDepth;
        return await GetPolymorphicDescendantsWithUserAsync(parentObj.Id, actualMaxDepth, effectiveUser.Id, Configuration.DefaultCheckPermissionsOnLoad);
    }

    // ===== POLYMORPHIC METHODS WITH EXPLICIT USER =====
    
    public async Task<ITreeRedbObject> LoadPolymorphicTreeAsync(IRedbObject rootObj, IRedbUser user, int? maxDepth = null)
    {
        var actualMaxDepth = maxDepth ?? Configuration.DefaultMaxTreeDepth;
        return await LoadPolymorphicTreeWithUserAsync(rootObj.Id, actualMaxDepth, user.Id, Configuration.DefaultCheckPermissionsOnLoad);
    }
    
    public async Task<IEnumerable<ITreeRedbObject>> GetPolymorphicChildrenAsync(IRedbObject parentObj, IRedbUser user)
        => await GetPolymorphicChildrenWithUserAsync(parentObj.Id, user.Id, Configuration.DefaultCheckPermissionsOnLoad);
    
    public async Task<IEnumerable<ITreeRedbObject>> GetPolymorphicPathToRootAsync(IRedbObject obj, IRedbUser user)
        => await GetPolymorphicPathToRootWithUserAsync(obj.Id, user.Id, Configuration.DefaultCheckPermissionsOnLoad);
    
    public async Task<IEnumerable<ITreeRedbObject>> GetPolymorphicDescendantsAsync(IRedbObject parentObj, IRedbUser user, int? maxDepth = null)
    {
        var actualMaxDepth = maxDepth ?? Configuration.DefaultMaxTreeDepth;
        return await GetPolymorphicDescendantsWithUserAsync(parentObj.Id, actualMaxDepth, user.Id, Configuration.DefaultCheckPermissionsOnLoad);
    }

    // ============================================================
    // === PROTECTED VIRTUAL METHODS (can be overridden in derived classes) ===
    // ============================================================

    protected virtual async Task<TreeRedbObject<TProps>> LoadTreeWithUserAsync<TProps>(
        long rootId, int maxDepth = 10, long? userId = null, bool checkPermissions = false) where TProps : class, new()
    {
        var baseObject = await ObjectStorage.LoadAsync<TProps>(rootId, 1);
        if (baseObject == null)
            throw new InvalidOperationException($"Object with ID {rootId} not found");
        
        var treeObject = ConvertToTreeObject(baseObject);
        await LoadChildrenRecursively(treeObject, maxDepth - 1, userId, checkPermissions);
        
        return treeObject;
    }

    protected virtual async Task<IEnumerable<TreeRedbObject<TProps>>> GetChildrenWithUserAsync<TProps>(
        long parentId, long? userId = null, bool checkPermissions = false) where TProps : class, new()
    {
        var scheme = await SchemeSyncProvider.GetSchemeByTypeAsync<TProps>();
        if (scheme == null)
            throw new InvalidOperationException($"Scheme for type {typeof(TProps).Name} not found. Use SyncSchemeAsync<{typeof(TProps).Name}>() to create scheme.");
        
        var jsonResults = await Context.ExecuteJsonListAsync(Sql.Tree_SelectChildrenJson(), parentId, scheme.Id);
        
        var children = new List<TreeRedbObject<TProps>>();
        
        foreach (var json in jsonResults)
        {
            if (string.IsNullOrEmpty(json)) continue;
            
            try
            {
                var redbObj = Serializer.Deserialize<TProps>(json);
                if (redbObj == null) continue;
                
                if (checkPermissions && userId.HasValue)
                {
                    var canSelect = await PermissionProvider.CanUserSelectObject(redbObj);
                    if (!canSelect) continue;
                }
                
                var treeObj = ConvertToTreeObject(redbObj);
                children.Add(treeObj);
            }
            catch
            {
                throw;
            }
        }
        
        return children;
    }

    protected virtual async Task<IEnumerable<TreeRedbObject<TProps>>> GetPathToRootWithUserAsync<TProps>(
        long objectId, long? userId = null, bool checkPermissions = false) where TProps : class, new()
    {
        var path = new List<TreeRedbObject<TProps>>();
        var visited = new HashSet<long>();
        long? currentId = objectId;
        
        while (currentId.HasValue)
        {
            if (visited.Contains(currentId.Value))
                break;
            
            visited.Add(currentId.Value);
            
            try
            {
                var effectiveUser = SecurityContext.GetEffectiveUser();
                var obj = await ObjectStorage.LoadAsync<TProps>(currentId.Value, effectiveUser, 1);
                if (obj == null) break;
                
                var treeObj = ConvertToTreeObject(obj);
                path.Insert(0, treeObj);
                currentId = obj.parent_id;
            }
            catch (UnauthorizedAccessException)
            {
                break;
            }
            catch
            {
                throw;
            }
        }
        
        for (int i = 0; i < path.Count - 1; i++)
        {
            path[i + 1].Parent = path[i];
        }
        
        return path;
    }

    protected virtual async Task<IEnumerable<TreeRedbObject<TProps>>> GetDescendantsWithUserAsync<TProps>(
        long parentId, int maxDepth = 50, long? userId = null, bool checkPermissions = false) where TProps : class, new()
    {
        var descendants = new List<TreeRedbObject<TProps>>();
        await CollectDescendants(parentId, descendants, maxDepth, 0, userId, checkPermissions);
        return descendants;
    }

    protected virtual async Task MoveObjectWithUserAsync(long objectId, long? newParentId, long userId, bool checkPermissions = true)
    {
        if (checkPermissions)
        {
            // Use direct ID-based permission check (no need to load full object with Props)
            var canEdit = await PermissionProvider.CanUserEditObject(objectId, userId);
            if (!canEdit)
                throw new UnauthorizedAccessException($"User {userId} does not have permission to edit object {objectId}");
        }
        
        if (newParentId.HasValue)
        {
            var parentExists = await Context.ExecuteScalarAsync<long?>(Sql.Tree_ObjectExists(), newParentId.Value);
            if (!parentExists.HasValue)
                throw new ArgumentException($"Parent object {newParentId} does not exist");
            
            await ValidateNoCyclicReference(objectId, newParentId.Value);
        }
        
        var rowsAffected = await Context.ExecuteAsync(
            Sql.Tree_UpdateParent(),
            newParentId.HasValue ? (object)newParentId.Value : DBNull.Value,
            DateTimeOffset.Now,
            userId,
            objectId);
        
        if (rowsAffected == 0)
            throw new ArgumentException($"Object {objectId} not found");
    }

    protected virtual async Task<long> CreateChildWithUserAsync<TProps>(
        TreeRedbObject<TProps> obj, long parentId, long? userId = null, bool checkPermissions = false) where TProps : class, new()
    {
        if (obj.id == 0)
        {
            obj.id = await Context.NextObjectIdAsync();
        }
        
        obj.parent_id = parentId == 0 ? null : parentId;
        
        var effectiveUser = SecurityContext.GetEffectiveUser();
        return await ObjectStorage.SaveAsync(obj, effectiveUser);
    }

    protected virtual async Task<int> DeleteSubtreeWithUserAsync(long parentId, IRedbUser user)
    {
        var checkPermissions = Configuration.DefaultCheckPermissionsOnDelete;
        
        if (checkPermissions)
        {
            var obj = await ObjectStorage.LoadAsync<object>(parentId, 1);
            var canDelete = await PermissionProvider.CanUserDeleteObject(obj!, user);
            if (!canDelete)
                throw new UnauthorizedAccessException($"User {user.Id} does not have permission to delete subtree of object {parentId}");
        }

        var descendants = await GetDescendantsWithUserAsync<object>(parentId, 100, user.Id, false);
        var objectIds = descendants.Select(d => d.id).ToList();
        objectIds.Add(parentId);

        await Context.ExecuteAsync(Sql.Tree_DeleteValuesByObjectIds(), objectIds.ToArray());
        var deletedCount = await Context.ExecuteAsync(Sql.Tree_DeleteObjectsByIds(), objectIds.ToArray());
        
        return deletedCount;
    }

    // ============================================================
    // === POLYMORPHIC IMPLEMENTATION ===
    // ============================================================

    protected virtual async Task<IRedbObject> LoadDynamicObjectAsync(long objectId, IRedbUser? user = null)
    {
        if (Configuration.DefaultCheckPermissionsOnLoad && user != null)
        {
            var canRead = await PermissionProvider.CanUserSelectObject(objectId, user.Id);
            if (!canRead)
                throw new UnauthorizedAccessException($"User {user.Id} does not have permission to read object {objectId}");
        }

        var result = await Context.QueryFirstOrDefaultAsync<SchemeWithJson>(Sql.Tree_SelectSchemeAndJson(), objectId);

        if (result == null || string.IsNullOrEmpty(result.JsonData))
            throw new InvalidOperationException($"Object with ID {objectId} not found");

        var propsType = Cache.GetClrType(result.SchemeId) ?? typeof(object);
        return Serializer.DeserializeDynamic(result.JsonData, propsType);
    }
    
    protected virtual async Task<ITreeRedbObject> LoadPolymorphicTreeWithUserAsync(
        long rootId, int maxDepth = 10, long? userId = null, bool checkPermissions = false)
    {
        var user = userId.HasValue ? await GetUserByIdAsync(userId.Value) : null;
        var baseObject = await LoadDynamicObjectAsync(rootId, user);
        var treeObject = ConvertToPolymorphicTreeObjectWithProps(baseObject);
        
        await LoadPolymorphicChildrenRecursively(treeObject, maxDepth - 1, userId, checkPermissions);
        
        return treeObject;
    }

    protected virtual async Task<IEnumerable<ITreeRedbObject>> GetPolymorphicChildrenWithUserAsync(
        long parentId, long? userId = null, bool checkPermissions = false)
    {
        var results = await Context.QueryAsync<ChildObjectInfo>(Sql.Tree_SelectPolymorphicChildren(), parentId);
        
        var children = new List<ITreeRedbObject>();
        
        foreach (var result in results)
        {
            if (string.IsNullOrEmpty(result.JsonData)) continue;
            
            try
            {
                if (checkPermissions && userId.HasValue)
                {
                    var canSelect = await PermissionProvider.CanUserSelectObject(result.ObjectId, userId.Value);
                    if (!canSelect) continue;
                }
                
                var propsType = Cache.GetClrType(result.SchemeId) ?? typeof(object);
                var typedObject = Serializer.DeserializeDynamic(result.JsonData, propsType);
                var treeObj = ConvertToPolymorphicTreeObjectWithProps(typedObject);
                children.Add(treeObj);
            }
            catch
            {
                throw;
            }
        }
        
        return children;
    }

    protected virtual async Task<IEnumerable<ITreeRedbObject>> GetPolymorphicPathToRootWithUserAsync(
        long objectId, long? userId = null, bool checkPermissions = false)
    {
        var path = new List<ITreeRedbObject>();
        var visited = new HashSet<long>();
        long? currentId = objectId;
        
        while (currentId.HasValue)
        {
            if (visited.Contains(currentId.Value))
                break;
            
            visited.Add(currentId.Value);
            
            try
            {
                var user = userId.HasValue ? await GetUserByIdAsync(userId.Value) : null;
                var typedObject = await LoadDynamicObjectAsync(currentId.Value, user);
                
                if (checkPermissions && userId.HasValue)
                {
                    var canSelect = await PermissionProvider.CanUserSelectObject(currentId.Value, userId.Value);
                    if (!canSelect) break;
                }
                
                var treeObj = ConvertToPolymorphicTreeObjectWithProps(typedObject);
                path.Insert(0, treeObj);
                currentId = typedObject.ParentId;
            }
            catch (UnauthorizedAccessException)
            {
                break;
            }
            catch
            {
                throw;
            }
        }
        
        for (int i = 0; i < path.Count - 1; i++)
        {
            path[i + 1].Parent = path[i];
        }
        
        return path;
    }

    protected virtual async Task<IEnumerable<ITreeRedbObject>> GetPolymorphicDescendantsWithUserAsync(
        long parentId, int maxDepth = 50, long? userId = null, bool checkPermissions = false)
    {
        var descendants = new List<ITreeRedbObject>();
        await CollectPolymorphicDescendants(parentId, descendants, maxDepth, 0, userId, checkPermissions);
        return descendants;
    }

    // ============================================================
    // === PRIVATE HELPER METHODS ===
    // ============================================================

    private async Task LoadChildrenRecursively<TProps>(
        TreeRedbObject<TProps> parent, int remainingDepth, long? userId, bool checkPermissions) where TProps : class, new()
    {
        if (remainingDepth <= 0) return;
        
        var children = await GetChildrenWithUserAsync<TProps>(parent.id, userId, checkPermissions);
        
        foreach (var child in children)
        {
            child.Parent = parent;
            parent.Children.Add(child);
            await LoadChildrenRecursively(child, remainingDepth - 1, userId, checkPermissions);
        }
    }

    private async Task CollectDescendants<TProps>(
        long parentId, List<TreeRedbObject<TProps>> descendants, int maxDepth, int currentDepth, long? userId, bool checkPermissions) where TProps : class, new()
    {
        if (currentDepth >= maxDepth) return;
        
        var children = await GetChildrenWithUserAsync<TProps>(parentId, userId, checkPermissions);
        
        foreach (var child in children)
        {
            descendants.Add(child);
            await CollectDescendants(child.id, descendants, maxDepth, currentDepth + 1, userId, checkPermissions);
        }
    }

    private async Task ValidateNoCyclicReference(long objectId, long newParentId)
    {
        var visited = new HashSet<long>();
        long? currentId = newParentId;
        
        while (currentId.HasValue)
        {
            if (currentId.Value == objectId)
                throw new InvalidOperationException($"Cannot move object {objectId}: this would create a cyclic reference");
            
            if (visited.Contains(currentId.Value))
                throw new InvalidOperationException("Cyclic reference detected in existing data structure");
            
            visited.Add(currentId.Value);
            
            var parent = await Context.ExecuteScalarAsync<long?>(Sql.Tree_SelectParentId(), currentId.Value);
            currentId = parent;
        }
    }

    private async Task LoadPolymorphicChildrenRecursively(ITreeRedbObject parent, int remainingDepth, long? userId, bool checkPermissions)
    {
        if (remainingDepth <= 0) return;
        
        var children = await GetPolymorphicChildrenWithUserAsync(parent.Id, userId, checkPermissions);
        
        foreach (var child in children)
        {
            child.Parent = parent;
            parent.Children.Add(child);
            await LoadPolymorphicChildrenRecursively(child, remainingDepth - 1, userId, checkPermissions);
        }
    }

    private async Task CollectPolymorphicDescendants(
        long parentId, List<ITreeRedbObject> descendants, int maxDepth, int currentDepth, long? userId, bool checkPermissions)
    {
        if (currentDepth >= maxDepth) return;
        
        var children = await GetPolymorphicChildrenWithUserAsync(parentId, userId, checkPermissions);
        
        foreach (var child in children)
        {
            descendants.Add(child);
            await CollectPolymorphicDescendants(child.Id, descendants, maxDepth, currentDepth + 1, userId, checkPermissions);
        }
    }

    private Task<IRedbUser?> GetUserByIdAsync(long userId)
    {
        return Task.FromResult<IRedbUser?>(new DummyUser { Id = userId });
    }

    // ============================================================
    // === CONVERSION HELPERS ===
    // ============================================================

    protected TreeRedbObject<TProps> ConvertToTreeObject<TProps>(RedbObject<TProps> source) where TProps : class, new()
    {
        return new TreeRedbObject<TProps>
        {
            id = source.id,
            parent_id = source.parent_id,
            scheme_id = source.scheme_id,
            owner_id = source.owner_id,
            who_change_id = source.who_change_id,
            date_create = source.date_create,
            date_modify = source.date_modify,
            date_begin = source.date_begin,
            date_complete = source.date_complete,
            key = source.key,
            value_long = source.value_long,
            value_string = source.value_string,
            value_guid = source.value_guid,
            value_bool = source.value_bool,
            value_double = source.value_double,
            value_numeric = source.value_numeric,
            value_datetime = source.value_datetime,
            value_bytes = source.value_bytes,
            name = source.name,
            note = source.note,
            hash = source.hash,
            Props = source.Props
        };
    }

    protected ITreeRedbObject ConvertToPolymorphicTreeObjectWithProps(IRedbObject source)
    {
        var redbObj = source as RedbObject;
        if (redbObj == null)
            throw new InvalidOperationException($"Object must be of type RedbObject, got: {source.GetType()}");

        return new TreeRedbObjectDynamic(source)
        {
            id = redbObj.id,
            parent_id = redbObj.parent_id,
            scheme_id = redbObj.scheme_id,
            owner_id = redbObj.owner_id,
            who_change_id = redbObj.who_change_id,
            date_create = redbObj.date_create,
            date_modify = redbObj.date_modify,
            date_begin = redbObj.date_begin,
            date_complete = redbObj.date_complete,
            key = redbObj.key,
            value_long = redbObj.value_long,
            value_string = redbObj.value_string,
            value_guid = redbObj.value_guid,
            value_bool = redbObj.value_bool,
            value_double = redbObj.value_double,
            value_numeric = redbObj.value_numeric,
            value_datetime = redbObj.value_datetime,
            value_bytes = redbObj.value_bytes,
            name = redbObj.name,
            note = redbObj.note,
            hash = redbObj.hash
        };
    }

    // ============================================================
    // === HELPER CLASSES ===
    // ============================================================

    private class DummyUser : IRedbUser
    {
        public long Id { get; set; }
        public string Login { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
        public string Password { get; set; } = string.Empty;
        public bool Enabled { get; set; } = true;
        public DateTimeOffset DateRegister { get; set; } = DateTimeOffset.UtcNow;
        public DateTimeOffset? DateDismiss { get; set; }
        public string? Phone { get; set; }
        public string? Email { get; set; }
        public long? Key { get; set; }
        public long? CodeInt { get; set; }
        public string? CodeString { get; set; }
        public Guid? CodeGuid { get; set; }
        public string? Note { get; set; }
        public Guid? Hash { get; set; }
    }

    protected class TreeRedbObjectDynamic : TreeRedbObject, ITreeRedbObject
    {
        public IRedbObject SourceObject { get; }

        public TreeRedbObjectDynamic(IRedbObject source)
        {
            SourceObject = source;
        }
    }
}

/// <summary>
/// DTO for scheme + JSON query result.
/// </summary>
public class SchemeWithJson
{
    public long SchemeId { get; set; }
    public string? JsonData { get; set; }
}

/// <summary>
/// DTO for child object query result.
/// </summary>
public class ChildObjectInfo
{
    public long ObjectId { get; set; }
    public long SchemeId { get; set; }
    public string? JsonData { get; set; }
}

