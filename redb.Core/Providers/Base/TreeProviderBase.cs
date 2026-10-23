using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
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

    public async Task InitializeTypeRegistryAsync(CancellationToken cancellationToken = default)
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
    public async Task<TreeRedbObject<TProps>> LoadTreeAsync<TProps>(long rootObjectId, int? maxDepth = null, CancellationToken cancellationToken = default) where TProps : class, new()
    {
        var effectiveUser = SecurityContext.GetEffectiveUser();
        var actualMaxDepth = maxDepth ?? Configuration.DefaultMaxTreeDepth;
        return await LoadTreeWithUserAsync<TProps>(rootObjectId, actualMaxDepth, effectiveUser.Id, Configuration.DefaultCheckPermissionsOnLoad);
    }
    
    public async Task<TreeRedbObject<TProps>> LoadTreeAsync<TProps>(IRedbObject rootObj, int? maxDepth = null, CancellationToken cancellationToken = default) where TProps : class, new()
    {
        var effectiveUser = SecurityContext.GetEffectiveUser();
        var actualMaxDepth = maxDepth ?? Configuration.DefaultMaxTreeDepth;
        return await LoadTreeWithUserAsync<TProps>(rootObj.Id, actualMaxDepth, effectiveUser.Id, Configuration.DefaultCheckPermissionsOnLoad);
    }
    
    public async Task<IEnumerable<TreeRedbObject<TProps>>> GetChildrenAsync<TProps>(IRedbObject parentObj, CancellationToken cancellationToken = default) where TProps : class, new()
    {
        var effectiveUser = SecurityContext.GetEffectiveUser();
        return await GetChildrenWithUserAsync<TProps>(parentObj.Id, effectiveUser.Id, Configuration.DefaultCheckPermissionsOnLoad);
    }
    
    public async Task<IEnumerable<TreeRedbObject<TProps>>> GetPathToRootAsync<TProps>(IRedbObject obj, CancellationToken cancellationToken = default) where TProps : class, new()
    {
        var effectiveUser = SecurityContext.GetEffectiveUser();
        return await GetPathToRootWithUserAsync<TProps>(obj.Id, effectiveUser.Id, Configuration.DefaultCheckPermissionsOnLoad);
    }
    
    public async Task<IEnumerable<TreeRedbObject<TProps>>> GetDescendantsAsync<TProps>(IRedbObject parentObj, int? maxDepth = null, CancellationToken cancellationToken = default) where TProps : class, new()
    {
        var effectiveUser = SecurityContext.GetEffectiveUser();
        var actualMaxDepth = maxDepth ?? Configuration.DefaultMaxTreeDepth;
        return await GetDescendantsWithUserAsync<TProps>(parentObj.Id, actualMaxDepth, effectiveUser.Id, Configuration.DefaultCheckPermissionsOnLoad);
    }
    
    public async Task MoveObjectAsync(IRedbObject obj, IRedbObject? newParentObj, CancellationToken cancellationToken = default)
    {
        var effectiveUser = SecurityContext.GetEffectiveUser();
        await MoveObjectWithUserAsync(obj.Id, newParentObj?.Id, effectiveUser.Id, Configuration.DefaultCheckPermissionsOnSave);
    }
    
    public async Task<long> CreateChildAsync<TProps>(TreeRedbObject<TProps> obj, IRedbObject parentObj, CancellationToken cancellationToken = default) where TProps : class, new()
    {
        var effectiveUser = SecurityContext.GetEffectiveUser();
        return await CreateChildWithUserAsync(obj, parentObj.Id, effectiveUser.Id, Configuration.DefaultCheckPermissionsOnSave);
    }

    public async Task<int> DeleteSubtreeAsync(IRedbObject parentObj, CancellationToken cancellationToken = default)
    {
        var effectiveUser = SecurityContext.GetEffectiveUser();
        return await DeleteSubtreeWithUserAsync(parentObj.Id, effectiveUser);
    }

    // ===== OVERLOADS WITH EXPLICIT USER =====
    
    /// <summary>
    /// Load tree by root object ID with explicit user.
    /// </summary>
    public async Task<TreeRedbObject<TProps>> LoadTreeAsync<TProps>(long rootObjectId, IRedbUser user, int? maxDepth = null, CancellationToken cancellationToken = default) where TProps : class, new()
    {
        var actualMaxDepth = maxDepth ?? Configuration.DefaultMaxTreeDepth;
        return await LoadTreeWithUserAsync<TProps>(rootObjectId, actualMaxDepth, user.Id, Configuration.DefaultCheckPermissionsOnLoad);
    }
    
    public async Task<TreeRedbObject<TProps>> LoadTreeAsync<TProps>(IRedbObject rootObj, IRedbUser user, int? maxDepth = null, CancellationToken cancellationToken = default) where TProps : class, new()
    {
        var actualMaxDepth = maxDepth ?? Configuration.DefaultMaxTreeDepth;
        return await LoadTreeWithUserAsync<TProps>(rootObj.Id, actualMaxDepth, user.Id, Configuration.DefaultCheckPermissionsOnLoad);
    }
    
    public async Task<IEnumerable<TreeRedbObject<TProps>>> GetChildrenAsync<TProps>(IRedbObject parentObj, IRedbUser user, CancellationToken cancellationToken = default) where TProps : class, new()
        => await GetChildrenWithUserAsync<TProps>(parentObj.Id, user.Id, Configuration.DefaultCheckPermissionsOnLoad);
    
    public async Task<IEnumerable<TreeRedbObject<TProps>>> GetPathToRootAsync<TProps>(IRedbObject obj, IRedbUser user, CancellationToken cancellationToken = default) where TProps : class, new()
        => await GetPathToRootWithUserAsync<TProps>(obj.Id, user.Id, Configuration.DefaultCheckPermissionsOnLoad);
    
    public async Task<IEnumerable<TreeRedbObject<TProps>>> GetDescendantsAsync<TProps>(IRedbObject parentObj, IRedbUser user, int? maxDepth = null, CancellationToken cancellationToken = default) where TProps : class, new()
    {
        var actualMaxDepth = maxDepth ?? Configuration.DefaultMaxTreeDepth;
        return await GetDescendantsWithUserAsync<TProps>(parentObj.Id, actualMaxDepth, user.Id, Configuration.DefaultCheckPermissionsOnLoad);
    }
    
    public async Task MoveObjectAsync(IRedbObject obj, IRedbObject? newParentObj, IRedbUser user, CancellationToken cancellationToken = default)
        => await MoveObjectWithUserAsync(obj.Id, newParentObj?.Id, user.Id, Configuration.DefaultCheckPermissionsOnSave, cancellationToken);
    
    public async Task<long> CreateChildAsync<TProps>(TreeRedbObject<TProps> obj, IRedbObject parentObj, IRedbUser user, CancellationToken cancellationToken = default) where TProps : class, new()
        => await CreateChildWithUserAsync(obj, parentObj.Id, user.Id, Configuration.DefaultCheckPermissionsOnSave);

    public async Task<int> DeleteSubtreeAsync(IRedbObject parentObj, IRedbUser user, CancellationToken cancellationToken = default)
        => await DeleteSubtreeWithUserAsync(parentObj.Id, user, cancellationToken);

    // ===== POLYMORPHIC METHODS =====
    
    public async Task<ITreeRedbObject> LoadPolymorphicTreeAsync(IRedbObject rootObj, int? maxDepth = null, CancellationToken cancellationToken = default)
    {
        var effectiveUser = SecurityContext.GetEffectiveUser();
        var actualMaxDepth = maxDepth ?? Configuration.DefaultMaxTreeDepth;
        return await LoadPolymorphicTreeWithUserAsync(rootObj.Id, actualMaxDepth, effectiveUser.Id, Configuration.DefaultCheckPermissionsOnLoad);
    }
    
    public async Task<IEnumerable<ITreeRedbObject>> GetPolymorphicChildrenAsync(IRedbObject parentObj, CancellationToken cancellationToken = default)
    {
        var effectiveUser = SecurityContext.GetEffectiveUser();
        return await GetPolymorphicChildrenWithUserAsync(parentObj.Id, effectiveUser.Id, Configuration.DefaultCheckPermissionsOnLoad);
    }
    
    public async Task<IEnumerable<ITreeRedbObject>> GetPolymorphicPathToRootAsync(IRedbObject obj, CancellationToken cancellationToken = default)
    {
        var effectiveUser = SecurityContext.GetEffectiveUser();
        return await GetPolymorphicPathToRootWithUserAsync(obj.Id, effectiveUser.Id, Configuration.DefaultCheckPermissionsOnLoad);
    }
    
    public async Task<IEnumerable<ITreeRedbObject>> GetPolymorphicDescendantsAsync(IRedbObject parentObj, int? maxDepth = null, CancellationToken cancellationToken = default)
    {
        var effectiveUser = SecurityContext.GetEffectiveUser();
        var actualMaxDepth = maxDepth ?? Configuration.DefaultMaxTreeDepth;
        return await GetPolymorphicDescendantsWithUserAsync(parentObj.Id, actualMaxDepth, effectiveUser.Id, Configuration.DefaultCheckPermissionsOnLoad);
    }

    // ===== POLYMORPHIC METHODS WITH EXPLICIT USER =====
    
    public async Task<ITreeRedbObject> LoadPolymorphicTreeAsync(IRedbObject rootObj, IRedbUser user, int? maxDepth = null, CancellationToken cancellationToken = default)
    {
        var actualMaxDepth = maxDepth ?? Configuration.DefaultMaxTreeDepth;
        return await LoadPolymorphicTreeWithUserAsync(rootObj.Id, actualMaxDepth, user.Id, Configuration.DefaultCheckPermissionsOnLoad);
    }
    
    public async Task<IEnumerable<ITreeRedbObject>> GetPolymorphicChildrenAsync(IRedbObject parentObj, IRedbUser user, CancellationToken cancellationToken = default)
        => await GetPolymorphicChildrenWithUserAsync(parentObj.Id, user.Id, Configuration.DefaultCheckPermissionsOnLoad, cancellationToken);
    
    public async Task<IEnumerable<ITreeRedbObject>> GetPolymorphicPathToRootAsync(IRedbObject obj, IRedbUser user, CancellationToken cancellationToken = default)
        => await GetPolymorphicPathToRootWithUserAsync(obj.Id, user.Id, Configuration.DefaultCheckPermissionsOnLoad, cancellationToken);
    
    public async Task<IEnumerable<ITreeRedbObject>> GetPolymorphicDescendantsAsync(IRedbObject parentObj, IRedbUser user, int? maxDepth = null, CancellationToken cancellationToken = default)
    {
        var actualMaxDepth = maxDepth ?? Configuration.DefaultMaxTreeDepth;
        return await GetPolymorphicDescendantsWithUserAsync(parentObj.Id, actualMaxDepth, user.Id, Configuration.DefaultCheckPermissionsOnLoad);
    }

    // ============================================================
    // === PROTECTED VIRTUAL METHODS (can be overridden in derived classes) ===
    // ============================================================

    protected virtual async Task<TreeRedbObject<TProps>> LoadTreeWithUserAsync<TProps>(
        long rootId, int maxDepth = 10, long? userId = null, bool checkPermissions = false, CancellationToken cancellationToken = default) where TProps : class, new()
    {
        var baseObject = await ObjectStorage.LoadAsync<TProps>(rootId, 1);
        if (baseObject == null)
            throw new InvalidOperationException($"Object with ID {rootId} not found");

        var treeObject = ConvertToTreeObject(baseObject);

        // The nodes come in walk order, a parent before its children.
        var byId = new Dictionary<long, TreeRedbObject<TProps>> { [rootId] = treeObject };
        foreach (var node in await LoadTypedSubtreeAsync<TProps>(rootId, maxDepth, userId, checkPermissions, cancellationToken))
        {
            var parent = byId[node.parent_id!.Value];
            node.Parent = parent;
            parent.Children.Add(node);
            byId[node.id] = node;
        }

        return treeObject;
    }

    protected virtual async Task<IEnumerable<TreeRedbObject<TProps>>> GetChildrenWithUserAsync<TProps>(
        long parentId, long? userId = null, bool checkPermissions = false, CancellationToken cancellationToken = default) where TProps : class, new()
    {
        var scheme = await SchemeSyncProvider.GetSchemeByTypeAsync<TProps>();
        if (scheme == null)
            throw new InvalidOperationException($"Scheme for type {typeof(TProps).Name} not found. Use SyncSchemeAsync<{typeof(TProps).Name}>() to create scheme.");
        
        var jsonResults = await Context.ExecuteJsonListAsync(Sql.Tree_SelectChildrenJson(), new object[] { parentId, scheme.Id }, cancellationToken);
        
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
        long objectId, long? userId = null, bool checkPermissions = false, CancellationToken cancellationToken = default) where TProps : class, new()
    {
        // One statement for the chain, one batch for the objects. The path ends where the load of an ancestor one by
        // one used to stop: at an ancestor of another scheme (or the RedbSchemeMismatchException under
        // ThrowOnSchemeMismatch), one the reader may not see, or one that is missing.
        var reader = SecurityContext.GetEffectiveUser();
        var chain = await SelectAncestorsAsync(objectId, cancellationToken);
        if (chain.Count == 0)
        {
            if (Configuration.ThrowOnObjectNotFound)
                throw new InvalidOperationException($"Object with ID {objectId} not found");
            return [];
        }

        var expectedScheme = Cache.GetSchemeIdByClrType(typeof(TProps)) ?? (await SchemeSyncProvider.GetSchemeByTypeAsync<TProps>())?.Id;

        // Asked one by one from the object up and only until the first refusal, as before: an ancestor above it is
        // never asked (the single check throws for an object with no permission row). A path is a few levels.
        var kept = new List<TreeWalkRow>();
        foreach (var row in chain)
        {
            if (expectedScheme.HasValue && row.IdScheme != expectedScheme.Value)
            {
                if (Configuration.ThrowOnSchemeMismatch)
                    throw new Exceptions.RedbSchemeMismatchException(row.Id, typeof(TProps), expectedScheme.Value, row.IdScheme);
                break;
            }
            if (Configuration.DefaultCheckPermissionsOnLoad && !await PermissionProvider.CanUserSelectObject(row.Id, reader.Id, cancellationToken))
                break;
            kept.Add(row);
        }
        if (kept.Count == chain.Count && chain[^1].IdParent is { } dangling && chain[^1].Depth < MaxAncestorDepth
            && Configuration.ThrowOnObjectNotFound)
            throw new InvalidOperationException($"Object with ID {dangling} not found");

        if (expectedScheme.HasValue)
            EnsureClrTypeRegistered<TProps>(expectedScheme.Value);
        var loaded = await LoadNodesAsync(kept.Select(r => r.Id).ToList(), reader, 1, cancellationToken);

        var path = new List<TreeRedbObject<TProps>>();
        foreach (var row in kept)
        {
            if (!loaded.TryGetValue(row.Id, out var obj))
                break;
            path.Insert(0, ConvertToTreeObject(AsTyped<TProps>(obj)));
        }

        for (int i = 0; i < path.Count - 1; i++)
        {
            path[i + 1].Parent = path[i];
        }

        return path;
    }

    protected virtual async Task<IEnumerable<TreeRedbObject<TProps>>> GetDescendantsWithUserAsync<TProps>(
        long parentId, int maxDepth = 50, long? userId = null, bool checkPermissions = false, CancellationToken cancellationToken = default) where TProps : class, new()
    {
        // In walk order: a child, everything below it, then the next child.
        return await LoadTypedSubtreeAsync<TProps>(parentId, maxDepth, userId, checkPermissions, cancellationToken);
    }

    protected virtual async Task MoveObjectWithUserAsync(long objectId, long? newParentId, long userId, bool checkPermissions = true, CancellationToken cancellationToken = default)
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
            var parentExists = await Context.ExecuteScalarAsync<long?>(Sql.Tree_ObjectExists(), new object[] { newParentId.Value }, cancellationToken);
            if (!parentExists.HasValue)
                throw new ArgumentException($"Parent object {newParentId} does not exist");
            
            await ValidateNoCyclicReference(objectId, newParentId.Value);
        }
        
        var rowsAffected = await Context.ExecuteAsync(
            Sql.Tree_UpdateParent(), new object[] { newParentId.HasValue ? (object)newParentId.Value : DBNull.Value,
            DateTimeOffset.UtcNow,
            userId,
            objectId }, cancellationToken);
        
        if (rowsAffected == 0)
            throw new ArgumentException($"Object {objectId} not found");

        // The move bypasses the save path: the database drops the object's hash (Tree_UpdateParent) and the cached copy
        // goes with it - under SkipHashValidationOnCacheCheck a load never asks the database and would keep answering
        // with the old parent until the entry expired.
        if (Configuration.EnablePropsCache && SchemeSyncProvider.PropsCache.Instance != null)
            SchemeSyncProvider.PropsCache.Remove(objectId);
    }

    protected virtual async Task<long> CreateChildWithUserAsync<TProps>(
        TreeRedbObject<TProps> obj, long parentId, long? userId = null, bool checkPermissions = false, CancellationToken cancellationToken = default) where TProps : class, new()
    {
        if (obj.id == 0)
        {
            obj.id = await Context.NextObjectIdAsync();
        }
        
        obj.parent_id = parentId == 0 ? null : parentId;
        
        var effectiveUser = SecurityContext.GetEffectiveUser();
        return await ObjectStorage.SaveAsync(obj, effectiveUser);
    }

    protected virtual async Task<int> DeleteSubtreeWithUserAsync(long parentId, IRedbUser user, CancellationToken cancellationToken = default)
    {
        var checkPermissions = Configuration.DefaultCheckPermissionsOnDelete;
        
        if (checkPermissions)
        {
            var canDelete = await PermissionProvider.CanUserDeleteObject(parentId, user.Id);
            if (!canDelete)
                throw new UnauthorizedAccessException($"User {user.Id} does not have permission to delete subtree of object {parentId}");
        }

        // Every descendant of any scheme, down to the same 100 levels the walk one parent at a time went to.
        var objectIds = (await SelectSubtreeAsync(parentId, null, 100, cancellationToken)).Select(r => r.Id).ToList();
        objectIds.Add(parentId);

        // One transaction for both statements. They used to run in sequence, and a failure in the
        // second (a live FK reference pointing into the subtree) left the first committed: the
        // objects survived with their values already gone (DataLoss p.2).
        var ids = objectIds.ToArray();
        await Context.ExecuteAtomicAsync(async () =>
        {
            await Context.ExecuteAsync(Sql.Tree_DeleteValuesByObjectIds(), new object[] { ids }, cancellationToken);
            await Context.ExecuteAsync(Sql.Tree_DeleteObjectsByIds(), new object[] { ids }, cancellationToken);
        }, cancellationToken);

        // The collected id list IS the subtree (self + descendants), so its count is the correct,
        // dialect-independent number of objects deleted. The raw DELETE rows-affected is unreliable
        // here: on SQLite the `_id_parent ON DELETE CASCADE` FK removes child rows as a side effect of
        // deleting the parent, so changes() under-counts them (it equals rows-affected on PG/MSSql,
        // which have no such cascade on _id_parent).
        return objectIds.Count;
    }

    // ============================================================
    // === POLYMORPHIC IMPLEMENTATION ===
    // ============================================================

    protected virtual async Task<IRedbObject> LoadDynamicObjectAsync(long objectId, IRedbUser? user = null, CancellationToken cancellationToken = default)
    {
        if (Configuration.DefaultCheckPermissionsOnLoad && user != null)
        {
            var canRead = await PermissionProvider.CanUserSelectObject(objectId, user.Id);
            if (!canRead)
                throw new UnauthorizedAccessException($"User {user.Id} does not have permission to read object {objectId}");
        }

        var result = await Context.QueryFirstOrDefaultAsync<SchemeWithJson>(Sql.Tree_SelectSchemeAndJson(), new object[] { objectId }, cancellationToken);

        if (result == null || string.IsNullOrEmpty(result.JsonData))
            throw new InvalidOperationException($"Object with ID {objectId} not found");

        var propsType = Cache.GetClrType(result.SchemeId) ?? typeof(object);
        return Serializer.DeserializeDynamic(result.JsonData, propsType);
    }
    
    protected virtual async Task<ITreeRedbObject> LoadPolymorphicTreeWithUserAsync(
        long rootId, int maxDepth = 10, long? userId = null, bool checkPermissions = false, CancellationToken cancellationToken = default)
    {
        var user = userId.HasValue ? await GetUserByIdAsync(userId.Value) : null;
        var baseObject = await LoadDynamicObjectAsync(rootId, user);
        var treeObject = ConvertToPolymorphicTreeObjectWithProps(baseObject);

        // The nodes come in walk order, a parent before its children.
        var byId = new Dictionary<long, ITreeRedbObject> { [rootId] = treeObject };
        foreach (var node in await LoadPolymorphicSubtreeAsync(rootId, maxDepth, userId, checkPermissions, cancellationToken))
        {
            var parent = byId[node.ParentId!.Value];
            node.Parent = parent;
            parent.Children.Add(node);
            byId[node.Id] = node;
        }

        return treeObject;
    }

    protected virtual async Task<IEnumerable<ITreeRedbObject>> GetPolymorphicChildrenWithUserAsync(
        long parentId, long? userId = null, bool checkPermissions = false, CancellationToken cancellationToken = default)
    {
        var results = await Context.QueryAsync<ChildObjectInfo>(Sql.Tree_SelectPolymorphicChildren(), new object[] { parentId }, cancellationToken);
        
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
        long objectId, long? userId = null, bool checkPermissions = false, CancellationToken cancellationToken = default)
    {
        // One statement for the chain, one batch for the objects. The path ends where the load of an ancestor one by
        // one used to stop, at one the user may not see; a missing ancestor is an error, as it was.
        var chain = await SelectAncestorsAsync(objectId, cancellationToken);
        if (chain.Count == 0)
            throw new InvalidOperationException($"Object with ID {objectId} not found");
        if (chain[^1].IdParent is { } dangling && chain[^1].Depth < MaxAncestorDepth)
            throw new InvalidOperationException($"Object with ID {dangling} not found");

        var user = userId.HasValue ? await GetUserByIdAsync(userId.Value) : null;
        var check = userId.HasValue && (checkPermissions || Configuration.DefaultCheckPermissionsOnLoad);

        // Asked one by one from the object up and only until the first refusal, as before (see the typed path).
        var kept = new List<TreeWalkRow>();
        foreach (var row in chain)
        {
            if (check && !await PermissionProvider.CanUserSelectObject(row.Id, userId!.Value, cancellationToken))
                break;
            kept.Add(row);
        }
        var loaded = await LoadNodesAsync(kept.Select(r => r.Id).ToList(), user ?? SecurityContext.GetEffectiveUser(),
            Configuration.DefaultLoadDepth, cancellationToken);

        var path = new List<ITreeRedbObject>();
        foreach (var row in kept)
        {
            if (!loaded.TryGetValue(row.Id, out var obj))
                break;
            path.Insert(0, ConvertToPolymorphicTreeObjectWithProps(obj));
        }

        for (int i = 0; i < path.Count - 1; i++)
        {
            path[i + 1].Parent = path[i];
        }

        return path;
    }

    protected virtual async Task<IEnumerable<ITreeRedbObject>> GetPolymorphicDescendantsWithUserAsync(
        long parentId, int maxDepth = 50, long? userId = null, bool checkPermissions = false, CancellationToken cancellationToken = default)
    {
        // In walk order: a child, everything below it, then the next child.
        return await LoadPolymorphicSubtreeAsync(parentId, maxDepth, userId, checkPermissions, cancellationToken);
    }

    // ============================================================
    // === PRIVATE HELPER METHODS ===
    // ============================================================

    // ============================================================
    // === WALKS IN ONE STATEMENT ===
    // A subtree or a chain of ancestors used to be read one parent at a time, a query or two per node (11 thousand
    // nodes: 27-44 s). It is now one statement for the shape (Tree_SelectSubtreeRows, Tree_SelectAncestorRows), one
    // permission batch when permissions are checked, and one batch load of the objects.
    // ============================================================

    /// <summary>A row of <see cref="ISqlDialect.Tree_SelectSubtreeRows"/> or <see cref="ISqlDialect.Tree_SelectAncestorRows"/>.</summary>
    protected sealed class TreeWalkRow
    {
        public long Id { get; set; }
        public long? IdParent { get; set; }
        public long IdScheme { get; set; }
        public int Depth { get; set; }
    }

    /// <summary>How far up a chain of ancestors is read: well past any real tree, short of a runaway cycle.</summary>
    internal const int MaxAncestorDepth = 10_000;

    /// <summary>
    /// The descendants of an object down to <paramref name="maxDepth"/>, children of a parent in their order. A node
    /// met twice means a cycle in _id_parent: an error, where the walk one parent at a time went round it to the depth.
    /// </summary>
    private async Task<List<TreeWalkRow>> SelectSubtreeAsync(long rootId, long? schemeId, int maxDepth, CancellationToken cancellationToken)
    {
        if (maxDepth <= 0)
            return [];

        var rows = await Context.QueryAsync<TreeWalkRow>(Sql.Tree_SelectSubtreeRows(),
            new object[] { rootId, (object?)schemeId ?? DBNull.Value, maxDepth }, cancellationToken);
        var seen = new HashSet<long> { rootId };
        foreach (var row in rows)
            if (!seen.Add(row.Id))
                throw new InvalidOperationException(
                    $"Cyclic reference in the tree under object {rootId}: object {row.Id} is reached twice. " +
                    "Fix _id_parent of the objects on the cycle.");
        return rows;
    }

    /// <summary>
    /// An object (depth 0) and its ancestors, nearest first. An object met twice means a cycle in _id_parent: an error,
    /// where the walk up used to stop at it silently.
    /// </summary>
    private async Task<List<TreeWalkRow>> SelectAncestorsAsync(long objectId, CancellationToken cancellationToken)
    {
        var rows = await Context.QueryAsync<TreeWalkRow>(Sql.Tree_SelectAncestorRows(),
            new object[] { objectId, MaxAncestorDepth }, cancellationToken);
        var seen = new HashSet<long>();
        foreach (var row in rows)
            if (!seen.Add(row.Id))
                throw new InvalidOperationException(
                    $"Cyclic reference in the tree: object {row.Id} is its own ancestor (path from object {objectId}). " +
                    "Fix _id_parent of the objects on the cycle.");
        if (rows.Count > 0 && rows[^1].Depth >= MaxAncestorDepth && rows[^1].IdParent.HasValue)
            throw new InvalidOperationException(
                $"The chain of ancestors of object {objectId} is longer than {MaxAncestorDepth} levels.");
        return rows;
    }

    /// <summary>
    /// The rows reachable from the root through rows that pass <paramref name="keep"/>, in walk order: a child,
    /// everything below it, then the next child. A row that does not pass is left out with its whole subtree, as the
    /// walk one parent at a time never descended into it.
    /// </summary>
    private static List<TreeWalkRow> Reachable(long rootId, List<TreeWalkRow> rows, Func<TreeWalkRow, bool> keep)
    {
        var children = rows.ToLookup(r => r.IdParent!.Value);
        var result = new List<TreeWalkRow>(rows.Count);
        var stack = new Stack<IEnumerator<TreeWalkRow>>();
        stack.Push(children[rootId].GetEnumerator());
        while (stack.Count > 0)
        {
            var siblings = stack.Peek();
            if (!siblings.MoveNext())
            {
                stack.Pop();
                continue;
            }
            if (!keep(siblings.Current))
                continue;
            result.Add(siblings.Current);
            stack.Push(children[siblings.Current.Id].GetEnumerator());
        }
        return result;
    }

    /// <summary>
    /// The rows of the subtree the user may read, asked level by level: one permission batch per level, for the rows
    /// under a parent already kept that pass <paramref name="eligible"/>. A row under a refused one is never asked, as
    /// the walk one parent at a time never asked it (the single check throws for an object with no permission row).
    /// </summary>
    private async Task<HashSet<long>> SelectReadableAsync(
        long rootId, List<TreeWalkRow> rows, long userId, Func<TreeWalkRow, bool> eligible, CancellationToken cancellationToken)
    {
        var children = rows.ToLookup(r => r.IdParent!.Value);
        var readable = new HashSet<long>();
        var parents = new List<long> { rootId };
        while (parents.Count > 0)
        {
            var level = parents.SelectMany(p => children[p]).Where(eligible).ToList();
            if (level.Count == 0)
                break;
            var allowed = await PermissionProvider.GetSelectableObjectIdsAsync(level.Select(r => r.Id).ToList(), userId, cancellationToken);
            parents = level.Where(r => allowed.Contains(r.Id)).Select(r => r.Id).ToList();
            readable.UnionWith(parents);
        }
        return readable;
    }

    /// <summary>One batch load of objects (polymorphic: each in the type of its scheme), by id.</summary>
    private async Task<Dictionary<long, IRedbObject>> LoadNodesAsync(List<long> ids, IRedbUser user, int depth, CancellationToken cancellationToken)
    {
        if (ids.Count == 0)
            return new Dictionary<long, IRedbObject>();
        var loaded = await ObjectStorage.LoadAsync(ids, user, depth, cancellationToken);
        return loaded.ToDictionary(o => o.Id);
    }

    /// <summary>
    /// Binds the scheme to <typeparamref name="TProps"/> in the type registry when nothing is bound to it yet: the
    /// batch load picks the type of an object by its scheme, and the walk one parent at a time read the children of
    /// this scheme as <typeparamref name="TProps"/> directly.
    /// </summary>
    private void EnsureClrTypeRegistered<TProps>(long schemeId) where TProps : class, new()
    {
        if (Cache.GetClrType(schemeId) == null)
            Cache.RegisterClrType(typeof(TProps).FullName ?? typeof(TProps).Name, schemeId, typeof(TProps));
    }

    private static RedbObject<TProps> AsTyped<TProps>(IRedbObject obj) where TProps : class, new()
        => obj as RedbObject<TProps>
           ?? throw new InvalidOperationException(
               $"Object {obj.Id} of scheme {obj.SchemeId} loads as {obj.GetType().Name}, not as RedbObject<{typeof(TProps).Name}>: " +
               "another type is bound to that scheme.");

    /// <summary>
    /// The descendants of <paramref name="rootId"/> of the scheme of <typeparamref name="TProps"/>, in walk order, each
    /// with its parent_id: what the walk one parent at a time returned (GetChildrenWithUserAsync per node), with the
    /// same reader for the permission check (the effective user, as CanUserSelectObject of an object asks).
    /// </summary>
    private async Task<List<TreeRedbObject<TProps>>> LoadTypedSubtreeAsync<TProps>(
        long rootId, int maxDepth, long? userId, bool checkPermissions, CancellationToken cancellationToken) where TProps : class, new()
    {
        var scheme = await SchemeSyncProvider.GetSchemeByTypeAsync<TProps>();
        if (scheme == null)
            throw new InvalidOperationException($"Scheme for type {typeof(TProps).Name} not found. Use SyncSchemeAsync<{typeof(TProps).Name}>() to create scheme.");

        var rows = await SelectSubtreeAsync(rootId, scheme.Id, maxDepth, cancellationToken);
        if (rows.Count == 0)
            return [];

        var reader = SecurityContext.GetEffectiveUser();
        var readable = checkPermissions && userId.HasValue
            ? await SelectReadableAsync(rootId, rows, reader.Id, _ => true, cancellationToken)
            : null;
        var kept = Reachable(rootId, rows, r => readable == null || readable.Contains(r.Id));

        EnsureClrTypeRegistered<TProps>(scheme.Id);
        var loaded = await LoadNodesAsync(kept.Select(r => r.Id).ToList(), reader, Configuration.DefaultLoadDepth, cancellationToken);

        // An object gone between the walk and the load is left out with its subtree.
        return Reachable(rootId, kept, r => loaded.ContainsKey(r.Id))
            .Select(r => ConvertToTreeObject(AsTyped<TProps>(loaded[r.Id])))
            .ToList();
    }

    /// <summary>
    /// The descendants of <paramref name="rootId"/> of every scheme, in walk order: what the walk one parent at a time
    /// returned (GetPolymorphicChildrenWithUserAsync per node). An object whose scheme has no CLR type is left out with
    /// its subtree, as there.
    /// </summary>
    private async Task<List<ITreeRedbObject>> LoadPolymorphicSubtreeAsync(
        long rootId, int maxDepth, long? userId, bool checkPermissions, CancellationToken cancellationToken)
    {
        var rows = await SelectSubtreeAsync(rootId, null, maxDepth, cancellationToken);
        if (rows.Count == 0)
            return [];

        bool Typed(TreeWalkRow r) => Cache.GetClrType(r.IdScheme) != null;
        var readable = checkPermissions && userId.HasValue
            ? await SelectReadableAsync(rootId, rows, userId.Value, Typed, cancellationToken)
            : null;
        var kept = Reachable(rootId, rows, r => Typed(r) && (readable == null || readable.Contains(r.Id)));

        var user = userId.HasValue ? await GetUserByIdAsync(userId.Value) : null;
        var loaded = await LoadNodesAsync(kept.Select(r => r.Id).ToList(), user ?? SecurityContext.GetEffectiveUser(),
            Configuration.DefaultLoadDepth, cancellationToken);

        return Reachable(rootId, kept, r => loaded.ContainsKey(r.Id))
            .Select(r => ConvertToPolymorphicTreeObjectWithProps(loaded[r.Id]))
            .ToList();
    }

    private async Task ValidateNoCyclicReference(long objectId, long newParentId, CancellationToken cancellationToken = default)
    {
        // The new parent and its ancestors, one statement. Read row by row, in the order the walk up used to read them.
        var rows = await Context.QueryAsync<TreeWalkRow>(
            Sql.Tree_SelectAncestorRows(), new object[] { newParentId, MaxAncestorDepth }, cancellationToken);
        var visited = new HashSet<long>();
        foreach (var row in rows)
        {
            if (row.Id == objectId)
                throw new InvalidOperationException($"Cannot move object {objectId}: this would create a cyclic reference");

            if (!visited.Add(row.Id))
                throw new InvalidOperationException("Cyclic reference detected in existing data structure");
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
            value_unique = source.value_unique,
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
            value_unique = redbObj.value_unique,
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

