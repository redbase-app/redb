using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using redb.Core.Data;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Core.Models.Enums;
using redb.Core.Models.Permissions;
using redb.Core.Models.Security;
using redb.Core.Query;
using Microsoft.Extensions.Logging;

namespace redb.Core.Providers.Base;

/// <summary>
/// Base implementation of permission provider with common business logic.
/// Database-specific providers inherit from this class and provide ISqlDialect.
/// 
/// IMPORTANT: Uses DB-specific SQL function for effective permissions!
/// PostgreSQL: get_user_permissions_for_object()
/// MSSQL: Must provide equivalent CTE/JOIN query
/// </summary>
public abstract class PermissionProviderBase : IPermissionProvider
{
    protected readonly IRedbContext Context;
    protected readonly IRedbSecurityContext SecurityContext;
    protected readonly ISqlDialect Sql;
    protected readonly ILogger? Logger;

    // Permission cache for SQL results
    private static readonly ConcurrentDictionary<string, (UserPermissionResult result, DateTimeOffset cachedAt)> PermissionCache = new();
    private static readonly TimeSpan CacheLifetime = TimeSpan.FromMinutes(5);
    private static long _cacheRequests;
    private static long _cacheHits;

    protected PermissionProviderBase(
        IRedbContext context,
        IRedbSecurityContext securityContext,
        ISqlDialect sql,
        ILogger? logger = null)
    {
        Context = context ?? throw new ArgumentNullException(nameof(context));
        SecurityContext = securityContext ?? throw new ArgumentNullException(nameof(securityContext));
        Sql = sql ?? throw new ArgumentNullException(nameof(sql));
        Logger = logger;
    }

    // ============================================================
    // === EFFECTIVE PERMISSION VIA SQL ===
    // ============================================================

    /// <summary>
    /// Get effective permission via SQL (uses DB function or query).
    /// Override in derived class for DB-specific optimizations.
    /// </summary>
    protected virtual async Task<UserPermissionResult?> GetEffectivePermissionViaSqlAsync(long objectId, long userId)
    {
        var cacheKey = $"{userId}_{objectId}";
        Interlocked.Increment(ref _cacheRequests);

        if (PermissionCache.TryGetValue(cacheKey, out var cached))
        {
            var isExpired = DateTimeOffset.UtcNow - cached.cachedAt > CacheLifetime;
            if (!isExpired)
            {
                Interlocked.Increment(ref _cacheHits);
                return cached.result;
            }
        }

        var result = await Context.QueryFirstOrDefaultAsync<UserPermissionResult>(
            Sql.Permissions_GetEffectiveForObject(), objectId, userId);

        if (result != null)
        {
            PermissionCache[cacheKey] = (result, DateTimeOffset.UtcNow);
        }

        return result;
    }

    /// <summary>
    /// Invalidate permission cache. Called after permission changes.
    /// </summary>
    protected static void InvalidatePermissionCache(long? userId = null, long? objectId = null)
    {
        if (userId.HasValue && objectId.HasValue)
        {
            PermissionCache.TryRemove($"{userId}_{objectId}", out _);
        }
        else if (userId.HasValue)
        {
            var keysToRemove = PermissionCache.Keys
                .Where(k => k.StartsWith($"{userId}_"))
                .ToList();
            foreach (var key in keysToRemove)
                PermissionCache.TryRemove(key, out _);
        }
        else if (objectId.HasValue)
        {
            var keysToRemove = PermissionCache.Keys
                .Where(k => k.EndsWith($"_{objectId}"))
                .ToList();
            foreach (var key in keysToRemove)
                PermissionCache.TryRemove(key, out _);
        }
        else
        {
            PermissionCache.Clear();
        }
    }

    /// <summary>
    /// Get cache statistics for monitoring.
    /// </summary>
    public static string GetCacheStatistics()
    {
        var hitRate = _cacheRequests > 0 ? (double)_cacheHits / _cacheRequests * 100 : 0;
        return $"Permission cache: Requests={_cacheRequests}, Hits={_cacheHits}, " +
               $"Hit Rate={hitRate:F1}%, Entries={PermissionCache.Count}";
    }

    // ============================================================
    // === BASE METHODS (using SecurityContext) ===
    // ============================================================

    /// <inheritdoc />
    public virtual IQueryable<long> GetReadableObjectIds()
    {
        var effectiveUser = ((RedbSecurityContext)SecurityContext).GetEffectiveUser();
        return GetReadableObjectIds(effectiveUser.Id);
    }

    /// <inheritdoc />
    public virtual async Task<bool> CanUserEditObject(IRedbObject obj)
    {
        var effectiveUser = ((RedbSecurityContext)SecurityContext).GetEffectiveUser();
        return await CanUserEditObject(obj.Id, effectiveUser.Id);
    }

    /// <inheritdoc />
    public virtual async Task<bool> CanUserSelectObject(IRedbObject obj)
    {
        var effectiveUser = ((RedbSecurityContext)SecurityContext).GetEffectiveUser();
        return await CanUserSelectObject(obj.Id, effectiveUser.Id);
    }

    /// <inheritdoc />
    public virtual async Task<bool> CanUserInsertScheme(IRedbScheme scheme)
    {
        var effectiveUser = ((RedbSecurityContext)SecurityContext).GetEffectiveUser();
        return await CanUserInsertScheme(scheme.Id, effectiveUser.Id);
    }

    /// <inheritdoc />
    public virtual async Task<bool> CanUserDeleteObject(IRedbObject obj)
    {
        var effectiveUser = ((RedbSecurityContext)SecurityContext).GetEffectiveUser();
        return await CanUserDeleteObject(obj.Id, effectiveUser.Id);
    }

    // ============================================================
    // === METHODS WITH EXPLICIT USER ===
    // ============================================================

    /// <inheritdoc />
    public virtual IQueryable<long> GetReadableObjectIds(IRedbUser user)
    {
        return GetReadableObjectIds(user.Id);
    }

    /// <inheritdoc />
    public virtual IQueryable<long> GetReadableObjectIds(long userId)
    {
        var ids = GetReadableObjectIdsAsync(userId).GetAwaiter().GetResult();
        return ids.AsQueryable();
    }

    /// <summary>
    /// Async version of GetReadableObjectIds.
    /// </summary>
    protected virtual async Task<List<long>> GetReadableObjectIdsAsync(long userId)
    {
        return await Context.QueryAsync<long>(Sql.Permissions_SelectReadableObjectIds(), userId);
    }

    /// <inheritdoc />
    public virtual async Task<bool> CanUserEditObject(long objectId, long userId)
    {
        if (userId == 0) return true; // System user can do everything
        
        var permission = await GetEffectivePermissionViaSqlAsync(objectId, userId);
        if (permission == null)
            throw new InvalidOperationException($"Cannot get permissions for object {objectId} and user {userId}.");
        return permission.CanUpdate;
    }

    /// <inheritdoc />
    public virtual async Task<bool> CanUserSelectObject(long objectId, long userId)
    {
        if (userId == 0) return true;
        
        var permission = await GetEffectivePermissionViaSqlAsync(objectId, userId);
        if (permission == null)
            throw new InvalidOperationException($"Cannot get permissions for object {objectId} and user {userId}.");
        return permission.CanSelect;
    }

    /// <inheritdoc />
    public virtual async Task<bool> CanUserInsertScheme(long schemeId, long userId)
    {
        if (userId == 0) return true;
        
        var permission = await GetEffectivePermissionViaSqlAsync(schemeId, userId);
        if (permission == null)
            throw new InvalidOperationException($"Cannot get permissions for scheme {schemeId} and user {userId}.");
        return permission.CanInsert;
    }

    /// <inheritdoc />
    public virtual async Task<bool> CanUserDeleteObject(long objectId, long userId)
    {
        if (userId == 0) return true;
        
        var permission = await GetEffectivePermissionViaSqlAsync(objectId, userId);
        if (permission == null)
            throw new InvalidOperationException($"Cannot get permissions for object {objectId} and user {userId}.");
        return permission.CanDelete;
    }

    // ============================================================
    // === OVERLOADS WITH OBJECTS ===
    // ============================================================

    /// <inheritdoc />
    public virtual async Task<bool> CanUserEditObject(IRedbObject obj, IRedbUser user)
        => await CanUserEditObject(obj.Id, user.Id);

    /// <inheritdoc />
    public virtual async Task<bool> CanUserSelectObject(IRedbObject obj, IRedbUser user)
        => await CanUserSelectObject(obj.Id, user.Id);

    /// <inheritdoc />
    public virtual async Task<bool> CanUserInsertScheme(IRedbScheme scheme, IRedbUser user)
        => await CanUserInsertScheme(scheme.Id, user.Id);

    /// <inheritdoc />
    public virtual async Task<bool> CanUserDeleteObject(IRedbObject obj, IRedbUser user)
        => await CanUserDeleteObject(obj.Id, user.Id);

    /// <inheritdoc />
    public virtual async Task<bool> CanUserEditObject(RedbObject obj)
    {
        var effectiveUser = ((RedbSecurityContext)SecurityContext).GetEffectiveUser();
        return await CanUserEditObject(obj.Id, effectiveUser.Id);
    }

    /// <inheritdoc />
    public virtual async Task<bool> CanUserSelectObject(RedbObject obj)
    {
        var effectiveUser = ((RedbSecurityContext)SecurityContext).GetEffectiveUser();
        return await CanUserSelectObject(obj.Id, effectiveUser.Id);
    }

    /// <inheritdoc />
    public virtual async Task<bool> CanUserDeleteObject(RedbObject obj)
    {
        var effectiveUser = ((RedbSecurityContext)SecurityContext).GetEffectiveUser();
        return await CanUserDeleteObject(obj.Id, effectiveUser.Id);
    }

    /// <inheritdoc />
    public virtual async Task<bool> CanUserEditObject(RedbObject obj, IRedbUser user)
        => await CanUserEditObject(obj.Id, user.Id);

    /// <inheritdoc />
    public virtual async Task<bool> CanUserSelectObject(RedbObject obj, IRedbUser user)
        => await CanUserSelectObject(obj.Id, user.Id);

    /// <inheritdoc />
    public virtual async Task<bool> CanUserInsertScheme(RedbObject obj, IRedbUser user)
        => await CanUserInsertScheme(obj.SchemeId, user.Id);

    /// <inheritdoc />
    public virtual async Task<bool> CanUserDeleteObject(RedbObject obj, IRedbUser user)
        => await CanUserDeleteObject(obj.Id, user.Id);

    // ============================================================
    // === CRUD PERMISSIONS ===
    // ============================================================

    /// <inheritdoc />
    public virtual async Task<IRedbPermission> CreatePermissionAsync(PermissionRequest request, IRedbUser? currentUser = null)
    {
        var newId = await Context.NextObjectIdAsync();
        var newPermission = new RedbPermission
        {
            Id = newId,
            IdUser = request.UserId,
            IdRole = request.RoleId,
            IdRef = request.ObjectId,
            Select = request.CanSelect,
            Insert = request.CanInsert,
            Update = request.CanUpdate,
            Delete = request.CanDelete
        };

        await Context.ExecuteAsync(Sql.Permissions_Insert(),
            newId, (object?)request.UserId ?? DBNull.Value, (object?)request.RoleId ?? DBNull.Value, request.ObjectId,
            request.CanSelect, request.CanInsert, request.CanUpdate, request.CanDelete);

        InvalidatePermissionCache(request.UserId, request.ObjectId);
        if (request.ObjectId != 0)
            InvalidatePermissionCache(null, request.ObjectId);

        await OnPermissionCreatedAsync(newPermission, currentUser);

        return newPermission;
    }

    /// <inheritdoc />
    public virtual async Task<IRedbPermission> UpdatePermissionAsync(IRedbPermission permission, PermissionRequest request, IRedbUser? currentUser = null)
    {
        var result = await Context.ExecuteAsync(Sql.Permissions_Update(),
            request.CanSelect, request.CanInsert, request.CanUpdate, request.CanDelete, permission.Id);

        if (result == 0)
            throw new ArgumentException($"Permission with ID {permission.Id} not found");

        InvalidatePermissionCache(permission.IdUser, permission.IdRef);

        var updated = new RedbPermission
        {
            Id = permission.Id,
            IdUser = permission.IdUser,
            IdRole = permission.IdRole,
            IdRef = permission.IdRef,
            Select = request.CanSelect,
            Insert = request.CanInsert,
            Update = request.CanUpdate,
            Delete = request.CanDelete
        };

        await OnPermissionUpdatedAsync(updated, currentUser);

        return updated;
    }

    /// <inheritdoc />
    public virtual async Task<bool> DeletePermissionAsync(IRedbPermission permission, IRedbUser? currentUser = null)
    {
        var result = await Context.ExecuteAsync(Sql.Permissions_Delete(), permission.Id);

        InvalidatePermissionCache(permission.IdUser, permission.IdRef);
        if (permission.IdRef != 0)
            InvalidatePermissionCache(null, permission.IdRef);

        if (result > 0)
            await OnPermissionDeletedAsync(permission, currentUser);

        return result > 0;
    }

    // ============================================================
    // === PERMISSION SEARCH ===
    // ============================================================

    /// <inheritdoc />
    public virtual async Task<List<IRedbPermission>> GetPermissionsByUserAsync(IRedbUser user)
    {
        var permissions = await Context.QueryAsync<RedbPermission>(Sql.Permissions_SelectByUser(), user.Id);
        return permissions.Cast<IRedbPermission>().ToList();
    }

    /// <inheritdoc />
    public virtual async Task<List<IRedbPermission>> GetPermissionsByRoleAsync(IRedbRole role)
    {
        var permissions = await Context.QueryAsync<RedbPermission>(Sql.Permissions_SelectByRole(), role.Id);
        return permissions.Cast<IRedbPermission>().ToList();
    }

    /// <inheritdoc />
    public virtual async Task<List<IRedbPermission>> GetPermissionsByObjectAsync(IRedbObject obj)
    {
        var permissions = await Context.QueryAsync<RedbPermission>(Sql.Permissions_SelectByObject(), obj.Id);
        return permissions.Cast<IRedbPermission>().ToList();
    }

    /// <inheritdoc />
    public virtual async Task<IRedbPermission?> GetPermissionByIdAsync(long permissionId)
    {
        return await Context.QueryFirstOrDefaultAsync<RedbPermission>(Sql.Permissions_SelectById(), permissionId);
    }

    // ============================================================
    // === PERMISSION MANAGEMENT ===
    // ============================================================

    /// <inheritdoc />
    public virtual async Task<bool> GrantPermissionAsync(IRedbUser user, IRedbObject obj, PermissionAction actions, IRedbUser? currentUser = null)
    {
        return await GrantPermissionInternalAsync(user.Id, null, obj.Id, actions, currentUser);
    }

    /// <inheritdoc />
    public virtual async Task<bool> GrantPermissionAsync(IRedbRole role, IRedbObject obj, PermissionAction actions, IRedbUser? currentUser = null)
    {
        return await GrantPermissionInternalAsync(null, role.Id, obj.Id, actions, currentUser);
    }

    /// <summary>
    /// Internal method to grant permission.
    /// </summary>
    protected virtual async Task<bool> GrantPermissionInternalAsync(long? userId, long? roleId, long objectId, PermissionAction actions, IRedbUser? currentUser = null)
    {
        var existingPermission = await Context.QueryFirstOrDefaultAsync<RedbPermission>(
            Sql.Permissions_SelectByUserRoleObject(),
            (object?)userId ?? DBNull.Value, (object?)roleId ?? DBNull.Value, objectId);

        if (existingPermission != null)
        {
            var newSelect = existingPermission.Select == true || actions.HasFlag(PermissionAction.Select);
            var newInsert = existingPermission.Insert == true || actions.HasFlag(PermissionAction.Insert);
            var newUpdate = existingPermission.Update == true || actions.HasFlag(PermissionAction.Update);
            var newDelete = existingPermission.Delete == true || actions.HasFlag(PermissionAction.Delete);

            await Context.ExecuteAsync(Sql.Permissions_Update(),
                newSelect, newInsert, newUpdate, newDelete, existingPermission.Id);
        }
        else
        {
            var newId = await Context.NextObjectIdAsync();
            await Context.ExecuteAsync(Sql.Permissions_Insert(),
                newId, (object?)userId ?? DBNull.Value, (object?)roleId ?? DBNull.Value, objectId,
                actions.HasFlag(PermissionAction.Select),
                actions.HasFlag(PermissionAction.Insert),
                actions.HasFlag(PermissionAction.Update),
                actions.HasFlag(PermissionAction.Delete));
        }

        InvalidatePermissionCache(userId, objectId);
        return true;
    }

    /// <inheritdoc />
    public virtual async Task<bool> RevokePermissionAsync(IRedbUser user, IRedbObject obj, IRedbUser? currentUser = null)
    {
        return await RevokePermissionInternalAsync(user.Id, null, obj.Id, currentUser);
    }

    /// <inheritdoc />
    public virtual async Task<bool> RevokePermissionAsync(IRedbRole role, IRedbObject obj, IRedbUser? currentUser = null)
    {
        return await RevokePermissionInternalAsync(null, role.Id, obj.Id, currentUser);
    }

    /// <summary>
    /// Internal method to revoke permission.
    /// </summary>
    protected virtual async Task<bool> RevokePermissionInternalAsync(long? userId, long? roleId, long objectId, IRedbUser? currentUser = null)
    {
        var result = await Context.ExecuteAsync(Sql.Permissions_DeleteByUserRoleObject(),
            (object?)userId ?? DBNull.Value, (object?)roleId ?? DBNull.Value, objectId);
        InvalidatePermissionCache(userId, objectId);
        return result > 0;
    }

    /// <inheritdoc />
    public virtual async Task<int> RevokeAllUserPermissionsAsync(IRedbUser user, IRedbUser? currentUser = null)
    {
        InvalidatePermissionCache(user.Id, null);
        return await Context.ExecuteAsync(Sql.Permissions_DeleteByUser(), user.Id);
    }

    /// <inheritdoc />
    public virtual async Task<int> RevokeAllRolePermissionsAsync(IRedbRole role, IRedbUser? currentUser = null)
    {
        InvalidatePermissionCache();
        return await Context.ExecuteAsync(Sql.Permissions_DeleteByRole(), role.Id);
    }

    // ============================================================
    // === EFFECTIVE PERMISSIONS ===
    // ============================================================

    /// <inheritdoc />
    public virtual async Task<EffectivePermissionResult> GetEffectivePermissionsAsync(IRedbUser user, IRedbObject obj)
    {
        return await GetEffectivePermissionsAsync(user.Id, obj.Id);
    }

    /// <summary>
    /// Get effective permissions for user and object.
    /// </summary>
    protected virtual async Task<EffectivePermissionResult> GetEffectivePermissionsAsync(long userId, long objectId)
    {
        // Get user's direct permissions
        var userPermissions = await Context.QueryAsync<RedbPermission>(
            Sql.Permissions_SelectByUser() + $" AND (_id_ref = {Sql.FormatParameter(2)} OR _id_ref = 0)", userId, objectId);

        // Get user's roles
        var userRoles = await Context.QueryAsync<RedbUserRole>(Sql.Permissions_SelectUserRoleIds(), userId);
        var roleIds = userRoles.Select(ur => ur.IdRole).ToList();

        // Get role permissions
        List<RedbPermission> rolePermissions = [];
        if (roleIds.Count > 0)
        {
            var roleIdList = string.Join(",", roleIds);
            rolePermissions = await Context.QueryAsync<RedbPermission>(
                $@"SELECT _id AS Id, _id_user AS IdUser, _id_role AS IdRole, _id_ref AS IdRef,
                   _select AS ""Select"", _insert AS ""Insert"", _update AS ""Update"", _delete AS ""Delete""
                   FROM _permissions WHERE _id_role IN ({roleIdList}) AND (_id_ref = {Sql.FormatParameter(1)} OR _id_ref = 0)", objectId);
        }

        return BuildEffectivePermissionResult(userId, objectId, userPermissions, rolePermissions);
    }

    /// <inheritdoc />
    public virtual async Task<Dictionary<IRedbObject, EffectivePermissionResult>> GetEffectivePermissionsBatchAsync(IRedbUser user, IRedbObject[] objects)
    {
        var objectIds = objects.Select(o => o.Id).ToArray();
        var results = await GetEffectivePermissionsBatchAsync(user.Id, objectIds);

        var finalResults = new Dictionary<IRedbObject, EffectivePermissionResult>();
        foreach (var obj in objects)
        {
            if (results.TryGetValue(obj.Id, out var result))
                finalResults[obj] = result;
        }
        return finalResults;
    }

    /// <summary>
    /// Batch get effective permissions.
    /// </summary>
    protected virtual async Task<Dictionary<long, EffectivePermissionResult>> GetEffectivePermissionsBatchAsync(long userId, long[] objectIds)
    {
        if (objectIds == null || objectIds.Length == 0)
            return new Dictionary<long, EffectivePermissionResult>();

        var objectIdList = string.Join(",", objectIds);

        var userPermissions = await Context.QueryAsync<RedbPermission>(
            $@"SELECT _id AS Id, _id_user AS IdUser, _id_role AS IdRole, _id_ref AS IdRef,
               _select AS ""Select"", _insert AS ""Insert"", _update AS ""Update"", _delete AS ""Delete""
               FROM _permissions WHERE _id_user = {Sql.FormatParameter(1)} AND _id_ref IN ({objectIdList})", userId);

        var userRoles = await Context.QueryAsync<RedbUserRole>(Sql.Permissions_SelectUserRoleIds(), userId);
        var roleIds = userRoles.Select(ur => ur.IdRole).ToList();

        List<RedbPermission> rolePermissions = [];
        if (roleIds.Count > 0)
        {
            var roleIdList = string.Join(",", roleIds);
            rolePermissions = await Context.QueryAsync<RedbPermission>(
                $@"SELECT _id AS Id, _id_user AS IdUser, _id_role AS IdRole, _id_ref AS IdRef,
                   _select AS ""Select"", _insert AS ""Insert"", _update AS ""Update"", _delete AS ""Delete""
                   FROM _permissions WHERE _id_role IN ({roleIdList}) AND _id_ref IN ({objectIdList})");
        }

        var userPermissionsByObject = userPermissions.GroupBy(p => p.IdRef).ToDictionary(g => g.Key, g => g.ToList());
        var rolePermissionsByObject = rolePermissions.GroupBy(p => p.IdRef).ToDictionary(g => g.Key, g => g.ToList());

        var result = new Dictionary<long, EffectivePermissionResult>();
        foreach (var objectId in objectIds)
        {
            var userPerms = userPermissionsByObject.GetValueOrDefault(objectId, []);
            var rolePerms = rolePermissionsByObject.GetValueOrDefault(objectId, []);
            result[objectId] = BuildEffectivePermissionResult(userId, objectId, userPerms, rolePerms);
        }

        return result;
    }

    /// <inheritdoc />
    public virtual async Task<List<EffectivePermissionResult>> GetAllEffectivePermissionsAsync(IRedbUser user)
    {
        return await GetAllEffectivePermissionsAsync(user.Id);
    }

    /// <summary>
    /// Get all effective permissions for user.
    /// </summary>
    protected virtual async Task<List<EffectivePermissionResult>> GetAllEffectivePermissionsAsync(long userId)
    {
        var userPermissions = await Context.QueryAsync<RedbPermission>(Sql.Permissions_SelectByUser(), userId);
        var userRoles = await Context.QueryAsync<RedbUserRole>(Sql.Permissions_SelectUserRoleIds(), userId);
        var roleIds = userRoles.Select(ur => ur.IdRole).ToList();

        List<RedbPermission> rolePermissions = [];
        if (roleIds.Count > 0)
        {
            var roleIdList = string.Join(",", roleIds);
            rolePermissions = await Context.QueryAsync<RedbPermission>(
                $@"SELECT _id AS Id, _id_user AS IdUser, _id_role AS IdRole, _id_ref AS IdRef,
                   _select AS ""Select"", _insert AS ""Insert"", _update AS ""Update"", _delete AS ""Delete""
                   FROM _permissions WHERE _id_role IN ({roleIdList})");
        }

        var allObjectIds = userPermissions.Select(p => p.IdRef)
            .Concat(rolePermissions.Select(p => p.IdRef))
            .Distinct()
            .ToList();

        var userPermissionsByObject = userPermissions.GroupBy(p => p.IdRef).ToDictionary(g => g.Key, g => g.ToList());
        var rolePermissionsByObject = rolePermissions.GroupBy(p => p.IdRef).ToDictionary(g => g.Key, g => g.ToList());

        var result = new List<EffectivePermissionResult>();
        foreach (var objectId in allObjectIds)
        {
            var userPerms = userPermissionsByObject.GetValueOrDefault(objectId, []);
            var rolePerms = rolePermissionsByObject.GetValueOrDefault(objectId, []);
            var effectiveResult = BuildEffectivePermissionResult(userId, objectId, userPerms, rolePerms);

            if (effectiveResult.HasAnyPermission)
                result.Add(effectiveResult);
        }

        return result.OrderBy(r => r.ObjectId).ToList();
    }

    /// <summary>
    /// Build EffectivePermissionResult from user and role permissions.
    /// </summary>
    private static EffectivePermissionResult BuildEffectivePermissionResult(
        long userId, long objectId,
        List<RedbPermission> userPermissions,
        List<RedbPermission> rolePermissions)
    {
        var result = new EffectivePermissionResult
        {
            UserId = userId,
            ObjectId = objectId,
            PermissionSourceId = objectId,
            PermissionType = "Combined",
            CanSelect = userPermissions.Any(p => p.Select == true) || rolePermissions.Any(p => p.Select == true),
            CanInsert = userPermissions.Any(p => p.Insert == true) || rolePermissions.Any(p => p.Insert == true),
            CanUpdate = userPermissions.Any(p => p.Update == true) || rolePermissions.Any(p => p.Update == true),
            CanDelete = userPermissions.Any(p => p.Delete == true) || rolePermissions.Any(p => p.Delete == true)
        };

        if (userPermissions.Count != 0 && rolePermissions.Count != 0)
        {
            result.PermissionType = "UserAndRole";
        }
        else if (userPermissions.Count != 0)
        {
            result.PermissionType = "User";
            result.PermissionUserId = userId;
        }
        else if (rolePermissions.Count != 0)
        {
            result.PermissionType = "Role";
            result.RoleId = rolePermissions[0].IdRole;
        }

        return result;
    }

    // ============================================================
    // === STATISTICS ===
    // ============================================================

    /// <inheritdoc />
    public virtual async Task<int> GetPermissionCountAsync()
    {
        return await Context.ExecuteScalarAsync<int>(Sql.Permissions_Count());
    }

    /// <inheritdoc />
    public virtual async Task<int> GetUserPermissionCountAsync(IRedbUser user)
    {
        return await GetUserPermissionCountAsync(user.Id);
    }

    /// <summary>
    /// Get permission count for user.
    /// </summary>
    protected virtual async Task<int> GetUserPermissionCountAsync(long userId)
    {
        var directPermissions = await Context.ExecuteScalarAsync<int>(Sql.Permissions_CountByUser(), userId);

        var userRoles = await Context.QueryAsync<RedbUserRole>(Sql.Permissions_SelectUserRoleIds(), userId);
        var roleIds = userRoles.Select(ur => ur.IdRole).ToList();

        int rolePermissions = 0;
        if (roleIds.Count > 0)
        {
            var roleIdList = string.Join(",", roleIds);
            rolePermissions = await Context.ExecuteScalarAsync<int>(
                $"SELECT COUNT(*) FROM _permissions WHERE _id_role IN ({roleIdList})");
        }

        return directPermissions + rolePermissions;
    }

    /// <inheritdoc />
    public virtual async Task<int> GetRolePermissionCountAsync(IRedbRole role)
    {
        return await Context.ExecuteScalarAsync<int>(Sql.Permissions_CountByRole(), role.Id);
    }

    // ============================================================
    // === LIFECYCLE HOOKS ===
    // ============================================================

    /// <summary>
    /// Called after permission is created. Override in Pro for audit.
    /// </summary>
    protected virtual Task OnPermissionCreatedAsync(IRedbPermission permission, IRedbUser? currentUser)
    {
        return Task.CompletedTask;
    }

    /// <summary>
    /// Called after permission is updated. Override in Pro for audit.
    /// </summary>
    protected virtual Task OnPermissionUpdatedAsync(IRedbPermission permission, IRedbUser? currentUser)
    {
        return Task.CompletedTask;
    }

    /// <summary>
    /// Called after permission is deleted. Override in Pro for audit.
    /// </summary>
    protected virtual Task OnPermissionDeletedAsync(IRedbPermission permission, IRedbUser? currentUser)
    {
        return Task.CompletedTask;
    }
}

