using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using redb.Core.Data;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Core.Models.Roles;
using redb.Core.Query;
using Microsoft.Extensions.Logging;

namespace redb.Core.Providers.Base;

/// <summary>
/// Base implementation of role provider with common business logic.
/// Database-specific providers inherit from this class and provide ISqlDialect.
/// 
/// Usage:
/// public class PostgresRoleProvider : RoleProviderBase
/// {
///     public PostgresRoleProvider(IRedbContext context, IRedbSecurityContext security)
///         : base(context, security, new PostgreSqlDialect()) { }
/// }
/// </summary>
public abstract class RoleProviderBase : IRoleProvider
{
    protected readonly IRedbContext Context;
    protected readonly IRedbSecurityContext SecurityContext;
    protected readonly ISqlDialect Sql;
    protected readonly ILogger? Logger;

    protected RoleProviderBase(
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
    // === CRUD ROLES ===
    // ============================================================

    /// <inheritdoc />
    public virtual async Task<IRedbRole> CreateRoleAsync(CreateRoleRequest request, IRedbUser? currentUser = null)
    {
        ValidateRoleName(request.Name);

        // Check name uniqueness
        var existingRole = await GetRoleByNameInternalAsync(request.Name);
        if (existingRole != null)
            throw new InvalidOperationException($"Role with name '{request.Name}' already exists");

        // Create new role
        var newRoleId = await Context.NextObjectIdAsync();
        await InsertRoleInternalAsync(newRoleId, request.Name);

        var newRole = new RedbRole
        {
            Id = newRoleId,
            Name = request.Name
        };

        // Assign users to role if specified
        if (request.UserLogins?.Length > 0)
        {
            await AssignUsersByLoginsAsync(newRole, request.UserLogins);
        }
        else if (request.Users?.Length > 0)
        {
            await AssignUsersByObjectsAsync(newRole, request.Users);
        }

        // Lifecycle hook for derived classes
        await OnRoleCreatedAsync(newRole, currentUser);

        return newRole;
    }

    /// <inheritdoc />
    public virtual async Task<IRedbRole> UpdateRoleAsync(IRedbRole role, string newName, IRedbUser? currentUser = null)
    {
        ValidateRoleName(newName);

        var dbRole = await GetRoleByIdInternalAsync(role.Id);
        if (dbRole == null)
            throw new ArgumentException($"Role with ID {role.Id} not found");

        // Check name uniqueness if changed
        if (dbRole.Name != newName)
        {
            var existingRole = await Context.ExecuteScalarAsync<long?>(
                Sql.Roles_ExistsByNameExcluding(), newName, role.Id);
            if (existingRole.HasValue)
                throw new InvalidOperationException($"Role with name '{newName}' already exists");

            await Context.ExecuteAsync(Sql.Roles_UpdateName(), newName, role.Id);
            dbRole.Name = newName;
        }

        await OnRoleUpdatedAsync(dbRole, currentUser);

        return dbRole;
    }

    /// <inheritdoc />
    public virtual async Task<bool> DeleteRoleAsync(IRedbRole role, IRedbUser? currentUser = null)
    {
        var dbRole = await GetRoleByIdInternalAsync(role.Id);
        if (dbRole == null)
            return false;

        // Cascade delete user-role associations
        await Context.ExecuteAsync(Sql.UsersRoles_DeleteByRole(), role.Id);

        // Cascade delete role permissions
        await Context.ExecuteAsync(Sql.Permissions_DeleteByRole(), role.Id);

        // Delete role itself
        var result = await Context.ExecuteAsync(Sql.Roles_Delete(), role.Id);

        if (result > 0)
            await OnRoleDeletedAsync(role, currentUser);

        return result > 0;
    }

    // ============================================================
    // === SEARCH ROLES ===
    // ============================================================

    /// <inheritdoc />
    public virtual async Task<IRedbRole?> GetRoleByIdAsync(long roleId)
    {
        return await GetRoleByIdInternalAsync(roleId);
    }

    /// <inheritdoc />
    public virtual async Task<IRedbRole?> GetRoleByNameAsync(string roleName)
    {
        return await GetRoleByNameInternalAsync(roleName);
    }

    /// <inheritdoc />
    public virtual async Task<IRedbRole> LoadRoleAsync(long roleId)
    {
        var role = await GetRoleByIdAsync(roleId);
        if (role == null)
            throw new ArgumentException($"Role with ID {roleId} not found");
        return role;
    }

    /// <inheritdoc />
    public virtual async Task<IRedbRole> LoadRoleAsync(string roleName)
    {
        var role = await GetRoleByNameAsync(roleName);
        if (role == null)
            throw new ArgumentException($"Role with name '{roleName}' not found");
        return role;
    }

    /// <inheritdoc />
    public virtual async Task<List<IRedbRole>> GetRolesAsync()
    {
        var roles = await Context.QueryAsync<RedbRole>(Sql.Roles_SelectAll());
        return roles.Cast<IRedbRole>().ToList();
    }

    // ============================================================
    // === USER-ROLE MANAGEMENT ===
    // ============================================================

    /// <inheritdoc />
    public virtual async Task<bool> AssignUserToRoleAsync(IRedbUser user, IRedbRole role, IRedbUser? currentUser = null)
    {
        // Check user exists
        var userExists = await Context.ExecuteScalarAsync<long?>(Sql.Users_ExistsById(), user.Id);
        if (!userExists.HasValue)
            throw new ArgumentException($"User with ID {user.Id} not found");

        // Check role exists
        var roleExists = await Context.ExecuteScalarAsync<long?>(Sql.Roles_SelectById().Replace("SELECT _id AS Id, _name AS Name, _id_configuration AS IdConfiguration FROM _roles WHERE", "SELECT _id FROM _roles WHERE"), role.Id);
        if (!roleExists.HasValue)
            throw new ArgumentException($"Role with ID {role.Id} not found");

        // Check if already assigned
        var existingAssignment = await Context.ExecuteScalarAsync<long?>(Sql.UsersRoles_Exists(), user.Id, role.Id);
        if (existingAssignment.HasValue)
            return true; // Already assigned

        // Create assignment
        var userRoleId = await Context.NextObjectIdAsync();
        var result = await Context.ExecuteAsync(Sql.UsersRoles_Insert(), userRoleId, user.Id, role.Id);
        return result > 0;
    }

    /// <inheritdoc />
    public virtual async Task<bool> RemoveUserFromRoleAsync(IRedbUser user, IRedbRole role, IRedbUser? currentUser = null)
    {
        var result = await Context.ExecuteAsync(Sql.UsersRoles_Delete(), user.Id, role.Id);
        return result > 0;
    }

    /// <inheritdoc />
    public virtual async Task<bool> SetUserRolesAsync(IRedbUser user, IRedbRole[] roles, IRedbUser? currentUser = null)
    {
        // Check user exists
        var userExists = await Context.ExecuteScalarAsync<long?>(Sql.Users_ExistsById(), user.Id);
        if (!userExists.HasValue)
            throw new ArgumentException($"User with ID {user.Id} not found");

        // Delete all existing user roles
        await Context.ExecuteAsync(Sql.UsersRoles_DeleteByUser(), user.Id);

        // Add new roles
        if (roles is { Length: > 0 })
        {
            foreach (var role in roles)
            {
                var roleExists = await Context.ExecuteScalarAsync<long?>(
                    "SELECT _id FROM _roles WHERE _id = " + Sql.FormatParameter(1), role.Id);
                if (roleExists.HasValue)
                {
                    var userRoleId = await Context.NextObjectIdAsync();
                    await Context.ExecuteAsync(Sql.UsersRoles_Insert(), userRoleId, user.Id, role.Id);
                }
            }
        }

        return true;
    }

    /// <inheritdoc />
    public virtual async Task<List<IRedbRole>> GetUserRolesAsync(IRedbUser user)
    {
        var roles = await Context.QueryAsync<RedbRole>(Sql.UsersRoles_SelectRolesByUser(), user.Id);
        return roles.Cast<IRedbRole>().ToList();
    }

    /// <inheritdoc />
    public virtual async Task<List<IRedbUser>> GetRoleUsersAsync(IRedbRole role)
    {
        var users = await Context.QueryAsync<RedbUser>(Sql.UsersRoles_SelectUsersByRole(), role.Id);
        return users.Cast<IRedbUser>().ToList();
    }

    /// <inheritdoc />
    public virtual async Task<bool> UserHasRoleAsync(IRedbUser user, IRedbRole role)
    {
        var exists = await Context.ExecuteScalarAsync<long?>(Sql.UsersRoles_Exists(), user.Id, role.Id);
        return exists.HasValue;
    }

    // ============================================================
    // === VALIDATION ===
    // ============================================================

    /// <inheritdoc />
    public virtual async Task<bool> IsRoleNameAvailableAsync(string roleName, IRedbRole? excludeRole = null)
    {
        if (excludeRole != null)
        {
            var exists = await Context.ExecuteScalarAsync<long?>(
                Sql.Roles_ExistsByNameExcluding(), roleName, excludeRole.Id);
            return !exists.HasValue;
        }
        else
        {
            var exists = await Context.ExecuteScalarAsync<long?>(Sql.Roles_ExistsByName(), roleName);
            return !exists.HasValue;
        }
    }

    // ============================================================
    // === STATISTICS ===
    // ============================================================

    /// <inheritdoc />
    public virtual async Task<int> GetRoleCountAsync()
    {
        return await Context.ExecuteScalarAsync<int>(Sql.Roles_Count());
    }

    /// <inheritdoc />
    public virtual async Task<int> GetRoleUserCountAsync(IRedbRole role)
    {
        return await Context.ExecuteScalarAsync<int>(Sql.UsersRoles_CountByRole(), role.Id);
    }

    /// <inheritdoc />
    public virtual async Task<Dictionary<IRedbRole, int>> GetRoleStatisticsAsync()
    {
        var roles = await Context.QueryAsync<RedbRole>(Sql.Roles_SelectAll());

        var result = new Dictionary<IRedbRole, int>();
        foreach (var role in roles)
        {
            var count = await Context.ExecuteScalarAsync<int>(Sql.UsersRoles_CountByRole(), role.Id);
            result[role] = count;
        }
        return result;
    }

    // ============================================================
    // === CONFIGURATION MANAGEMENT ===
    // ============================================================

    /// <inheritdoc />
    public virtual async Task<long?> GetRoleConfigurationIdAsync(long roleId)
    {
        return await Context.ExecuteScalarAsync<long?>(Sql.Roles_SelectConfigurationId(), roleId);
    }

    /// <inheritdoc />
    public virtual async Task SetRoleConfigurationAsync(long roleId, long? configId)
    {
        var result = await Context.ExecuteAsync(Sql.Roles_UpdateConfiguration(), (object?)configId ?? DBNull.Value, roleId);
        if (result == 0)
            throw new ArgumentException($"Role with ID {roleId} not found");
    }

    // ============================================================
    // === PROTECTED VIRTUAL METHODS (for override in derived classes) ===
    // ============================================================

    /// <summary>
    /// Insert role into database. Override for DB-specific optimizations (e.g., RETURNING).
    /// </summary>
    protected virtual async Task InsertRoleInternalAsync(long roleId, string name)
    {
        await Context.ExecuteAsync(Sql.Roles_Insert(), roleId, name);
    }

    /// <summary>
    /// Get role by ID from database.
    /// </summary>
    protected virtual async Task<RedbRole?> GetRoleByIdInternalAsync(long roleId)
    {
        return await Context.QueryFirstOrDefaultAsync<RedbRole>(Sql.Roles_SelectById(), roleId);
    }

    /// <summary>
    /// Get role by name from database.
    /// </summary>
    protected virtual async Task<RedbRole?> GetRoleByNameInternalAsync(string name)
    {
        return await Context.QueryFirstOrDefaultAsync<RedbRole>(Sql.Roles_SelectByName(), name);
    }

    // ============================================================
    // === LIFECYCLE HOOKS (for Pro features like audit) ===
    // ============================================================

    /// <summary>
    /// Called after role is created. Override in Pro to add audit logging.
    /// </summary>
    protected virtual Task OnRoleCreatedAsync(IRedbRole role, IRedbUser? currentUser)
    {
        return Task.CompletedTask;
    }

    /// <summary>
    /// Called after role is updated. Override in Pro to add audit logging.
    /// </summary>
    protected virtual Task OnRoleUpdatedAsync(IRedbRole role, IRedbUser? currentUser)
    {
        return Task.CompletedTask;
    }

    /// <summary>
    /// Called after role is deleted. Override in Pro to add audit logging.
    /// </summary>
    protected virtual Task OnRoleDeletedAsync(IRedbRole role, IRedbUser? currentUser)
    {
        return Task.CompletedTask;
    }

    // ============================================================
    // === PRIVATE HELPERS ===
    // ============================================================

    private void ValidateRoleName(string? name)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Role name is required");

        if (name.Length < 2)
            throw new ArgumentException("Role name must be at least 2 characters");
    }

    private async Task AssignUsersByLoginsAsync(IRedbRole role, string[] userLogins)
    {
        foreach (var userLogin in userLogins)
        {
            var userId = await Context.ExecuteScalarAsync<long?>(Sql.Users_SelectIdByLogin(), userLogin);
            if (!userId.HasValue)
                throw new ArgumentException($"User with login '{userLogin}' not found");

            var userRoleId = await Context.NextObjectIdAsync();
            await Context.ExecuteAsync(Sql.UsersRoles_Insert(), userRoleId, userId.Value, role.Id);
        }
    }

    private async Task AssignUsersByObjectsAsync(IRedbRole role, IRedbUser[] users)
    {
        foreach (var user in users)
        {
            var userExists = await Context.ExecuteScalarAsync<long?>(Sql.Users_ExistsById(), user.Id);
            if (!userExists.HasValue)
                throw new ArgumentException($"User with ID {user.Id} ('{user.Login}') not found");

            var userRoleId = await Context.NextObjectIdAsync();
            await Context.ExecuteAsync(Sql.UsersRoles_Insert(), userRoleId, user.Id, role.Id);
        }
    }
}

