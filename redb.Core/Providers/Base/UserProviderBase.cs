using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using redb.Core.Data;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Core.Models.Users;
using redb.Core.Query;
using redb.Core.Security;
using Microsoft.Extensions.Logging;

namespace redb.Core.Providers.Base;

/// <summary>
/// Base implementation of user provider with common business logic.
/// Database-specific providers inherit from this class and provide ISqlDialect and IPasswordHasher.
/// </summary>
public abstract class UserProviderBase : IUserProvider
{
    protected readonly IRedbContext Context;
    protected readonly IRedbSecurityContext SecurityContext;
    protected readonly ISqlDialect Sql;
    protected readonly IPasswordHasher PasswordHasher;
    protected readonly ILogger? Logger;

    protected UserProviderBase(
        IRedbContext context,
        IRedbSecurityContext securityContext,
        ISqlDialect sql,
        IPasswordHasher passwordHasher,
        ILogger? logger = null)
    {
        Context = context ?? throw new ArgumentNullException(nameof(context));
        SecurityContext = securityContext ?? throw new ArgumentNullException(nameof(securityContext));
        Sql = sql ?? throw new ArgumentNullException(nameof(sql));
        PasswordHasher = passwordHasher ?? throw new ArgumentNullException(nameof(passwordHasher));
        Logger = logger;
    }

    // ============================================================
    // === CRUD OPERATIONS ===
    // ============================================================

    /// <inheritdoc />
    public virtual async Task<IRedbUser> CreateUserAsync(CreateUserRequest request, IRedbUser? currentUser = null)
    {
        ValidateCreateRequest(request);

        if (!await IsLoginAvailableAsync(request.Login))
            throw new InvalidOperationException($"Login '{request.Login}' is already taken");

        var hashedPassword = PasswordHasher.HashPassword(request.Password);
        var newUserId = await Context.NextObjectIdAsync();

        var newUser = new RedbUser
        {
            Id = newUserId,
            Login = request.Login,
            Password = hashedPassword,
            Name = request.Name,
            Email = request.Email,
            Phone = request.Phone,
            Enabled = request.Enabled,
            DateRegister = request.DateRegister ?? DateTimeOffset.Now,
            DateDismiss = null,
            Key = request.Key,
            CodeInt = request.CodeInt,
            CodeString = request.CodeString,
            CodeGuid = request.CodeGuid,
            Note = request.Note,
            Hash = Guid.NewGuid()
        };

        await Context.ExecuteAsync(Sql.Users_Insert(),
            newUser.Id, newUser.Login, newUser.Password, newUser.Name, 
            (object?)newUser.Phone ?? DBNull.Value, (object?)newUser.Email ?? DBNull.Value,
            newUser.Enabled, newUser.DateRegister, (object?)newUser.DateDismiss ?? DBNull.Value,
            (object?)newUser.Key ?? DBNull.Value, (object?)newUser.CodeInt ?? DBNull.Value,
            (object?)newUser.CodeString ?? DBNull.Value, (object?)newUser.CodeGuid ?? DBNull.Value,
            (object?)newUser.Note ?? DBNull.Value, newUser.Hash);

        // Assign roles
        if (request.RoleNames?.Length > 0)
        {
            await AssignRolesByNamesAsync(newUser.Id, request.RoleNames);
        }
        else if (request.Roles?.Length > 0)
        {
            await AssignRolesByObjectsAsync(newUser.Id, request.Roles);
        }

        await OnUserCreatedAsync(newUser, currentUser);

        return newUser;
    }

    /// <inheritdoc />
    public virtual async Task<IRedbUser> UpdateUserAsync(IRedbUser user, UpdateUserRequest request, IRedbUser? currentUser = null)
    {
        if (user.Id == 0 || user.Id == 1)
            throw new InvalidOperationException($"System user with ID {user.Id} cannot be modified");

        var dbUser = await Context.QueryFirstOrDefaultAsync<RedbUser>(Sql.Users_SelectById(), user.Id);
        if (dbUser == null)
            throw new ArgumentException($"User with ID {user.Id} not found");

        var dataChanged = false;

        if (request.Login != null)
        {
            var exists = await Context.ExecuteScalarAsync<long?>(Sql.Users_ExistsByLoginExcluding(), request.Login, user.Id);
            if (exists.HasValue)
                throw new InvalidOperationException($"User with login '{request.Login}' already exists");
            dbUser.Login = request.Login;
            dataChanged = true;
        }

        if (request.Name != null) { dbUser.Name = request.Name; dataChanged = true; }
        if (request.Phone != null) { dbUser.Phone = request.Phone; dataChanged = true; }
        if (request.Email != null) { dbUser.Email = request.Email; dataChanged = true; }
        if (request.Enabled.HasValue) { dbUser.Enabled = request.Enabled.Value; dataChanged = true; }
        if (request.DateDismiss.HasValue) { dbUser.DateDismiss = request.DateDismiss.Value; dataChanged = true; }
        if (request.Key.HasValue) { dbUser.Key = request.Key.Value; dataChanged = true; }
        if (request.CodeInt.HasValue) { dbUser.CodeInt = request.CodeInt.Value; dataChanged = true; }
        if (request.CodeString != null) { dbUser.CodeString = string.IsNullOrEmpty(request.CodeString) ? null : request.CodeString; dataChanged = true; }
        if (request.CodeGuid.HasValue) { dbUser.CodeGuid = request.CodeGuid.Value; dataChanged = true; }
        if (request.Note != null) { dbUser.Note = string.IsNullOrEmpty(request.Note) ? null : request.Note; dataChanged = true; }

        // Update roles
        if (request.RoleNames != null)
        {
            await Context.ExecuteAsync(Sql.UsersRoles_DeleteByUser(), user.Id);
            await AssignRolesByNamesAsync(user.Id, request.RoleNames);
            dataChanged = true;
        }
        else if (request.Roles != null)
        {
            await Context.ExecuteAsync(Sql.UsersRoles_DeleteByUser(), user.Id);
            await AssignRolesByObjectsAsync(user.Id, request.Roles);
            dataChanged = true;
        }

        if (dataChanged)
        {
            dbUser.Hash = Guid.NewGuid();
            await Context.ExecuteAsync(Sql.Users_Update(),
                dbUser.Login, dbUser.Name, (object?)dbUser.Phone ?? DBNull.Value, 
                (object?)dbUser.Email ?? DBNull.Value, dbUser.Enabled,
                (object?)dbUser.DateDismiss ?? DBNull.Value, (object?)dbUser.Key ?? DBNull.Value,
                (object?)dbUser.CodeInt ?? DBNull.Value, (object?)dbUser.CodeString ?? DBNull.Value,
                (object?)dbUser.CodeGuid ?? DBNull.Value, (object?)dbUser.Note ?? DBNull.Value,
                dbUser.Hash, dbUser.Id);
        }

        await OnUserUpdatedAsync(dbUser, currentUser);

        return dbUser;
    }

    /// <inheritdoc />
    public virtual async Task<bool> DeleteUserAsync(IRedbUser user, IRedbUser? currentUser = null)
    {
        if (user.Id == 0 || user.Id == 1)
            throw new InvalidOperationException($"System user with ID {user.Id} cannot be deleted");

        var dbUser = await Context.QueryFirstOrDefaultAsync<RedbUser>(Sql.Users_SelectById(), user.Id);
        if (dbUser == null)
            return false;

        if (!dbUser.Enabled && dbUser.DateDismiss != null)
            return true; // Already soft-deleted

        // Delete associations
        await Context.ExecuteAsync(Sql.UsersRoles_DeleteByUser(), user.Id);
        await Context.ExecuteAsync(Sql.Permissions_DeleteByUser(), user.Id);

        // Soft delete
        var timestamp = DateTimeOffset.Now.ToString("yyyyMMddHHmmssfff");
        var newLogin = $"{dbUser.Login}_DEL_{timestamp}";
        var newName = $"{dbUser.Name}_DEL_{timestamp}";

        var result = await Context.ExecuteAsync(Sql.Users_SoftDelete(),
            newLogin, newName, false, DateTimeOffset.Now, user.Id);

        if (result > 0)
            await OnUserDeletedAsync(user, currentUser);

        return result > 0;
    }

    // ============================================================
    // === SEARCH AND RETRIEVAL ===
    // ============================================================

    /// <inheritdoc />
    public virtual async Task<IRedbUser?> GetUserByIdAsync(long userId)
    {
        return await Context.QueryFirstOrDefaultAsync<RedbUser>(Sql.Users_SelectById(), userId);
    }

    /// <inheritdoc />
    public virtual async Task<IRedbUser?> GetUserByLoginAsync(string login)
    {
        return await Context.QueryFirstOrDefaultAsync<RedbUser>(Sql.Users_SelectByLogin(), login);
    }

    /// <inheritdoc />
    public virtual async Task<IRedbUser> LoadUserAsync(string login)
    {
        var user = await GetUserByLoginAsync(login);
        if (user == null)
            throw new ArgumentException($"User with login '{login}' not found");
        return user;
    }

    /// <inheritdoc />
    public virtual async Task<IRedbUser> LoadUserAsync(long userId)
    {
        var user = await GetUserByIdAsync(userId);
        if (user == null)
            throw new ArgumentException($"User with ID {userId} not found");
        return user;
    }

    /// <inheritdoc />
    public virtual async Task<List<IRedbUser>> GetUsersAsync(UserSearchCriteria? criteria = null)
    {
        // Build dynamic SQL - this is DB-specific, override in derived class if needed
        var sql = BuildUserSearchSql(criteria, out var parameters);
        var users = await Context.QueryAsync<RedbUser>(sql, parameters);
        return users.Cast<IRedbUser>().ToList();
    }

    /// <summary>
    /// Build SQL for user search. Override in derived class for DB-specific syntax.
    /// </summary>
    protected virtual string BuildUserSearchSql(UserSearchCriteria? criteria, out object[] parameters)
    {
        var conditions = new List<string>();
        var paramList = new List<object>();
        int paramIndex = 1;

        if (criteria != null)
        {
            if (criteria.ExcludeSystemUsers)
                conditions.Add("_id > 1");

            if (!string.IsNullOrEmpty(criteria.LoginPattern))
            {
                conditions.Add($"_login ILIKE {Sql.FormatParameter(paramIndex++)}");
                paramList.Add($"%{criteria.LoginPattern}%");
            }

            if (!string.IsNullOrEmpty(criteria.NamePattern))
            {
                conditions.Add($"_name ILIKE {Sql.FormatParameter(paramIndex++)}");
                paramList.Add($"%{criteria.NamePattern}%");
            }

            if (!string.IsNullOrEmpty(criteria.EmailPattern))
            {
                conditions.Add($"_email ILIKE {Sql.FormatParameter(paramIndex++)}");
                paramList.Add($"%{criteria.EmailPattern}%");
            }

            if (criteria.Enabled.HasValue)
            {
                conditions.Add($"_enabled = {Sql.FormatParameter(paramIndex++)}");
                paramList.Add(criteria.Enabled.Value);
            }

            if (criteria.RoleId.HasValue)
            {
                conditions.Add($"_id IN (SELECT _id_user FROM _users_roles WHERE _id_role = {Sql.FormatParameter(paramIndex++)})");
                paramList.Add(criteria.RoleId.Value);
            }

            if (criteria.RegisteredFrom.HasValue)
            {
                conditions.Add($"_date_register >= {Sql.FormatParameter(paramIndex++)}");
                paramList.Add(criteria.RegisteredFrom.Value);
            }

            if (criteria.RegisteredTo.HasValue)
            {
                conditions.Add($"_date_register <= {Sql.FormatParameter(paramIndex++)}");
                paramList.Add(criteria.RegisteredTo.Value);
            }

            if (criteria.KeyValue.HasValue)
            {
                conditions.Add($"_key = {Sql.FormatParameter(paramIndex++)}");
                paramList.Add(criteria.KeyValue.Value);
            }

            if (criteria.CodeIntValue.HasValue)
            {
                conditions.Add($"_code_int = {Sql.FormatParameter(paramIndex++)}");
                paramList.Add(criteria.CodeIntValue.Value);
            }

            if (!string.IsNullOrEmpty(criteria.CodeStringPattern))
            {
                conditions.Add($"_code_string ILIKE {Sql.FormatParameter(paramIndex++)}");
                paramList.Add($"%{criteria.CodeStringPattern}%");
            }

            if (!string.IsNullOrEmpty(criteria.NotePattern))
            {
                conditions.Add($"_note ILIKE {Sql.FormatParameter(paramIndex++)}");
                paramList.Add($"%{criteria.NotePattern}%");
            }

            if (criteria.CodeGuidValue.HasValue)
            {
                conditions.Add($"_code_guid = {Sql.FormatParameter(paramIndex++)}");
                paramList.Add(criteria.CodeGuidValue.Value);
            }
        }

        var sql = """
            SELECT _id AS Id, _login AS Login, _name AS Name, _password AS Password, 
                   _phone AS Phone, _email AS Email, _enabled AS Enabled,
                   _date_register AS DateRegister, _date_dismiss AS DateDismiss,
                   _key AS Key, _code_int AS CodeInt, _code_string AS CodeString,
                   _code_guid AS CodeGuid, _note AS Note, _hash AS Hash
            FROM _users
            """;

        if (conditions.Count > 0)
            sql += " WHERE " + string.Join(" AND ", conditions);
        else if (criteria == null)
            sql += " WHERE _id > 1";

        if (criteria != null)
        {
            var sortColumn = criteria.SortBy switch
            {
                UserSortField.Id => "_id",
                UserSortField.Login => "_login",
                UserSortField.Name => "_name",
                UserSortField.Email => "_email",
                UserSortField.DateRegister => "_date_register",
                UserSortField.DateDismiss => "_date_dismiss",
                UserSortField.Enabled => "_enabled",
                UserSortField.Key => "_key",
                UserSortField.CodeInt => "_code_int",
                UserSortField.CodeString => "_code_string",
                UserSortField.Note => "_note",
                _ => "_name"
            };
            var sortDir = criteria.SortDirection == UserSortDirection.Ascending ? "ASC" : "DESC";
            sql += $" ORDER BY {sortColumn} {sortDir}";

            sql += Sql.FormatPagination(criteria.Limit > 0 ? criteria.Limit : null, criteria.Offset > 0 ? criteria.Offset : null);
        }
        else
        {
            sql += " ORDER BY _name";
        }

        parameters = paramList.ToArray();
        return sql;
    }

    // ============================================================
    // === AUTHENTICATION ===
    // ============================================================

    /// <inheritdoc />
    public virtual async Task<IRedbUser?> ValidateUserAsync(string login, string password)
    {
        if (string.IsNullOrWhiteSpace(login) || string.IsNullOrWhiteSpace(password))
            return null;

        try
        {
            var user = await Context.QueryFirstOrDefaultAsync<RedbUser>(Sql.Users_SelectByLogin(), login);
            if (user == null || !user.Enabled)
                return null;

            if (!PasswordHasher.VerifyPassword(password, user.Password))
                return null;

            return user;
        }
        catch
        {
            return null;
        }
    }

    /// <inheritdoc />
    public virtual async Task<bool> ChangePasswordAsync(IRedbUser user, string currentPassword, string newPassword, IRedbUser? currentUser = null)
    {
        if (user == null) throw new ArgumentNullException(nameof(user));
        if (string.IsNullOrWhiteSpace(currentPassword)) throw new ArgumentException("Current password cannot be empty");
        if (string.IsNullOrWhiteSpace(newPassword)) throw new ArgumentException("New password cannot be empty");
        if (user.Id == 0 || user.Id == 1) throw new InvalidOperationException("Cannot change password for system users");

        var dbUser = await Context.QueryFirstOrDefaultAsync<RedbUser>(Sql.Users_SelectById(), user.Id);
        if (dbUser == null) throw new InvalidOperationException($"User with ID {user.Id} not found");
        if (!dbUser.Enabled) throw new InvalidOperationException("Cannot change password for disabled user");

        if (!PasswordHasher.VerifyPassword(currentPassword, dbUser.Password))
            throw new UnauthorizedAccessException("Invalid current password");

        if (PasswordHasher.VerifyPassword(newPassword, dbUser.Password))
            throw new ArgumentException("New password must be different from current");

        var hashedPassword = PasswordHasher.HashPassword(newPassword);
        await Context.ExecuteAsync(Sql.Users_UpdatePassword(), hashedPassword, user.Id);

        return true;
    }

    /// <inheritdoc />
    public virtual async Task<bool> SetPasswordAsync(IRedbUser user, string newPassword, IRedbUser? currentUser = null)
    {
        if (user.Id == 0)
            throw new InvalidOperationException("System user password (ID 0) cannot be changed");

        if (currentUser != null && currentUser.Id != 1 && currentUser.Id != user.Id)
            throw new UnauthorizedAccessException("Only admin can set passwords for other users");

        if (string.IsNullOrWhiteSpace(newPassword) || newPassword.Length < 4)
            throw new ArgumentException("Password must be at least 4 characters");

        var exists = await Context.ExecuteScalarAsync<long?>(Sql.Users_ExistsById(), user.Id);
        if (!exists.HasValue)
            throw new ArgumentException($"User with ID {user.Id} not found");

        var hashedPassword = PasswordHasher.HashPassword(newPassword);
        var result = await Context.ExecuteAsync(Sql.Users_UpdatePassword(), hashedPassword, user.Id);
        return result > 0;
    }

    // ============================================================
    // === STATUS MANAGEMENT ===
    // ============================================================

    /// <inheritdoc />
    public virtual async Task<bool> EnableUserAsync(IRedbUser user, IRedbUser? currentUser = null)
    {
        if (user.Id == 0 || user.Id == 1)
            return true;

        var result = await Context.ExecuteAsync(Sql.Users_UpdateStatus(), true, DBNull.Value, user.Id);
        return result >= 0;
    }

    /// <inheritdoc />
    public virtual async Task<bool> DisableUserAsync(IRedbUser user, IRedbUser? currentUser = null)
    {
        if (user.Id == 0 || user.Id == 1)
            throw new InvalidOperationException($"System user with ID {user.Id} cannot be disabled");

        if (currentUser != null && currentUser.Id == user.Id)
            throw new InvalidOperationException("User cannot disable themselves");

        var result = await Context.ExecuteAsync(Sql.Users_UpdateStatus(), false, DateTimeOffset.UtcNow, user.Id);
        return result >= 0;
    }

    // ============================================================
    // === VALIDATION ===
    // ============================================================

    /// <inheritdoc />
    public virtual async Task<UserValidationResult> ValidateUserDataAsync(CreateUserRequest request)
    {
        var result = new UserValidationResult();

        if (string.IsNullOrWhiteSpace(request.Login))
            result.AddError("Login", "Login is required");
        else
        {
            if (request.Login.Length < 3)
                result.AddError("Login", "Login must be at least 3 characters");
            if (!await IsLoginAvailableAsync(request.Login))
                result.AddError("Login", $"Login '{request.Login}' is already taken");
            if (request.Login.Contains(' ') || request.Login.Contains('@'))
                result.AddError("Login", "Login cannot contain spaces or @");
        }

        if (string.IsNullOrWhiteSpace(request.Name))
            result.AddError("Name", "Name is required");

        if (string.IsNullOrWhiteSpace(request.Password))
            result.AddError("Password", "Password is required");
        else if (request.Password.Length < 4)
            result.AddError("Password", "Password must be at least 4 characters");

        if (!string.IsNullOrWhiteSpace(request.Email))
        {
            if (!request.Email.Contains('@') || !request.Email.Contains('.'))
                result.AddError("Email", "Invalid email format");

            var emailExists = await Context.ExecuteScalarAsync<long?>(Sql.Users_ExistsByEmail(), request.Email);
            if (emailExists.HasValue)
                result.AddError("Email", $"Email '{request.Email}' is already taken");
        }

        if (!string.IsNullOrWhiteSpace(request.Phone) && request.Phone.Length < 7)
            result.AddError("Phone", "Phone number is too short");

        if (request.CodeInt.HasValue)
        {
            if (request.CodeInt.Value < 0)
                result.AddError("CodeInt", "Code cannot be negative");
            if (request.CodeInt.Value > 999999999)
                result.AddError("CodeInt", "Code is too large");
        }

        if (!string.IsNullOrWhiteSpace(request.CodeString))
        {
            if (request.CodeString.Length > 50)
                result.AddError("CodeString", "Code string is too long (max 50)");
            if (request.CodeString.Contains(';') || request.CodeString.Contains('|'))
                result.AddError("CodeString", "Code string cannot contain ; or |");
        }

        if (!string.IsNullOrWhiteSpace(request.Note) && request.Note.Length > 1000)
            result.AddError("Note", "Note is too long (max 1000)");

        return result;
    }

    /// <inheritdoc />
    public virtual async Task<bool> IsLoginAvailableAsync(string login, long? excludeUserId = null)
    {
        if (excludeUserId.HasValue)
        {
            var exists = await Context.ExecuteScalarAsync<long?>(Sql.Users_ExistsByLoginExcluding(), login, excludeUserId.Value);
            return !exists.HasValue;
        }
        else
        {
            var exists = await Context.ExecuteScalarAsync<long?>(Sql.Users_ExistsByLogin(), login);
            return !exists.HasValue;
        }
    }

    // ============================================================
    // === STATISTICS ===
    // ============================================================

    /// <inheritdoc />
    public virtual async Task<int> GetUserCountAsync(bool includeDisabled = false)
    {
        return await Context.ExecuteScalarAsync<int>(includeDisabled ? Sql.Users_Count() : Sql.Users_CountEnabled());
    }

    /// <inheritdoc />
    public virtual Task<int> GetActiveUserCountAsync(DateTimeOffset fromDate, DateTimeOffset toDate)
    {
        throw new NotImplementedException("GetActiveUserCountAsync requires activity logging");
    }

    // ============================================================
    // === CONFIGURATION MANAGEMENT ===
    // ============================================================

    /// <inheritdoc />
    public virtual async Task<long?> GetUserConfigurationIdAsync(long userId)
    {
        return await Context.ExecuteScalarAsync<long?>(Sql.Users_SelectConfigurationId(), userId);
    }

    /// <inheritdoc />
    public virtual async Task SetUserConfigurationAsync(long userId, long? configId)
    {
        var result = await Context.ExecuteAsync(Sql.Users_UpdateConfiguration(), (object?)configId ?? DBNull.Value, userId);
        if (result == 0)
            throw new ArgumentException($"User with ID {userId} not found");
    }

    /// <inheritdoc />
    public virtual async Task<List<IRedbRole>> GetUserRolesAsync(long userId)
    {
        var roles = await Context.QueryAsync<RedbRole>(Sql.UsersRoles_SelectRolesByUser(), userId);
        return roles.Cast<IRedbRole>().ToList();
    }

    // ============================================================
    // === PRIVATE HELPERS ===
    // ============================================================

    private void ValidateCreateRequest(CreateUserRequest request)
    {
        if (request == null) throw new ArgumentNullException(nameof(request));
        if (string.IsNullOrWhiteSpace(request.Login)) throw new ArgumentException("Login cannot be empty");
        if (string.IsNullOrWhiteSpace(request.Password)) throw new ArgumentException("Password cannot be empty");
        if (string.IsNullOrWhiteSpace(request.Name)) throw new ArgumentException("Name cannot be empty");
    }

    private async Task AssignRolesByNamesAsync(long userId, string[] roleNames)
    {
        foreach (var roleName in roleNames)
        {
            var role = await Context.QueryFirstOrDefaultAsync<RedbRole>(Sql.Roles_SelectIdByName(), roleName);
            if (role == null)
                throw new ArgumentException($"Role '{roleName}' not found");

            var userRoleId = await Context.NextObjectIdAsync();
            await Context.ExecuteAsync(Sql.UsersRoles_Insert(), userRoleId, userId, role.Id);
        }
    }

    private async Task AssignRolesByObjectsAsync(long userId, IRedbRole[] roles)
    {
        foreach (var role in roles)
        {
            var exists = await Context.ExecuteScalarAsync<long?>(Sql.Roles_ExistsById(), role.Id);
            if (!exists.HasValue)
                throw new ArgumentException($"Role with ID {role.Id} ('{role.Name}') not found");

            var userRoleId = await Context.NextObjectIdAsync();
            await Context.ExecuteAsync(Sql.UsersRoles_Insert(), userRoleId, userId, role.Id);
        }
    }

    // ============================================================
    // === LIFECYCLE HOOKS ===
    // ============================================================

    protected virtual Task OnUserCreatedAsync(IRedbUser user, IRedbUser? currentUser) => Task.CompletedTask;
    protected virtual Task OnUserUpdatedAsync(IRedbUser user, IRedbUser? currentUser) => Task.CompletedTask;
    protected virtual Task OnUserDeletedAsync(IRedbUser user, IRedbUser? currentUser) => Task.CompletedTask;
}

