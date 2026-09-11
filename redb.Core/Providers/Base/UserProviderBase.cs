using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
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
    public virtual async Task<IRedbUser> CreateUserAsync(CreateUserRequest request, IRedbUser? currentUser = null, CancellationToken cancellationToken = default)
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
            DateRegister = request.DateRegister ?? DateTimeOffset.UtcNow,
            DateDismiss = null,
            Key = request.Key,
            CodeInt = request.CodeInt,
            CodeString = request.CodeString,
            CodeGuid = request.CodeGuid,
            Note = request.Note,
            Hash = Guid.NewGuid()
        };

        await Context.ExecuteAsync(Sql.Users_Insert(), new object[] { newUser.Id, newUser.Login, newUser.Password, newUser.Name, 
            (object?)newUser.Phone ?? DBNull.Value, (object?)newUser.Email ?? DBNull.Value,
            newUser.Enabled, newUser.DateRegister, (object?)newUser.DateDismiss ?? DBNull.Value,
            (object?)newUser.Key ?? DBNull.Value, (object?)newUser.CodeInt ?? DBNull.Value,
            (object?)newUser.CodeString ?? DBNull.Value, (object?)newUser.CodeGuid ?? DBNull.Value,
            (object?)newUser.Note ?? DBNull.Value, newUser.Hash }, cancellationToken);

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
    public virtual async Task<IRedbUser> UpdateUserAsync(IRedbUser user, UpdateUserRequest request, IRedbUser? currentUser = null, CancellationToken cancellationToken = default)
    {
        if (user.Id == 0 || user.Id == -1)
            throw new InvalidOperationException($"System user with ID {user.Id} cannot be modified");

        var dbUser = await Context.QueryFirstOrDefaultAsync<RedbUser>(Sql.Users_SelectById(), new object[] { user.Id }, cancellationToken);
        if (dbUser == null)
            throw new ArgumentException($"User with ID {user.Id} not found");

        var dataChanged = false;

        if (request.Login != null)
        {
            var exists = await Context.ExecuteScalarAsync<long?>(Sql.Users_ExistsByLoginExcluding(), new object[] { request.Login, user.Id }, cancellationToken);
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
            await Context.ExecuteAsync(Sql.UsersRoles_DeleteByUser(), new object[] { user.Id }, cancellationToken);
            await AssignRolesByNamesAsync(user.Id, request.RoleNames);
            dataChanged = true;
        }
        else if (request.Roles != null)
        {
            await Context.ExecuteAsync(Sql.UsersRoles_DeleteByUser(), new object[] { user.Id }, cancellationToken);
            await AssignRolesByObjectsAsync(user.Id, request.Roles);
            dataChanged = true;
        }

        if (dataChanged)
        {
            dbUser.Hash = Guid.NewGuid();
            await Context.ExecuteAsync(Sql.Users_Update(), new object[] { dbUser.Login, dbUser.Name, (object?)dbUser.Phone ?? DBNull.Value, 
                (object?)dbUser.Email ?? DBNull.Value, dbUser.Enabled,
                (object?)dbUser.DateDismiss ?? DBNull.Value, (object?)dbUser.Key ?? DBNull.Value,
                (object?)dbUser.CodeInt ?? DBNull.Value, (object?)dbUser.CodeString ?? DBNull.Value,
                (object?)dbUser.CodeGuid ?? DBNull.Value, (object?)dbUser.Note ?? DBNull.Value,
                dbUser.Hash, dbUser.Id }, cancellationToken);
        }

        await OnUserUpdatedAsync(dbUser, currentUser);

        return dbUser;
    }

    /// <inheritdoc />
    public virtual async Task<bool> DeleteUserAsync(IRedbUser user, IRedbUser? currentUser = null, CancellationToken cancellationToken = default)
    {
        if (user.Id == 0 || user.Id == 1 || user.Id == -1)
            throw new InvalidOperationException($"System user with ID {user.Id} cannot be deleted");

        var dbUser = await Context.QueryFirstOrDefaultAsync<RedbUser>(Sql.Users_SelectById(), new object[] { user.Id }, cancellationToken);
        if (dbUser == null)
            return false;

        if (!dbUser.Enabled && dbUser.DateDismiss != null)
            return true; // Already soft-deleted

        // Delete associations
        await Context.ExecuteAsync(Sql.UsersRoles_DeleteByUser(), new object[] { user.Id }, cancellationToken);
        await Context.ExecuteAsync(Sql.Permissions_DeleteByUser(), new object[] { user.Id }, cancellationToken);

        // Soft delete — login STAYS as-is (immutable per protect_system_users trigger).
        // Name is suffixed for tombstoning visibility. Soft-deleted rows are filtered out
        // of normal queries by the _enabled=false predicate; their login slot remains
        // occupied so re-registration with the same login is blocked until an explicit
        // hard DELETE removes the row.
        var timestamp = DateTimeOffset.UtcNow.ToString("yyyyMMddHHmmssfff");
        var newName = $"{dbUser.Name}_DEL_{timestamp}";

        var result = await Context.ExecuteAsync(Sql.Users_SoftDelete(), new object[] { newName, false, DateTimeOffset.UtcNow, user.Id }, cancellationToken);

        if (result > 0)
            await OnUserDeletedAsync(user, currentUser);

        return result > 0;
    }

    // ============================================================
    // === SEARCH AND RETRIEVAL ===
    // ============================================================

    /// <inheritdoc />
    public virtual async Task<IRedbUser?> GetUserByIdAsync(long userId, CancellationToken cancellationToken = default)
    {
        return await Context.QueryFirstOrDefaultAsync<RedbUser>(Sql.Users_SelectById(), new object[] { userId }, cancellationToken);
    }

    /// <inheritdoc />
    public virtual async Task<List<IRedbUser>> GetUsersByIdsAsync(IEnumerable<long> userIds, CancellationToken cancellationToken = default)
    {
        var ids = userIds.ToArray();
        if (ids.Length == 0)
            return new List<IRedbUser>();
        var users = await Context.QueryAsync<RedbUser>(Sql.Users_SelectByIds(), new object[] { (object)ids }, cancellationToken);
        return users.Cast<IRedbUser>().ToList();
    }

    /// <inheritdoc />
    public virtual async Task<IRedbUser?> GetUserByLoginAsync(string login, CancellationToken cancellationToken = default)
    {
        return await Context.QueryFirstOrDefaultAsync<RedbUser>(Sql.Users_SelectByLogin(), new object[] { login }, cancellationToken);
    }

    /// <inheritdoc />
    public virtual async Task<IRedbUser?> GetUserByEmailAsync(string email, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(email)) return null;
        return await Context.QueryFirstOrDefaultAsync<RedbUser>(Sql.Users_SelectByEmail(), new object[] { email }, cancellationToken);
    }

    /// <inheritdoc />
    public virtual async Task<IRedbUser> LoadUserAsync(string login, CancellationToken cancellationToken = default)
    {
        var user = await GetUserByLoginAsync(login);
        if (user == null)
            throw new ArgumentException($"User with login '{login}' not found");
        return user;
    }

    /// <inheritdoc />
    public virtual async Task<IRedbUser> LoadUserAsync(long userId, CancellationToken cancellationToken = default)
    {
        var user = await GetUserByIdAsync(userId);
        if (user == null)
            throw new ArgumentException($"User with ID {userId} not found");
        return user;
    }

    /// <inheritdoc />
    public virtual async Task<List<IRedbUser>> GetUsersAsync(UserSearchCriteria? criteria = null, CancellationToken cancellationToken = default)
    {
        // Build dynamic SQL - this is DB-specific, override in derived class if needed
        var sql = BuildUserSearchSql(criteria, out var parameters);
        var users = await Context.QueryAsync<RedbUser>(sql, parameters, cancellationToken);
        return users.Cast<IRedbUser>().ToList();
    }

    /// <inheritdoc />
    public virtual async Task<int> CountUsersAsync(UserSearchCriteria? criteria = null, CancellationToken cancellationToken = default)
    {
        var sql = BuildUserCountSql(criteria, out var parameters);
        return await Context.ExecuteScalarAsync<int>(sql, parameters, cancellationToken);
    }

    /// <summary>
    /// Build SQL for user search. Override in derived class for DB-specific syntax.
    /// </summary>
    protected virtual string BuildUserSearchSql(UserSearchCriteria? criteria, out object[] parameters)
    {
        var whereClause = BuildUserWhereClause(criteria, out parameters);
        var sql = Sql.Users_SelectAllColumns() + whereClause;

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
            sql += $" ORDER BY {Sql.QuoteIdentifier(sortColumn)} {sortDir}";

            sql += " " + Sql.FormatPagination(criteria.Limit > 0 ? criteria.Limit : null, criteria.Offset > 0 ? criteria.Offset : null);
        }
        else
        {
            sql += $" ORDER BY {Sql.QuoteIdentifier("_name")}";
        }

        return sql;
    }

    /// <summary>
    /// Build SQL for counting users matching criteria.
    /// </summary>
    protected virtual string BuildUserCountSql(UserSearchCriteria? criteria, out object[] parameters)
    {
        var whereClause = BuildUserWhereClause(criteria, out parameters);
        return Sql.Users_CountFrom() + whereClause;
    }

    /// <summary>
    /// Build WHERE clause for user queries. Shared by search and count.
    /// </summary>
    protected virtual string BuildUserWhereClause(UserSearchCriteria? criteria, out object[] parameters)
    {
        var conditions = new List<string>();
        var paramList = new List<object>();
        int paramIndex = 1;

        if (criteria != null)
        {
            if (criteria.ExcludeSystemUsers)
                conditions.Add("_id > 0");

            if (!string.IsNullOrEmpty(criteria.LoginPattern))
            {
                conditions.Add(Sql.FormatCaseInsensitiveLike("_login", Sql.FormatParameter(paramIndex++)));
                paramList.Add($"%{EscapeLikeWildcards(criteria.LoginPattern)}%");
            }

            if (!string.IsNullOrEmpty(criteria.LoginExact))
            {
                conditions.Add(Sql.FormatCaseInsensitiveLike("_login", Sql.FormatParameter(paramIndex++)));
                paramList.Add(EscapeLikeWildcards(criteria.LoginExact));
            }

            if (!string.IsNullOrEmpty(criteria.LoginStartsWith))
            {
                conditions.Add(Sql.FormatCaseInsensitiveLike("_login", Sql.FormatParameter(paramIndex++)));
                paramList.Add($"{EscapeLikeWildcards(criteria.LoginStartsWith)}%");
            }

            if (!string.IsNullOrEmpty(criteria.LoginNotEqual))
            {
                conditions.Add($"NOT ({Sql.FormatCaseInsensitiveLike("_login", Sql.FormatParameter(paramIndex++))})");
                paramList.Add(EscapeLikeWildcards(criteria.LoginNotEqual));
            }

            if (!string.IsNullOrEmpty(criteria.NamePattern))
            {
                conditions.Add(Sql.FormatCaseInsensitiveLike("_name", Sql.FormatParameter(paramIndex++)));
                paramList.Add($"%{EscapeLikeWildcards(criteria.NamePattern)}%");
            }

            if (!string.IsNullOrEmpty(criteria.NameExact))
            {
                conditions.Add(Sql.FormatCaseInsensitiveLike("_name", Sql.FormatParameter(paramIndex++)));
                paramList.Add(EscapeLikeWildcards(criteria.NameExact));
            }

            if (!string.IsNullOrEmpty(criteria.NameStartsWith))
            {
                conditions.Add(Sql.FormatCaseInsensitiveLike("_name", Sql.FormatParameter(paramIndex++)));
                paramList.Add($"{EscapeLikeWildcards(criteria.NameStartsWith)}%");
            }

            if (!string.IsNullOrEmpty(criteria.NameNotEqual))
            {
                conditions.Add($"NOT ({Sql.FormatCaseInsensitiveLike("_name", Sql.FormatParameter(paramIndex++))})");
                paramList.Add(EscapeLikeWildcards(criteria.NameNotEqual));
            }

            if (!string.IsNullOrEmpty(criteria.EmailPattern))
            {
                conditions.Add(Sql.FormatCaseInsensitiveLike("_email", Sql.FormatParameter(paramIndex++)));
                paramList.Add($"%{EscapeLikeWildcards(criteria.EmailPattern)}%");
            }

            if (!string.IsNullOrEmpty(criteria.EmailExact))
            {
                conditions.Add(Sql.FormatCaseInsensitiveLike("_email", Sql.FormatParameter(paramIndex++)));
                paramList.Add(EscapeLikeWildcards(criteria.EmailExact));
            }

            if (!string.IsNullOrEmpty(criteria.EmailStartsWith))
            {
                conditions.Add(Sql.FormatCaseInsensitiveLike("_email", Sql.FormatParameter(paramIndex++)));
                paramList.Add($"{EscapeLikeWildcards(criteria.EmailStartsWith)}%");
            }

            if (!string.IsNullOrEmpty(criteria.EmailNotEqual))
            {
                conditions.Add($"NOT ({Sql.FormatCaseInsensitiveLike("_email", Sql.FormatParameter(paramIndex++))})");
                paramList.Add(EscapeLikeWildcards(criteria.EmailNotEqual));
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
                conditions.Add(Sql.FormatCaseInsensitiveLike("_code_string", Sql.FormatParameter(paramIndex++)));
                paramList.Add($"%{EscapeLikeWildcards(criteria.CodeStringPattern)}%");
            }

            if (!string.IsNullOrEmpty(criteria.NotePattern))
            {
                conditions.Add(Sql.FormatCaseInsensitiveLike("_note", Sql.FormatParameter(paramIndex++)));
                paramList.Add($"%{EscapeLikeWildcards(criteria.NotePattern)}%");
            }

            if (criteria.CodeGuidValue.HasValue)
            {
                conditions.Add($"_code_guid = {Sql.FormatParameter(paramIndex++)}");
                paramList.Add(criteria.CodeGuidValue.Value);
            }
        }

        parameters = paramList.ToArray();

        if (conditions.Count > 0)
            return " WHERE " + string.Join(" AND ", conditions);
        if (criteria == null)
            return " WHERE _id > 0";
        return "";
    }

    /// <summary>
    /// Escapes LIKE/ILIKE wildcard characters (%, _, \) in user input
    /// so they are treated as literal characters.
    /// </summary>
    protected static string EscapeLikeWildcards(string value)
        => value.Replace("\\", "\\\\").Replace("%", "\\%").Replace("_", "\\_");

    // ============================================================
    // === AUTHENTICATION ===
    // ============================================================

    /// <inheritdoc />
    public virtual async Task<IRedbUser?> ValidateUserAsync(string login, string password, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(login) || string.IsNullOrWhiteSpace(password))
            return null;

        try
        {
            var user = await Context.QueryFirstOrDefaultAsync<RedbUser>(Sql.Users_SelectByLogin(), new object[] { login }, cancellationToken);
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
    public virtual async Task<bool> ChangePasswordAsync(IRedbUser user, string currentPassword, string newPassword, IRedbUser? currentUser = null, CancellationToken cancellationToken = default)
    {
        if (user == null) throw new ArgumentNullException(nameof(user));
        if (string.IsNullOrWhiteSpace(currentPassword)) throw new ArgumentException("Current password cannot be empty");
        if (string.IsNullOrWhiteSpace(newPassword)) throw new ArgumentException("New password cannot be empty");
        if (user.Id == 0) throw new InvalidOperationException("Cannot change password for system user (ID 0)");

        var dbUser = await Context.QueryFirstOrDefaultAsync<RedbUser>(Sql.Users_SelectById(), new object[] { user.Id }, cancellationToken);
        if (dbUser == null) throw new InvalidOperationException($"User with ID {user.Id} not found");
        if (!dbUser.Enabled) throw new InvalidOperationException("Cannot change password for disabled user");

        // Wrong current password is the most common user error on a "change password" form, and the
        // method is Task<bool> documented as "true if changed" — so it returns false here, it does not
        // throw. (Programmer/precondition errors above — null args, disabled/system user — still throw.)
        if (!PasswordHasher.VerifyPassword(currentPassword, dbUser.Password))
            return false;

        if (PasswordHasher.VerifyPassword(newPassword, dbUser.Password))
            throw new ArgumentException("New password must be different from current");

        var hashedPassword = PasswordHasher.HashPassword(newPassword);
        await Context.ExecuteAsync(Sql.Users_UpdatePassword(), new object[] { hashedPassword, user.Id }, cancellationToken);

        return true;
    }

    /// <inheritdoc />
    public virtual async Task<bool> SetPasswordAsync(IRedbUser user, string newPassword, IRedbUser? currentUser = null, CancellationToken cancellationToken = default)
    {
        if (user.Id == 0)
            throw new InvalidOperationException("System user password (ID 0) cannot be changed");

        if (currentUser != null && currentUser.Id != 1 && currentUser.Id != user.Id)
            throw new UnauthorizedAccessException("Only admin can set passwords for other users");

        if (string.IsNullOrWhiteSpace(newPassword) || newPassword.Length < 4)
            throw new ArgumentException("Password must be at least 4 characters");

        var exists = await Context.ExecuteScalarAsync<long?>(Sql.Users_ExistsById(), new object[] { user.Id }, cancellationToken);
        if (!exists.HasValue)
            throw new ArgumentException($"User with ID {user.Id} not found");

        var hashedPassword = PasswordHasher.HashPassword(newPassword);
        var result = await Context.ExecuteAsync(Sql.Users_UpdatePassword(), new object[] { hashedPassword, user.Id }, cancellationToken);
        return result > 0;
    }

    // ============================================================
    // === STATUS MANAGEMENT ===
    // ============================================================

    /// <inheritdoc />
    public virtual async Task<bool> EnableUserAsync(IRedbUser user, IRedbUser? currentUser = null, CancellationToken cancellationToken = default)
    {
        if (user.Id == 0 || user.Id == -1)
            return true;

        var result = await Context.ExecuteAsync(Sql.Users_UpdateStatus(), new object[] { true, DBNull.Value, user.Id }, cancellationToken);
        return result >= 0;
    }

    /// <inheritdoc />
    public virtual async Task<bool> DisableUserAsync(IRedbUser user, IRedbUser? currentUser = null, CancellationToken cancellationToken = default)
    {
        if (user.Id == 0 || user.Id == -1)
            throw new InvalidOperationException($"System user with ID {user.Id} cannot be disabled");

        if (currentUser != null && currentUser.Id == user.Id)
            throw new InvalidOperationException("User cannot disable themselves");

        var result = await Context.ExecuteAsync(Sql.Users_UpdateStatus(), new object[] { false, DateTimeOffset.UtcNow, user.Id }, cancellationToken);
        return result >= 0;
    }

    // ============================================================
    // === VALIDATION ===
    // ============================================================

    /// <inheritdoc />
    public virtual async Task<UserValidationResult> ValidateUserDataAsync(CreateUserRequest request, CancellationToken cancellationToken = default)
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

            var emailExists = await Context.ExecuteScalarAsync<long?>(Sql.Users_ExistsByEmail(), new object[] { request.Email }, cancellationToken);
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
    public virtual async Task<bool> IsLoginAvailableAsync(string login, long? excludeUserId = null, CancellationToken cancellationToken = default)
    {
        if (excludeUserId.HasValue)
        {
            var exists = await Context.ExecuteScalarAsync<long?>(Sql.Users_ExistsByLoginExcluding(), new object[] { login, excludeUserId.Value }, cancellationToken);
            return !exists.HasValue;
        }
        else
        {
            var exists = await Context.ExecuteScalarAsync<long?>(Sql.Users_ExistsByLogin(), new object[] { login }, cancellationToken);
            return !exists.HasValue;
        }
    }

    // ============================================================
    // === STATISTICS ===
    // ============================================================

    /// <inheritdoc />
    public virtual async Task<int> GetUserCountAsync(bool includeDisabled = false, CancellationToken cancellationToken = default)
    {
        return await Context.ExecuteScalarAsync<int>(includeDisabled ? Sql.Users_Count() : Sql.Users_CountEnabled(), System.Array.Empty<object>(), cancellationToken);
    }

    /// <inheritdoc />
    public virtual Task<int> GetActiveUserCountAsync(DateTimeOffset fromDate, DateTimeOffset toDate, CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException("GetActiveUserCountAsync requires activity logging");
    }

    // ============================================================
    // === CONFIGURATION MANAGEMENT ===
    // ============================================================

    /// <inheritdoc />
    public virtual async Task<long?> GetUserConfigurationIdAsync(long userId, CancellationToken cancellationToken = default)
    {
        return await Context.ExecuteScalarAsync<long?>(Sql.Users_SelectConfigurationId(), new object[] { userId }, cancellationToken);
    }

    /// <inheritdoc />
    public virtual async Task SetUserConfigurationAsync(long userId, long? configId, CancellationToken cancellationToken = default)
    {
        var result = await Context.ExecuteAsync(Sql.Users_UpdateConfiguration(), new object[] { (object?)configId ?? DBNull.Value, userId }, cancellationToken);
        if (result == 0)
            throw new ArgumentException($"User with ID {userId} not found");
    }

    /// <inheritdoc />
    public virtual async Task<List<IRedbRole>> GetUserRolesAsync(long userId, CancellationToken cancellationToken = default)
    {
        var roles = await Context.QueryAsync<RedbRole>(Sql.UsersRoles_SelectRolesByUser(), new object[] { userId }, cancellationToken);
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

    private async Task AssignRolesByNamesAsync(long userId, string[] roleNames, CancellationToken cancellationToken = default)
    {
        foreach (var roleName in roleNames)
        {
            var role = await Context.QueryFirstOrDefaultAsync<RedbRole>(Sql.Roles_SelectIdByName(), new object[] { roleName }, cancellationToken);
            if (role == null)
                throw new ArgumentException($"Role '{roleName}' not found");

            var userRoleId = await Context.NextObjectIdAsync();
            await Context.ExecuteAsync(Sql.UsersRoles_Insert(), new object[] { userRoleId, userId, role.Id }, cancellationToken);
        }
    }

    private async Task AssignRolesByObjectsAsync(long userId, IRedbRole[] roles, CancellationToken cancellationToken = default)
    {
        foreach (var role in roles)
        {
            var exists = await Context.ExecuteScalarAsync<long?>(Sql.Roles_ExistsById(), new object[] { role.Id }, cancellationToken);
            if (!exists.HasValue)
                throw new ArgumentException($"Role with ID {role.Id} ('{role.Name}') not found");

            var userRoleId = await Context.NextObjectIdAsync();
            await Context.ExecuteAsync(Sql.UsersRoles_Insert(), new object[] { userRoleId, userId, role.Id }, cancellationToken);
        }
    }

    // ============================================================
    // === LIFECYCLE HOOKS ===
    // ============================================================

    protected virtual Task OnUserCreatedAsync(IRedbUser user, IRedbUser? currentUser) => Task.CompletedTask;
    protected virtual Task OnUserUpdatedAsync(IRedbUser user, IRedbUser? currentUser) => Task.CompletedTask;
    protected virtual Task OnUserDeletedAsync(IRedbUser user, IRedbUser? currentUser) => Task.CompletedTask;
}

