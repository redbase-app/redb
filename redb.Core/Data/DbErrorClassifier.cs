using redb.Core.Exceptions;

namespace redb.Core.Data;

/// <summary>
/// Classifies provider exceptions by their native error code without referencing the drivers.
///
/// <para>
/// One place for every duck-typed code check. Before this type there were two independent copies of
/// the same reflection walk — <c>DeadlockRetryHelper.IsDeadlock</c> and
/// <c>RedbServiceBase.IsUndefinedFunctionError</c> — and V4 needed a third (insufficient privilege)
/// and a fourth (unique violation). Four copies of "walk the inner-exception chain, read
/// <c>SqlState</c> or <c>Number</c> by reflection" is how one of them ends up subtly different.
/// </para>
///
/// <para>
/// Reflection rather than a driver reference on purpose: <c>redb.Core</c> must not depend on Npgsql,
/// Microsoft.Data.SqlClient or Microsoft.Data.Sqlite. The property names (<c>SqlState</c>,
/// <c>Number</c>, <c>SqliteErrorCode</c>, <c>SqliteExtendedErrorCode</c>) are the drivers' public
/// API and have been stable for years.
/// </para>
/// </summary>
public static class DbErrorClassifier
{
    /// <summary>SQL Server 1205, PostgreSQL 40P01. The retry helper's condition.</summary>
    public static bool IsDeadlock(Exception ex) =>
        Any(ex, pgState: s => s == "40P01", mssqlNumber: n => n == 1205, sqlite: null);

    /// <summary>
    /// PostgreSQL 40001 (serialization_failure), SQL Server 3960/3961 (snapshot update conflict).
    /// BR-1: under elevated isolation the database aborts the loser; the WHOLE transaction must be
    /// retried by its owner - retrying one statement inside the aborted transaction is useless.
    /// SQLite never raises it (a single serial writer). Exposed so callers classify without
    /// provider-specific code.
    /// </summary>
    public static bool IsSerializationFailure(Exception ex) =>
        Any(ex, pgState: s => s == "40001", mssqlNumber: n => n == 3960 || n == 3961, sqlite: null);

    /// <summary>
    /// The function or procedure named in the statement does not exist: PostgreSQL 42883, SQL Server
    /// 195 ("not a recognized built-in function name") or 4121 ("Cannot find either column ... or
    /// the user-defined function"). Used to recognise a database that has never had the PVT module.
    /// </summary>
    public static bool IsUndefinedFunction(Exception ex) =>
        Any(ex, pgState: s => s == "42883", mssqlNumber: n => n is 195 or 4121, sqlite: null);

    /// <summary>
    /// The connected role is not allowed to do what the statement does: PostgreSQL 42501
    /// (<c>insufficient_privilege</c>), SQL Server 229 (permission denied on object), 262 (permission
    /// denied for CREATE/ALTER), 297 (user does not have permission), 3701 ("cannot drop ... because
    /// it does not exist or you do not have permission" — the module redeploy drops functions it has
    /// just read the version from, so in that context the second reading is the only one possible),
    /// 15247 (permission denied on server). This is what an application without owner rights gets
    /// when it tries to apply a schema upgrade — and what turns into
    /// <see cref="Exceptions.RedbSchemaOutdatedException"/> instead of a raw driver error.
    /// </summary>
    public static bool IsInsufficientPrivilege(Exception ex) =>
        Any(ex, pgState: s => s == "42501", mssqlNumber: n => n is 229 or 262 or 297 or 3701 or 15247, sqlite: null);

    /// <summary>
    /// A unique index or constraint rejected the row: PostgreSQL 23505, SQL Server 2601 (unique
    /// index) or 2627 (unique constraint), SQLite extended code 2067 (SQLITE_CONSTRAINT_UNIQUE).
    /// </summary>
    public static bool IsUniqueViolation(Exception ex) =>
        Any(ex, pgState: s => s == "23505", mssqlNumber: n => n is 2601 or 2627, sqlite: c => c == 2067);

    /// <summary>
    /// What the driver said about a unique violation: the constraint or index name, its own detail
    /// line, and — when the message carries the key tuple of a _values index — the structure id
    /// (the first component of UIX__values__structure_unique). All best-effort: SQLite names only
    /// the columns, and a message format is not a contract.
    /// </summary>
    public static (string? ConstraintName, string? Detail, long? StructureId, RedbUniqueViolationKind Kind) DescribeUniqueViolation(Exception ex)
    {
        for (var cur = ex; cur != null; cur = cur.InnerException)
        {
            var type = cur.GetType();
            switch (type.Name)
            {
                case "PostgresException":
                {
                    var constraint = type.GetProperty("ConstraintName")?.GetValue(cur) as string;
                    var detail = type.GetProperty("Detail")?.GetValue(cur) as string;
                    long? structureId = null;
                    if (constraint != null && constraint.Contains("values") && detail != null)
                    {
                        var m = System.Text.RegularExpressions.Regex.Match(detail, @"=\((\d+),");
                        if (m.Success && long.TryParse(m.Groups[1].Value, out var id)) structureId = id;
                    }
                    return (constraint, detail, structureId, ClassifyUniqueIndex(constraint));
                }

                case "SqlException":
                {
                    var message = cur.Message;
                    var cm = System.Text.RegularExpressions.Regex.Match(message, @"(?:index|constraint) '([^']+)'");
                    var constraint = cm.Success ? cm.Groups[1].Value : null;
                    long? structureId = null;
                    if (constraint != null && constraint.Contains("values"))
                    {
                        var m = System.Text.RegularExpressions.Regex.Match(message, @"duplicate key value is \((\d+),");
                        if (m.Success && long.TryParse(m.Groups[1].Value, out var id)) structureId = id;
                    }
                    return (constraint, message, structureId, ClassifyUniqueIndex(constraint));
                }

                case "SqliteException":
                    // "UNIQUE constraint failed: _values._id_structure, _values._unique" - columns only:
                    // no key tuple, so no structure id - but the columns still classify the index.
                    return (cur.Message, cur.Message, null, ClassifyUniqueIndex(cur.Message));
            }
        }

        return (null, null, null, RedbUniqueViolationKind.Unknown);
    }

    /// <summary>
    /// Classifies which unique index the driver named (BR-8, 2026-09-02). PostgreSQL and MSSQL
    /// report the index name (UIX__objects__scheme_unique / UIX__values__structure_unique);
    /// SQLite reports column names ("_objects._id_scheme, _objects._value_unique"). Both
    /// spellings match, so callers branch on the enum instead of provider-specific strings.
    /// </summary>
    private static RedbUniqueViolationKind ClassifyUniqueIndex(string? text)
    {
        if (string.IsNullOrEmpty(text)) return RedbUniqueViolationKind.Unknown;
        if (text.Contains("objects", StringComparison.OrdinalIgnoreCase)
            && (text.Contains("scheme_unique", StringComparison.OrdinalIgnoreCase)
                || text.Contains("_value_unique", StringComparison.OrdinalIgnoreCase)))
            return RedbUniqueViolationKind.ObjectKey;
        if (text.Contains("values", StringComparison.OrdinalIgnoreCase)
            && text.Contains("unique", StringComparison.OrdinalIgnoreCase))
            return RedbUniqueViolationKind.Property;
        return RedbUniqueViolationKind.Unknown;
    }

    private static bool Any(
        Exception ex,
        Func<string, bool> pgState,
        Func<int, bool> mssqlNumber,
        Func<int, bool>? sqlite)
    {
        for (var cur = ex; cur != null; cur = cur.InnerException)
        {
            var type = cur.GetType();
            switch (type.Name)
            {
                case "PostgresException":
                    if (type.GetProperty("SqlState")?.GetValue(cur) is string state && pgState(state))
                        return true;
                    break;

                case "SqlException":
                    if (type.GetProperty("Number")?.GetValue(cur) is int number && mssqlNumber(number))
                        return true;
                    break;

                case "SqliteException":
                    if (sqlite is null)
                        break;
                    // The extended code carries the CONSTRAINT sub-kind; the plain code is the family.
                    if (type.GetProperty("SqliteExtendedErrorCode")?.GetValue(cur) is int ext && sqlite(ext))
                        return true;
                    if (type.GetProperty("SqliteErrorCode")?.GetValue(cur) is int code && sqlite(code))
                        return true;
                    break;
            }
        }

        return false;
    }
}
