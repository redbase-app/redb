using System;

namespace redb.Core.Exceptions;

/// <summary>
/// A save was rejected because a <c>[RedbUnique]</c> key is already taken by another object of the
/// scheme. Raised in place of the driver's own unique-violation error (PostgreSQL 23505, SQL Server
/// 2601/2627, SQLite 2067) so that callers can catch one type on every provider and read which key
/// collided, rather than match on message text.
///
/// <para>
/// Everything but <see cref="Cause"/> is best-effort: the driver message names the index and, on
/// PostgreSQL and SQL Server, the key tuple — from which the structure and therefore the scheme and
/// property are resolved. SQLite names only the columns. When something could not be resolved the
/// property is <c>null</c>; the original exception is always attached.
/// </para>
/// </summary>
public class RedbUniqueViolationException : Exception
{
    /// <summary>Scheme of the object that was being saved, when resolved.</summary>
    public long? SchemeId { get; }

    /// <summary>Scheme name, when resolved.</summary>
    public string? SchemeName { get; }

    /// <summary>Structure (field) whose key collided, when the driver reported the key tuple.</summary>
    public long? StructureId { get; }

    /// <summary>Property backing that structure, when resolved.</summary>
    public string? PropertyName { get; }

    /// <summary>Name of the unique index or constraint the database reported, when it did.</summary>
    public string? ConstraintName { get; }

    /// <summary>The driver's own detail line (PostgreSQL <c>DETAIL</c>, SQL Server message), verbatim.</summary>
    public string? Detail { get; }

    /// <summary>Which unique index rejected the save - the object key (<c>ValueUnique</c>) or a
    /// <c>[RedbUnique]</c> property (BR-8, 2026-09-02). Classified on every provider: PostgreSQL
    /// and MSSQL name the index, SQLite names the columns; both spellings resolve.</summary>
    public RedbUniqueViolationKind Kind { get; }

    /// <summary>The driver exception.</summary>
    public Exception Cause { get; }

    public RedbUniqueViolationException(
        long? schemeId,
        string? schemeName,
        long? structureId,
        string? propertyName,
        string? constraintName,
        string? detail,
        Exception cause,
        RedbUniqueViolationKind kind = RedbUniqueViolationKind.Unknown)
        : base(BuildMessage(schemeId, schemeName, structureId, propertyName, constraintName, detail, kind), cause)
    {
        SchemeId = schemeId;
        SchemeName = schemeName;
        StructureId = structureId;
        PropertyName = propertyName;
        ConstraintName = constraintName;
        Detail = detail;
        Cause = cause;
        Kind = kind;
    }

    private static string BuildMessage(
        long? schemeId, string? schemeName, long? structureId, string? propertyName,
        string? constraintName, string? detail, RedbUniqueViolationKind kind)
    {
        var where = kind == RedbUniqueViolationKind.ObjectKey && propertyName == null
            ? "the object key (ValueUnique)" + (schemeName != null ? $" of scheme '{schemeName}'" : schemeId.HasValue ? $" of scheme id={schemeId}" : "")
            : propertyName != null
            ? $"property '{propertyName}'" + (schemeName != null ? $" of scheme '{schemeName}'" : schemeId.HasValue ? $" of scheme id={schemeId}" : "")
            : structureId.HasValue
                ? $"structure id={structureId}"
                : constraintName != null ? $"index '{constraintName}'" : "a unique key";

        var tail = string.IsNullOrWhiteSpace(detail) ? "" : $" Database says: {detail}";

        return $"Unique key violated on {where}: another object of the scheme already holds this value. " +
               $"Keys are compared on their canonical form (see UniqueKeyEncoder); a soft-deleted object " +
               $"releases its key, a live one does not.{tail}";
    }
}
