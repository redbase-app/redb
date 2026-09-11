using System;

namespace redb.Core.Exceptions;

/// <summary>
/// Thrown by scheme synchronisation when <c>[RedbUnique]</c> sits where it cannot be enforced: on a
/// property inside an array, a dictionary or a nested class, on a reference, a list item, an enum, a
/// collection, or on a type with no canonical form. Rejected at synchronisation, not at insert —
/// otherwise the behaviour would degrade into an index that silently never fires.
///
/// <para>
/// Also thrown by <c>GetByUniqueAsync</c> when asked for a property that is not a unique key.
/// </para>
/// </summary>
public class RedbUniqueKeyDefinitionException : Exception
{
    /// <summary>CLR type or scheme the property belongs to.</summary>
    public string SchemeName { get; }

    /// <summary>The offending property.</summary>
    public string PropertyName { get; }

    /// <summary>Why it cannot be a unique key.</summary>
    public string Reason { get; }

    public RedbUniqueKeyDefinitionException(string schemeName, string propertyName, string reason)
        : base($"[RedbUnique] on '{schemeName}.{propertyName}' cannot be enforced: {reason}. " +
               "A unique key must be a scalar property — string, integer, Guid, bool, double, " +
               "decimal, date/time or byte[] — of the Props class or of a nested class reached " +
               "without crossing a collection.")
    {
        SchemeName = schemeName;
        PropertyName = propertyName;
        Reason = reason;
    }
}
