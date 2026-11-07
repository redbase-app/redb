namespace redb.Core.Exceptions;

/// <summary>
/// Thrown when a type resolves to a scheme (by explicit name, FullName, short-name fallback or as the
/// loser of a creation race) that is owned by a different namespace (<c>_schemes._name_space</c>). Adopting it would silently attach
/// one project's type to another project's data — two unrelated <c>Order</c> classes are the textbook
/// case. Schemes written before namespaces were recorded carry NULL there and are still adopted, with
/// a warning, because a pre-namespace database has no other way in (V4, К7).
/// </summary>
public class RedbSchemeNamespaceMismatchException : Exception
{
    /// <summary>The type that asked for the scheme.</summary>
    public Type DeclaringType { get; }

    /// <summary>The scheme name the type resolved to.</summary>
    public string SchemeName { get; }

    /// <summary>Id of the scheme the type resolved to.</summary>
    public long SchemeId { get; }

    /// <summary>The namespace recorded on the scheme — its owner.</summary>
    public string SchemeNameSpace { get; }

    /// <summary>The namespace of the requesting type (null for the global namespace).</summary>
    public string? TypeNameSpace { get; }

    public RedbSchemeNamespaceMismatchException(
        Type declaringType, string schemeName, long schemeId, string schemeNameSpace, string? typeNameSpace)
        : base($"Type '{declaringType.FullName ?? declaringType.Name}' resolves to scheme id={schemeId} " +
               $"named '{schemeName}', but that scheme belongs to namespace " +
               $"'{schemeNameSpace}', not '{typeNameSpace ?? "<global>"}'. Adopting it would attach this " +
               "type to another project's data (a scheme has one owner). If the scheme really is this type's - " +
               "the type moved - set _schemes._name_space to the new namespace (or NULL to re-adopt); otherwise " +
               "give one of the two types a different scheme name.")
    {
        DeclaringType = declaringType;
        SchemeName = schemeName;
        SchemeId = schemeId;
        SchemeNameSpace = schemeNameSpace;
        TypeNameSpace = typeNameSpace;
    }
}
