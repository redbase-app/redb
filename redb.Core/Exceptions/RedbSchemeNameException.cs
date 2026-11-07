namespace redb.Core.Exceptions;

/// <summary>
/// Thrown when a type declares an explicit scheme name (<c>[RedbScheme(Name = "...")]</c>) that
/// breaks the scheme naming rules. Raised before any SQL is issued, so the developer gets the
/// offending type and the broken rule instead of a raw constraint violation from the driver.
/// </summary>
public class RedbSchemeNameException : Exception
{
    /// <summary>The class that declared the invalid name.</summary>
    public Type DeclaringType { get; }

    /// <summary>The rejected name, as written in the attribute.</summary>
    public string? SchemeName { get; }

    public RedbSchemeNameException(Type declaringType, string? schemeName, string reason)
        : base($"Type '{declaringType.FullName ?? declaringType.Name}' declares " +
               $"[RedbScheme(Name = \"{schemeName}\")], which is not a valid scheme name: {reason}. " +
               "Scheme names follow C# identifier rules (Latin letters, digits, '_', '.' and '+'); " +
               "use Alias for human-readable titles.")
    {
        DeclaringType = declaringType;
        SchemeName = schemeName;
    }
}
