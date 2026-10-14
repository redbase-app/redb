using System.Security.Cryptography;
using System.Text;

namespace redb.Export.Flat;

/// <summary>
/// Names of the flat tables and columns, and the rule that keeps every identifier within the
/// strictest provider limit.
/// <para>
/// Table names come from the code as they are: the Props class name for a scheme table and the
/// property path (<c>EmployeeProps.HomeAddress.Building</c>) for every table below it. Service
/// tables and service columns keep the redb spelling so they cannot be mistaken for user members.
/// </para>
/// </summary>
public static class FlatNaming
{
    /// <summary>
    /// Identifier limit applied on every provider: the 63 bytes of PostgreSQL, the strictest of the
    /// three, so that a table carries the same name in any target database.
    /// </summary>
    public const int MaxIdentifierBytes = 63;

    /// <summary>Shared header table: one row per object, the flat copy of <c>_objects</c>.</summary>
    public const string ObjectsTable = "objects";

    /// <summary>Flat copy of <c>_lists</c>.</summary>
    public const string ListsTable = "lists";

    /// <summary>Flat copy of <c>_list_items</c>.</summary>
    public const string ListItemsTable = "list_items";

    /// <summary>Primary key column of every flat table.</summary>
    public const string IdColumn = "_id";

    /// <summary>Reference from a nested or collection table to the row that owns it.</summary>
    public const string OwnerColumn = "_owner_id";

    /// <summary>Position of an array element.</summary>
    public const string IndexColumn = "_idx";

    /// <summary>Dictionary key; the prefix of the per-component columns of a tuple key.</summary>
    public const string KeyColumn = "_key";

    /// <summary>Payload of a collection whose elements are not classes.</summary>
    public const string ValueColumn = "_value";

    // "~" plus eight hex digits.
    private const int HashSuffixBytes = 9;

    /// <summary>
    /// Returns <paramref name="fullName"/> unchanged when it fits <see cref="MaxIdentifierBytes"/>;
    /// otherwise cuts it on a character boundary and appends <c>~</c> with eight hex digits of the
    /// SHA-256 of the full name. The result is stable across runs and providers.
    /// </summary>
    public static string Shorten(string fullName)
    {
        ArgumentException.ThrowIfNullOrEmpty(fullName);

        var bytes = Encoding.UTF8.GetBytes(fullName);
        if (bytes.Length <= MaxIdentifierBytes)
            return fullName;

        var budget = MaxIdentifierBytes - HashSuffixBytes;
        var kept = new StringBuilder();
        var used = 0;
        foreach (var rune in fullName.EnumerateRunes())
        {
            if (used + rune.Utf8SequenceLength > budget)
                break;
            kept.Append(rune.ToString());
            used += rune.Utf8SequenceLength;
        }

        var hash = Convert.ToHexString(SHA256.HashData(bytes), 0, 4).ToLowerInvariant();
        return $"{kept}~{hash}";
    }

    /// <summary>Full (unshortened) name of the table that holds <paramref name="propertyName"/> of <paramref name="ownerFullName"/>.</summary>
    public static string ChildTable(string ownerFullName, string propertyName) => $"{ownerFullName}.{propertyName}";
}
