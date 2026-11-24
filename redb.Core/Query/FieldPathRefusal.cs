using System;
using System.Text.RegularExpressions;

namespace redb.Core.Query;

/// <summary>
/// The one refusal of a field path a query cannot read: a filter, a sort, an aggregate, a group key or a window key
/// whose path is not a chain of fields of the queried object, or that a provider cannot read. It is thrown before any
/// statement runs, as <see cref="NotSupportedException"/>, so the query never answers with another field's value.
/// </summary>
public static class FieldPathRefusal
{
    /// <summary>The message of the refusal for the path <paramref name="path"/> (an expression or a field path).</summary>
    public static string Message(string path) =>
        $"A query cannot read '{path}': the path must be a chain of fields of the queried object. " +
        "A filter, a sort and a window key take no array or list element by index and no method call inside the path " +
        "(First(), ElementAt(...)); a sort and a window key take no dictionary entry either. The aggregates of " +
        "redb.Postgres, redb.MSSql and redb.SQLite read no field of an element of an array of classes by index " +
        "(Items[0].Price); redb Pro does. In a filter use Contains(...) for an array of simple values, Any(...) for an " +
        "array of list items, or a dictionary with its key; a value of your own goes on the other side of the comparison.";

    private static readonly Regex FieldOfElementByIndex = new(@"\[\d+\]\.", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// For the providers whose aggregates are compiled by the SQL module in the database: refuses a field of an element
    /// of an array of classes by index (Items[0].Rank). The module reads no row for it and the aggregate came back as 0
    /// or null. A field of a dictionary entry (ByKey[k].Rank) and an element of an array of simple values (Tags[0]) are
    /// read. An entry of a dictionary with an integer key whose value is a class (ById[5].Rank) cannot be told apart
    /// from an element by the path, and is refused as well.
    /// </summary>
    public static void EnsureModuleAggregateCanRead(string? fieldPath)
    {
        if (fieldPath != null && FieldOfElementByIndex.IsMatch(fieldPath))
            throw new NotSupportedException(Message(fieldPath));
    }
}
