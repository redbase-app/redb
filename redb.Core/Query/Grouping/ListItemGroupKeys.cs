using System.Reflection;
using System.Runtime.CompilerServices;
using redb.Core.Models.Entities;
using redb.Core.Providers;

namespace redb.Core.Query.Grouping;

/// <summary>
/// A query provider that hands the list provider of its service to the groupings it runs. Set by
/// <see cref="Providers.Base.QueryableProviderBase"/> on every query it creates.
/// </summary>
internal interface IListItemKeySource
{
    IListProvider? ListItems { get; set; }
}

/// <summary>
/// The key of a grouping by a list item (<c>GroupBy(p => p.Status)</c>) is the item a loaded object carries: Id,
/// IdList, Value, Alias, IdObject. Every engine groups by the item id and returns only that id, which becomes a stub
/// item; after materializing, the stubs of a result are made the items of their ids, read in one batch through the
/// list provider. The SQL and its preview stay the same on every engine; the extra read happens only when a result
/// carries a list item. An id that names no item any more leaves its stub with the id alone.
/// </summary>
internal static class ListItemGroupKeys
{
    private const int MaxDepth = 3;

    public static async Task FillAsync<TResult>(List<TResult> rows, IListProvider? lists, string api, CancellationToken cancellationToken)
    {
        if (rows.Count == 0 || !MayCarryListItem(typeof(TResult), 0))
            return;

        var stubs = new List<RedbListItem>();
        foreach (var row in rows)
            Collect(row, stubs, 0);
        if (stubs.Count == 0)
            return;

        if (lists == null)
            throw new InvalidOperationException(
                $"{api}: the key of this grouping is a list item, and the query has no list provider to read the items with.");

        var items = (await lists.GetListItemsByIdsAsync(stubs.Select(s => s.Id), cancellationToken)).ToDictionary(i => i.Id);
        foreach (var stub in stubs)
            if (items.TryGetValue(stub.Id, out var item))
                stub.TakeFrom(item);
    }

    private static bool MayCarryListItem(Type type, int depth)
    {
        if (type == typeof(RedbListItem)) return true;
        if (depth >= MaxDepth || !IsComposite(type)) return false;
        return type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Any(p => p.GetIndexParameters().Length == 0 && MayCarryListItem(p.PropertyType, depth + 1));
    }

    private static void Collect(object? value, List<RedbListItem> stubs, int depth)
    {
        switch (value)
        {
            case null:
                return;
            case RedbListItem item:
                if (item.Id != 0) stubs.Add(item);
                return;
        }
        var type = value.GetType();
        if (depth >= MaxDepth || !IsComposite(type))
            return;
        foreach (var p in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
            if (p.GetIndexParameters().Length == 0 && MayCarryListItem(p.PropertyType, depth + 1))
                Collect(p.GetValue(value), stubs, depth + 1);
    }

    // A result row or a composite key: an anonymous type, a record or class the selector builds. Strings, values,
    // collections and the model types of redb are not walked.
    private static bool IsComposite(Type type)
        => type.IsClass && type != typeof(string) && !typeof(System.Collections.IEnumerable).IsAssignableFrom(type)
           && (type.IsDefined(typeof(CompilerGeneratedAttribute), false) || type.Namespace?.StartsWith("redb.Core") != true);
}
