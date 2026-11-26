using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace redb.Core.Utils;

/// <summary>
/// The order in which a dictionary read from the database lists its entries: by key, the same on every provider and
/// in every read. A dictionary is stored as an unordered set (its hash ignores the order of entries), and a read used
/// to list them in the order of its rows (Pro) or of the JSON the free modules built (by key length on PostgreSQL, by
/// key text on SQL Server and SQLite), so the order moved between providers and, in Pro, between reads.
/// Simple keys sort by their value: strings ordinally, numbers, dates, times, Guids and enums as values, byte arrays
/// by their bytes. Composite keys (value tuples, classes) sort by the text they are stored under.
/// </summary>
public static class RedbDictionaryOrder
{
    private static readonly ConditionalWeakTable<Type, IComparer<object>> ByKeyType = new();

    /// <summary>The comparer of keys of type <paramref name="keyType"/>.</summary>
    public static IComparer<object> For(Type keyType) => ByKeyType.GetValue(keyType, Create);

    /// <summary>The entries in the order a read lists them.</summary>
    public static List<KeyValuePair<TKey, TValue>> Sort<TKey, TValue>(List<KeyValuePair<TKey, TValue>> entries)
        where TKey : notnull
    {
        var comparer = For(typeof(TKey));
        entries.Sort((a, b) => comparer.Compare(a.Key, b.Key));
        return entries;
    }

    private static IComparer<object> Create(Type keyType)
    {
        if (keyType == typeof(string))
            return Comparer<object>.Create(static (a, b) => string.CompareOrdinal((string)a, (string)b));
        if (keyType == typeof(byte[]))
            return Comparer<object>.Create(static (a, b) => ((byte[])a).AsSpan().SequenceCompareTo((byte[])b));
        if (!RedbKeySerializer.IsComplexKey(keyType) && typeof(IComparable).IsAssignableFrom(keyType))
            return Comparer<object>.Create(static (a, b) => ((IComparable)a).CompareTo(b));
        return Comparer<object>.Create((a, b) => string.CompareOrdinal(
            RedbKeySerializer.SerializeObject(a, keyType), RedbKeySerializer.SerializeObject(b, keyType)));
    }
}
