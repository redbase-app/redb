using System;
using System.Collections.Generic;
using redb.Core.Attributes;

namespace redb.Tests.Integration.Models;

/// <summary>
/// Dictionaries, arrays and lists of nullable value types, each with a null among the values, and a dictionary of a
/// class whose nullable field is null in one entry: everything must read back as saved, the nulls in place.
/// </summary>
[RedbScheme("TestNullableCollections")]
public class NullableCollectionsProps
{
    public string Tag { get; set; } = string.Empty;

    public Dictionary<string, int?>? Ints { get; set; }
    public Dictionary<string, long?>? Longs { get; set; }
    public Dictionary<string, decimal?>? Decimals { get; set; }
    public Dictionary<string, DateTime?>? Dates { get; set; }
    public Dictionary<string, bool?>? Flags { get; set; }
    public Dictionary<string, Guid?>? Guids { get; set; }

    public int?[]? IntArray { get; set; }
    public List<long?>? LongList { get; set; }
    public List<decimal?>? DecimalList { get; set; }
    public bool?[]? FlagArray { get; set; }

    public Dictionary<string, NullableCollectionsEntry>? Entries { get; set; }
}

public class NullableCollectionsEntry
{
    public string Name { get; set; } = string.Empty;
    public int? Score { get; set; }
    public DateTime? At { get; set; }
    public int?[]? Marks { get; set; }
}
