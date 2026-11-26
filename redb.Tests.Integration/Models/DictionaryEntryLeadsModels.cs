using redb.Core.Attributes;

namespace redb.Tests.Integration.Models;

/// <summary>
/// Dictionaries whose entries are filtered by their value: a tuple key with a text value, a text key with a number,
/// and a text key with a class. Every other key holds the same values, so an answer that lost the key shows at once.
/// </summary>
[RedbScheme("dict_entry_leads")]
public class DictionaryEntryLeadsProps
{
    public long Rank { get; set; }
    public string? Tag { get; set; }
    public Dictionary<(int Year, string Quarter), string>? Reviews { get; set; }
    public Dictionary<string, decimal>? Scores { get; set; }
    public Dictionary<string, DictionaryEntryPlace>? Places { get; set; }
}

public class DictionaryEntryPlace
{
    public string City { get; set; } = "";
    public long Floor { get; set; }
}
