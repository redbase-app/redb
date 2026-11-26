using redb.Core.Attributes;

namespace redb.Tests.Integration.Models;

/// <summary>
/// Property names that differ from a C# keyword only by case (ref, default, event, string, object). C# keywords are
/// case-sensitive, so these are ordinary property names. PostgreSQL refused them at scheme sync ("Field name is a C#
/// reserved word"), which compared the name in lower case. Every [RedbScheme] type syncs when a fixture starts, so the
/// old refusal stopped the whole PostgreSQL suite.
/// </summary>
[RedbScheme("keyword_names")]
public class KeywordNamesProps
{
    public long Ref { get; set; }
    public string Default { get; set; } = "";
    public DateTime Event { get; set; }
    public string? String { get; set; }
    public decimal Object { get; set; }
}
