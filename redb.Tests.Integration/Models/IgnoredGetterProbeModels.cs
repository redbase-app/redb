using System.Collections.Concurrent;
using redb.Core.Attributes;
using redb.Core.Models.Entities;

namespace redb.Tests.Integration.Models;

/// <summary>
/// Props with references and two <c>[RedbIgnore]</c> properties whose getters count their reads per object label, so
/// suites running in parallel on several hosts never share a count. One property has a setter: the Pro reference
/// substitution walks read-write properties only.
/// </summary>
public class IgnoredGetterProbeProps
{
    public static readonly ConcurrentDictionary<string, int> TechnicalReads = new();

    private string? _settable;

    public string? Label { get; set; }

    public List<RedbObject<CtProbeChildProps>>? Refs { get; set; }

    [RedbIgnore]
    public string Technical
    {
        get { CountRead(); return "technical"; }
    }

    [RedbIgnore]
    public string? TechnicalSettable
    {
        get { CountRead(); return _settable; }
        set => _settable = value;
    }

    private void CountRead() => TechnicalReads.AddOrUpdate(Label ?? "", 1, (_, n) => n + 1);
}
