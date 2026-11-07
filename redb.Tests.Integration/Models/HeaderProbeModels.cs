using redb.Core.Attributes;

namespace redb.Tests.Integration.Models;

/// <summary>
/// Props for the props-cache header-freshness pins: a tiny scheme whose objects get their
/// HEADER (name, note, value_unique) changed behind the cache's back by "another node".
/// </summary>
[RedbScheme(Name = "HeaderProbe")]
public class HeaderProbeProps
{
    public string? Title { get; set; }
}
