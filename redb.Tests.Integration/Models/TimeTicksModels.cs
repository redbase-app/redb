using redb.Core.Attributes;

namespace redb.Tests.Integration.Models;

/// <summary>TimeSpan and TimeOnly in every shape a props class holds them: plain, nullable, in an array, a list and a dictionary.</summary>
[RedbScheme("TestTimeTicks")]
public class TimeTicksProps
{
    public string Tag { get; set; } = "";
    public TimeSpan Span { get; set; }
    public TimeSpan? MaybeSpan { get; set; }
    public TimeOnly Clock { get; set; }
    public TimeOnly? MaybeClock { get; set; }
    public TimeSpan[]? Spans { get; set; }
    public List<TimeOnly>? Clocks { get; set; }
    public TimeSpan?[]? MaybeSpans { get; set; }
    public Dictionary<string, TimeSpan>? SpanByName { get; set; }
}
