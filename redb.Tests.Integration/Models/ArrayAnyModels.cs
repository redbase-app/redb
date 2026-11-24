using redb.Core.Attributes;

namespace redb.Tests.Integration.Models;

/// <summary>A list of texts next to the arrays of <see cref="ArrayValuesProps"/>, for Any() over a List&lt;T&gt;.</summary>
[RedbScheme("TestArrayAny")]
public class ArrayAnyProps
{
    public string Marker { get; set; } = string.Empty;
    public List<string>? Labels { get; set; }
    public List<long>? Numbers { get; set; }
}
