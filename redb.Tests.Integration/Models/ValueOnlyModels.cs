using redb.Core.Attributes;

namespace redb.Tests.Integration.Models;

/// <summary>
/// A scheme with no properties at all: the object lives in its base fields (name, value_*, unique key) - flags,
/// counters, simple values in the objects table. Loaded, it carries no Props and no _values rows.
/// </summary>
[RedbScheme]
public class ValueOnlyProps
{
}
