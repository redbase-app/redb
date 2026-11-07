using redb.Core.Attributes;

namespace redb.Tests.Integration.Models;

/// <summary>
/// The free-form _tags marker (V4): [RedbTags] on the class writes the scheme's marker, on a
/// property - the structure's; a column WITHOUT the attribute is never touched by sync, so
/// direct writes by applications and extensions survive.
/// </summary>
[RedbScheme(Name = "TagsProbe")]
[RedbTags("scheme-level,reserved")]
public class TagsProbeProps
{
    [RedbTags("prop-level")]
    public string? Marked { get; set; }

    public string? Plain { get; set; }
}
