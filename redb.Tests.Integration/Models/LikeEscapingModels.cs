using redb.Core.Attributes;

namespace redb.Tests.Integration.Models;

/// <summary>
/// Probe scheme for the LIKE-metacharacter escaping suite (BR-7, 2026-09-02).
/// Synced by the tests that use it, not by the fixtures.
/// </summary>
[RedbScheme(Name = "LikeProbe")]
public class LikeProbeProps
{
    public string? S { get; set; }
}
