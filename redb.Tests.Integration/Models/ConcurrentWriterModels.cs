using System;
using redb.Core.Attributes;

namespace redb.Tests.Integration.Models;

/// <summary>
/// A row written by many concurrent writers at once: one new object with a few root values, the shape of a key ring
/// element or a replay reservation. Synced by the tests that use it.
/// </summary>
[RedbScheme(Name = "ConcurrentWriterRow")]
public class ConcurrentWriterRowProps
{
    public string? Tag { get; set; }
    public string? Key { get; set; }
    public string? Payload { get; set; }
    public DateTimeOffset? WrittenAt { get; set; }
}
