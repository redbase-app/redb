using redb.Core.Attributes;

namespace redb.Examples.Models;

/// <summary>
/// Device with every V4 key shape beyond the plain scalar one (E004):
/// - a key on a scalar INSIDE a nested class (<see cref="DeviceIdentity.Serial"/>);
/// - a key on a whole SUBTREE - the content of a nested class or a collection is the key;
/// - ELEMENT keys on collections, scoped to one collection or to the whole scheme;
/// - a free-form <c>[RedbTags]</c> marker on the scheme and on a structure.
/// </summary>
[RedbTags("examples,e004")]
public class DeviceProps
{
    public string? Title { get; set; }

    /// <summary>Nested class whose Serial is a key: unique per scheme, like a root key.</summary>
    public DeviceIdentity? Identity { get; set; }

    /// <summary>Subtree key: two devices cannot carry the same configuration content.</summary>
    [RedbUnique]
    public DeviceConfig? Config { get; set; }

    /// <summary>Subtree key on a collection: the ordered list of ports is the key.</summary>
    [RedbUnique]
    public List<long>? Ports { get; set; }

    /// <summary>Element keys, Collection scope: no duplicate label inside ONE device.</summary>
    [RedbUnique(Scope = UniqueScope.Collection)]
    public List<string>? Labels { get; set; }

    /// <summary>Element keys, Scheme scope: a MAC address belongs to one device in the whole scheme.</summary>
    [RedbUnique(Scope = UniqueScope.Scheme)]
    [RedbTags("network")]
    public List<string>? MacAddresses { get; set; }
}

public class DeviceIdentity
{
    [RedbUnique]
    public string? Serial { get; set; }

    public string? Vendor { get; set; }
}

public class DeviceConfig
{
    public string? Region { get; set; }
    public long? Tier { get; set; }
}
