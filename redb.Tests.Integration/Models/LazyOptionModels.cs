using redb.Core.Attributes;
using redb.Core.Models.Entities;

namespace redb.Tests.Integration.Models;

/// <summary>
/// V4 Л2 (LAZY_REFERENCES_PLAN §3.2–3.3): the `virtual` marker under test. <see cref="Next"/> and
/// <see cref="Children"/> are virtual references — with the option on the builders emit stubs for
/// them regardless of depth; <see cref="Plain"/> is deliberately NOT virtual and always loads
/// eagerly. Self-referencing, so one scheme covers chains and collections.
/// </summary>
[RedbScheme]
public class LazyOptionNodeProps
{
    public string? Label { get; set; }

    /// <summary>Virtual reference — lazy when the option is on.</summary>
    public virtual RedbObject<LazyOptionNodeProps>? Next { get; set; }

    /// <summary>NOT virtual — always eager, whatever the option says.</summary>
    public RedbObject<LazyOptionNodeProps>? Plain { get; set; }

    /// <summary>Virtual collection of references — a list of stubs when the option is on.</summary>
    public virtual List<RedbObject<LazyOptionNodeProps>>? Children { get; set; }
}
