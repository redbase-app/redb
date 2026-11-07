using redb.Core.Attributes;

namespace redb.Tests.Integration.Models;

// Props types dedicated to the scheme-naming suite. Every test drives the sync itself and drops the
// schemes it touches first, so each scenario starts from a known state regardless of which provider
// (or which of Free/Pro) ran before it.
//
// NOTE: no type here may declare an INVALID explicit name, and no two may declare the SAME one.
// InitializeAsync auto-syncs every [RedbScheme] type in every loaded assembly and rethrows
// (RedbServiceBase.AutoSyncSchemesAsync), so a single bad name would break fixture start-up for the
// whole test project. That is the intended product behaviour — a bad name must not boot. The
// negative cases are therefore covered by SchemeNameValidatorTests against the validator directly.

/// <summary>Baseline: no attribute name — the scheme must keep living by FullName. Scenario A/B.</summary>
[RedbScheme("Именование: по типу")]
public class NamingByTypeProps
{
    public string Title { get; set; } = "";
}

/// <summary>Explicit name, no alias. Scenarios C, D, E, K, L, N, P.</summary>
[RedbScheme(Name = "naming.pinned")]
public class NamingPinnedProps
{
    public string Title { get; set; } = "";
}

/// <summary>Explicit name together with a human-readable alias. Scenario M.</summary>
[RedbScheme("Именование: имя и алиас", Name = "naming.with_alias")]
public class NamingWithAliasProps
{
    public string Title { get; set; } = "";
}

/// <summary>Explicit name used to stage the split-brain scenario F.</summary>
[RedbScheme(Name = "naming.split")]
public class NamingSplitProps
{
    public string Title { get; set; } = "";
}

/// <summary>Alias-only, mutated across scenarios O/Q/R by editing the alias behind the library's back.</summary>
[RedbScheme("Именование: алиас исходный")]
public class NamingAliasProps
{
    public string Title { get; set; } = "";
}

/// <summary>Explicit name used by the concurrent-creation scenarios S and T.</summary>
[RedbScheme(Name = "naming.race")]
public class NamingRaceProps
{
    public string Title { get; set; } = "";
}
