using redb.Core.Attributes;

namespace redb.Tests.Integration.Models;

/// <summary>
/// A scheme only PostgresProPvtPrefilterEquivalenceTests.AllOfATextField_ProbeThenCounts_SameAnswers saves into: more objects
/// than the counterexample probe of AllAsync reads (1 000), so its counts decide.
/// </summary>
[RedbScheme("AllTextProbe")]
public class AllTextProbeProps
{
    public string Name { get; set; } = string.Empty;
    public string? Note { get; set; }
}
