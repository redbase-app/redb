using redb.Core.Attributes;

namespace redb.Tests.Integration.Models;

/// <summary>
/// A scheme no test saves an object into: the aggregates over an empty scheme are compared on it
/// (PostgresProPvtPrefilterEquivalenceTests.UnfilteredAggregate_ReadFromTheIndexOfValues_SameAnswers).
/// </summary>
[RedbScheme("UnfilteredAggregateEmpty")]
public class UnfilteredAggregateEmptyProps
{
    public long Number { get; set; }
    public decimal Amount { get; set; }
    public double Ratio { get; set; }
}
