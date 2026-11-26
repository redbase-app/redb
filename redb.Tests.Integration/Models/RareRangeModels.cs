using redb.Core.Attributes;

namespace redb.Tests.Integration.Models;

/// <summary>
/// An open range on one field next to a page sorted by another: Score spread so that Score &lt; x keeps 12, about 900,
/// about 1 500 or about 2 900 of 3 000 objects; some objects lack the range value, some the sort value.
/// </summary>
[RedbScheme("rare_range_pages")]
public class RareRangeProps
{
    public long? Score { get; set; }
    public decimal? Salary { get; set; }
    public DateTime? When { get; set; }
}
