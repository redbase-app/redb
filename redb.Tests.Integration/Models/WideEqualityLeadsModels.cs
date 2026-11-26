using redb.Core.Attributes;

namespace redb.Tests.Integration.Models;

/// <summary>
/// An equality that is narrow by its shape on objects where the data makes it wide (Tag == "wide" on 81 % of them) or
/// rare ("rare" on 1 %), next to number and date sort fields, the date absent on some objects.
/// </summary>
[RedbScheme("wide_equality_leads")]
public class WideEqualityLeadsProps
{
    public string Tag { get; set; } = "";
    // The same split as a number: 1 rare, 2 wide, 3 other (SQL Server counts a number on its index of values, not a text).
    public long Bucket { get; set; }
    public long Score { get; set; }
    public DateTime? When { get; set; }
}
