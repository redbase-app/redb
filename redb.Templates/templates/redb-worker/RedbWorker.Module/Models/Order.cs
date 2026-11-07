using redb.Core.Attributes;

namespace RedbWorker.Module.Models;

/// <summary>An accepted order, stored in redb. The order id is also the object's unique key.</summary>
[RedbScheme("Order")]
public class Order
{
    public string OrderId { get; set; } = "";

    public string Customer { get; set; } = "";

    public decimal Amount { get; set; }

    public string Currency { get; set; } = "";

    /// <summary>The name of the file the order came in.</summary>
    public string SourceFile { get; set; } = "";
}
