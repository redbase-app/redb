using redb.Core.Attributes;

namespace RouteModule.Props;

/// <summary>
/// The props of an order object. The class is the scheme: &lt;syncScheme&gt; in context.xml creates
/// or updates it at context start, the routes save and query objects of it. A new property is a new
/// field of the scheme on the next start; no DDL anywhere.
/// </summary>
[RedbScheme("modulectx order")]
public class OrderProps
{
    /// <summary>Who ordered.</summary>
    public string Customer { get; set; } = string.Empty;

    /// <summary>The order amount; changes on every save of the demo.</summary>
    public decimal Amount { get; set; }

    /// <summary>The order status.</summary>
    public string Status { get; set; } = "new";
}
