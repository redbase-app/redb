using System.Xml.Linq;
using Microsoft.Extensions.Logging;
using redb.Route.Core;
using redb.Route.Validation;

namespace RouteModule.Routes;

/// <summary>
/// The route of routes/orders.route.xml written in C#, on a timer of its own: the same schema check, the same
/// stylesheet and the same rejected path. The order document is built with LINQ to XML, and the schema and
/// stylesheet are the files of resources/ compiled into this assembly.
/// </summary>
public sealed class OrdersRoute : RouteBuilder
{
    protected override void Configure()
    {
        // A document that does not match the schema: the exchange ends, the reason goes to the rejected route.
        OnException<ValidationException>()
            .Handled()
            .SetHeader("rejectReason", Expr("${exception.message}"))
            .To("direct://cs-modulectx-rejected");

        // Every {{cs.orders.period:5000}} ms a test document; every fourth one has a line with qty 0.
        From("timer://cs-modulectx-orders?period={{cs.orders.period:5000}}")
            .RouteId("cs-modulectx-orders")
            .Description("Checks an XML order against the XSD and turns it into an invoice, the C# form")
            .SetBody(exchange => Order(Convert.ToInt32(exchange.In.Headers["CamelTimerCounter"])).ToString(SaveOptions.DisableFormatting))
            .ValidateXsd(Resource("order.xsd"))
            .XsltContent(Resource("order-to-invoice.xslt"))
            .Log("invoice: ${body}");

        From("direct://cs-modulectx-rejected")
            .RouteId("cs-modulectx-rejected")
            .Description("Logs the documents the schema refused, the C# form")
            .Log("order rejected: ${header.rejectReason}", LogLevel.Warning);
    }

    // The same document as the XML route writes as text.
    private static XElement Order(int tick) =>
        new("order", new XAttribute("id", $"cs-order-{tick}"),
            new XElement("customer", "ACME"),
            new XElement("line", new XAttribute("sku", "A-1"), new XAttribute("qty", tick % 4 == 3 ? 0 : 2), new XAttribute("price", "9.50")),
            new XElement("line", new XAttribute("sku", "B-2"), new XAttribute("qty", 1), new XAttribute("price", "20")));

    // A file of resources/, embedded under its file name (EmbeddedResource in RouteModule.csproj).
    private static string Resource(string name)
    {
        using var stream = typeof(OrdersRoute).Assembly.GetManifestResourceStream(name)
            ?? throw new InvalidOperationException($"{name} is not embedded in the module assembly.");
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }
}
