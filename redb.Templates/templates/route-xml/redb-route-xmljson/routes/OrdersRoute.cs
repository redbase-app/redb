using System.Xml;
using System.Xml.Linq;
using System.Xml.Schema;
using redb.Route.Core;
using redb.Route.DataFormats.XmlJson;

namespace RouteModule.Routes;

/// <summary>
/// The round trip of routes/orders.route.xml and routes/api.route.xml written in C#, on a timer of its own: an XML
/// order to JSON and straight back to XML, without the HTTP hop. The options the XML gives a named format in
/// context.xml are set on each step here, and the schema is the file of resources/ compiled into this assembly.
/// </summary>
public sealed class OrdersRoute : RouteBuilder
{
    private const string OrdersNs = "urn:example:orders";

    // One compiled schema for the checks and for the format; it is only read.
    private static readonly XmlSchemaSet Schema = LoadSchema();

    protected override void Configure()
    {
        // Every {{cs.orders.period:5000}} ms an order: the even ones have one Item, the odd ones two.
        From("timer://cs-modulectx-orders?period={{cs.orders.period:5000}}")
            .RouteId("cs-modulectx-orders")
            .Description("Turns an XML order into JSON and back, the C# form")
            .SetBody(exchange => Order(Convert.ToInt32(exchange.In.Headers["CamelTimerCounter"])).ToString(SaveOptions.DisableFormatting))
            .ValidateXsd(Schema)
            .MarshalXmlJson(Options)
            .ConvertBody(typeof(string))
            .Log("cs json: ${body}")
            .UnmarshalXmlJson<string>(Options)
            .ValidateXsd(Schema)
            .Log("cs xml: ${body}");
    }

    // The options of <dataFormat name="orders-json"> in context.xml, with the schema already compiled.
    private static void Options(XmlJsonOptions options)
    {
        options.SchemaSet = Schema;
        options.Namespaces = XmlJsonNamespaces.Drop;
        options.Prefixes["o"] = OrdersNs;
    }

    // The same document the XML route writes as text.
    private static XElement Order(int tick)
    {
        XNamespace ns = OrdersNs;
        var items = new XElement(ns + "Items", Item(ns, "A-1", 2, "9.50"));
        if (tick % 2 == 1)
            items.Add(Item(ns, "B-2", 1, "20"));
        return new XElement(ns + "Order", new XAttribute("id", $"cs-order-{tick}"),
            new XElement(ns + "Customer", "ACME"),
            new XElement(ns + "Express", tick % 3 == 0 ? "true" : "false"),
            items);
    }

    private static XElement Item(XNamespace ns, string sku, int qty, string price) =>
        new(ns + "Item", new XElement(ns + "Sku", sku), new XElement(ns + "Qty", qty), new XElement(ns + "Price", price));

    // resources/order.xsd, embedded under its file name (EmbeddedResource in RouteModule.csproj).
    private static XmlSchemaSet LoadSchema()
    {
        using var stream = typeof(OrdersRoute).Assembly.GetManifestResourceStream("order.xsd")
            ?? throw new InvalidOperationException("order.xsd is not embedded in the module assembly.");
        using var reader = XmlReader.Create(stream);
        var set = new XmlSchemaSet();
        set.Add(OrdersNs, reader);
        set.Compile();
        return set;
    }
}
