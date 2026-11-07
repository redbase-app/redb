using System.Xml.Linq;
using Microsoft.Extensions.Logging;
using redb.Route.As2;
using redb.Route.Core;

namespace RouteModule.Routes;

/// <summary>
/// A sibling of routes/send.route.xml in C#: an XML invoice instead of the X12 test document, sent on a timer of its
/// own under the same partner agreement. The route reads the outcome of the MDN as a typed value.
/// </summary>
public sealed class InvoiceSendRoute : RouteBuilder
{
    protected override void Configure()
    {
        // The partner did not answer, or its MDN did not confirm the transfer: sent again twice, half a minute apart.
        OnException<Exception>()
            .MaximumRedeliveries(2)
            .RedeliveryDelay(TimeSpan.FromSeconds(30))
            .Handled()
            .Log("the partner did not confirm the invoice: ${exception.message}", LogLevel.Error);

        // Every {{as2.invoicePeriod:90000}} ms an invoice, signed and encrypted for the partner like the XML document.
        From("timer://cs-modulectx-as2-invoice?period={{as2.invoicePeriod:90000}}")
            .RouteId("cs-modulectx-as2-invoice")
            .Description("Sends a test XML invoice to the AS2 partner")
            .Process(exchange =>
            {
                var number = Convert.ToInt32(exchange.In.Headers["CamelTimerCounter"]);
                exchange.In.Headers[As2Headers.Subject] = $"invoice {number}";
                // The MIME type of the document inside the signed and encrypted message.
                exchange.In.ContentType = "application/xml";
                exchange.In.Body = new XElement("invoice", new XAttribute("number", $"INV-{number}"),
                    new XElement("line", new XAttribute("sku", "A-1"), new XAttribute("qty", 2), new XAttribute("price", "9.50")),
                    new XElement("total", "19.00")).ToString(SaveOptions.DisableFormatting);
            })
            .To("as2:/send?connectionFactory=partner")
            .Process(exchange =>
            {
                // MdnConfirmed: positive disposition, matching MIC and a valid signature where the agreement asks for one.
                var confirmed = exchange.In.Headers[As2Headers.MdnConfirmed] is true;
                exchange.In.Body = confirmed ? "delivered and confirmed" : "MDN did not confirm";
            })
            .Log("invoice ${header.Message-ID}: ${body}");
    }
}
