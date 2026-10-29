using System.Xml.Linq;
using Microsoft.Extensions.Logging;
using redb.Route.As4;
using redb.Route.Core;

namespace RouteModule.Routes;

/// <summary>
/// A sibling of routes/send.route.xml in C#: credit notes, sent on a timer of their own to the same partner under the
/// same agreement, with the same reception awareness. The payload carries its MIME type, so the partner gets
/// application/xml instead of application/octet-stream.
/// </summary>
public sealed class CreditNoteSendRoute : RouteBuilder
{
    private static readonly XNamespace Ns = "urn:example:credit-note";

    protected override void Configure()
    {
        // No signed receipt came back: the same message is sent again twice, half a minute apart, then logged.
        OnException(typeof(As4ReceiptException), typeof(HttpRequestException))
            .MaximumRedeliveries(2)
            .RedeliveryDelay(TimeSpan.FromSeconds(30))
            .Handled()
            .Log("no receipt from the partner for ${header.redbAs4.messageId}: ${exception.message}", LogLevel.Error);

        // The partner refused the message with an ebMS error: its answer, not redelivered.
        OnException<As4ErrorSignalException>()
            .Handled()
            .Log("the partner refused ${header.redbAs4.messageId}: ${exception.message}", LogLevel.Error);

        // Every {{as4.creditNotePeriod:90000}} ms a credit note, signed, compressed and encrypted like the invoice.
        From("timer://cs-modulectx-as4-credit-note?period={{as4.creditNotePeriod:90000}}")
            .RouteId("cs-modulectx-as4-credit-note")
            .Description("Sends a test credit note to the AS4 partner")
            .Process(exchange =>
            {
                var number = Convert.ToInt32(exchange.In.Headers["CamelTimerCounter"]);
                // The content type travels as the MimeType part property of the payload.
                exchange.In.ContentType = "application/xml";
                exchange.In.Body = new XElement(Ns + "creditNote",
                    new XElement(Ns + "number", $"CN-{number}"),
                    new XElement(Ns + "amount", "12.50")).ToString(SaveOptions.DisableFormatting);
            })
            .To("as4:?connectionFactory=as4-node&partner=partner")
            .Log("sent credit note ${header.redbAs4.messageId}, receipt ${header.redbAs4.receiptMessageId}");
    }
}
