using System.Net;
using Microsoft.Extensions.Logging;
using redb.Route.Core;
using redb.Route.Mail;

namespace RouteModule.Routes;

/// <summary>
/// A sibling of routes/send.route.xml in C#: an HTML report on a timer of its own, through the same smtp bean, to the
/// same recipient. The mailbox reader of routes/read.route.xml receives it like any other message.
/// </summary>
public sealed class HtmlReportRoute : RouteBuilder
{
    protected override void Configure()
    {
        // A send that failed is tried again twice, ten seconds apart, as in the XML sender.
        OnException<Exception>()
            .MaximumRedeliveries(2)
            .RedeliveryDelay(TimeSpan.FromSeconds(10))
            .Handled()
            .Log("could not send ${header.redbMail.Subject}: ${exception.message}", LogLevel.Error);

        // Every {{mail.htmlPeriod:120000}} ms an HTML message with a small table built in C#.
        From("timer://cs-modulectx-mail-html?period={{mail.htmlPeriod:120000}}")
            .RouteId("cs-modulectx-mail-html")
            .Description("Sends an HTML report over SMTP")
            .Process(exchange =>
            {
                var tick = Convert.ToInt32(exchange.In.Headers["CamelTimerCounter"]);
                var rows = new[] { ("orders", tick * 3), ("refunds", tick % 2), ("customers", 2) }
                    .Select(r => $"<tr><td>{WebUtility.HtmlEncode(r.Item1)}</td><td>{r.Item2}</td></tr>");
                exchange.In.Headers[MailHeaders.Subject] = $"HTML report {tick}";
                exchange.In.Body = $"<h1>Report {tick}</h1><table>{string.Join("", rows)}</table>";
            })
            .To("smtp://{{mail.smtpHost:localhost}}?connectionFactory=smtp&from={{mail.from}}&to={{mail.to}}&contentType=text/html&disconnect=true")
            .Log("sent ${header.redbMail.Subject}");
    }
}
