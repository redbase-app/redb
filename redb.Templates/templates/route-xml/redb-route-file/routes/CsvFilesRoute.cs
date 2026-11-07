using System.Globalization;
using Microsoft.Extensions.Logging;
using redb.Route.Core;

namespace RouteModule.Routes;

/// <summary>
/// A sibling of routes/files.route.xml in C#: CSV order files through an inbox folder of its own,
/// {{files.csInbox:inbox-cs}}, with the same done/ and error/ folders and receipts into the same outbox. The
/// XML inbox and this one are different folders, so no file is read twice.
/// </summary>
public sealed class CsvFilesRoute : RouteBuilder
{
//#if (transport == "file")
    // The folders are on the worker's file system: no connection bean.
    private const string Connection = "";
//#else
    // The same remote connection bean as the XML routes.
    private const string Connection = "connectionFactory=remote&";
//#endif

    protected override void Configure()
    {
        // Test input: every {{files.csPeriod:30000}} ms a CSV file of three order lines; every third file has a line
        // without an amount.
        From("timer://cs-modulectx-files-feed?period={{files.csPeriod:30000}}")
            .RouteId("cs-modulectx-files-feed")
            .Description("Puts a test CSV order file into the C# inbox")
            .SetBody(exchange =>
            {
                var tick = Convert.ToInt32(exchange.In.Headers["CamelTimerCounter"]);
                var third = tick % 3 == 2 ? "C-3;" : $"C-3;{tick + 3}";
                return $"sku;amount\nA-1;{tick + 1}\nB-2;{tick + 2}\n{third}\n";
            })
            .To("file://{{files.csInbox:inbox-cs}}?" + Connection + "fileName=orders-${header.CamelTimerCounter}.csv");

        // The C# inbox: every 5 seconds the *.csv files; a finished file moves to done/, a failed one to error/.
        From("file://{{files.csInbox:inbox-cs}}?" + Connection + "antInclude=*.csv&delay=5000&moveTo=done&moveFailed=error")
            .RouteId("cs-modulectx-files-in")
            .Description("Processes the CSV files of the C# inbox")
            .OnException<FormatException>()
                .Log("refused ${header.redbFile.NameOnly}: ${exception.message}", LogLevel.Warning)
            .EndOnException()
            .ConvertBody(typeof(string))
            .Process(exchange =>
            {
                // A line without an amount fails the file: it moves to error/.
                var total = ((string)exchange.In.Body!)
                    .Split('\n', StringSplitOptions.RemoveEmptyEntries)
                    .Skip(1)
                    .Sum(line => decimal.Parse(line.Split(';')[1], CultureInfo.InvariantCulture));
                exchange.In.Headers["total"] = total;
            })
            .Log("processed ${header.redbFile.NameOnly}: total ${header.total}")
            .SetBody(Expr("received ${header.redbFile.NameOnly}, total ${header.total}"))
            .To("file://{{files.outbox:outbox}}?" + Connection + "fileName=${header.redbFile.NameOnly}.receipt");
    }
}
