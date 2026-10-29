using System.Globalization;
using System.Text;
using redb.Route.Core;
using redb.Route.S3;

namespace RouteModule.Routes;

/// <summary>
/// A sibling of routes/orders.route.xml in C#: price lists as CSV objects under the prefix prices/ of the same
/// inbox bucket, written and read on a trigger of their own through the same s3 connection bean. The XML reader
/// takes orders/*.json only, so the two never read the same object.
/// </summary>
public sealed class PriceListRoute : RouteBuilder
{
    protected override void Configure()
    {
        // Writer: every {{prices.period:30000}} ms a price list of three items. The object key comes from the header
        // S3Headers.Key, set in C#, instead of a keyName option in the URI.
        From("timer://cs-modulectx-s3-prices?period={{prices.period:30000}}")
            .RouteId("cs-modulectx-s3-prices-write")
            .Description("Writes a CSV price list into the inbox bucket")
            .Process(exchange =>
            {
                var tick = Convert.ToInt32(exchange.In.Headers["CamelTimerCounter"]);
                var csv = new StringBuilder("sku;price\n");
                foreach (var (sku, price) in new[] { ("A-1", 9.50m), ("B-2", 20m), ("C-3", 4.25m) })
                    csv.Append(sku).Append(';').Append((price + tick).ToString(CultureInfo.InvariantCulture)).Append('\n');
                exchange.In.Headers[S3Headers.Key] = $"prices/prices-{tick}.csv";
                exchange.In.Body = csv.ToString();
            })
            .To("s3://{{s3.inbox:incoming}}?connectionFactory=s3&contentType=text/csv&autoCreateBucket=true")
            .Log("written price list ${header.redbS3.ProducedKey}");

        // Reader: the new price lists, deleted once the route finished with them.
        From("s3://{{s3.inbox:incoming}}?connectionFactory=s3&prefix=prices/&include=*.csv&delay=5000&deleteAfterRead=true&autoCreateBucket=true")
            .RouteId("cs-modulectx-s3-prices-read")
            .Description("Reads the price lists and logs the cheapest item")
            .ConvertBody(typeof(string))
            .Process(exchange =>
            {
                var cheapest = ((string)exchange.In.Body!)
                    .Split('\n', StringSplitOptions.RemoveEmptyEntries)
                    .Skip(1)
                    .Select(line => line.Split(';'))
                    .MinBy(cells => decimal.Parse(cells[1], CultureInfo.InvariantCulture))!;
                exchange.In.Body = $"{cheapest[0]} at {cheapest[1]}";
            })
            .Log("price list ${header.redbS3.Key}: cheapest ${body}");
    }
}
