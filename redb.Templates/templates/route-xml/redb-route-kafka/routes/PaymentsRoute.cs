using System.Collections.Concurrent;
using System.Text.Json;
using redb.Route.Core;
using redb.Route.Kafka;

namespace RouteModule.Routes;

/// <summary>
/// A sibling of routes/orders.route.xml in C#: payments on a topic of their own, cs-payments, keyed by customer,
/// and a consumer that keeps a running total per customer. The same kafka connection bean; its own timer, topic
/// and consumer group, so it never reads what the XML routes read.
/// </summary>
public sealed class PaymentsRoute : RouteBuilder
{
    // Running totals of the customers whose partitions this worker reads.
    private readonly ConcurrentDictionary<string, decimal> _totals = new();

    protected override void Configure()
    {
        // Producer: a payment every {{payments.period:4000}} ms for one of three customers. The record key is the
        // customer, so the payments of one customer land in one partition, in order.
        From("timer://cs-modulectx-payments?period={{payments.period:4000}}")
            .RouteId("cs-modulectx-payments-out")
            .Description("Publishes a test payment to the topic cs-payments")
            .SetHeader(KafkaHeaders.Key, Expr("customer-${header.CamelTimerCounter % 3}"))
            .SetBody(Expr("""{"customer":"customer-${header.CamelTimerCounter % 3}","amount":${header.CamelTimerCounter}}"""))
            .To("kafka://cs-payments?connectionFactory=kafka");

        // Consumer in a group of its own: the offset is committed after the route finished with a record.
        From("kafka://cs-payments?connectionFactory=kafka&groupId=cs-modulectx")
            .RouteId("cs-modulectx-payments-in")
            .Description("Keeps a running total per customer")
            .ConvertBody(typeof(string))
            .Process(exchange =>
            {
                using var json = JsonDocument.Parse((string)exchange.In.Body!);
                var customer = json.RootElement.GetProperty("customer").GetString()!;
                var amount = json.RootElement.GetProperty("amount").GetDecimal();
                exchange.In.Headers["total"] = _totals.AddOrUpdate(customer, amount, (_, sum) => sum + amount);
                exchange.In.Headers["customer"] = customer;
            })
            .Log("payment of ${header.customer}: total ${header.total} (partition ${header.redbKafka.Partition}, offset ${header.redbKafka.Offset})");
    }
}
