using redb.Core;
using redb.Core.Models.Entities;
using redb.Route.Abstractions;
using RedbWorker.Module.Models;
using RedbWorker.Module.Xml;

namespace RedbWorker.Module.Services;

/// <summary>The work done on an order. Each method is one <c>ProcessWithRedb</c> step of a route.</summary>
public static class OrderService
{
    /// <summary>
    /// Stores the order, or answers it as a duplicate when an order with the same id is already there.
    /// Sets the receipt as the new body.
    /// </summary>
    /// <remarks>
    /// Runs inside <c>.Transacted()</c>. The order id is the object's unique key, so two files with the
    /// same id that arrive at the same time cannot both be stored: the second fails on the key, rolls
    /// back, stays in the inbox and is answered as a duplicate on the next poll.
    /// </remarks>
    public static async Task RegisterAsync(IRedbService redb, IExchange exchange, CancellationToken ct)
    {
        var xml = (OrderXml)exchange.In.Body!;
        exchange.In.Headers[WorkerHeaders.OrderId] = xml.OrderId;

        var isDuplicate = await redb.Query<Order>().Where(o => o.OrderId == xml.OrderId).AnyAsync();
        if (isDuplicate)
        {
            exchange.In.Headers[WorkerHeaders.Status] = ReceiptStatuses.Duplicate;
            exchange.In.Body = new OrderReceiptXml
            {
                OrderId = xml.OrderId,
                Status = ReceiptStatuses.Duplicate,
                Reason = "An order with this id was received before.",
            };
            return;
        }

        await redb.SaveAsync(new RedbObject<Order>
        {
            Name = xml.OrderId,
            ValueUnique = xml.OrderId,
            Props = new Order
            {
                OrderId = xml.OrderId,
                Customer = xml.Customer,
                Amount = xml.Amount,
                Currency = xml.Currency,
                SourceFile = exchange.In.GetHeader<string>(WorkerHeaders.FileName) ?? "",
            },
        }, ct);

        exchange.In.Headers[WorkerHeaders.Status] = ReceiptStatuses.Accepted;
        exchange.In.Body = new OrderReceiptXml { OrderId = xml.OrderId, Status = ReceiptStatuses.Accepted };
    }
}
