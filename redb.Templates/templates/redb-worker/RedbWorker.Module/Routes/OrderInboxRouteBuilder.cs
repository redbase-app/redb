using redb.Route.Abstractions;
using redb.Route.Core;
using redb.Route.File;
using redb.Route.RedbCore.Extensions;
using RedbWorker.Module.Services;
using RedbWorker.Module.Xml;

namespace RedbWorker.Module.Routes;

/// <summary>
/// inbox -> schema check -> one transaction in redb -> receipt in the outbox.
/// <list type="bullet">
///   <item>A processed file moves to the archive, a file that failed moves to the error folder.</item>
///   <item>A schema violation is the sender's mistake, not a failure: <see cref="ExceptionRouteBuilder"/>
///     answers it with a Rejected receipt and the file goes to the archive like any other.</item>
///   <item>Anything else (the file is not XML at all, the database is down) fails the exchange: the
///     transaction rolls back and the file goes to the error folder.</item>
/// </list>
/// </summary>
public sealed class OrderInboxRouteBuilder : RouteBuilder
{
    protected override void Configure()
    {
        var settings = ModuleSettings.FromContext(Context!);

        From(FileDsl.Read(settings.Inbox)
                .Include("*.xml")
                .Delay(1000)
                .MoveTo(settings.Archive)
                // Without MoveFailed a failed file stays in the inbox and is picked up again on the next
                // poll: the better choice when failures are temporary, such as a database restart.
                .MoveFailed(settings.Error))
            .RouteId("order-inbox")
            .SetHeader(WorkerHeaders.FileName, e => e.In.GetHeader<string>(FileHeaders.FileNameOnly))
            .ConvertBody<string>()
            .Log("${header.worker.fileName}: received")

            .ValidateXsd(XmlSchemas.Order)
            .Unmarshal<OrderXml>("application/xml")

            // Everything the order writes to redb commits together or not at all.
            .Transacted()
                .ProcessWithRedb(OrderService.RegisterAsync)
            .EndTransaction()

            .Log("${header.worker.fileName}: order ${header.worker.orderId} ${header.worker.status}")
            .To(RouteUris.WriteReceipt);

        // The receipt, shared by this route and by the exception handler.
        From(RouteUris.WriteReceipt)
            .RouteId("write-receipt")
            .Marshal("application/xml")
            .To(FileDsl.Write(settings.Outbox).FileName("${header.worker.fileName}.receipt.xml"));
    }
}

/// <summary>Internal endpoints of the module.</summary>
public static class RouteUris
{
    public const string WriteReceipt = "direct:write-receipt";
}
