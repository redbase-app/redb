using Microsoft.Extensions.Logging;
using redb.Route.Core;
using redb.Route.Validation;
using RedbWorker.Module.Xml;

namespace RedbWorker.Module.Routes;

/// <summary>
/// Exception handlers for the whole context: an <c>OnException</c> declared in any route builder wraps
/// every route of the module.
/// <para>
/// Only for a failure whose right outcome is "take the file and answer it". A handled exception is a
/// success for the file consumer, so the file moves to the archive. A catch-all
/// <c>OnException&lt;Exception&gt;().Handled(true)</c> would be wrong here: it would archive a file the
/// database never saw.
/// </para>
/// </summary>
public sealed class ExceptionRouteBuilder : RouteBuilder
{
    protected override void Configure()
    {
        OnException<ValidationException>()
            .Handled(true)
            .Process(e => e.In.Body = new OrderReceiptXml
            {
                Status = ReceiptStatuses.Rejected,
                Reason = e.Exception?.Message ?? "The file does not match the order schema.",
            })
            .Log("${header.worker.fileName}: rejected, the file does not match the order schema", LogLevel.Warning)
            .To(RouteUris.WriteReceipt);
    }
}
