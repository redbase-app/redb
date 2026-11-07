using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Route.Abstractions;
using redb.Route.Core;
using redb.Route.File;
using RedbWorker.Module.Models;
using RedbWorker.Module.Routes;

namespace RedbWorker.Module;

/// <summary>
/// The module's entry point. A Tsak worker finds it by convention (a public static class
/// <c>InitRoute</c> with a <c>main(IRouteContext)</c> method) and awaits it when the module loads;
/// RedbWorker.Host calls the same method, so the route code exists once.
/// <para>Order: settings, components, the database, then the routes.</para>
/// </summary>
public static class InitRoute
{
    public static async Task<IRouteContext> main(IRouteContext context)
    {
        var settings = ModuleSettings.FromContext(context);
        settings.EnsureFolders();

        var logger = context.GetService<ILoggerFactory>()?.CreateLogger("RedbWorker");
        var services = context.GetServiceProvider()
            ?? throw new InvalidOperationException("The route context has no service provider; redb is not available.");

        if (!context.HasComponent("file"))
            context.AddComponent(new FileComponent());

        // The module's schemes, through a scope of its own: the connection goes back to the pool as soon
        // as this is done. Every ProcessWithRedb step gets its own IRedbService the same way: the host's
        // default redb, registered in its DI container. Under Tsak that is Tsak:Redb:Provider.
        await using (var scope = services.CreateAsyncScope())
        {
            var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
            await redb.SyncSchemeAsync<Order>();
        }

        var routes = (RouteContext)context;
        routes.AddRoutes(new ExceptionRouteBuilder());
        routes.AddRoutes(new OrderInboxRouteBuilder());

        logger?.LogInformation("RedbWorker module ready: inbox {Inbox}, receipts to {Outbox}", settings.Inbox, settings.Outbox);
        return context;
    }
}
