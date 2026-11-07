using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using redb.Core;
using redb.Route.Abstractions;
using redb.Route.Controllers;
using redb.Route.Controllers.Extensions;
using redb.Route.Core;
using redb.Route.Http;
using RedbIdentityApp.Module.Controllers;
using RedbIdentityApp.Module.Identity;
using RedbIdentityApp.Module.Props;

namespace RedbIdentityApp.Module;

/// <summary>
/// The module's entry point. The Tsak worker finds it by convention (a public static class
/// <c>InitRoute</c> with a <c>main(IRouteContext)</c> method) and awaits it when the module loads, after
/// redb.Identity (manifest.json lists it as a dependency).
/// <para>
/// Order: settings, the services that talk to Identity, the database, the routes; then, once the context
/// has started, the setup of the module's clients and scopes at Identity, in the background.
/// </para>
/// <para>
/// Identity runs in the same worker, so every call to it is in process over direct-vm. An application
/// that runs elsewhere talks to Identity over HTTP instead (discovery, token, introspection endpoints);
/// redb.Identity's demos show that client side.
/// </para>
/// </summary>
public static class InitRoute
{
    public static async Task<IRouteContext> main(IRouteContext context)
    {
        var settings = ModuleSettings.FromContext(context);
        var services = context.GetServiceProvider()
            ?? throw new InvalidOperationException("The route context has no service provider; redb is not available.");
        var loggers = services.GetService<ILoggerFactory>() ?? NullLoggerFactory.Instance;
        var logger = loggers.CreateLogger("RedbIdentityApp");

        // One HTTP server per port for the whole process; the Tsak worker registers it.
        if (!context.HasComponent("http"))
            context.AddComponent(new HttpComponent { ServerManager = services.GetRequiredService<SharedHttpServerManager>() });

        // The services that talk to Identity, shared by the routes and the controllers (through the registry).
        var calls = new IdentityCalls(context, services.GetRequiredService<IServiceScopeFactory>());
        var introspector = new TokenIntrospector(calls, settings, loggers.CreateLogger("RedbIdentityApp.Introspection"));
        var management = new IdentityManagement(calls, settings, loggers.CreateLogger("RedbIdentityApp.Management"));
        context.AddToRegistry(BearerTokenValidator.RegistryName, new BearerTokenValidator(introspector, loggers.CreateLogger("RedbIdentityApp.Bearer")));
        context.AddToRegistry(typeof(ModuleSettings).FullName!, settings);
        context.AddToRegistry(typeof(IdentityManagement).FullName!, management);

        // The module's scheme, through a scope of its own: the connection goes back to the pool at once.
        await using (var scope = services.CreateAsyncScope())
        {
            await scope.ServiceProvider.GetRequiredService<IRedbService>().SyncSchemeAsync<Note>();
        }

        var routes = (RouteContext)context;

        // Revocations reported by Identity's audit (README, "Revocation").
        var revocations = new RevocationEvents(introspector, loggers.CreateLogger("RedbIdentityApp.Revocation"));
        routes.AddRoutes(r => r
            .From(settings.EventsEndpoint(context))
            .RouteId("identity-events")
            .Process(revocations.Process));

        // The API: the bearer check (introspection), the gate (503 when Identity is down, 403 without the
        // API scope), then every controller of this assembly. The dispatcher matches the method and the path
        // against the [Route] and [HttpGet] attributes; no match answers 404.
        var registry = new ControllerRegistry();
        registry.RegisterAssembly(typeof(NotesController).Assembly);
        var gate = new IdentityGate(context.ContextId, settings.ApiScope);

        routes.AddRoutes(r => r
            .From($"http:{settings.ApiHost}:{settings.ApiPort}/{{**path}}?inOut=true" +
                  $"&inboundAuth=bearer&tokenValidator=#{BearerTokenValidator.RegistryName}&inboundRealm={context.ContextId}")
            .RouteId("api")
            .Process(gate.Process)
            .RedbHttpController(registry));

        context.AddLifecycleListener(new ModuleLifecycle(new IdentitySetup(management, settings, logger), [introspector, calls]));

        logger.LogInformation("RedbIdentityApp API on {Host}:{Port}; Identity events at {Events}",
            settings.ApiHost, settings.ApiPort, settings.EventsEndpoint(context));
        return context;
    }

    /// <summary>Starts the Identity setup once the context runs, and stops it with the context.</summary>
    private sealed class ModuleLifecycle(IdentitySetup setup, IDisposable[] owned) : IRouteLifecycleListener
    {
        private readonly CancellationTokenSource _stopping = new();
        private Task? _setup;

        public Task OnContextStarted(IRouteContext context, CancellationToken ct)
        {
            _setup = Task.Run(() => setup.RunAsync(_stopping.Token));
            return Task.CompletedTask;
        }

        public async Task OnContextStopping(IRouteContext context, CancellationToken ct)
        {
            await _stopping.CancelAsync();
            if (_setup is not null)
                await _setup.ConfigureAwait(false);
        }

        public Task OnContextStopped(IRouteContext context, CancellationToken ct)
        {
            foreach (var resource in owned)
                resource.Dispose();
            _stopping.Dispose();
            return Task.CompletedTask;
        }
    }
}
