using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Route.Abstractions;
using redb.Route.Controllers;
using redb.Route.Controllers.Extensions;
using redb.Route.Core;
using redb.Route.Http;
using RedbBff.Backend.Controllers;
using RedbBff.Backend.Props;

namespace RedbBff.Backend;

/// <summary>
/// The module's entry point. A Tsak worker finds it by convention (a public static class
/// <c>InitRoute</c> with a <c>main(IRouteContext)</c> method) and awaits it when the module loads;
/// RedbBff.Host calls the same method, so the backend exists once.
/// <para>Order: settings, components, the service key, the database, then the route.</para>
/// </summary>
public static class InitRoute
{
    public static async Task<IRouteContext> main(IRouteContext context)
    {
        var settings = ModuleSettings.FromContext(context);
        var services = context.GetServiceProvider()
            ?? throw new InvalidOperationException("The route context has no service provider; redb is not available.");

        // One HTTP server per port for the whole process. The host registers it: the Tsak worker does, and
        // so does RedbBff.Host (AddRedbRouteHttpHosting).
        if (!context.HasComponent("http"))
            context.AddComponent(new HttpComponent { ServerManager = services.GetRequiredService<SharedHttpServerManager>() });

        context.AddToRegistry(ServiceKeyValidator.RegistryName, new ServiceKeyValidator(settings.ServiceKey));

        // The module's schemes and the sample data, through a scope of its own: the connection goes back to
        // the pool as soon as this is done.
        await using (var scope = services.CreateAsyncScope())
        {
            var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
            await redb.SyncSchemeAsync<Product>();
            await redb.SyncSchemeAsync<Category>();
            await SeedAsync(redb);
        }

        // Every controller of this assembly behind one HTTP endpoint. The dispatcher matches the method and
        // the path against the [Route] and [HttpGet] attributes; no match answers 404.
        var registry = new ControllerRegistry();
        registry.RegisterAssembly(typeof(ProductsController).Assembly);

        ((RouteContext)context).AddRoutes(r => r
            .From($"http:{settings.ApiHost}:{settings.ApiPort}/{{**path}}?inOut=true" +
                  $"&inboundAuth=bearer&tokenValidator=#{ServiceKeyValidator.RegistryName}&inboundRealm=backend")
            .RouteId("backend-api")
            .RedbHttpController(registry));

        context.GetService<ILoggerFactory>()?.CreateLogger("RedbBff").LogInformation(
            "RedbBff backend ready on {Host}:{Port}", settings.ApiHost, settings.ApiPort);
        return context;
    }

    // Sample data on the first run, so the pages are not empty.
    private static async Task SeedAsync(IRedbService redb)
    {
        if (!await redb.Query<Product>().AnyAsync())
        {
            var samples = new (string Name, string Category, decimal Price, bool InStock)[]
            {
                ("Laptop 14", "Laptops", 1299m, true),
                ("Laptop 16", "Laptops", 1899m, false),
                ("Monitor 27", "Monitors", 349m, true),
                ("Keyboard", "Accessories", 79m, true),
                ("Mouse", "Accessories", 39m, true),
            };
            foreach (var s in samples)
            {
                await redb.SaveAsync(new RedbObject<Product>
                {
                    Name = s.Name,
                    Props = new Product { Category = s.Category, Price = s.Price, InStock = s.InStock },
                });
            }
        }

        if (!await redb.TreeQuery<Category>().AnyAsync())
        {
            var root = new TreeRedbObject<Category> { Name = "Electronics", Props = new Category { SortOrder = 1 } };
            await redb.SaveAsync(root);

            var computers = new TreeRedbObject<Category> { Name = "Computers", Props = new Category { SortOrder = 1 } };
            await redb.CreateChildAsync(computers, root);
            await redb.CreateChildAsync(new TreeRedbObject<Category> { Name = "Laptops", Props = new Category { SortOrder = 1 } }, computers);
            await redb.CreateChildAsync(new TreeRedbObject<Category> { Name = "Monitors", Props = new Category { SortOrder = 2 } }, root);
        }
    }
}
