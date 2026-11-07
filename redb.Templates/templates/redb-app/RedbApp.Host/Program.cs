// The API's own host. It does what a Tsak worker does for the module, and nothing more:
//   1) reads appsettings.json, then environment variables,
//   2) registers redb and the HTTP server of the process, initializes redb,
//   3) builds the context configuration (module config file, then the Override section),
//   4) awaits RedbApp.Api.InitRoute.main(ctx), the method the Tsak worker calls,
//   5) starts the context and runs until Ctrl+C or SIGTERM.

using System.Runtime.InteropServices;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Models.Configuration;
using redb.Core.Pro.Extensions;
using redb.Route.Core;
using redb.Route.Http;
using redb.SQLite.Pro.Extensions;
using RedbApp.Api;
using RedbApp.Host;

var configuration = new ConfigurationBuilder()
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: false)
    .AddEnvironmentVariables()
    .Build();

var connectionString = configuration.GetConnectionString("Redb")
    ?? throw new InvalidOperationException("ConnectionStrings:Redb is missing in appsettings.json.");

var services = new ServiceCollection();
services.AddSingleton<IConfiguration>(configuration);
services.AddLogging(b => b
    .AddSimpleConsole(o => { o.SingleLine = true; o.TimestampFormat = "HH:mm:ss "; })
    .SetMinimumLevel(LogLevel.Information));

services.AddRedbPro(options => options
    .UseSqlite(connectionString)
    // To use PostgreSQL or SQL Server instead: swap the package in RedbApp.Host.csproj
    // (redb.Postgres.Pro / redb.MSSql.Pro), replace the using redb.SQLite.Pro.Extensions above
    // with redb.Postgres.Pro.Extensions / redb.MSSql.Pro.Extensions, and use one of:
    //.UsePostgres(connectionString)
    //.UseMsSql(connectionString)
    .Configure(c =>
    {
        // Narrows the object set before the pivot step, so a selective filter does not scan the
        // whole scheme. It never changes results.
        c.EnablePvtPrefilter = true;

        // Saves only the properties that changed since the object was loaded.
        c.PropsSaveStrategy = PropsSaveStrategy.ChangeTracking;

        // Props cache: off by default. Turn it on when the same objects are read again and
        // again; the limit is per process, entries past the TTL are loaded again.
        // c.EnablePropsCache = true;
        // c.PropsCacheMaxSize = 10_000;
        // c.PropsCacheTtl = TimeSpan.FromMinutes(60);
    }));

// The HTTP servers of the process, one per port, as the Tsak worker registers them.
services.AddRedbRouteHttpHosting();

await using var provider = services.BuildServiceProvider();

// ensureCreated builds the RedBase tables on the first run and is idempotent afterwards. It does not
// create the database itself: for PostgreSQL and SQL Server the database has to exist already.
await using (var scope = provider.CreateAsyncScope())
{
    await scope.ServiceProvider.GetRequiredService<IRedbService>().InitializeAsync(ensureCreated: true);
}

var moduleConfig = Path.Combine(AppContext.BaseDirectory, "RedbApp.Api.config.json");
var contextName = ContextConfiguration.ContextNameOf(moduleConfig);
var context = new RouteContext(provider, contextName, provider.GetRequiredService<ILoggerFactory>());
ContextConfiguration.ApplyTo(context, ContextConfiguration.Build(configuration, contextName, moduleConfig));

await InitRoute.main(context);   // <- the exact method the Tsak worker calls
await context.Start();

var settings = ModuleSettings.FromContext(context);
Console.WriteLine();
Console.WriteLine($"RedbApp API on http://localhost:{settings.ApiPort}/api (OpenAPI: /api/products/openapi.json).");
Console.WriteLine("Start the client in a second terminal: dotnet run --project RedbApp.Web");
Console.WriteLine("Ctrl+C to exit.");
Console.WriteLine();

var stop = new TaskCompletionSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; stop.TrySetResult(); };
// docker stop sends SIGTERM, not Ctrl+C.
using var sigterm = PosixSignalRegistration.Create(PosixSignal.SIGTERM, c => { c.Cancel = true; stop.TrySetResult(); });
await stop.Task;

await context.DisposeAsync();
