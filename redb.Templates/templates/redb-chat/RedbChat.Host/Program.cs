// The module's own host. It does what a Tsak worker does for the module, plus a console chat:
//   1) reads appsettings.json, then environment variables,
//   2) registers redb and the HTTP server of the process, initializes redb,
//   3) builds the context configuration (module config file, then the Override section),
//   4) awaits RedbChat.Module.InitRoute.main(ctx), the method the Tsak worker calls,
//   5) starts the context, then reads chat messages from the console until Ctrl+C or SIGTERM.

using System.Runtime.InteropServices;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Models.Configuration;
using redb.Core.Pro.Extensions;
using redb.Route.Core;
using redb.Route.Http;
using redb.Route.Llm;
using redb.SQLite.Pro.Extensions;
using RedbChat.Host;
using RedbChat.Module;

var configuration = new ConfigurationBuilder()
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: false)
    .AddEnvironmentVariables()
    .Build();

var connectionString = configuration.GetConnectionString("Redb")
    ?? throw new InvalidOperationException("ConnectionStrings:Redb is missing in appsettings.json.");

// The module's context and its settings file: the same names a Tsak worker uses.
var moduleConfig = Path.Combine(AppContext.BaseDirectory, "RedbChat.Module.config.json");
var contextName = ContextConfiguration.ContextNameOf(moduleConfig);

// Checked before anything is created: otherwise the module would fail later, once the database file
// has already been made.
if (string.IsNullOrWhiteSpace(configuration[$"Tsak:Contexts:{contextName}:Override:Llm:ApiKey"]))
    throw new InvalidOperationException(
        $"The LLM API key is not set for context '{contextName}'. Put it into RedbChat.Host/appsettings.json " +
        $"(Tsak:Contexts:{contextName}:Override:Llm:ApiKey), or set the environment variable " +
        $"Tsak__Contexts__{contextName}__Override__Llm__ApiKey.");

var services = new ServiceCollection();
services.AddSingleton<IConfiguration>(configuration);

// Warning keeps the chat readable (Information shows every route step and the token counts).
// Override it with the standard key, for example Logging__LogLevel__Default=Information.
services.AddLogging(b => b
    .AddSimpleConsole(o => { o.SingleLine = true; o.TimestampFormat = "HH:mm:ss "; })
    .SetMinimumLevel(configuration.GetValue("Logging:LogLevel:Default", LogLevel.Warning)));

services.AddRedbPro(options => options
    .UseSqlite(connectionString)
    // To use PostgreSQL or SQL Server instead: swap the package in RedbChat.Host.csproj
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

var context = new RouteContext(provider, contextName, provider.GetRequiredService<ILoggerFactory>());
ContextConfiguration.ApplyTo(context, ContextConfiguration.Build(configuration, contextName, moduleConfig));

await InitRoute.main(context);   // <- the exact method the Tsak worker calls
await context.Start();

var settings = ModuleSettings.FromContext(context);
Console.WriteLine();
Console.WriteLine($"RedbChat: {settings.Provider}/{settings.ModelId}. The history is kept in RedBase between runs.");
Console.WriteLine($"  HTTP:    curl -d \"hello\" -H \"{ChatNames.ChatIdHeader}: my-chat\" http://localhost:{settings.HttpPort}/api/chat");
#if (audit)
Console.WriteLine("  Console: type a message; /new starts a new conversation, /audit shows your latest answers.");
#else
Console.WriteLine("  Console: type a message; /new starts a new conversation.");
#endif
Console.WriteLine("Ctrl+C to exit.");
Console.WriteLine();

using var stopping = new CancellationTokenSource();
_ = Task.Run(() => ChatConsoleAsync(context, provider, stopping.Token), stopping.Token);

var stop = new TaskCompletionSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; stop.TrySetResult(); };
// docker stop sends SIGTERM, not Ctrl+C.
using var sigterm = PosixSignalRegistration.Create(PosixSignal.SIGTERM, c => { c.Cancel = true; stop.TrySetResult(); });
await stop.Task;

await stopping.CancelAsync();
await context.DisposeAsync();

// Reads a line, sends it to direct:chat, prints the answer. Without a terminal (a container started
// without -it) ReadLine returns null and only the HTTP endpoint is left.
static async Task ChatConsoleAsync(RouteContext context, IServiceProvider provider, CancellationToken stopping)
{
    using var producer = new ProducerTemplate(context);
    producer.Start();

    var chatId = "console";
    var user = Environment.UserName;

    while (!stopping.IsCancellationRequested)
    {
        Console.Write("> ");
        var line = Console.ReadLine();
        if (line is null)
            return;
        line = line.Trim();
        if (line.Length == 0)
            continue;

        if (line == "/new")
        {
            chatId = $"console-{DateTime.UtcNow:yyyyMMddHHmmss}";
            Console.WriteLine($"New conversation: {chatId}");
            continue;
        }
#if (audit)
        if (line == "/audit")
        {
            await using var scope = provider.CreateAsyncScope();
            var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
            foreach (var a in await AuditQueries.LatestAnswersAsync(redb, user, 5))
                Console.WriteLine($"{a.At:u} {a.Model} in={a.TokensIn} out={a.TokensOut}: {a.Text}");
            continue;
        }
#endif

        var message = new Message(line);
        message.Headers[LlmHeaders.ConversationId] = chatId;
        message.Headers[ChatNames.UserIdHeader] = user;

        try
        {
            Console.WriteLine(await producer.RequestBody(ChatNames.ChatUri, message, stopping));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            Console.WriteLine($"The call failed: {ex.Message}");
        }
    }
}
