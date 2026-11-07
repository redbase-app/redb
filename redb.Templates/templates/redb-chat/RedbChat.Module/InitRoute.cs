using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Route.Abstractions;
using redb.Route.Core;
#if (UseShell)
using redb.Route.Exec;
#endif
using redb.Route.Http;
using redb.Route.Llm;
using redb.Route.Llm.Abstractions.Tools;
using redb.Route.Llm.Engine;
using redb.Route.Llm.Engine.Storage;
using redb.Route.Llm.Storage.Redb;
using redb.Route.Llm.Storage.Redb.Schemas;
using RedbChat.Module.Routes;
#if (UseShell || UseMcp)
using RedbChat.Module.Tools;
#endif

namespace RedbChat.Module;

/// <summary>
/// The module's entry point. A Tsak worker finds it by convention (a public static class
/// <c>InitRoute</c> with a <c>main(IRouteContext)</c> method) and awaits it when the module loads;
/// RedbChat.Host calls the same method, so the chat exists once.
/// <para>Order: settings, components, the model connection, the database, the tools, the engine, the routes.</para>
/// </summary>
public static class InitRoute
{
    public static async Task<IRouteContext> main(IRouteContext context)
    {
        var settings = ModuleSettings.FromContext(context);
        var loggerFactory = context.GetService<ILoggerFactory>();
        var services = context.GetServiceProvider()
            ?? throw new InvalidOperationException("The route context has no service provider; redb is not available.");

        // One HTTP server per port for the whole process. The host registers it: the Tsak worker does, and
        // so does RedbChat.Host (AddRedbRouteHttpHosting).
        if (!context.HasComponent("http"))
            context.AddComponent(new HttpComponent { ServerManager = services.GetRequiredService<SharedHttpServerManager>() });
        if (!context.HasComponent("llm"))
            context.AddComponent(new LlmComponent());
#if (UseShell)
        if (!context.HasComponent("exec"))
            context.AddComponent(new ExecComponent());
#endif

        // The model. Provider and model come from the module's config file, the key from the Override
        // layer. For DeepSeek set Provider "deepseek" and ModelId "deepseek-chat" (see appsettings.json).
        context.AddToRegistry(ChatNames.LlmFactory, new LlmConnectionFactory
        {
            Name = ChatNames.LlmFactory,
            Provider = settings.Provider,
            ModelId = settings.ModelId,
            ApiKey = settings.ApiKey,
            Temperature = settings.Temperature,
            MaxTokens = settings.MaxTokens,
        });

        // The history lives in redb: a conversation and its messages are redb objects, so a chat goes on
        // after a restart. The schemes are synced here, through a scope of the module's own.
        await using (var scope = services.CreateAsyncScope())
        {
            var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
            await redb.SyncSchemeAsync<ConversationProps>();
            await redb.SyncSchemeAsync<MessageProps>();
        }

        // Tools: every tool the model may call is a descriptor in this registry.
        var toolRegistry = new ToolDescriptorRegistry();
        context.AddService(typeof(IToolDescriptorRegistry), toolRegistry);
        var tools = new List<string>();
#if (UseShell)
        tools.Add(ShellToolRouteBuilder.ToolName);   // a route with .AsLlmTool(...) registers itself
#endif
#if (UseMcp)
        tools.AddRange(await McpTools.RegisterAsync(context, toolRegistry, settings, loggerFactory));
#endif

        // The engine runs the model and the tool calls. It sends tool calls through a producer template.
        var producer = new ProducerTemplate(context);
        producer.Start();
        context.AddService(typeof(IProducerTemplate), producer);
        context.AddService(typeof(IConversationStore), new RedbConversationStore(context));
        context.AddService(typeof(IAgentEngine), AgentEngine.FromContext(context));

        var routes = (RouteContext)context;
        routes.AddRoutes(new ChatRouteBuilder(settings, tools));
#if (UseShell)
        routes.AddRoutes(new ShellToolRouteBuilder());
#endif

        loggerFactory?.CreateLogger("RedbChat").LogInformation(
            "RedbChat module ready: {Provider}/{Model}, HTTP on {Host}:{Port}, tools: {Tools}",
            settings.Provider, settings.ModelId, settings.HttpHost, settings.HttpPort,
            tools.Count == 0 ? "none" : string.Join(", ", tools));
        return context;
    }
}
