using System.Globalization;
using redb.Route.Abstractions;

namespace RedbChat.Module;

/// <summary>
/// The module's settings, read from the properties of its route context. Both hosts fill them the same
/// way: RedbChat.Module.config.json first, then the <c>Tsak:Contexts:{context}:Override</c> section of
/// the host configuration, which wins. The API key belongs only in the Override layer (RedbChat.Host
/// appsettings.json, or the environment variable <c>Tsak__Contexts__redbchat__Override__Llm__ApiKey</c>),
/// never in the module's config file, which ships inside the package.
/// </summary>
public sealed record ModuleSettings(
    string Provider,
    string ModelId,
    string ApiKey,
    double Temperature,
    int MaxTokens,
    string SystemPrompt,
#if (UseMcp)
    string McpFolder,
#endif
    string HttpHost,
    int HttpPort)
{
    public static ModuleSettings FromContext(IRouteContext context)
    {
        var llm = Section(context, "Llm");
        var chat = Section(context, "Chat");
        var http = Section(context, "Http");
#if (UseMcp)
        var mcp = Section(context, "Mcp");
#endif

        var apiKey = Optional(llm, "ApiKey");
        if (string.IsNullOrWhiteSpace(apiKey))
            throw new InvalidOperationException(
                $"The LLM API key is not set for context '{context.ContextId}'. Put it into " +
                $"RedbChat.Host/appsettings.json (Tsak:Contexts:{context.ContextId}:Override:Llm:ApiKey), " +
                $"or set the environment variable Tsak__Contexts__{context.ContextId}__Override__Llm__ApiKey.");

        return new(
            Provider: Required(context, llm, "Llm", "Provider"),
            ModelId: Required(context, llm, "Llm", "ModelId"),
            ApiKey: apiKey,
            Temperature: double.Parse(Required(context, llm, "Llm", "Temperature"), CultureInfo.InvariantCulture),
            MaxTokens: int.Parse(Required(context, llm, "Llm", "MaxTokens"), CultureInfo.InvariantCulture),
            SystemPrompt: Required(context, chat, "Chat", "SystemPrompt"),
#if (UseMcp)
            // Relative paths resolve against the current directory of the process.
            McpFolder: Path.GetFullPath(Required(context, mcp, "Mcp", "Folder")),
#endif
            // 127.0.0.1 by default: the endpoint has no authentication and every call costs tokens. A container
            // sets 0.0.0.0 (see the Dockerfile).
            HttpHost: Required(context, http, "Http", "Host"),
            HttpPort: int.Parse(Required(context, http, "Http", "Port"), CultureInfo.InvariantCulture));
    }

    private static IDictionary<string, object?> Section(IRouteContext context, string name) =>
        context.GetProperty<IDictionary<string, object?>>(name) ?? throw Missing(context, name);

    private static string? Optional(IDictionary<string, object?> section, string key) =>
        section.TryGetValue(key, out var value) ? Convert.ToString(value, CultureInfo.InvariantCulture) : null;

    private static string Required(IRouteContext context, IDictionary<string, object?> section, string sectionName, string key) =>
        Optional(section, key) is { Length: > 0 } text ? text : throw Missing(context, $"{sectionName}:{key}");

    private static InvalidOperationException Missing(IRouteContext context, string key) => new(
        $"{key} is not configured for context '{context.ContextId}': set it in RedbChat.Module.config.json " +
        $"or in Tsak:Contexts:{context.ContextId}:Override.");
}
