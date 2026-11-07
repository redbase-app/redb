using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using redb.Route.Abstractions;
using redb.Route.Llm.Abstractions.Tools;
using redb.Route.Llm.Mcp;
using redb.Route.Llm.Mcp.Transport;

namespace RedbChat.Module.Tools;

/// <summary>
/// The tools of an MCP server, given to the model as its own tools.
/// <para>
/// The server here is the reference filesystem server, started over stdio with <c>npx</c> (Node.js must be
/// installed) and pinned to one folder (<c>Mcp:Folder</c>). Any other MCP server works the same way: change
/// the command below, for example <c>uvx mcp-server-fetch</c>.
/// </para>
/// </summary>
public static class McpTools
{
    private const string ServerName = "files";

    /// <summary>Starts the server, lists its tools, registers each one and returns the names the model sees.</summary>
    public static async Task<IReadOnlyList<string>> RegisterAsync(
        IRouteContext context, ToolDescriptorRegistry toolRegistry, ModuleSettings settings, ILoggerFactory? loggerFactory)
    {
        Directory.CreateDirectory(settings.McpFolder);

        // npx is a script on Windows, so it is started through cmd there.
        string[] server = ["npx", "-y", "@modelcontextprotocol/server-filesystem", settings.McpFolder];
        var transport = OperatingSystem.IsWindows()
            ? McpTransport.Stdio(command: "cmd", arguments: ["/c", .. server])
            : McpTransport.Stdio(command: server[0], arguments: server[1..]);

        // The server process lives as long as this one: it exits when its standard input closes.
        var client = new StdioMcpClient(
            serverName: ServerName,
            transport: transport,
            logger: loggerFactory?.CreateLogger("Mcp.files") ?? NullLogger.Instance);

        // The first start downloads the server package, which takes a while.
        using (var init = new CancellationTokenSource(TimeSpan.FromMinutes(2)))
            await client.InitializeAsync(init.Token);

        var mcpRegistry = new McpRegistry();
        mcpRegistry.Register(client);
        context.AddComponent(new McpComponent(mcpRegistry));

        var names = new List<string>();
        foreach (var tool in await client.ListToolsAsync())
        {
            if (string.IsNullOrWhiteSpace(tool.Name))
                continue;

            var modelName = McpToolDescriptor.BuildModelFacingName(ServerName, tool.Name);
            toolRegistry.Register(new McpToolDescriptor(ServerName, tool.Name, new LlmToolCapability
            {
                Name = modelName,
                Description = tool.Description ?? $"MCP tool '{tool.Name}' of the '{ServerName}' server.",
                InputSchema = McpToolDescriptor.BuildInputSchema(tool),
                Safety = new LlmToolSafety
                {
                    // The server can write files in its folder; mark it as external so it is audited as such.
                    SideEffect = ToolSideEffect.External,
                    Cost = ToolCostClass.Cheap,
                    RequiresApproval = false,
                },
            }));
            names.Add(modelName);
        }
        return names;
    }
}
