using redb.Route.Core;
using redb.Route.Exec;
using redb.Route.Llm.Abstractions.Tools;
using redb.Route.Llm.Extensions;

namespace RedbChat.Module.Tools;

/// <summary>
/// A tool is a route: <c>.AsLlmTool(...)</c> after <c>From</c> publishes the route to the model with a
/// name, a description and an input schema. The model's call arrives as the body; what the route returns
/// goes back to the model.
/// <para>
/// This one runs a system command through <c>exec:</c>. The allowlist is the security boundary: a command
/// outside it is refused before a process starts. It lists single read-only programs on purpose; adding a
/// shell (cmd, sh, pwsh) would let the model run anything.
/// </para>
/// </summary>
public sealed class ShellToolRouteBuilder : RouteBuilder
{
    public const string ToolName = "system_info";

    protected override void Configure()
    {
        var allowed = OperatingSystem.IsWindows()
            ? new[] { "hostname", "whoami", "ipconfig" }
            : new[] { "hostname", "whoami", "uname", "uptime", "df" };

        From("direct:tool-system-info")
            .AsLlmTool(ToolName)
                .Description(
                    "Run one read-only system command on the host and return its output. Input: " +
                    "{\"command\":\"<name>\",\"args\":[\"...\"]}. Output: {\"stdout\":\"...\",\"stderr\":\"...\",\"exitCode\":N}. " +
                    $"Allowed commands: {string.Join(", ", allowed)}.")
                .Input("""
                    {
                      "type": "object",
                      "properties": {
                        "command": { "type": "string" },
                        "args":    { "type": "array", "items": { "type": "string" } }
                      },
                      "required": ["command"]
                    }
                    """)
                .SideEffect(ToolSideEffect.ReadOnly)
                .Cost(ToolCostClass.Cheap)
            .Then()
            .Log("tool " + ToolName + ": ${body}")
            .To(ExecDsl.Run()
                .AllowedCommands(allowed)
                .TimeoutMs(5_000)
                .MaxStdoutBytes(8_192)
                .MaxStderrBytes(8_192));
    }
}
