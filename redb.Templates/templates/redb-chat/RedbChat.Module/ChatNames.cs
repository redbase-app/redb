namespace RedbChat.Module;

/// <summary>Names shared by the routes and the hosts.</summary>
public static class ChatNames
{
    /// <summary>The chat itself: a message in, the model's answer out. The console and HTTP both call it.</summary>
    public const string ChatUri = "direct:chat";

    /// <summary>The registry name of the LLM connection factory.</summary>
    public const string LlmFactory = "chat-llm";

    /// <summary>
    /// The conversation a message belongs to. Messages with the same id share their history, which
    /// RedBase keeps between restarts. HTTP callers send it as <c>X-Chat-Id</c>.
    /// </summary>
    public const string ChatIdHeader = "X-Chat-Id";

    /// <summary>Who is talking. HTTP callers send it as <c>X-User-Id</c>; the console uses the OS user name.</summary>
    public const string UserIdHeader = "X-User-Id";
}
