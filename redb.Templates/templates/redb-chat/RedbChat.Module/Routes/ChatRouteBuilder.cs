using redb.Route.Core;
using redb.Route.Llm;
using LlmDsl = redb.Route.Llm.Fluent.Llm;

namespace RedbChat.Module.Routes;

/// <summary>
/// The chat: <c>direct:chat</c> takes a message and answers it, and an HTTP endpoint forwards to it.
/// The console of RedbChat.Host sends to <c>direct:chat</c> directly.
/// </summary>
public sealed class ChatRouteBuilder(ModuleSettings settings, IReadOnlyList<string> tools) : RouteBuilder
{
    protected override void Configure()
    {
        var llm = LlmDsl.Factory(ChatNames.LlmFactory)
            // The history of the conversation named in the llm.conversation.id header is loaded before the
            // call and the new messages are stored after it.
            .ConversationFromHeader()
            // A tool call and its answer is one iteration; the limit stops a model that keeps calling.
            .MaxIterations(8)
#if (audit)
            // Stamped on every stored message: who asked, and free-form tags. AuditQueries.cs reads them back.
            .User("${header.X-User-Id}")
            .Audit("app", "redbchat")
            // The prompt's name and version, so a stored answer says which prompt produced it.
            .PromptTemplate("chat-system", "v1")
#endif
            ;
        if (tools.Count > 0)
            llm = llm.Tools(string.Join(",", tools));

        From(ChatNames.ChatUri)
            .RouteId("chat")
            .ConvertBody<string>()
            .Process(e =>
            {
                e.In.Headers[LlmHeaders.SystemPrompt] = settings.SystemPrompt;
                if (e.In.GetHeader<string>(LlmHeaders.ConversationId) is not { Length: > 0 })
                    e.In.Headers[LlmHeaders.ConversationId] = "default";
            })
            .Log("chat ${header.llm.conversation.id}: ${body}")
            .To(llm.AsUri())
            .Log("chat ${header.llm.conversation.id}: tokens in ${header.llm.tokens.in}, out ${header.llm.tokens.out}");

        // POST the message as the body. X-Chat-Id picks the conversation, X-User-Id names the caller.
        From($"http:{settings.HttpHost}:{settings.HttpPort}/api/chat?inOut=true")
            .RouteId("chat-http")
            .Process(e =>
            {
                e.In.Headers[LlmHeaders.ConversationId] =
                    e.In.GetHeader<string>(ChatNames.ChatIdHeader) is { Length: > 0 } chatId ? chatId : "default";
                if (e.In.GetHeader<string>(ChatNames.UserIdHeader) is not { Length: > 0 })
                    e.In.Headers[ChatNames.UserIdHeader] = "anonymous";
            })
            .To(ChatNames.ChatUri);
    }
}
