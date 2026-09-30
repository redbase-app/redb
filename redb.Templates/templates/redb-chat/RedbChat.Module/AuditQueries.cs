using redb.Core;
using redb.Route.Llm.Storage.Redb.Schemas;

namespace RedbChat.Module;

/// <summary>
/// Reading the audit trail back. Every stored message carries the user and the audit tags the chat route
/// stamps on it (<c>.User(...)</c>, <c>.Audit(...)</c>), and they are typed properties: the filter below,
/// including the dictionary lookup, runs in the database.
/// </summary>
public static class AuditQueries
{
    /// <summary>The model's latest answers to one user, newest first.</summary>
    public static async Task<IReadOnlyList<AuditLine>> LatestAnswersAsync(IRedbService redb, string userId, int count)
    {
        var messages = await redb.Query<MessageProps>()
            .Where(m => m.UserId == userId && m.Role == "assistant" && m.AuditTags!["app"] == "redbchat")
            .OrderByDescending(m => m.CreatedAtUtc)
            .Take(count)
            .ToListAsync();

        return messages
            .Select(m => new AuditLine(
                m.Props!.CreatedAtUtc,
                m.Props.ModelId,
                m.Props.InputTokens,
                m.Props.OutputTokens,
                string.Concat(m.Props.Content.Select(c => c.Text))))
            .ToList();
    }
}

public sealed record AuditLine(DateTimeOffset At, string? Model, int TokensIn, int TokensOut, string Text);
