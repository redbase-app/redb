using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using redb.Route.Abstractions;
using redb.Route.Core;

namespace RedbIdentityApp.Module.Identity;

/// <summary>
/// Calls into redb.Identity over direct-vm: Identity runs in the same Tsak worker, so a call is a method
/// call on Identity's route, without a socket, TLS or serialization.
/// <para>
/// One <see cref="ProducerTemplate"/> for the module, created once: it caches the producer of each
/// endpoint. Each call gets an exchange of its own with a DI scope of its own, so parallel calls never
/// share a database connection, and the exchange is always disposed.
/// </para>
/// <para>
/// A failure inside Identity arrives as <see cref="IExchange.Exception"/>, not as a throw, and Identity's
/// own error handling may already have marked it handled: the exception alone decides.
/// </para>
/// </summary>
public sealed class IdentityCalls : IDisposable
{
    private readonly ProducerTemplate _producer;
    private readonly IServiceScopeFactory _scopes;

    public IdentityCalls(IRouteContext context, IServiceScopeFactory scopes)
    {
        _producer = new ProducerTemplate(context);
        _producer.Start();
        _scopes = scopes;
    }

    /// <summary>
    /// Sends <paramref name="body"/> to <paramref name="endpoint"/> and returns Identity's answer. The
    /// exchange is handed to <paramref name="prepare"/> first (headers, a preceding call on the same
    /// exchange) and disposed afterwards. Throws <see cref="IdentityUnavailableException"/> when Identity
    /// failed or did not answer in time.
    /// </summary>
    public async Task<IdentityAnswer> SendAsync(
        string endpoint,
        object? body,
        Func<IExchange, CancellationToken, Task<IdentityAnswer?>>? prepare,
        CancellationToken ct)
    {
        var exchange = new Exchange(new Message { Body = body }, _scopes);
        try
        {
            if (prepare is not null && await prepare(exchange, ct).ConfigureAwait(false) is { } refused)
                return refused;

            await _producer.RequestAsync(endpoint, exchange, ct).ConfigureAwait(false);
            if (exchange.Exception is { } failure)
                throw new IdentityUnavailableException($"redb.Identity failed at {endpoint}.", failure);

            return IdentityAnswer.From(exchange);
        }
        catch (OperationCanceledException ex)
        {
            // The caller's token carries the timeout (see the callers).
            throw new IdentityUnavailableException($"redb.Identity did not answer at {endpoint} in time.", ex);
        }
        catch (Exception ex) when (ex is not IdentityUnavailableException)
        {
            // The call itself failed: Identity's routes are not started yet (the worker is still loading
            // it), or it was unloaded and the direct-vm endpoint has no consumer.
            throw new IdentityUnavailableException($"redb.Identity could not be called at {endpoint}.", ex);
        }
        finally
        {
            await exchange.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Runs one more call on an exchange that is already open (Identity's management gate).</summary>
    public Task RequestOnAsync(string endpoint, IExchange exchange, CancellationToken ct) =>
        _producer.RequestAsync(endpoint, exchange, ct);

    public void Dispose() => _producer.Dispose();
}

/// <summary>
/// What Identity answered: the body, plus the error code when the body is an OAuth or management error
/// (<c>{ error, error_description }</c>). Identity answers with typed objects, dictionaries or anonymous
/// objects depending on the route; <see cref="Json"/> reads any of them the same way.
/// </summary>
public sealed record IdentityAnswer(object? Body, string? Error, string? ErrorDescription)
{
    public bool IsError => Error is not null;

    /// <summary>The body as JSON, whatever its .NET type.</summary>
    public JsonElement Json => Body switch
    {
        null => default,
        JsonElement element => element,
        byte[] bytes => JsonDocument.Parse(bytes).RootElement.Clone(),
        string text when text.TrimStart().StartsWith('{') || text.TrimStart().StartsWith('[') => JsonDocument.Parse(text).RootElement.Clone(),
        _ => JsonSerializer.SerializeToElement(Body, Body.GetType()),
    };

    public static IdentityAnswer From(IExchange exchange)
    {
        var body = exchange.Out?.Body ?? exchange.In.Body;
        var answer = new IdentityAnswer(body, null, null);
        var json = body is null ? default : answer.Json;
        return json.ValueKind == JsonValueKind.Object && json.TryGetProperty("error", out var error) && error.ValueKind == JsonValueKind.String
            ? answer with
            {
                Error = error.GetString(),
                ErrorDescription = json.TryGetProperty("error_description", out var description) ? description.GetString() : null,
            }
            : answer;
    }

    public static IdentityAnswer Refused(string error, string? description) => new(null, error, description);
}

/// <summary>Identity failed, timed out, or is not running: the module answers 503, never 401.</summary>
public sealed class IdentityUnavailableException : Exception
{
    public IdentityUnavailableException(string message) : base(message) { }
    public IdentityUnavailableException(string message, Exception inner) : base(message, inner) { }
}
