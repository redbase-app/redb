using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using Microsoft.AspNetCore.Components.Authorization;
using RedbIdentityApp.Contracts;
using RedbIdentityApp.Web.Auth;

namespace RedbIdentityApp.Web.Services;

/// <summary>
/// The module's API, called with the signed-in user's access token. The HttpClient comes configured from
/// Program.cs (the module's address). What the module answers becomes something a page can show:
/// <list type="bullet">
///   <item>401: the token is no longer good (revoked at Identity): the session ends,
///   <see cref="SessionEndedException"/>;</item>
///   <item>403: the user lacks the right (<see cref="ModuleException.Forbidden"/>);</item>
///   <item>503: Identity is down (<see cref="IdentityUnavailableException"/>).</item>
/// </list>
/// </summary>
public sealed class ModuleClient(
    HttpClient http,
    AuthenticationStateProvider authentication,
    AccessTokens tokens,
    UserTokenStore store)
{
    public Task<MeDto> MeAsync() => SendAsync<MeDto>(HttpMethod.Get, "api/me", null);

    public Task<List<NoteDto>> NotesAsync() => SendAsync<List<NoteDto>>(HttpMethod.Get, "api/notes", null);

    public Task<NoteDto> AddNoteAsync(NoteDto note) => SendAsync<NoteDto>(HttpMethod.Post, "api/notes", note);

    public Task DeleteNoteAsync(long id) => SendAsync<object>(HttpMethod.Delete, $"api/notes/{id}", null);

    public Task<Page<UserSummaryDto>> UsersAsync(int page) => SendAsync<Page<UserSummaryDto>>(HttpMethod.Get, $"api/admin/users?page={page}", null);

    private async Task<T> SendAsync<T>(HttpMethod method, string uri, object? body)
    {
        var sessionKey = (await authentication.GetAuthenticationStateAsync()).User.FindFirst(UserTokenStore.SessionClaim)?.Value
            ?? throw new SessionEndedException();
        var accessToken = await tokens.GetAsync(sessionKey, CancellationToken.None);

        using var request = new HttpRequestMessage(method, uri);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
        if (body is not null)
            request.Content = JsonContent.Create(body, body.GetType());

        using var response = await http.SendAsync(request);
        switch (response.StatusCode)
        {
            case HttpStatusCode.Unauthorized:
                await store.RemoveAsync(sessionKey, CancellationToken.None);
                throw new SessionEndedException();
            case HttpStatusCode.ServiceUnavailable:
                throw new IdentityUnavailableException();
            case HttpStatusCode.NoContent:
                return default!;
        }

        if (response.IsSuccessStatusCode)
            return (await response.Content.ReadFromJsonAsync<T>())!;

        var error = response.Content.Headers.ContentType?.MediaType == "application/json"
            ? await response.Content.ReadFromJsonAsync<ApiError>()
            : null;
        throw new ModuleException(
            error?.Message ?? $"The module answered {(int)response.StatusCode} {response.ReasonPhrase}.",
            response.StatusCode == HttpStatusCode.Forbidden);
    }
}

/// <summary>A module call that did not succeed; the message is meant for the user.</summary>
public sealed class ModuleException(string message, bool forbidden) : Exception(message)
{
    public bool Forbidden { get; } = forbidden;
}
