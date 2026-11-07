using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using Microsoft.AspNetCore.Components;
using RedbApp.Models;

namespace RedbApp.Web.Services;

/// <summary>
/// The API calls of the client. Every call carries the token of the <see cref="Session"/>; a 401 answer
/// means the token is gone or expired, so the session ends and the user is sent to the sign-in page.
/// </summary>
public sealed class ApiClient(HttpClient http, Session session, NavigationManager navigation)
{
    public async Task<string?> LoginAsync(LoginRequest request)
    {
        using var response = await http.PostAsJsonAsync("api/auth/login", request);
        if (!response.IsSuccessStatusCode)
            return await ErrorOf(response);

        session.SignIn((await response.Content.ReadFromJsonAsync<LoginResponse>())!);
        return null;
    }

    public Task<ProductPage> ProductsAsync(string? search, bool inStockOnly, string sort, int page) =>
        GetAsync<ProductPage>($"api/products?search={Uri.EscapeDataString(search ?? "")}" +
                              $"&inStock={(inStockOnly ? "true" : "false")}&sort={Uri.EscapeDataString(sort)}&page={page}");

    public Task<ProductDto> ProductAsync(long id) => GetAsync<ProductDto>($"api/products/{id}");

    public Task SaveProductAsync(ProductDto product) => product.Id == 0
        ? SendAsync(HttpMethod.Post, "api/products", product)
        : SendAsync(HttpMethod.Put, $"api/products/{product.Id}", product);

    public Task DeleteProductAsync(long id) => SendAsync(HttpMethod.Delete, $"api/products/{id}", null);

    public Task<List<CategoryNode>> CategoriesAsync() => GetAsync<List<CategoryNode>>("api/categories");

    public Task AddCategoryAsync(NewCategory category) => SendAsync(HttpMethod.Post, "api/categories", category);

    public Task DeleteCategoryAsync(long id) => SendAsync(HttpMethod.Delete, $"api/categories/{id}", null);

    private async Task<T> GetAsync<T>(string uri)
    {
        using var response = await SendRawAsync(HttpMethod.Get, uri, null);
        return (await response.Content.ReadFromJsonAsync<T>())!;
    }

    private async Task SendAsync(HttpMethod method, string uri, object? body)
    {
        using var response = await SendRawAsync(method, uri, body);
    }

    private async Task<HttpResponseMessage> SendRawAsync(HttpMethod method, string uri, object? body)
    {
        using var request = new HttpRequestMessage(method, uri);
        if (session.Token is { } token)
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        if (body is not null)
            request.Content = JsonContent.Create(body, body.GetType());

        var response = await http.SendAsync(request);
        if (response.StatusCode == HttpStatusCode.Unauthorized)
        {
            response.Dispose();
            session.SignOut();
            navigation.NavigateTo("login");
            throw new ApiException("Your session has ended. Please sign in again.");
        }
        if (!response.IsSuccessStatusCode)
        {
            var message = await ErrorOf(response);
            response.Dispose();
            throw new ApiException(message);
        }
        return response;
    }

    private static async Task<string> ErrorOf(HttpResponseMessage response)
    {
        var error = response.Content.Headers.ContentType?.MediaType == "application/json"
            ? await response.Content.ReadFromJsonAsync<ApiError>()
            : null;
        return error?.Message ?? $"The server answered {(int)response.StatusCode} {response.ReasonPhrase}.";
    }
}

/// <summary>An API call that did not succeed; the message is meant for the user.</summary>
public sealed class ApiException(string message) : Exception(message);
