using System.Net.Http.Json;
using RedbBff.Models;

namespace RedbBff.Web.Services;

/// <summary>
/// The backend calls of the web server. The HttpClient comes configured from Program.cs: the backend's
/// address and the service key as the bearer token.
/// </summary>
public sealed class BackendClient(HttpClient http)
{
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
        using var response = await http.GetAsync(uri);
        await EnsureSuccessAsync(response);
        return (await response.Content.ReadFromJsonAsync<T>())!;
    }

    private async Task SendAsync(HttpMethod method, string uri, object? body)
    {
        using var request = new HttpRequestMessage(method, uri);
        if (body is not null)
            request.Content = JsonContent.Create(body, body.GetType());
        using var response = await http.SendAsync(request);
        await EnsureSuccessAsync(response);
    }

    private static async Task EnsureSuccessAsync(HttpResponseMessage response)
    {
        if (response.IsSuccessStatusCode)
            return;

        var error = response.Content.Headers.ContentType?.MediaType == "application/json"
            ? await response.Content.ReadFromJsonAsync<ApiError>()
            : null;
        throw new BackendException(error?.Message ?? $"The backend answered {(int)response.StatusCode} {response.ReasonPhrase}.");
    }
}

/// <summary>A backend call that did not succeed; the message is meant for the user.</summary>
public sealed class BackendException(string message) : Exception(message);
