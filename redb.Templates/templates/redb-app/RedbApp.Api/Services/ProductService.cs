using System.ComponentModel.DataAnnotations;
using System.Globalization;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Core.Query;
using redb.Route.Abstractions;
using redb.Route.Http;
using RedbApp.Api.Props;
using RedbApp.Models;

namespace RedbApp.Api.Services;

/// <summary>
/// The product operations. Each method is one <c>ProcessWithRedb</c> step: it gets the request from the
/// exchange (path parameters as headers, query parameters as <c>query.*</c> headers, the JSON body already
/// bound to its type) and leaves the answer as the body.
/// </summary>
public static class ProductService
{
    public const int PageSize = 10;

    /// <summary><c>GET /api/products?search=&amp;inStock=&amp;sort=&amp;page=</c>. Search, sorting and paging run in the database.</summary>
    public static async Task ListAsync(IRedbService redb, IExchange e, CancellationToken ct)
    {
        var search = e.In.GetHeader<string>("query.search")?.Trim();
        var inStockOnly = e.In.GetHeader<string>("query.inStock") == "true";
        var sort = e.In.GetHeader<string>("query.sort") ?? "name";
        var page = int.TryParse(e.In.GetHeader<string>("query.page"), CultureInfo.InvariantCulture, out var p) ? p : 1;

        // The filter is built from scratch for the count and for the page, so the two queries share no state.
        IRedbQueryable<Product> Filtered()
        {
            var query = redb.Query<Product>();
            // Name is the object's own field, so it is filtered with WhereRedb; Props fields use Where.
            if (!string.IsNullOrEmpty(search))
                query = query.WhereRedb(x => x.Name.Contains(search));
            if (inStockOnly)
                query = query.Where(x => x.InStock);
            return query;
        }

        var total = await Filtered().CountAsync(ct);
        var pageCount = Math.Max(1, (total + PageSize - 1) / PageSize);
        page = Math.Clamp(page, 1, pageCount);

        IRedbQueryable<Product> sorted = sort switch
        {
            "price" => Filtered().OrderBy(x => x.Price),
            "-price" => Filtered().OrderByDescending(x => x.Price),
            "category" => Filtered().OrderBy(x => x.Category),
            _ => Filtered().OrderByRedb(x => x.Name),
        };
        var items = await sorted.Skip((page - 1) * PageSize).Take(PageSize).ToListAsync(ct);

        e.In.Body = new ProductPage { Items = items.Select(ToDto).ToList(), Total = total, Page = page, PageSize = PageSize };
    }

    /// <summary><c>GET /api/products/{id}</c>.</summary>
    public static async Task GetAsync(IRedbService redb, IExchange e, CancellationToken ct)
    {
        var product = await redb.LoadAsync<Product>(IdOf(e), cancellationToken: ct);
        e.In.Body = product is null ? NotFound(e) : ToDto(product);
    }

    /// <summary><c>POST /api/products</c>: creates the product and answers 201 with it.</summary>
    public static async Task CreateAsync(IRedbService redb, IExchange e, CancellationToken ct)
    {
        var dto = (ProductDto)e.In.Body!;
        if (Invalid(e, dto))
            return;

        var product = new RedbObject<Product> { Props = new Product() };
        Apply(product, dto);
        await redb.SaveAsync(product, ct);

        e.In.Headers[HttpHeaders.ResponseCode] = 201;
        e.In.Body = ToDto(product);
    }

    /// <summary><c>PUT /api/products/{id}</c>. With change tracking on, the save writes only the fields that changed.</summary>
    public static async Task UpdateAsync(IRedbService redb, IExchange e, CancellationToken ct)
    {
        var dto = (ProductDto)e.In.Body!;
        if (Invalid(e, dto))
            return;

        var product = await redb.LoadAsync<Product>(IdOf(e), cancellationToken: ct);
        if (product is null)
        {
            e.In.Body = NotFound(e);
            return;
        }

        Apply(product, dto);
        await redb.SaveAsync(product, ct);
        e.In.Body = ToDto(product);
    }

    /// <summary><c>DELETE /api/products/{id}</c>: 204, or 404 when there was nothing to delete.</summary>
    public static async Task DeleteAsync(IRedbService redb, IExchange e, CancellationToken ct)
    {
        e.In.Body = await redb.DeleteAsync(IdOf(e), ct) ? null : NotFound(e);
    }

    private static long IdOf(IExchange e) =>
        long.TryParse(e.In.GetHeader<string>("id"), CultureInfo.InvariantCulture, out var id) ? id : 0;

    private static ApiError NotFound(IExchange e)
    {
        e.In.Headers[HttpHeaders.ResponseCode] = 404;
        return new ApiError { Message = $"Product {e.In.GetHeader<string>("id")} was not found." };
    }

    // The browser validates the form, but the API cannot trust that: the same attributes are checked here.
    private static bool Invalid(IExchange e, ProductDto dto)
    {
        var errors = new List<ValidationResult>();
        if (Validator.TryValidateObject(dto, new ValidationContext(dto), errors, validateAllProperties: true))
            return false;

        e.In.Headers[HttpHeaders.ResponseCode] = 400;
        e.In.Body = new ApiError { Message = string.Join(" ", errors.Select(r => r.ErrorMessage)) };
        return true;
    }

    private static void Apply(RedbObject<Product> product, ProductDto dto)
    {
        product.Name = dto.Name;
        product.Props!.Category = dto.Category;
        product.Props.Price = dto.Price;
        product.Props.InStock = dto.InStock;
        product.Props.Description = dto.Description;
    }

    private static ProductDto ToDto(RedbObject<Product> o) => new()
    {
        Id = o.Id,
        Name = o.Name ?? "",
        Category = o.Props!.Category,
        Price = o.Props.Price,
        InStock = o.Props.InStock,
        Description = o.Props.Description,
    };
}
