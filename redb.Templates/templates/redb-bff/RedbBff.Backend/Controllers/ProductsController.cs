using System.Globalization;
using redb.Core.Models.Entities;
using redb.Core.Query;
using redb.Route.Controllers.Attributes;
using RedbBff.Backend.Props;
using RedbBff.Models;

namespace RedbBff.Backend.Controllers;

/// <summary>
/// The products. Attributes route the request: <c>[HttpGet("{id}")]</c> binds the path segment to the
/// <c>id</c> parameter, <c>[FromQuery]</c> a query parameter, <c>[FromBody]</c> the JSON body. The return
/// value is the JSON answer; <c>null</c> answers 204.
/// </summary>
[Route("/api/products")]
public sealed class ProductsController : ApiController
{
    public const int PageSize = 10;

    /// <summary><c>GET /api/products?search=&amp;inStock=&amp;sort=&amp;page=</c>. Search, sorting and paging run in the database.</summary>
    [HttpGet]
    public async Task<ProductPage> List(
        [FromQuery("search")] string? search,
        [FromQuery("inStock")] string? inStock,
        [FromQuery("sort")] string? sort,
        [FromQuery("page")] string? page)
    {
        var redb = Redb;
        var term = search?.Trim();
        var inStockOnly = inStock == "true";

        // The filter is built from scratch for the count and for the page, so the two queries share no state.
        IRedbQueryable<Product> Filtered()
        {
            var query = redb.Query<Product>();
            // Name is the object's own field, so it is filtered with WhereRedb; Props fields use Where.
            if (!string.IsNullOrEmpty(term))
                query = query.WhereRedb(x => x.Name.Contains(term));
            if (inStockOnly)
                query = query.Where(x => x.InStock);
            return query;
        }

        var total = await Filtered().CountAsync();
        var pageCount = Math.Max(1, (total + PageSize - 1) / PageSize);
        var number = Math.Clamp(int.TryParse(page, CultureInfo.InvariantCulture, out var p) ? p : 1, 1, pageCount);

        IRedbQueryable<Product> sorted = sort switch
        {
            "price" => Filtered().OrderBy(x => x.Price),
            "-price" => Filtered().OrderByDescending(x => x.Price),
            "category" => Filtered().OrderBy(x => x.Category),
            _ => Filtered().OrderByRedb(x => x.Name),
        };
        var items = await sorted.Skip((number - 1) * PageSize).Take(PageSize).ToListAsync();

        return new ProductPage { Items = items.Select(ToDto).ToList(), Total = total, Page = number, PageSize = PageSize };
    }

    [HttpGet("{id}")]
    public async Task<object> Get([FromRoute("id")] long id)
    {
        var product = await Redb.LoadAsync<Product>(id);
        return product is null ? NotFound($"Product {id} was not found.") : ToDto(product);
    }

    /// <summary>Creates the product and answers 201 with it.</summary>
    [HttpPost]
    public async Task<object> Create([FromBody] ProductDto dto)
    {
        if (Invalid(dto) is { } error)
            return error;

        var product = new RedbObject<Product> { Props = new Product() };
        Apply(product, dto);
        await Redb.SaveAsync(product);
        return Status(201, ToDto(product));
    }

    /// <summary>With change tracking on, the save writes only the fields that changed.</summary>
    [HttpPut("{id}")]
    public async Task<object> Update([FromRoute("id")] long id, [FromBody] ProductDto dto)
    {
        if (Invalid(dto) is { } error)
            return error;

        var redb = Redb;
        var product = await redb.LoadAsync<Product>(id);
        if (product is null)
            return NotFound($"Product {id} was not found.");

        Apply(product, dto);
        await redb.SaveAsync(product);
        return ToDto(product);
    }

    [HttpDelete("{id}")]
    public async Task<object?> Delete([FromRoute("id")] long id) =>
        await Redb.DeleteAsync(id) ? null : NotFound($"Product {id} was not found.");

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
