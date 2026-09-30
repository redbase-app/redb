using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Core.Query;
using RedbRazor.Models;

namespace RedbRazor.Pages;

/// <summary>
/// Product list. Search, sorting and paging all run in the database: the page never loads more than
/// one page of objects.
/// </summary>
public class IndexModel(IRedbService redb) : PageModel
{
    public const int PageSize = 10;

    [BindProperty(SupportsGet = true)] public string? Search { get; set; }
    [BindProperty(SupportsGet = true)] public bool InStockOnly { get; set; }
    [BindProperty(SupportsGet = true)] public string Sort { get; set; } = "name";
    [BindProperty(SupportsGet = true, Name = "p")] public int PageNumber { get; set; } = 1;

    public List<RedbObject<Product>> Items { get; private set; } = [];
    public int Total { get; private set; }
    public int PageCount => Math.Max(1, (Total + PageSize - 1) / PageSize);

    public async Task OnGetAsync()
    {
        Total = await Filtered().CountAsync();
        PageNumber = Math.Clamp(PageNumber, 1, PageCount);

        Items = await Sorted(Filtered())
            .Skip((PageNumber - 1) * PageSize)
            .Take(PageSize)
            .ToListAsync();
    }

    public async Task<IActionResult> OnPostDeleteAsync(long id)
    {
        await redb.DeleteAsync(id);
        return RedirectToPage(new { Search, InStockOnly, Sort, p = PageNumber });
    }

    // The filter is built from scratch for the count and for the page, so the two queries do not
    // share state.
    private IRedbQueryable<Product> Filtered()
    {
        var query = redb.Query<Product>();

        // Name is the object's own field, so it is filtered with WhereRedb; Props fields use Where.
        // The value goes through a local, so the expression captures a plain value, not the page model.
        var search = Search?.Trim();
        if (!string.IsNullOrEmpty(search))
            query = query.WhereRedb(x => x.Name.Contains(search));

        if (InStockOnly)
            query = query.Where(x => x.InStock);

        return query;
    }

    private IRedbQueryable<Product> Sorted(IRedbQueryable<Product> query) => Sort switch
    {
        "price" => query.OrderBy(x => x.Price),
        "-price" => query.OrderByDescending(x => x.Price),
        "category" => query.OrderBy(x => x.Category),
        _ => query.OrderByRedb(x => x.Name),
    };
}
