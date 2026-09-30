using System.ComponentModel.DataAnnotations;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using redb.Core;
using redb.Core.Models.Entities;
using RedbRazor.Models;

namespace RedbRazor.Pages;

/// <summary>
/// Create (no id) and edit (with id) on one page.
/// </summary>
public class EditModel(IRedbService redb) : PageModel
{
    [BindProperty] public ProductInput Input { get; set; } = new();

    public long? Id { get; private set; }

    public async Task<IActionResult> OnGetAsync(long? id)
    {
        if (id is null)
            return Page();

        var product = await redb.LoadAsync<Product>(id.Value);
        if (product is null)
            return NotFound();

        Id = product.Id;
        Input = new ProductInput
        {
            Name = product.Name ?? string.Empty,
            Category = product.Props!.Category,
            Price = product.Props.Price,
            InStock = product.Props.InStock,
            Description = product.Props.Description,
        };
        return Page();
    }

    public async Task<IActionResult> OnPostAsync(long? id)
    {
        Id = id;
        if (!ModelState.IsValid)
            return Page();

        RedbObject<Product> product;
        if (id is null)
        {
            product = new RedbObject<Product> { Props = new Product() };
        }
        else
        {
            // Load, then change: with change tracking on, the save writes only the fields that
            // differ from what was loaded.
            product = await redb.LoadAsync<Product>(id.Value) ?? throw new InvalidOperationException($"Product {id} not found.");
        }

        product.Name = Input.Name;
        product.Props!.Category = Input.Category;
        product.Props.Price = Input.Price;
        product.Props.InStock = Input.InStock;
        product.Props.Description = Input.Description;

        await redb.SaveAsync(product);
        return RedirectToPage("/Index");
    }

    /// <summary>What the form posts. Validation lives here, not on the Props class.</summary>
    public class ProductInput
    {
        [Required, StringLength(200)]
        public string Name { get; set; } = string.Empty;

        [Required, StringLength(100)]
        public string Category { get; set; } = string.Empty;

        [Range(0, 1_000_000)]
        public decimal Price { get; set; }

        public bool InStock { get; set; }

        [StringLength(2000)]
        public string? Description { get; set; }
    }
}
