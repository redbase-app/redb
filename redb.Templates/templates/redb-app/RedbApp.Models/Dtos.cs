using System.ComponentModel.DataAnnotations;

namespace RedbApp.Models;

public sealed class LoginRequest
{
    [Required] public string Login { get; set; } = "";
    [Required] public string Password { get; set; } = "";
}

public sealed class LoginResponse
{
    public string Token { get; set; } = "";
    public string Login { get; set; } = "";
    public string Role { get; set; } = "";
    public DateTimeOffset ExpiresAt { get; set; }
}

/// <summary>A product as the API sends and receives it. Validation runs on the form and again on the server.</summary>
public sealed class ProductDto
{
    public long Id { get; set; }

    [Required, StringLength(200)]
    public string Name { get; set; } = "";

    [Required, StringLength(100)]
    public string Category { get; set; } = "";

    [Range(0, 1_000_000)]
    public decimal Price { get; set; }

    public bool InStock { get; set; }

    [StringLength(2000)]
    public string? Description { get; set; }
}

/// <summary>One page of the product list, and the total the filter matches.</summary>
public sealed class ProductPage
{
    public List<ProductDto> Items { get; set; } = [];
    public int Total { get; set; }
    public int Page { get; set; }
    public int PageSize { get; set; }
}

/// <summary>A node of the category tree with its children.</summary>
public sealed class CategoryNode
{
    public long Id { get; set; }
    public string Name { get; set; } = "";
    public List<CategoryNode> Children { get; set; } = [];
}

public sealed class NewCategory
{
    [Required, StringLength(100)]
    public string Name { get; set; } = "";

    /// <summary>The parent node; null for a top-level category.</summary>
    public long? ParentId { get; set; }
}

/// <summary>The body of an error answer.</summary>
public sealed class ApiError
{
    public string Message { get; set; } = "";
}
