using System.ComponentModel.DataAnnotations;

namespace RedbIdentityApp.Contracts;

/// <summary>Who the module sees behind the bearer token: what Identity answered to the introspection.</summary>
public sealed class MeDto
{
    public string Subject { get; set; } = "";
    public long? UserId { get; set; }
    public string? UserName { get; set; }
    public string? ClientId { get; set; }
    public List<string> Scopes { get; set; } = [];
    public List<string> Roles { get; set; } = [];
    public DateTimeOffset? ExpiresAt { get; set; }
}

/// <summary>A note of the signed-in user. Each user sees only their own notes.</summary>
public sealed class NoteDto
{
    public long Id { get; set; }

    [Required, StringLength(200, MinimumLength = 1)]
    public string Title { get; set; } = "";

    [StringLength(4000)]
    public string? Text { get; set; }

    public DateTimeOffset CreatedAt { get; set; }
}

/// <summary>One row of the administrator's user list, read from Identity's management API.</summary>
public sealed class UserSummaryDto
{
    public long Id { get; set; }
    public string Login { get; set; } = "";
    public string? Name { get; set; }
    public string? Email { get; set; }
    public bool Enabled { get; set; }
}

/// <summary>A page of a list.</summary>
public sealed class Page<T>
{
    public List<T> Items { get; set; } = [];
    public int Total { get; set; }
}

/// <summary>An error answer of the module; the message is meant for the user.</summary>
public sealed class ApiError
{
    public string Message { get; set; } = "";
}
