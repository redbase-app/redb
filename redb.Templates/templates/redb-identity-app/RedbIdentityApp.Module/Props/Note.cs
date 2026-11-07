using redb.Core.Attributes;

namespace RedbIdentityApp.Module.Props;

/// <summary>
/// A note of a user, stored in the worker's default redb. The title is the object's name; the owner is
/// the token's subject, so each user sees only their own notes.
/// </summary>
[RedbScheme("redbidentityapp.note")]
public class Note
{
    public string Owner { get; set; } = "";
    public string? Text { get; set; }
}
