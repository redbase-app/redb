using redb.Core.Models.Entities;
using redb.Route.Controllers.Attributes;
using RedbIdentityApp.Contracts;
using RedbIdentityApp.Module.Props;

namespace RedbIdentityApp.Module.Controllers;

/// <summary>
/// The notes of the signed-in user. Every query is filtered by the token's subject: a user never reaches
/// another user's note, not even by its id.
/// </summary>
[Route("/api/notes")]
public sealed class NotesController : ApiController
{
    /// <summary><c>GET /api/notes</c>: the caller's notes, newest first.</summary>
    [HttpGet]
    public async Task<object> List()
    {
        if (!IsUser)
            return Status(403, new ApiError { Message = "Notes belong to users; a client token has none." });

        var owner = Subject;
        var notes = await Redb.Query<Note>()
            .Where(n => n.Owner == owner)
            .OrderByDescendingRedb(n => n.DateCreate)
            .Take(100)
            .ToListAsync();
        return notes.Select(ToDto).ToList();
    }

    /// <summary><c>POST /api/notes</c>: a new note of the caller.</summary>
    [HttpPost]
    public async Task<object> Create([FromBody] NoteDto note)
    {
        if (!IsUser)
            return Status(403, new ApiError { Message = "Notes belong to users; a client token has none." });
        if (Invalid(note) is { } invalid)
            return invalid;

        var saved = new RedbObject<Note>
        {
            Name = note.Title.Trim(),
            Props = new Note { Owner = Subject, Text = note.Text },
        };
        await Redb.SaveAsync(saved);
        return Status(201, ToDto(saved));
    }

    /// <summary><c>DELETE /api/notes/{id}</c>: only a note of the caller; any other id is "not found".</summary>
    [HttpDelete("{id}")]
    public async Task<object?> Delete(long id)
    {
        var note = await Redb.LoadAsync<Note>(id);
        if (note is null || note.Props.Owner != Subject)
            return NotFound("No such note.");

        return await Redb.DeleteAsync(id) ? null : NotFound("No such note.");
    }

    private static NoteDto ToDto(RedbObject<Note> note) => new()
    {
        Id = note.Id,
        Title = note.Name ?? "",
        Text = note.Props.Text,
        CreatedAt = note.DateCreate,
    };
}
