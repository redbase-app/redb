using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Route.Controllers.Attributes;
using RedbBff.Backend.Props;
using RedbBff.Models;

namespace RedbBff.Backend.Controllers;

/// <summary>The category tree: every root with its subtree, add a node, delete a subtree.</summary>
[Route("/api/categories")]
public sealed class CategoriesController : ApiController
{
    /// <summary>The top-level nodes from a tree query, each loaded with its subtree.</summary>
    [HttpGet]
    public async Task<List<CategoryNode>> Tree()
    {
        var redb = Redb;
        var roots = await redb.TreeQuery<Category>().WhereRoots().ToListAsync();
        var result = new List<CategoryNode>();
        foreach (var root in roots)
            result.Add(ToNode(await redb.LoadTreeAsync<Category>(root.Id)));
        return result;
    }

    /// <summary>A top-level node, or a child of <c>ParentId</c>.</summary>
    [HttpPost]
    public async Task<object> Create([FromBody] NewCategory request)
    {
        if (Invalid(request) is { } error)
            return error;

        var redb = Redb;
        var node = new TreeRedbObject<Category> { Name = request.Name.Trim(), Props = new Category() };
        if (request.ParentId is { } parentId)
        {
            var parent = await redb.LoadAsync<Category>(parentId);
            if (parent is null)
                return NotFound($"Category {parentId} was not found.");
            await redb.CreateChildAsync(node, parent);
        }
        else
        {
            await redb.SaveAsync(node);
        }
        return Status(201, new CategoryNode { Id = node.Id, Name = node.Name ?? "" });
    }

    /// <summary>Deleting a node deletes its whole subtree.</summary>
    [HttpDelete("{id}")]
    public async Task<object?> Delete([FromRoute("id")] long id) =>
        await Redb.DeleteAsync(id) ? null : NotFound($"Category {id} was not found.");

    private static CategoryNode ToNode(ITreeRedbObject node) => new()
    {
        Id = node.Id,
        Name = node.Name ?? "",
        Children = node.Children.Select(ToNode).ToList(),
    };
}
