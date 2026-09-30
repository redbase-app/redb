using System.Globalization;
using redb.Core;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Route.Abstractions;
using redb.Route.Http;
using RedbApp.Api.Props;
using RedbApp.Models;

namespace RedbApp.Api.Services;

/// <summary>The category tree: every root with its subtree, add a node, delete a subtree.</summary>
public static class CategoryService
{
    /// <summary><c>GET /api/categories</c>: the top-level nodes from a tree query, each loaded with its subtree.</summary>
    public static async Task TreeAsync(IRedbService redb, IExchange e, CancellationToken ct)
    {
        var roots = await redb.TreeQuery<Category>().WhereRoots().ToListAsync(ct);
        var result = new List<CategoryNode>();
        foreach (var root in roots)
            result.Add(ToNode(await redb.LoadTreeAsync<Category>(root.Id)));
        e.In.Body = result;
    }

    /// <summary><c>POST /api/categories</c>: a top-level node, or a child of <c>ParentId</c>.</summary>
    public static async Task CreateAsync(IRedbService redb, IExchange e, CancellationToken ct)
    {
        var request = (NewCategory)e.In.Body!;
        if (string.IsNullOrWhiteSpace(request.Name))
        {
            e.In.Headers[HttpHeaders.ResponseCode] = 400;
            e.In.Body = new ApiError { Message = "The name is required." };
            return;
        }

        var node = new TreeRedbObject<Category> { Name = request.Name.Trim(), Props = new Category() };
        if (request.ParentId is { } parentId)
        {
            var parent = await redb.LoadAsync<Category>(parentId, cancellationToken: ct);
            if (parent is null)
            {
                e.In.Headers[HttpHeaders.ResponseCode] = 404;
                e.In.Body = new ApiError { Message = $"Category {parentId} was not found." };
                return;
            }
            await redb.CreateChildAsync(node, parent);
        }
        else
        {
            await redb.SaveAsync(node, ct);
        }

        e.In.Headers[HttpHeaders.ResponseCode] = 201;
        e.In.Body = new CategoryNode { Id = node.Id, Name = node.Name ?? "" };
    }

    /// <summary><c>DELETE /api/categories/{id}</c>: deleting a node deletes its whole subtree.</summary>
    public static async Task DeleteAsync(IRedbService redb, IExchange e, CancellationToken ct)
    {
        var id = long.TryParse(e.In.GetHeader<string>("id"), CultureInfo.InvariantCulture, out var v) ? v : 0;
        if (await redb.DeleteAsync(id, ct))
        {
            e.In.Body = null;
            return;
        }
        e.In.Headers[HttpHeaders.ResponseCode] = 404;
        e.In.Body = new ApiError { Message = $"Category {id} was not found." };
    }

    private static CategoryNode ToNode(ITreeRedbObject node) => new()
    {
        Id = node.Id,
        Name = node.Name ?? "",
        Children = node.Children.Select(ToNode).ToList(),
    };
}
