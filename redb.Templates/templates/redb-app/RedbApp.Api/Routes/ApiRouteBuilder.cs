using redb.Route.Abstractions;
using redb.Route.Core;
using redb.Route.Http.Rest;
using redb.Route.RedbCore.Extensions;
using RedbApp.Api.Services;
using RedbApp.Models;

namespace RedbApp.Api.Routes;

/// <summary>
/// The product and category API. Every verb is a route of its own: the REST declaration binds the request
/// and forwards to a <c>direct:</c> endpoint, where one <c>ProcessWithRedb</c> step does the work. Code in the
/// same process can send to the same <c>direct:</c> endpoints with a <c>ProducerTemplate</c>.
/// <para>Every call needs the token from <c>POST /api/auth/login</c> in <c>Authorization: Bearer ...</c>.</para>
/// <para>An OpenAPI document of each declaration is served at <c>/api/products/openapi.json</c> and
/// <c>/api/categories/openapi.json</c>.</para>
/// </summary>
public sealed class ApiRouteBuilder(ModuleSettings settings) : RouteBuilder
{
    protected override void Configure()
    {
        this.Rest("/api/products", o => ApiOptions.Apply(o, settings, requireToken: true))
            .Get().Produces("application/json").OutType<ProductPage>().To("direct:products-list")
            .Get("/{id}").Produces("application/json").OutType<ProductDto>().To("direct:products-get")
            .Post().Consumes("application/json").Type<ProductDto>().OutType<ProductDto>().To("direct:products-create")
            .Put("/{id}").Consumes("application/json").Type<ProductDto>().OutType<ProductDto>().To("direct:products-update")
            .Delete("/{id}").To("direct:products-delete");

        From("direct:products-list").RouteId("products-list").ProcessWithRedb(ProductService.ListAsync);
        From("direct:products-get").RouteId("products-get").ProcessWithRedb(ProductService.GetAsync);
        From("direct:products-create").RouteId("products-create").ProcessWithRedb(ProductService.CreateAsync);
        From("direct:products-update").RouteId("products-update").ProcessWithRedb(ProductService.UpdateAsync);
        From("direct:products-delete").RouteId("products-delete").ProcessWithRedb(ProductService.DeleteAsync);

        this.Rest("/api/categories", o => ApiOptions.Apply(o, settings, requireToken: true))
            .Get().Produces("application/json").OutType<List<CategoryNode>>().To("direct:categories-tree")
            .Post().Consumes("application/json").Type<NewCategory>().OutType<CategoryNode>().To("direct:categories-create")
            .Delete("/{id}").To("direct:categories-delete");

        From("direct:categories-tree").RouteId("categories-tree").ProcessWithRedb(CategoryService.TreeAsync);
        From("direct:categories-create").RouteId("categories-create").ProcessWithRedb(CategoryService.CreateAsync);
        // Deleting a subtree removes several objects; the transaction makes it all or nothing.
        From("direct:categories-delete").RouteId("categories-delete")
            .Transacted()
                .ProcessWithRedb(CategoryService.DeleteAsync)
            .EndTransaction();
    }
}
