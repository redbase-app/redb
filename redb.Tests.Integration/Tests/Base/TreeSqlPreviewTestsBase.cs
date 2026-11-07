using redb.Core;
using redb.Core.Query;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// SQL preview of a tree query, for the Free providers. The base implementation of
/// <c>TreeQueryProviderBase.GetSqlPreviewAsync</c> asked the database for a preview routine of the retired search
/// module (<c>get_search_tree_sql_preview_base</c> / <c>dbo.get_search_tree_sql_preview_base</c>), which no provider
/// installs any more, so <c>ToSqlStringAsync</c> on a tree query failed instead of returning SQL. Each Free provider
/// now returns the statement its pvt module compiles for the query, and these tests pin that down.
/// </summary>
public abstract class TreeSqlPreviewTestsBase
{
    protected readonly IRedbService Redb;

    protected TreeSqlPreviewTestsBase(IRedbService redb) => Redb = redb;

    [Fact]
    public async Task Preview_TreeQuery_ReturnsCompiledSql()
    {
        var sql = await Redb.TreeQuery<TreeNodeProps>()
            .Where(n => n.IsActive)
            .Take(10)
            .ToSqlStringAsync();

        sql.Should().NotBeNullOrWhiteSpace("the preview of a tree query must return the statement the provider compiles");
        sql.ToUpperInvariant().Should().Contain("SELECT");
        sql.Should().Contain("_values", "the Props filter is read from the value rows");
        sql.Should().NotContain("get_search_tree_sql_preview_base",
            "no provider installs the preview routine of the retired search module");
    }

    [Fact]
    public async Task Preview_TreeQueryWithOrdering_ContainsOrderBy()
    {
        var sql = await Redb.TreeQuery<TreeNodeProps>()
            .Where(n => n.IsActive)
            .OrderBy(n => n.Budget)
            .Take(10)
            .ToSqlStringAsync();

        sql.Should().NotBeNullOrWhiteSpace();
        sql.ToUpperInvariant().Should().Contain("ORDER BY");
    }
}
