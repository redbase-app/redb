using redb.Core;
using redb.Core.Models.Entities;
using redb.Core.Query;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// A tree query keeps the order OrderBy asks for, and can show its SQL.
///
/// The order used to depend on the plan: the provider sorted the ids in an inner query and then
/// aggregated the rows over a join with no ORDER BY, so the result came back sorted only while
/// the planner chose a nested loop. The seed inserts in an order that is neither the ascending
/// nor the descending one, so insertion order cannot pass for either.
///
/// The preview used to ask the database for a function of the retired search module.
/// </summary>
public abstract class TreeOrderAndPreviewTestsBase
{
    protected readonly IRedbService Redb;

    protected TreeOrderAndPreviewTestsBase(IRedbService redb) => Redb = redb;

    private async Task<string> SeedAsync()
    {
        var tag = Guid.NewGuid().ToString("N")[..8];
        foreach (var (mail, age) in new[] { ("b", 20), ("c", 30), ("a", 10) })
        {
            await Redb.SaveAsync(new RedbObject<PersonProps>
            {
                name = $"order-{tag}-{mail}",
                Props = new PersonProps { Name = $"n-{mail}", Age = age, Email = $"{mail}@order-{tag}" }
            });
        }
        return tag;
    }

    private IRedbQueryable<PersonProps> Tree(string tag)
        => Redb.TreeQuery<PersonProps>().Where(p => p.Email.EndsWith($"@order-{tag}"));

    [Fact]
    public async Task TreeQuery_OrderBy_ReturnsRowsInThatOrder()
    {
        var tag = await SeedAsync();

        (await Tree(tag).OrderBy(p => p.Age).ToListAsync())
            .Select(r => r.Props.Age).Should().Equal([10, 20, 30], "OrderBy(Age)");
        (await Tree(tag).OrderByDescending(p => p.Age).ToListAsync())
            .Select(r => r.Props.Age).Should().Equal([30, 20, 10], "OrderByDescending(Age)");
        (await Tree(tag).OrderBy(p => p.Email).ToListAsync())
            .Select(r => r.Props.Age).Should().Equal([10, 20, 30], "OrderBy(Email): a, b, c");
        (await Tree(tag).OrderByDescending(p => p.Email).Take(2).ToListAsync())
            .Select(r => r.Props.Age).Should().Equal([30, 20], "OrderByDescending(Email) with Take");
    }

    [Fact]
    public async Task TreeQuery_ToSqlString_ReturnsAStatement()
    {
        var tag = await SeedAsync();

        var sql = await Tree(tag).OrderBy(p => p.Age).ToSqlStringAsync();

        sql.Should().NotBeNullOrWhiteSpace();
        sql.Should().Contain("SELECT", "the preview is the statement the query runs");
    }
}
