using redb.Core;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Core.Query;
using redb.Core.Query.Aggregation;
using redb.Core.Query.Window;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// A filter on a base field whose SQL is long, here <c>ParentId</c> in some hundreds of ids, next to a filter on a field
/// of the props. The answer is the children of the listed parents, on every provider. On SQL Server Free the base
/// condition is moved out of the props filter as SQL text inside a JSON value and read back with JSON_VALUE, which gives
/// NULL for a text over 4,000 characters: the condition vanished and the query returned every object the props filter
/// let through. Short lists were right.
/// </summary>
public abstract class LongBaseFilterTestsBase
{
    protected readonly IRedbService Redb;

    protected LongBaseFilterTestsBase(IRedbService redb) => Redb = redb;

    private const int Parents = 600;

    [Fact]
    public async Task ParentIdInHundredsOfIds_WithAPropsFilter_ReturnsOnlyTheirChildren()
    {
        await Redb.SyncSchemeAsync<SimpleProps>();
        var tag = "lbf-" + Guid.NewGuid().ToString("N")[..8];

        // Parents, and one more root whose children are not asked for.
        var roots = Enumerable.Range(0, Parents + 1)
            .Select(i => (IRedbObject)new RedbObject<SimpleProps> { name = $"{tag}-p{i}", Props = new SimpleProps { Title = tag } })
            .ToList();
        var rootIds = await Redb.SaveAsync(roots);
        var listed = rootIds.Take(Parents).ToList();
        var other = rootIds[^1];

        // One child per listed parent, every third without a value for N; ten children of the other root.
        var children = listed.Select((pid, i) => (IRedbObject)new RedbObject<SimpleProps>
            {
                name = $"{tag}-c{i}", parent_id = pid,
                Props = new SimpleProps { Title = tag, OptionalNumber = i % 3 == 0 ? null : i % 4 }
            })
            .Concat(Enumerable.Range(0, 10).Select(i => (IRedbObject)new RedbObject<SimpleProps>
            {
                name = $"{tag}-o{i}", parent_id = other, Props = new SimpleProps { Title = tag, OptionalNumber = 1 }
            }))
            .ToList();
        await Redb.SaveAsync(children);
        var withValue = Enumerable.Range(0, Parents).Count(i => i % 3 != 0);

        IRedbQueryable<SimpleProps> Query() => Redb.Query<SimpleProps>()
            .WhereRedb(o => o.ParentId != null && listed.Contains(o.ParentId.Value))
            .Where(p => p.Title == tag);

        (await Query().CountAsync()).Should().Be(Parents, "the children of the listed parents, not every object of the tag");
        (await Query().ToListAsync()).Should().HaveCount(Parents);
        (await Query().Where(p => p.OptionalNumber != null).CountAsync()).Should().Be(withValue);

        var groups = await Query().GroupBy(p => p.OptionalNumber).SelectAsync(g => new { g.Key, Count = Agg.Count(g) });
        groups.Sum(g => g.Count).Should().Be(Parents, "the groups cover the listed parents' children only");

        var sum = Enumerable.Range(0, Parents).Where(i => i % 3 != 0).Sum(i => (long)(i % 4));
        var agg = await Query().AggregateAsync(x => new { Total = Agg.Sum(x.Props.OptionalNumber), Count = Agg.Count() });
        agg.Total.Should().Be(sum, "an aggregate over the listed parents' children");
        agg.Count.Should().Be(Parents);

        var rows = await Query().WithWindow(w => w.OrderBy(p => p.OptionalNumber)).SelectAsync(x => new { x.Id, Row = Win.RowNumber() });
        rows.Should().HaveCount(Parents, "a window over the listed parents' children");

        var tree = await Redb.TreeQuery<SimpleProps>()
            .WhereRedb(o => o.ParentId != null && listed.Contains(o.ParentId.Value))
            .Where(p => p.Title == tag)
            .ToListAsync();
        tree.Should().HaveCount(Parents, "a tree query over the listed parents' children");
    }
}
