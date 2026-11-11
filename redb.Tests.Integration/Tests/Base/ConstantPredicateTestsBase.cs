using redb.Core;
using redb.Core.Models.Entities;
using redb.Core.Query;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// A predicate that is a constant, alone or inside a condition: Where(x => true) keeps every object, Where(x => false)
/// keeps none, and inside AND / OR / NOT the constant folds as in LINQ over the same objects in memory. The same for
/// WhereRedb and AllAsync, on a flat query and on a tree query, for the list, the count and Any.
///
/// Each test reads only the objects it seeded (by id, or by the root of its tree), so the shared database stays out.
/// </summary>
public abstract class ConstantPredicateTestsBase
{
    protected readonly IRedbService Redb;

    protected ConstantPredicateTestsBase(IRedbService redb) => Redb = redb;

    private const int Total = 4;   // ages 30, 31, 32, 33

    private sealed record Seeded(long RootId, List<long> Ids);

    private async Task<Seeded> SeedAsync()
    {
        await Redb.SyncSchemeAsync<EmployeeProps>();
        var tag = "cp-" + Guid.NewGuid().ToString("N")[..8];
        var root = new TreeRedbObject<EmployeeProps>
        {
            name = $"{tag}-root",
            Props = TestDataFactory.CreateEmployee(0, department: $"{tag}-root").Props
        };
        root.id = await Redb.SaveAsync(root);

        var ids = new List<long>();
        for (var i = 0; i < Total; i++)
        {
            var p = TestDataFactory.CreateEmployee(i + 1, firstName: $"{tag}-{i}", department: tag).Props;
            p.Age = 30 + i;
            ids.Add(await Redb.CreateChildAsync(new TreeRedbObject<EmployeeProps> { name = $"{tag}-{i}", Props = p }, root));
        }
        return new Seeded(root.id, ids);
    }

    private IRedbQueryable<EmployeeProps> Employees(Seeded s, bool tree)
    {
        if (tree)
            return Redb.TreeQuery<EmployeeProps>(s.RootId).WhereRedb(o => o.Id != s.RootId);
        var idSet = s.Ids.ToHashSet();
        return Redb.Query<EmployeeProps>().WhereRedb(o => idSet.Contains(o.Id));
    }

    /// <summary>The query of a case and the number of seeded objects it keeps.</summary>
    private static (IRedbQueryable<EmployeeProps> Query, int Expected) Case(IRedbQueryable<EmployeeProps> q, string kind)
    {
        // A bool value of the caller is the constant it holds.
        var yes = true;
        var no = false;
        return kind switch
        {
            "true" => (q.Where(e => true), Total),
            "false" => (q.Where(e => false), 0),
            "captured-true" => (q.Where(e => yes), Total),
            "captured-false" => (q.Where(e => no), 0),
            "field-and-captured-true" => (q.Where(e => e.Age >= 32 && yes), 2),
            "field-and-captured-false" => (q.Where(e => e.Age >= 32 && no), 0),
            "field-or-captured-true" => (q.Where(e => e.Age >= 32 || yes), Total),
            "field-or-captured-false" => (q.Where(e => e.Age >= 32 || no), 2),
            "not-captured-true" => (q.Where(e => !yes), 0),
            "not-captured-false" => (q.Where(e => !no), Total),
            "nested-captured" => (q.Where(e => (e.Age == 30 || no) && (yes || e.Age == 99)), 1),
            // NOT over a condition that folds to a constant.
            "not-or-literal-true" => (q.Where(e => !(e.Age >= 32 || true)), 0),
            "not-and-literal-false" => (q.Where(e => !(e.Age >= 32 && false)), Total),
            "not-or-literal-false" => (q.Where(e => !(e.Age >= 32 || false)), 2),
            "true-then-field" => (q.Where(e => true).Where(e => e.Age >= 32), 2),
            "field-then-true" => (q.Where(e => e.Age >= 32).Where(e => true), 2),
            "field-then-false" => (q.Where(e => e.Age >= 32).Where(e => false), 0),
            "nested-literal" => (q.Where(e => (e.Age == 30 || false) && (true || e.Age == 99)), 1),
            "field-or-literal-true" => (q.Where(e => e.Age >= 32 || true), Total),
            "field-or-literal-false" => (q.Where(e => e.Age >= 32 || false), 2),
            "field-and-literal-true" => (q.Where(e => e.Age >= 32 && true), 2),
            "field-and-literal-false" => (q.Where(e => e.Age >= 32 && false), 0),
            "redb-true" => (q.WhereRedb(o => true), Total),
            "redb-false" => (q.WhereRedb(o => false), 0),
            "redb-true-then-field" => (q.WhereRedb(o => true).Where(e => e.Age >= 32), 2),
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null)
        };
    }

    public static IEnumerable<object[]> Cases()
    {
        string[] kinds =
        [
            "true", "false", "true-then-field", "field-then-true", "field-then-false", "nested-literal",
            "field-or-literal-true", "field-or-literal-false", "field-and-literal-true", "field-and-literal-false",
            "redb-true", "redb-false", "redb-true-then-field",
            "captured-true", "captured-false", "field-and-captured-true", "field-and-captured-false",
            "field-or-captured-true", "field-or-captured-false", "not-captured-true", "not-captured-false",
            "nested-captured", "not-or-literal-true", "not-and-literal-false", "not-or-literal-false"
        ];
        foreach (var tree in new[] { false, true })
            foreach (var kind in kinds)
                yield return [tree, kind];
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task ConstantInWhere_ListCountAndAnyAgree(bool tree, string kind)
    {
        var s = await SeedAsync();
        var (query, expected) = Case(Employees(s, tree), kind);

        var list = await query.ToListAsync();
        var count = await query.CountAsync();
        var any = await query.AnyAsync();

        list.Should().HaveCount(expected);
        count.Should().Be(expected);
        any.Should().Be(expected > 0);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task All_WithConstant_AnswersAsLinq(bool tree)
    {
        var s = await SeedAsync();
        var q = Employees(s, tree);
        var none = q.Where(e => e.Age > 1000);

        (await q.AllAsync(e => true)).Should().BeTrue();
        (await q.AllAsync(e => false)).Should().BeFalse();
        (await none.AllAsync(e => true)).Should().BeTrue();
        (await none.AllAsync(e => false)).Should().BeTrue("All over an empty set is true for any predicate");
    }
}
