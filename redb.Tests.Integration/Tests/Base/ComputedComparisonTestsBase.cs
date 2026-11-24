using System.Linq.Expressions;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// A comparison of a field with a computed value answers as LINQ over the same objects does: a value of the caller built
/// from several parts (tag + "-x", a + 1, x ?? "d", a ternary), a field against an expression over fields (A == B + 1),
/// a field against a field (A > B), on either side of the operator. Each of these failed before with "variable 'p' ...
/// is not defined": the side without arithmetic was evaluated as a constant, the field side included.
///
/// Each case runs on four objects of its own and is checked against the same predicate compiled and run in memory.
/// </summary>
public abstract class ComputedComparisonTestsBase
{
    protected readonly IRedbService Redb;

    protected ComputedComparisonTestsBase(IRedbService redb) => Redb = redb;

    private static List<ComputedComparisonProps> Seeds(string tag) =>
    [
        new() { Tag = tag, First = "Ann", Last = "Lee", Full = "Ann Lee", A = 3, B = 2, Price = 10.5m, Limit = 21m },
        new() { Tag = tag, First = "Bob", Last = "Ray", Full = "Bob X", A = 5, B = 5, Price = 7m, Limit = 7m },
        new() { Tag = tag, First = "Cid", Last = "Moe", Full = tag + "-x", A = 1, B = 4, Price = 2.25m, Limit = 3m },
        new() { Tag = tag, First = "Dee", Last = "Ito", Full = "Dee-y", A = 9, B = 4, Price = 40m, Limit = 80m },
    ];

    public static IEnumerable<object[]> Cases() =>
    [
        // A value of the caller built from parts, the field alone on the other side.
        ["closure-concat"], ["closure-concat-reversed"], ["closure-arithmetic"], ["closure-coalesce"], ["closure-ternary"],
        // A field against an expression over fields, either side, and a field against a field.
        ["field-eq-field-plus-1"], ["field-plus-1-eq-field"], ["field-eq-field"], ["field-gt-field"],
        ["field-eq-field-times-2"], ["decimal-field-eq-field-times-2"], ["field-eq-concat-of-fields"],
        ["field-eq-field-concat-caller"], ["field-eq-field-minus-caller"], ["not-field-eq-field-plus-1"],
    ];

    private static Expression<Func<ComputedComparisonProps, bool>> Predicate(string kind, string tag)
    {
        var one = 1;
        string? none = null;
        var flag = true;
        var suffix = "-y";
        var k = 3;
        return kind switch
        {
            "closure-concat" => p => p.Full == tag + "-x",
            "closure-concat-reversed" => p => tag + "-x" == p.Full,
            "closure-arithmetic" => p => p.A == one + 2,
            "closure-coalesce" => p => p.Full == (none ?? tag + "-x"),
            "closure-ternary" => p => p.Full == (flag ? tag + "-x" : "other"),
            "field-eq-field-plus-1" => p => p.A == p.B + 1,
            "field-plus-1-eq-field" => p => p.B + 1 == p.A,
            "field-eq-field" => p => p.A == p.B,
            "field-gt-field" => p => p.A > p.B,
            "field-eq-field-times-2" => p => p.A * 2 > p.B + 4,
            "decimal-field-eq-field-times-2" => p => p.Limit == p.Price * 2,
            "field-eq-concat-of-fields" => p => p.Full == p.First + " " + p.Last,
            "field-eq-field-concat-caller" => p => p.Full == p.First + suffix,
            "field-eq-field-minus-caller" => p => p.A == p.B - k + 2,
            "not-field-eq-field-plus-1" => p => !(p.A == p.B + 1),
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null)
        };
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task ComputedComparison_AnswersAsLinq(string kind)
    {
        await Redb.SyncSchemeAsync<ComputedComparisonProps>();
        var tag = "cc-" + Guid.NewGuid().ToString("N")[..8];
        var seeds = Seeds(tag);
        await Redb.SaveAsync(seeds.Select((p, i) => new RedbObject<ComputedComparisonProps> { name = $"{tag}-{i}", Props = p }).ToList());

        var predicate = Predicate(kind, tag);
        var expected = seeds.Where(predicate.Compile()).Select(p => p.First).OrderBy(x => x).ToList();

        var query = Redb.Query<ComputedComparisonProps>().Where(p => p.Tag == tag).Where(predicate);
        var rows = await query.ToListAsync();
        rows.Select(r => r.Props.First).OrderBy(x => x).Should().Equal(expected, $"case {kind}");
        (await query.CountAsync()).Should().Be(expected.Count, $"case {kind}, count");
    }
}
