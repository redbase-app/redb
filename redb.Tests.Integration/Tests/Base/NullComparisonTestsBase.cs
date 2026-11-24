using System.Linq.Expressions;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Comparisons follow SQL's three-valued logic, the same on every provider, in Free and Pro, with the planner on and
/// off: an object without a value for a field matches neither <c>F == v</c> nor <c>F != v</c>. The same holds for
/// <c>!(F == v)</c>, for <c>F != G</c> when either side has no value, and for <c>!list.Contains(F)</c>. It differs from
/// LINQ to objects and from EF Core's default (both count such an object as "not equal"); the README says so, and these
/// tests keep the providers and the planner modes from drifting apart. <c>F != v || F == null</c> is the documented way
/// to include the objects without a value: it returns exactly the complement of <c>F == v</c>.
///
/// Every expectation is computed in memory from the seed with SQL's rule.
/// </summary>
public abstract class NullComparisonTestsBase
{
    protected readonly IRedbService Redb;

    protected NullComparisonTestsBase(IRedbService redb) => Redb = redb;

    private static readonly DateTime Day = new(2026, 3, 1, 0, 0, 0, DateTimeKind.Utc);

    private sealed record Seeded(string Tag, List<NullComparisonProps> Rows, long ItemA, long ItemB);

    /// <summary>
    /// 42 objects. Each field is empty at a period of its own (the second field of a pair at another), so the pairs
    /// hold both sides set, one side empty and both sides empty.
    /// </summary>
    private async Task<Seeded> SeedAsync()
    {
        await Redb.SyncSchemeAsync<NullComparisonProps>();
        var tag = "nc-" + Guid.NewGuid().ToString("N")[..8];
        var list = await Redb.ListProvider.SaveListAsync(RedbList.Create(tag, tag));
        var items = await Redb.ListProvider.AddItemsAsync(list, ["A", "B"]);

        var rows = Enumerable.Range(1, 42).Select(i => new NullComparisonProps
        {
            Tag = tag,
            N = i % 3 == 0 ? null : i % 4,
            N2 = i % 5 == 0 ? null : i % 3,
            S = i % 4 == 0 ? null : "s" + (i % 3),
            S2 = i % 6 == 0 ? null : "s" + (i % 2),
            D = i % 5 == 0 ? null : Day.AddDays(i % 3),
            D2 = i % 7 == 0 ? null : Day.AddDays(i % 2),
            B = i % 3 == 0 ? null : i % 2 == 0,
            B2 = i % 4 == 0 ? null : i % 3 == 0,
            L = i % 4 == 0 ? null : items[i % 2],
            L2 = i % 3 == 0 ? null : items[(i / 2) % 2],
        }).ToList();

        await Redb.SaveAsync(rows.Select((p, i) => new RedbObject<NullComparisonProps> { name = $"{tag}-{i}", Props = p }).ToList());
        return new Seeded(tag, rows, items[0].Id, items[1].Id);
    }

    private async Task EachPlannerModeAsync(Func<string, Task> check)
    {
        var restore = Redb.Configuration.EnablePvtPrefilter;
        try
        {
            foreach (var on in new[] { false, true })
            {
                Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = on);
                await check(on ? "planner on" : "planner off");
            }
        }
        finally
        {
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = restore);
        }
    }

    /// <summary>
    /// One form: the filter, SQL's answer computed in memory, and C#'s where the filter itself cannot run in memory
    /// (p.L!.Id on an object without a list item).
    /// </summary>
    private sealed record Case(string Name, Expression<Func<NullComparisonProps, bool>> Filter, Func<NullComparisonProps, bool> Sql,
        Func<NullComparisonProps, bool>? CSharp = null);

    private static IEnumerable<Case> Cases(Seeded s)
    {
        int? n = 1; string? text = "s1"; DateTime? d = Day.AddDays(1); bool? b = true; var item = s.ItemA;
        var numbers = new List<int?> { 1, 2 };
        var texts = new List<string?> { "s0", "s1" };
        var days = new List<DateTime?> { Day, Day.AddDays(1) };
        var flags = new List<bool?> { true };
        var itemIds = new List<long> { s.ItemA };

        // F != v
        yield return new("number !=", p => p.N != n, p => p.N.HasValue && p.N != n);
        yield return new("text !=", p => p.S != text, p => p.S != null && p.S != text);
        yield return new("date !=", p => p.D != d, p => p.D.HasValue && p.D != d);
        yield return new("flag !=", p => p.B != b, p => p.B.HasValue && p.B != b);
        yield return new("list item !=", p => p.L!.Id != item, p => p.L != null && p.L.Id != item, p => p.L?.Id != item);

        // !(F == v)
        yield return new("number !(==)", p => !(p.N == n), p => p.N.HasValue && p.N != n);
        yield return new("text !(==)", p => !(p.S == text), p => p.S != null && p.S != text);
        yield return new("date !(==)", p => !(p.D == d), p => p.D.HasValue && p.D != d);
        yield return new("flag !(==)", p => !(p.B == b), p => p.B.HasValue && p.B != b);
        yield return new("list item !(==)", p => !(p.L!.Id == item), p => p.L != null && p.L.Id != item, p => !(p.L?.Id == item));

        // F != G
        yield return new("number field != field", p => p.N != p.N2, p => p.N.HasValue && p.N2.HasValue && p.N != p.N2);
        yield return new("text field != field", p => p.S != p.S2, p => p.S != null && p.S2 != null && p.S != p.S2);
        yield return new("date field != field", p => p.D != p.D2, p => p.D.HasValue && p.D2.HasValue && p.D != p.D2);
        yield return new("flag field != field", p => p.B != p.B2, p => p.B.HasValue && p.B2.HasValue && p.B != p.B2);
        yield return new("list item field != field", p => p.L!.Id != p.L2!.Id, p => p.L != null && p.L2 != null && p.L.Id != p.L2.Id, p => p.L?.Id != p.L2?.Id);

        // !list.Contains(F)
        yield return new("number !Contains", p => !numbers.Contains(p.N), p => p.N.HasValue && !numbers.Contains(p.N));
        yield return new("text !Contains", p => !texts.Contains(p.S), p => p.S != null && !texts.Contains(p.S));
        yield return new("date !Contains", p => !days.Contains(p.D), p => p.D.HasValue && !days.Contains(p.D));
        yield return new("flag !Contains", p => !flags.Contains(p.B), p => p.B.HasValue && !flags.Contains(p.B));
        yield return new("list item !Contains", p => !itemIds.Contains(p.L!.Id), p => p.L != null && !itemIds.Contains(p.L.Id), p => p.L == null || !itemIds.Contains(p.L.Id));
    }

    /// <summary>
    /// Each form against the expectation and, where SQL's rule leaves objects out, against LINQ's: the two must differ
    /// on this seed, or the test would not show which rule the provider follows.
    /// </summary>
    [Fact]
    public async Task NotEqualForms_LeaveOutObjectsWithoutAValue_AsSqlDoes()
    {
        var s = await SeedAsync();
        await EachPlannerModeAsync(async mode =>
        {
            foreach (var c in Cases(s))
            {
                var expected = s.Rows.Where(c.Sql).Count();
                var linq = s.Rows.Where(c.CSharp ?? c.Filter.Compile()).Count();
                linq.Should().BeGreaterThan(expected, $"the seed must tell SQL's rule from LINQ's for {c.Name}");

                var tag = s.Tag;
                var got = await Redb.Query<NullComparisonProps>().Where(p => p.Tag == tag).Where(c.Filter).CountAsync();
                got.Should().Be(expected, $"{c.Name}, {mode}: an object without a value matches neither == nor !=");

                var page = await Redb.Query<NullComparisonProps>().Where(p => p.Tag == tag).Where(c.Filter).ToListAsync();
                page.Should().HaveCount(expected, $"{c.Name}, {mode}, ToListAsync agrees with CountAsync");
            }
        });
    }

    /// <summary>
    /// Contains over a list of dates or flags reads the values. On SQLite the list went to the driver as a DateTime[]
    /// or bool[], which it cannot bind ("No mapping exists from object type System.DateTime[]").
    /// </summary>
    [Fact]
    public async Task ContainsOverAListOfDatesOrFlags_ReadsTheValues()
    {
        var s = await SeedAsync();
        var tag = s.Tag;
        var days = new List<DateTime?> { Day, Day.AddDays(2) };
        var flags = new List<bool?> { false };

        await EachPlannerModeAsync(async mode =>
        {
            (await Redb.Query<NullComparisonProps>().Where(p => p.Tag == tag && days.Contains(p.D)).CountAsync())
                .Should().Be(s.Rows.Count(p => p.D.HasValue && days.Contains(p.D)), $"dates, {mode}");
            (await Redb.Query<NullComparisonProps>().Where(p => p.Tag == tag && flags.Contains(p.B)).CountAsync())
                .Should().Be(s.Rows.Count(p => p.B.HasValue && flags.Contains(p.B)), $"flags, {mode}");
        });
    }

    /// <summary>
    /// Contains and !Contains over a list of every value type a field can have: a list of two values, a list with a
    /// null among them, a list of nulls only and an empty list. A null in the list is dropped ([a, null] filters as [a],
    /// a list of nulls as an empty list). Contains matches the objects whose value is in the list; !Contains the objects
    /// whose value is not, never an object without a value (the != rule); with an empty list Contains matches nothing
    /// and !Contains every object that has a value.
    /// </summary>
    [Fact]
    public async Task ContainsOverAListOfEachValueType_DropsTheNullsOfTheList()
    {
        await Redb.SyncSchemeAsync<ContainsTypesProps>();
        var tag = "ct-" + Guid.NewGuid().ToString("N")[..8];
        var g = new[] { Guid.Parse("11111111-1111-1111-1111-111111111111"), Guid.Parse("22222222-2222-2222-2222-222222222222"), Guid.Parse("33333333-3333-3333-3333-333333333333") };
        var dto = new DateTimeOffset(2026, 3, 1, 10, 0, 0, TimeSpan.Zero);

        // Object i holds value i % 3 of each type, or no value when i % 4 == 3.
        var rows = Enumerable.Range(0, 12).Select(i =>
        {
            var k = i % 3;
            var empty = i % 4 == 3;
            return new ContainsTypesProps
            {
                Tag = tag,
                I64 = empty ? null : 5_000_000_000L + k,
                I32 = empty ? null : 10 + k,
                I16 = empty ? null : (short)(20 + k),
                U8 = empty ? null : (byte)(30 + k),
                F64 = empty ? null : 1.5 + k,
                F32 = empty ? null : 2.5f + k,
                Dec = empty ? null : 3.25m + k,
                Flag = empty ? null : k == 1,
                Ch = empty ? null : (char)('a' + k),
                Text = empty ? null : "t" + k,
                Id = empty ? null : g[k],
                Dt = empty ? null : Day.AddDays(k),
                Dto = empty ? null : dto.AddHours(k),
                Day = empty ? null : DateOnly.FromDateTime(Day).AddDays(k),
                Time = empty ? null : new TimeOnly(8 + k, 30),
                Span = empty ? null : TimeSpan.FromMinutes(90 + k),
                Color = empty ? null : (ContainsColor)k
            };
        }).ToList();
        await Redb.SaveAsync(rows.Select((p, i) => new RedbObject<ContainsTypesProps> { name = $"{tag}-{i}", Props = p }).ToList());

        var failures = new List<string>();
        await EachPlannerModeAsync(async mode =>
        {
            await CheckAsync(p => p.I64, 5_000_000_000L, 5_000_000_001L);
            await CheckAsync(p => p.I32, 10, 11);
            await CheckAsync(p => p.I16, (short)20, (short)21);
            await CheckAsync(p => p.U8, (byte)30, (byte)31);
            await CheckAsync(p => p.F64, 1.5, 2.5);
            await CheckAsync(p => p.F32, 2.5f, 3.5f);
            await CheckAsync(p => p.Dec, 3.25m, 4.25m);
            await CheckAsync(p => p.Flag, false, true);
            await CheckAsync(p => p.Ch, 'a', 'b');
            await CheckTextAsync();
            await CheckAsync(p => p.Id, g[0], g[1]);
            await CheckAsync(p => p.Dt, Day, Day.AddDays(1));
            await CheckAsync(p => p.Dto, dto, dto.AddHours(1));
            await CheckAsync(p => p.Day, DateOnly.FromDateTime(Day), DateOnly.FromDateTime(Day).AddDays(1));
            await CheckAsync(p => p.Time, new TimeOnly(8, 30), new TimeOnly(9, 30));
            await CheckAsync(p => p.Span, TimeSpan.FromMinutes(90), TimeSpan.FromMinutes(91));
            await CheckAsync(p => p.Color, ContainsColor.Red, ContainsColor.Green);

            async Task CheckAsync<TV>(Expression<Func<ContainsTypesProps, TV?>> field, TV a, TV b) where TV : struct
            {
                var read = field.Compile();
                var name = ((MemberExpression)field.Body).Member.Name;
                foreach (var (label, list) in new (string, List<TV?>)[]
                         { ("two values", [a, b]), ("a null in the list", [a, null]), ("nulls only", [null, null]), ("empty", []) })
                {
                    var present = list.Where(v => v.HasValue).Select(v => v!.Value).ToList();
                    var expectedIn = rows.Count(r => read(r) is { } v && present.Contains(v));
                    var expectedNotIn = rows.Count(r => read(r) is { } v && !present.Contains(v));
                    await CountAsync($"{name} Contains, {label}, {mode}", Contains(field, list, negate: false), expectedIn);
                    await CountAsync($"{name} !Contains, {label}, {mode}", Contains(field, list, negate: true), expectedNotIn);
                }
            }

            async Task CheckTextAsync()
            {
                foreach (var (label, list) in new (string, List<string?>)[]
                         { ("two values", ["t0", "t1"]), ("a null in the list", ["t0", null]), ("nulls only", [null]), ("empty", []) })
                {
                    var present = list.OfType<string>().ToList();
                    await CountAsync($"Text Contains, {label}, {mode}", p => list.Contains(p.Text), rows.Count(r => r.Text != null && present.Contains(r.Text)));
                    await CountAsync($"Text !Contains, {label}, {mode}", p => !list.Contains(p.Text), rows.Count(r => r.Text != null && !present.Contains(r.Text)));
                }
            }
        });

        failures.Should().BeEmpty(string.Join("; ", failures));

        async Task CountAsync(string what, Expression<Func<ContainsTypesProps, bool>> filter, int expected)
        {
            try
            {
                var got = await Redb.Query<ContainsTypesProps>().Where(p => p.Tag == tag).Where(filter).CountAsync();
                if (got != expected) failures.Add($"{what}: {got}, expected {expected}");
            }
            catch (Exception ex)
            {
                failures.Add($"{what}: {ex.GetType().Name}: {ex.Message.Split('\n')[0]}");
            }
        }
    }

    /// <summary>A null list itself is refused with ArgumentNullException, not taken for an unsupported expression.</summary>
    [Fact]
    public async Task Contains_OfANullList_IsRefusedAsNull()
    {
        List<int?>? list = null;
        var act = async () => await Redb.Query<ContainsTypesProps>().Where(p => list!.Contains(p.I32)).CountAsync();
        (await act.Should().ThrowAsync<ArgumentNullException>()).Which.Message.Should().Contain("The list of a Contains filter is null");
    }

    private static Expression<Func<ContainsTypesProps, bool>> Contains<TV>(Expression<Func<ContainsTypesProps, TV?>> field, List<TV?> list, bool negate)
        where TV : struct
    {
        var contains = typeof(List<TV?>).GetMethod(nameof(List<TV?>.Contains))!;
        Expression body = Expression.Call(Expression.Constant(list), contains, field.Body);
        if (negate) body = Expression.Not(body);
        return Expression.Lambda<Func<ContainsTypesProps, bool>>(body, field.Parameters);
    }

    /// <summary>The documented way to include the objects without a value: the exact complement of F == v.</summary>
    [Fact]
    public async Task NotEqualOrNull_IsTheComplementOfEqual()
    {
        var s = await SeedAsync();
        var tag = s.Tag;
        int? n = 1; string? text = "s1"; DateTime? d = Day.AddDays(1); bool? b = true; var item = s.ItemA;

        var pairs = new (string Name, Expression<Func<NullComparisonProps, bool>> Equal, Expression<Func<NullComparisonProps, bool>> NotEqualOrNull,
            Func<NullComparisonProps, bool> EqualInMemory)[]
        {
            ("number", p => p.N == n, p => p.N != n || p.N == null, p => p.N == n),
            ("text", p => p.S == text, p => p.S != text || p.S == null, p => p.S == text),
            ("date", p => p.D == d, p => p.D != d || p.D == null, p => p.D == d),
            ("flag", p => p.B == b, p => p.B != b || p.B == null, p => p.B == b),
            ("list item", p => p.L!.Id == item, p => p.L!.Id != item || p.L == null, p => p.L?.Id == item),
        };

        await EachPlannerModeAsync(async mode =>
        {
            foreach (var (name, equal, rest, equalInMemory) in pairs)
            {
                var a = (await Redb.Query<NullComparisonProps>().Where(p => p.Tag == tag).Where(equal).ToListAsync()).Select(o => o.Id).ToHashSet();
                var r = (await Redb.Query<NullComparisonProps>().Where(p => p.Tag == tag).Where(rest).ToListAsync()).Select(o => o.Id).ToHashSet();

                a.Overlaps(r).Should().BeFalse($"{name}, {mode}: no object is both equal and not equal");
                (a.Count + r.Count).Should().Be(s.Rows.Count, $"{name}, {mode}: together they are every object");
                a.Count.Should().Be(s.Rows.Count(equalInMemory), $"{name}, {mode}: == is the same in SQL and in LINQ");
            }
        });
    }
}
