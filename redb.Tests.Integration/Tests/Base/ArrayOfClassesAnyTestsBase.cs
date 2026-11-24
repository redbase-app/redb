using System.Linq.Expressions;
using redb.Core;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Core.Query;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// <c>p.Lines.Any(l =&gt; ...)</c> over an array or list of classes. redb Pro reads it on every provider, a predicate
/// over two fields asks both of one element, and a field an element has no value for matches neither == nor != (SQL's
/// rule, as everywhere in redb). The free providers refuse it with a clear message before any SQL; SQL Server Free
/// answered 0 without a word, PostgreSQL and SQLite Free failed in their modules, and Pro could not resolve
/// "Lines[].Kind".
/// </summary>
public abstract class ArrayOfClassesAnyTestsBase
{
    protected readonly IRedbService Redb;

    protected ArrayOfClassesAnyTestsBase(IRedbService redb) => Redb = redb;

    /// <summary>True for the Pro suites, which read the form; the free ones refuse it.</summary>
    protected abstract bool ReadsAnyOverClasses { get; }

    private static readonly DateTime Day = new(2026, 3, 1, 0, 0, 0, DateTimeKind.Utc);

    private static List<OrderSheetProps> Rows(string tag) =>
    [
        // a+active and b+inactive: Kind a and Active true are on the same element.
        new() { Tag = tag, Number = 1, Lines = [new() { Kind = "a", Active = true, Qty = 5, Price = 1.5m, Due = Day, Wait = TimeSpan.FromHours(2) },
                                                new() { Kind = "b", Active = false, Qty = 1 }] },
        // a+inactive and b+active: Kind a and Active true are on different elements.
        new() { Tag = tag, Number = 2, Lines = [new() { Kind = "a", Active = false, Qty = 2, Price = 10m, Due = Day.AddDays(5) },
                                                new() { Kind = "b", Active = true, Qty = 9, Wait = TimeSpan.FromDays(3) }] },
        // an element without Kind and Active.
        new() { Tag = tag, Number = 3, Lines = [new() { Qty = 7, Price = 3m }], Extra = [new() { Kind = "x", Active = true, Qty = 1 }] },
        // an empty array and no array.
        new() { Tag = tag, Number = 4, Lines = [] },
        new() { Tag = tag, Number = 5 },
    ];

    private async Task<(string Tag, List<OrderSheetProps> Rows)> SeedAsync()
    {
        await Redb.SyncSchemeAsync<OrderSheetProps>();
        var tag = "aoc-" + Guid.NewGuid().ToString("N")[..8];
        var rows = Rows(tag);
        await Redb.SaveAsync(rows.Select(p => (IRedbObject)new RedbObject<OrderSheetProps> { name = $"{tag}-{p.Number}", Props = p }).ToList());
        return (tag, rows);
    }

    private async Task<int[]> NumbersAsync(string tag, Expression<Func<OrderSheetProps, bool>> filter)
        => (await Redb.Query<OrderSheetProps>().Where(p => p.Tag == tag).Where(filter).ToListAsync())
            .Select(o => o.Props.Number).OrderBy(n => n).ToArray();

    // SQL's rule over one element: a comparison with a field the element has no value for is not true.
    private static bool Eq<T>(T? a, T b) where T : struct => a.HasValue && a.Value.Equals(b);
    private static int[] Expect(List<OrderSheetProps> rows, Func<OrderSheetProps, bool> rule)
        => rows.Where(rule).Select(r => r.Number).OrderBy(n => n).ToArray();

    [Fact]
    public async Task AnyWithACondition_OverAnArrayOfClasses()
    {
        var (tag, rows) = await SeedAsync();
        if (!ReadsAnyOverClasses)
        {
            var refused = async () => await NumbersAsync(tag, p => p.Lines!.Any(l => l.Kind == "a"));
            (await refused.Should().ThrowAsync<NotSupportedException>()).Which.Message
                .Should().Contain("not supported by the free providers");
            return;
        }

        (await NumbersAsync(tag, p => p.Lines!.Any(l => l.Kind == "a"))).Should().Equal(1, 2);
        (await NumbersAsync(tag, p => p.Lines!.Any(l => l.Active == true))).Should().Equal(1, 2);

        // Both conditions of one element: object 2 has Kind a and Active true on different elements.
        (await NumbersAsync(tag, p => p.Lines!.Any(l => l.Kind == "a" && l.Active == true))).Should().Equal(1);
        (await NumbersAsync(tag, p => p.Lines!.Any(l => l.Kind == "b" && l.Qty > 5))).Should().Equal(2);
        (await NumbersAsync(tag, p => p.Lines!.Any(l => l.Kind == "b" || l.Qty == 7))).Should().Equal(1, 2, 3);

        // SQL's rule inside the element: the element without Kind matches neither == nor !=, nor its negation.
        (await NumbersAsync(tag, p => p.Lines!.Any(l => l.Kind != "a")))
            .Should().Equal(Expect(rows, r => r.Lines?.Any(l => l.Kind != null && l.Kind != "a") == true));
        (await NumbersAsync(tag, p => p.Lines!.Any(l => !(l.Kind == "a"))))
            .Should().Equal(Expect(rows, r => r.Lines?.Any(l => l.Kind != null && l.Kind != "a") == true));
        (await NumbersAsync(tag, p => p.Lines!.Any(l => l.Kind == null))).Should().Equal(3);
        (await NumbersAsync(tag, p => p.Lines!.Any(l => l.Kind != null && l.Active == false))).Should().Equal(1, 2);

        // Ranges and other value types of the element.
        (await NumbersAsync(tag, p => p.Lines!.Any(l => l.Qty >= 7))).Should().Equal(2, 3);
        (await NumbersAsync(tag, p => p.Lines!.Any(l => l.Price > 2m))).Should().Equal(2, 3);
        var due = Day.AddDays(1);
        (await NumbersAsync(tag, p => p.Lines!.Any(l => l.Due > due))).Should().Equal(2);
        (await NumbersAsync(tag, p => p.Lines!.Any(l => l.Wait > TimeSpan.FromDays(1)))).Should().Equal(2);
        var kinds = new List<string?> { "b", "z" };
        (await NumbersAsync(tag, p => p.Lines!.Any(l => kinds.Contains(l.Kind)))).Should().Equal(1, 2);

        // The negation of the whole Any: objects with no such element, an empty or a missing array included.
        (await NumbersAsync(tag, p => !p.Lines!.Any(l => l.Kind == "a"))).Should().Equal(3, 4, 5);

        // With other filters, and over a List<T>.
        (await NumbersAsync(tag, p => p.Number > 1 && p.Lines!.Any(l => l.Kind == "a"))).Should().Equal(2);
        (await NumbersAsync(tag, p => p.Extra!.Any(l => l.Kind == "x" && l.Active == true))).Should().Equal(3);
        (await Redb.Query<OrderSheetProps>().Where(p => p.Tag == tag).Where(p => p.Lines!.Any(l => l.Kind == "a")).CountAsync())
            .Should().Be(2);
    }

    [Fact]
    public async Task AnyOverAnArrayOfClasses_RefusesWhatItDoesNotRead()
    {
        var (tag, _) = await SeedAsync();
        if (!ReadsAnyOverClasses) return;

        var outer = async () => await NumbersAsync(tag, p => p.Lines!.Any(l => l.Qty == p.Number));
        (await outer.Should().ThrowAsync<NotSupportedException>()).Which.Message.Should().Contain("reads only the fields of the element");

        var text = async () => await NumbersAsync(tag, p => p.Lines!.Any(l => l.Kind!.StartsWith("a")));
        (await text.Should().ThrowAsync<NotSupportedException>()).Which.Message.Should().Contain("cannot read");
    }
}
