using redb.Core;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Core.Query;
using redb.Core.Query.Aggregation;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// TimeSpan and TimeOnly are stored as their Ticks in <c>_values._Long</c>. As text ("c" form) they compared, sorted and
/// grouped as strings: past a day ("10.02:00:00" below "2.02:00:00") and below zero the order was wrong, and a GroupBy
/// over a TimeSpan? key failed to build its result. Values past a day, negative values, sub-second ticks, nulls,
/// arrays, lists and dictionary values on every provider.
/// </summary>
public abstract class TimeTicksTestsBase
{
    protected readonly IRedbService Redb;

    protected TimeTicksTestsBase(IRedbService redb) => Redb = redb;

    private static readonly TimeSpan[] SpanValues =
    [
        TimeSpan.FromHours(-2),                                   // negative
        TimeSpan.FromMinutes(-1),
        TimeSpan.Zero,
        TimeSpan.FromHours(23),
        TimeSpan.FromDays(1).Add(TimeSpan.FromHours(2)),           // past a day
        TimeSpan.FromDays(2).Add(TimeSpan.FromTicks(1234567)),    // sub-second ticks
        TimeSpan.FromDays(10).Add(TimeSpan.FromHours(2)),
    ];

    private async Task<(string Tag, List<long> Ids)> SeedAsync()
    {
        await Redb.SyncSchemeAsync<TimeTicksProps>();
        var tag = "tt-" + Guid.NewGuid().ToString("N")[..8];
        var rows = SpanValues.Select((s, i) => new TimeTicksProps
        {
            Tag = tag,
            Span = s,
            MaybeSpan = i % 3 == 0 ? null : s,
            Clock = new TimeOnly(i * 3, 15, 30, 123).Add(TimeSpan.FromTicks(4567)),
            MaybeClock = i % 2 == 0 ? null : new TimeOnly(23, 59, 59, 999),
            Spans = i == 0 ? [] : [s, TimeSpan.FromDays(i), TimeSpan.FromHours(-i)],
            Clocks = [new TimeOnly(0, 0), new TimeOnly(i, 30)],
            MaybeSpans = [s, null],
            SpanByName = new() { ["a"] = s, ["b"] = TimeSpan.FromDays(-i) },
        }).ToList();
        var ids = await Redb.SaveAsync(rows.Select((p, i) => (IRedbObject)new RedbObject<TimeTicksProps> { name = $"{tag}-{i}", Props = p }).ToList());
        return (tag, ids);
    }

    private IRedbQueryable<TimeTicksProps> Tagged(string tag) => Redb.Query<TimeTicksProps>().Where(p => p.Tag == tag);

    [Fact]
    public async Task Values_AreStoredAsTicks_AndComeBackAsThemselves()
    {
        var (tag, ids) = await SeedAsync();

        // The stored form: Ticks in _Long, no text.
        var stored = await Redb.Context.ExecuteScalarAsync<long>(
            "SELECT v._Long FROM _values v JOIN _structures s ON s._id = v._id_structure " +
            $"WHERE v._id_object = {ids[4]} AND s._name = 'Span' AND v._array_index IS NULL");
        stored.Should().Be(SpanValues[4].Ticks);
        (await Redb.Context.ExecuteScalarAsync<long>(
            "SELECT COUNT(*) FROM _values v JOIN _structures s ON s._id = v._id_structure " +
            $"WHERE v._id_object = {ids[4]} AND s._name IN ('Span', 'Clock', 'MaybeSpan') AND v._String IS NOT NULL"))
            .Should().Be(0, "no TimeSpan or TimeOnly value is stored as text");

        for (var i = 0; i < ids.Count; i++)
        {
            var o = await Redb.LoadAsync<TimeTicksProps>(ids[i]);
            var p = o!.Props;
            var s = SpanValues[i];
            p.Span.Should().Be(s);
            p.MaybeSpan.Should().Be(i % 3 == 0 ? null : s);
            p.Clock.Should().Be(new TimeOnly(i * 3, 15, 30, 123).Add(TimeSpan.FromTicks(4567)));
            p.MaybeClock.Should().Be(i % 2 == 0 ? null : new TimeOnly(23, 59, 59, 999));
            p.Spans.Should().Equal(i == 0 ? [] : [s, TimeSpan.FromDays(i), TimeSpan.FromHours(-i)]);
            p.Clocks.Should().Equal(new TimeOnly(0, 0), new TimeOnly(i, 30));
            p.MaybeSpans.Should().Equal(s, null);
            p.SpanByName.Should().BeEquivalentTo(new Dictionary<string, TimeSpan> { ["a"] = s, ["b"] = TimeSpan.FromDays(-i) });
        }

        // The query path materializes the same values.
        var listed = await Tagged(tag).ToListAsync();
        listed.Select(o => o.Props.Span).Should().BeEquivalentTo(SpanValues);
    }

    [Fact]
    public async Task Filters_CompareChronologically_PastADay_AndBelowZero()
    {
        var (tag, _) = await SeedAsync();

        async Task<TimeSpan[]> Spans(System.Linq.Expressions.Expression<Func<TimeTicksProps, bool>> filter)
            => (await Tagged(tag).Where(filter).ToListAsync()).Select(o => o.Props.Span).OrderBy(x => x).ToArray();

        (await Spans(p => p.Span > TimeSpan.FromHours(20))).Should().Equal(SpanValues.Where(s => s > TimeSpan.FromHours(20)));
        (await Spans(p => p.Span < TimeSpan.Zero)).Should().Equal(SpanValues.Where(s => s < TimeSpan.Zero));
        (await Spans(p => p.Span >= TimeSpan.FromDays(1) && p.Span <= TimeSpan.FromDays(3)))
            .Should().Equal(SpanValues.Where(s => s >= TimeSpan.FromDays(1) && s <= TimeSpan.FromDays(3)));
        (await Spans(p => p.Span == SpanValues[5])).Should().Equal(SpanValues[5]);
        var some = new List<TimeSpan> { SpanValues[0], SpanValues[6] };
        (await Spans(p => some.Contains(p.Span))).Should().Equal(SpanValues[0], SpanValues[6]);
        (await Spans(p => p.MaybeSpan > TimeSpan.Zero)).Should().Equal(
            SpanValues.Where((s, i) => i % 3 != 0 && s > TimeSpan.Zero));

        (await Tagged(tag).Where(p => p.Clock > new TimeOnly(9, 0)).CountAsync())
            .Should().Be(SpanValues.Select((_, i) => new TimeOnly(i * 3, 15, 30)).Count(c => c > new TimeOnly(9, 0)));
    }

    [Fact]
    public async Task Sorting_IsChronological()
    {
        var (tag, _) = await SeedAsync();

        (await Tagged(tag).OrderBy(p => p.Span).ToListAsync()).Select(o => o.Props.Span)
            .Should().Equal(SpanValues.OrderBy(s => s));
        (await Tagged(tag).OrderByDescending(p => p.Span).ToListAsync()).Select(o => o.Props.Span)
            .Should().Equal(SpanValues.OrderByDescending(s => s));
        (await Tagged(tag).OrderBy(p => p.Clock).ToListAsync()).Select(o => o.Props.Clock)
            .Should().BeInAscendingOrder();
    }

    [Fact]
    public async Task GroupBy_And_MinMax_ReadTheTicks()
    {
        var (tag, _) = await SeedAsync();

        var groups = await Tagged(tag).GroupBy(p => p.MaybeSpan).SelectAsync(g => new { g.Key, Count = Agg.Count(g) });
        groups.Where(g => g.Key != null).Select(g => g.Key!.Value).OrderBy(x => x)
            .Should().Equal(SpanValues.Where((_, i) => i % 3 != 0).OrderBy(x => x));
        groups.Single(g => g.Key == null).Count.Should().Be(SpanValues.Where((_, i) => i % 3 == 0).Count());

        var minMax = await Tagged(tag).AggregateAsync(x => new { Min = Agg.Min(x.Props.Span), Max = Agg.Max(x.Props.Span) });
        minMax.Min.Should().Be(SpanValues.Min());
        minMax.Max.Should().Be(SpanValues.Max());
    }

    /// <summary>
    /// The statement UPGRADING.md gives for this database turns values stored as text before the change (the "c" form
    /// of TimeSpan, the "O" form of TimeOnly, with days, signs and fractions, in plain fields, arrays and dictionary
    /// values) into Ticks, and the objects read back as they were saved. The statement is read from UPGRADING.md, so the
    /// text and the test cannot drift.
    /// </summary>
    [Fact]
    public async Task UpgradingStatement_ConvertsValuesStoredAsText()
    {
        var (tag, ids) = await SeedAsync();
        var before = new List<TimeTicksProps>();
        foreach (var id in ids) before.Add((await Redb.LoadAsync<TimeTicksProps>(id))!.Props);

        // Back to the stored form of earlier versions: the text in _String, no Ticks.
        var idList = string.Join(", ", ids);
        var rows = await Redb.Context.QueryAsync<StoredValue>(
            "SELECT v._id AS Id, v._Long AS Ticks, t._name AS TypeName FROM _values v " +
            "JOIN _structures s ON s._id = v._id_structure JOIN _types t ON t._id = s._id_type " +
            $"WHERE v._id_object IN ({idList}) AND t._name IN ('TimeSpan', 'TimeOnly') AND v._Long IS NOT NULL");
        rows.Should().NotBeEmpty();
        foreach (var r in rows)
        {
            var text = r.TypeName == "TimeSpan"
                ? TimeSpan.FromTicks(r.Ticks!.Value).ToString("c", System.Globalization.CultureInfo.InvariantCulture)
                : new TimeOnly(r.Ticks!.Value).ToString("O", System.Globalization.CultureInfo.InvariantCulture);
            await Redb.Context.ExecuteAsync($"UPDATE _values SET _String = '{text}', _Long = NULL WHERE _id = {r.Id}");
        }

        await Redb.Context.ExecuteAsync(UpgradingStatement());

        (await Redb.Context.ExecuteScalarAsync<long>(
            "SELECT COUNT(*) FROM _values v JOIN _structures s ON s._id = v._id_structure JOIN _types t ON t._id = s._id_type " +
            $"WHERE v._id_object IN ({idList}) AND t._name IN ('TimeSpan', 'TimeOnly') AND v._String IS NOT NULL"))
            .Should().Be(0, "every value written by an earlier version is converted");
        for (var i = 0; i < ids.Count; i++)
        {
            var after = (await Redb.LoadAsync<TimeTicksProps>(ids[i]))!.Props;
            after.Should().BeEquivalentTo(before[i], $"object {i} reads back as it was saved");
        }
    }

    private sealed class StoredValue
    {
        public long Id { get; set; }
        public long? Ticks { get; set; }
        public string TypeName { get; set; } = "";
    }

    /// <summary>The SQL block that follows "PostgreSQL:", "SQL Server:" or "SQLite:" in the UPGRADING.md section.</summary>
    private string UpgradingStatement()
    {
        var ns = Redb.GetType().Namespace ?? "";
        var label = ns.Contains("Postgres") ? "PostgreSQL:" : ns.Contains("MSSql") ? "SQL Server:" : "SQLite:";
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        string? path = null;
        while (dir != null && path == null)
        {
            var candidate = Path.Combine(dir.FullName, "redb.DB", "UPGRADING.md");
            if (File.Exists(candidate)) path = candidate;
            // The module files at the root, next to redb.Tests.Integration or next to tests.
            else if (File.Exists(Path.Combine(dir.FullName, "UPGRADING.md"))
                     && (Directory.Exists(Path.Combine(dir.FullName, "redb.Tests.Integration")) || Directory.Exists(Path.Combine(dir.FullName, "tests"))))
                path = Path.Combine(dir.FullName, "UPGRADING.md");
            dir = dir.Parent;
        }
        path.Should().NotBeNull("UPGRADING.md is found above the test output");
        var text = File.ReadAllText(path!).Replace("\r\n", "\n");
        var section = text[text.IndexOf("### TimeSpan and TimeOnly are stored as ticks", StringComparison.Ordinal)..];
        var at = section.IndexOf("\n" + label + "\n", StringComparison.Ordinal);
        at.Should().BeGreaterThan(0, $"the section has a statement for {label}");
        var start = section.IndexOf("```sql\n", at, StringComparison.Ordinal) + "```sql\n".Length;
        var end = section.IndexOf("\n```", start, StringComparison.Ordinal);
        return section[start..end];
    }
}
