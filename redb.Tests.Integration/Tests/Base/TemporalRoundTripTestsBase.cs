using System.Globalization;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// The temporal contract, end to end against a real database.
///
/// <para>
/// <b>DateTime carries no time zone.</b> The clock reading is the datum: 14:00 written is 14:00
/// read, on any host, in any zone, through any read path — materialization, ordering, aggregate.
/// <b>DateTimeOffset is the opposite</b> and deliberately untouched: it is the native .NET type and
/// carries a real instant.
/// </para>
///
/// <para>
/// <b>Text form is invariant.</b> <see cref="DateOnly"/>, <see cref="TimeOnly"/> and
/// <see cref="TimeSpan"/> land in <c>_values._String</c>; their form must not follow the ambient
/// <see cref="CultureInfo"/>, or a row written on one host stops loading on another.
/// </para>
///
/// <para>
/// Why this suite exists: every other test in the repository uses <c>DateTime.UtcNow</c> or a
/// <c>DateTimeKind.Utc</c> literal, where the zone-less and the instant reading coincide exactly,
/// and none of them touches DateTimeOffset, DateOnly, TimeOnly or TimeSpan at all. The conversion
/// paths below were unreachable from a test.
/// </para>
/// </summary>
public abstract class TemporalRoundTripTestsBase
{
    protected readonly IRedbService Redb;
    private bool _seeded;

    protected TemporalRoundTripTestsBase(IRedbService redb) => Redb = redb;

    // A fixed non-UTC offset, so "instant" and "reading" are never accidentally equal.
    private static readonly TimeSpan Offset = TimeSpan.FromHours(3);

    private static DateTime Reading(int day, int hour) =>
        new(2026, 3, day, hour, 30, 0, DateTimeKind.Unspecified);

    /// <summary>Runs <paramref name="body"/> under a specific culture, restoring the previous one.</summary>
    private static async Task UnderCultureAsync(string name, Func<Task> body)
    {
        var previous = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = new CultureInfo(name);
            await body();
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }
    }

    /// <summary>
    /// Ten rows, one per day 1..10 of 2026-03, hour = 10 + (i % 3). Every temporal field moves
    /// together so a filter on one can be cross-checked against another.
    /// </summary>
    protected async Task EnsureSeededAsync()
    {
        if (_seeded) return;

        // Scheme name is the CLR FullName: the positional [RedbScheme("...")] argument is the
        // ALIAS, never the name. Deleting by the alias silently matched nothing and rows piled up
        // across tests, which is exactly how this line was wrong the first time.
        await Redb.Context.ExecuteAsync(
            "DELETE FROM _objects WHERE _id_scheme IN (SELECT _id FROM _schemes WHERE _name = "
            + $"'{typeof(TemporalProps).FullName}')");

        var rows = Enumerable.Range(1, 10).Select(i => new RedbObject<TemporalProps>
        {
            name = $"temporal-{i:00}",
            Props = new TemporalProps
            {
                When = Reading(i, 10 + (i % 3)),
                Moment = new DateTimeOffset(2026, 3, i, 10 + (i % 3), 30, 0, Offset),
                Day = new DateOnly(2026, 3, i),
                Clock = new TimeOnly(10 + (i % 3), 30, 0),
                Span = TimeSpan.FromDays(i).Add(TimeSpan.FromHours(2)),
                Label = $"row-{i:00}"
            }
        }).ToList();

        await Redb.SaveAsync(rows);
        _seeded = true;
    }

    // =====================================================================
    // Round trip
    // =====================================================================

    /// <summary>
    /// Every temporal type survives a save/load unchanged, whatever culture the host runs under.
    /// The DateOnly/TimeOnly/TimeSpan write path used to go through the current culture's short
    /// pattern (<c>"23.08.2026"</c>, <c>"2:30 PM"</c>) and the read path parsed it back the same
    /// way, so the pair only agreed while the culture stayed put.
    /// </summary>
    [Theory]
    [InlineData("ru-RU")]
    [InlineData("en-US")]
    [InlineData("de-DE")]
    public async Task RoundTrip_AllTemporalTypes_SurviveAnyCulture(string culture)
    {
        await UnderCultureAsync(culture, async () =>
        {
            var source = new TemporalRoundTripProps
            {
                When = new DateTime(2026, 8, 23, 14, 0, 0, DateTimeKind.Unspecified),
                Moment = new DateTimeOffset(2026, 8, 23, 14, 0, 0, Offset),
                Day = new DateOnly(2026, 8, 23),
                Clock = new TimeOnly(14, 30, 15),
                Span = TimeSpan.FromDays(3).Add(TimeSpan.FromHours(2)),
                Label = $"roundtrip-{culture}"
            };

            var obj = new RedbObject<TemporalRoundTripProps> { name = source.Label, Props = source };
            var id = await Redb.SaveAsync(obj);

            var loaded = await Redb.LoadAsync<TemporalRoundTripProps>(id);

            // Zone-less: the reading comes back as written, labelled UTC.
            loaded.Props.When.Should().Be(new DateTime(2026, 8, 23, 14, 0, 0, DateTimeKind.Utc));
            loaded.Props.When.Kind.Should().Be(DateTimeKind.Utc);

            // Zone-aware: the instant comes back, the offset is normalised away.
            loaded.Props.Moment.UtcDateTime.Should().Be(source.Moment.UtcDateTime);

            loaded.Props.Day.Should().Be(source.Day);
            loaded.Props.Clock.Should().Be(source.Clock);
            loaded.Props.Span.Should().Be(source.Span, "the day component must survive");
        });
    }

    /// <summary>
    /// A <see cref="DateTimeKind.Local"/> value keeps its reading rather than being shifted to the
    /// host's UTC offset. This is the whole point of the contract, and it is invisible on a host
    /// running in UTC — hence the explicit non-UTC assertion below.
    /// </summary>
    [Fact]
    public async Task RoundTrip_DateTime_KeepsReadingNotInstant()
    {
        var local = new DateTime(2026, 8, 23, 14, 0, 0, DateTimeKind.Local);

        var id = await Redb.SaveAsync(new RedbObject<TemporalRoundTripProps>
        {
            name = "kind-local",
            Props = new TemporalRoundTripProps { When = local, Label = "kind-local" }
        });

        var loaded = await Redb.LoadAsync<TemporalRoundTripProps>(id);

        loaded.Props.When.Hour.Should().Be(14);
        loaded.Props.When.Should().Be(new DateTime(2026, 8, 23, 14, 0, 0, DateTimeKind.Utc));
    }

    // =====================================================================
    // Comparison: DateTime
    // =====================================================================

    [Fact]
    public async Task Where_DateTime_GreaterThan()
    {
        await EnsureSeededAsync();
        var cutoff = Reading(5, 0);

        var results = await Redb.Query<TemporalProps>()
            .Where(t => t.When > cutoff)
            .Take(50).ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r => r.Props.When.Should().BeAfter(cutoff));
        results.Should().HaveCount(6, "days 5..10 all start after 2026-03-05T00:30 except day 5 itself is 10:30+");
    }

    [Fact]
    public async Task Where_DateTime_LessThan()
    {
        await EnsureSeededAsync();
        var cutoff = Reading(5, 0);

        var results = await Redb.Query<TemporalProps>()
            .Where(t => t.When < cutoff)
            .Take(50).ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r => r.Props.When.Should().BeBefore(cutoff));
    }

    [Fact]
    public async Task Where_DateTime_Between()
    {
        await EnsureSeededAsync();
        var from = Reading(3, 0);
        var to = Reading(7, 0);

        var results = await Redb.Query<TemporalProps>()
            .Where(t => t.When >= from && t.When < to)
            .Take(50).ToListAsync();

        results.Should().HaveCount(4);
        results.Should().AllSatisfy(r =>
        {
            r.Props.When.Should().BeOnOrAfter(from);
            r.Props.When.Should().BeBefore(to);
        });
    }

    /// <summary>
    /// Equality closes the loop: the value the filter is built from and the value the writer stored
    /// must be the same, or a saved row becomes unfindable by its own value. Write and search go
    /// through the same normalisation, which is what makes this hold.
    /// </summary>
    [Fact]
    public async Task Where_DateTime_Equal_FindsTheRowItWasWrittenFrom()
    {
        await EnsureSeededAsync();
        var exact = Reading(4, 11);

        var results = await Redb.Query<TemporalProps>()
            .Where(t => t.When == exact)
            .Take(50).ToListAsync();

        results.Should().ContainSingle();
        results[0].Props.Label.Should().Be("row-04");
    }

    // =====================================================================
    // Comparison: DateTimeOffset
    // =====================================================================

    [Fact]
    public async Task Where_DateTimeOffset_GreaterThan()
    {
        await EnsureSeededAsync();
        var cutoff = new DateTimeOffset(2026, 3, 5, 0, 0, 0, Offset);

        var results = await Redb.Query<TemporalProps>()
            .Where(t => t.Moment > cutoff)
            .Take(50).ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r => r.Props.Moment.Should().BeAfter(cutoff));
    }

    /// <summary>
    /// Two ways of writing the same instant must select the same rows. A DateTimeOffset carries a
    /// real moment, so 10:30+03:00 and 07:30Z are one and the same cutoff.
    /// </summary>
    [Fact]
    public async Task Where_DateTimeOffset_SameInstantDifferentOffset_SelectsSameRows()
    {
        await EnsureSeededAsync();
        var withOffset = new DateTimeOffset(2026, 3, 5, 0, 0, 0, Offset);
        var asUtc = withOffset.ToUniversalTime();

        var a = await Redb.Query<TemporalProps>().Where(t => t.Moment > withOffset).Take(50).ToListAsync();
        var b = await Redb.Query<TemporalProps>().Where(t => t.Moment > asUtc).Take(50).ToListAsync();

        a.Select(x => x.Id).OrderBy(x => x).Should().Equal(b.Select(x => x.Id).OrderBy(x => x));
    }

    // =====================================================================
    // Comparison: DateOnly
    // =====================================================================

    [Fact]
    public async Task Where_DateOnly_GreaterThan()
    {
        await EnsureSeededAsync();
        var cutoff = new DateOnly(2026, 3, 5);

        var results = await Redb.Query<TemporalProps>()
            .Where(t => t.Day > cutoff)
            .Take(50).ToListAsync();

        results.Should().HaveCount(5);
        results.Should().AllSatisfy(r => r.Props.Day.Should().BeAfter(cutoff));
    }

    [Fact]
    public async Task Where_DateOnly_Equal()
    {
        await EnsureSeededAsync();
        var exact = new DateOnly(2026, 3, 7);

        var results = await Redb.Query<TemporalProps>()
            .Where(t => t.Day == exact)
            .Take(50).ToListAsync();

        results.Should().ContainSingle();
        results[0].Props.Label.Should().Be("row-07");
    }

    // =====================================================================
    // Ordering and the analytics path
    // =====================================================================

    [Fact]
    public async Task OrderBy_DateTime_IsChronological()
    {
        await EnsureSeededAsync();

        var results = await Redb.Query<TemporalProps>()
            .OrderBy(t => t.When)
            .Take(50).ToListAsync();

        results.Select(r => r.Props.When).Should().BeInAscendingOrder();
    }

    /// <summary>
    /// Base-field aggregates go through <c>JsonValueConverter</c>, a different converter from the
    /// one object materialization uses. It used to parse a zoned ISO string into the caller's LOCAL
    /// zone, so the same stored field answered differently depending on how it was read. The two
    /// paths must agree exactly, and must not follow the ambient culture.
    /// </summary>
    [Fact]
    public async Task Aggregate_BaseDateField_AgreesWithMaterialization()
    {
        await EnsureSeededAsync();

        var min = await Redb.Query<TemporalProps>().MinRedbAsync(o => o.DateCreate);
        var max = await Redb.Query<TemporalProps>().MaxRedbAsync(o => o.DateCreate);

        var all = await Redb.Query<TemporalProps>().Take(50).ToListAsync();

        min.Should().NotBeNull();
        max.Should().NotBeNull();
        min!.Value.UtcDateTime.Should().Be(all.Min(r => r.DateCreate).UtcDateTime);
        max!.Value.UtcDateTime.Should().Be(all.Max(r => r.DateCreate).UtcDateTime);
    }

    [Fact]
    public async Task Aggregate_BaseDateField_IsCultureIndependent()
    {
        await EnsureSeededAsync();

        DateTimeOffset? underRu = default, underUs = default;
        await UnderCultureAsync("ru-RU", async () =>
            underRu = await Redb.Query<TemporalProps>().MinRedbAsync(o => o.DateCreate));
        await UnderCultureAsync("en-US", async () =>
            underUs = await Redb.Query<TemporalProps>().MinRedbAsync(o => o.DateCreate));

        underRu.Should().Be(underUs);
    }

    // KNOWN BOUNDARY, not covered by a test on purpose.
    //
    // MinAsync/MaxAsync over a *Props* field return through ExecuteAggregateAsync, whose contract
    // is Task<decimal?>. That works for a backend which stores datetimes as a number (SQLite, whose
    // REAL Julian day is decoded by TemporalDecoder) and cannot work on PostgreSQL or SQL Server,
    // where the aggregate comes back as an ISO string and the decimal conversion throws
    // FormatException. The failure is provider-shaped, so a shared assertion here would be either
    // wrong on one backend or vacuous on all. Widening the contract to a type-preserving one
    // touches every provider and both tiers — deferred, see DATETIME.md.
    //
    // Base-field aggregates (the two tests above) are unaffected: they return JSON and go through
    // JsonValueConverter.

    // =====================================================================
    // TimeOnly / TimeSpan: equality works, ordering is a documented boundary
    // =====================================================================

    [Fact]
    public async Task Where_TimeOnly_Equal()
    {
        await EnsureSeededAsync();
        var exact = new TimeOnly(12, 30, 0);   // i % 3 == 2 → days 2, 5, 8

        var results = await Redb.Query<TemporalProps>()
            .Where(t => t.Clock == exact)
            .Take(50).ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r => r.Props.Clock.Should().Be(exact));
    }

    [Fact]
    public async Task Where_TimeSpan_Equal()
    {
        await EnsureSeededAsync();
        var exact = TimeSpan.FromDays(4).Add(TimeSpan.FromHours(2));

        var results = await Redb.Query<TemporalProps>()
            .Where(t => t.Span == exact)
            .Take(50).ToListAsync();

        results.Should().ContainSingle();
        results[0].Props.Label.Should().Be("row-04");
    }

    /// <summary>
    /// KNOWN BOUNDARY, characterised on purpose rather than hidden.
    ///
    /// <para>
    /// <see cref="TimeSpan"/> and <see cref="TimeOnly"/> are stored in <c>_values._String</c>
    /// (db_type "String" in the <c>_types</c> seed), so SQL orders them lexicographically, not
    /// chronologically: <c>"10.02:00:00"</c> sorts before <c>"2.02:00:00"</c>. Equality is exact and
    /// is the supported operation; ordered comparison is not. The real fix is to move both to
    /// <c>_Long</c> (ticks), which changes db_type and needs a data migration — deferred, see
    /// DATETIME.md.
    /// </para>
    ///
    /// <para>
    /// This test pins today's behaviour. When the storage moves to ticks it will go red, which is
    /// the intended signal to delete it.
    /// </para>
    /// </summary>
    [Fact]
    public async Task Where_TimeSpan_OrderedComparison_IsLexicographic_KnownBoundary()
    {
        await EnsureSeededAsync();

        // Chronologically 10 days > 2 days. Lexicographically "10." < "2.".
        var results = await Redb.Query<TemporalProps>()
            .Where(t => t.Span > TimeSpan.FromDays(9).Add(TimeSpan.FromHours(2)))
            .Take(50).ToListAsync();

        var labels = results.Select(r => r.Props.Label).OrderBy(x => x).ToList();
        labels.Should().NotContain("row-10",
            "string ordering puts \"10.02:00:00\" below \"9.02:00:00\" — the documented boundary of " +
            "storing TimeSpan as text");
    }
}
