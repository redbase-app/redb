using System.Globalization;
using redb.Core.Models.Entities;
using redb.Core.Utils;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Unit;

/// <summary>
/// The canon of Props values inside the object hash (BR-11, redb.Tsak/docs/BOUNDARIES_AND_FOLLOWUPS.md): a value
/// canonicalises the same on every node, whatever the process culture, and a temporal value keeps its milliseconds.
/// The hash decides whether the ChangeTracking save writes at all, so a change the canon cannot see is a lost write.
/// Milliseconds are the precision every database keeps (SQLite stores an OLE date, truncated to milliseconds by .NET),
/// so the canon truncates there: a value survives the round trip with the hash it was saved with.
/// </summary>
public sealed class RedbHashPropsCanonTests
{
    private sealed class NumericProps
    {
        public double Ratio { get; set; }
        public float Weight { get; set; }
        public decimal Amount { get; set; }
    }

    private static readonly DateTimeOffset Moment = new(2026, 9, 16, 12, 30, 15, TimeSpan.Zero);

    private static Guid HashOf<TProps>(TProps props) where TProps : class, new()
        => RedbHash.ComputeFor(new RedbObject<TProps> { id = 1, Props = props })!.Value;

    private static Guid Under(string culture, Func<Guid> compute)
    {
        var previous = CultureInfo.CurrentCulture;
        CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo(culture);
        try { return compute(); }
        finally { CultureInfo.CurrentCulture = previous; }
    }

    [Fact]
    public void NumericProps_HashTheSame_UnderEveryCulture()
    {
        NumericProps Props() => new() { Ratio = 1.5, Weight = 2.25f, Amount = 3.125m };

        var ruRu = Under("ru-RU", () => HashOf(Props()));
        var enUs = Under("en-US", () => HashOf(Props()));
        var invariant = Under("", () => HashOf(Props()));

        ruRu.Should().Be(enUs, "a decimal separator of the process culture must not move the hash between nodes");
        ruRu.Should().Be(invariant);
    }

    [Fact]
    public void TemporalProps_HashTheSame_UnderEveryCulture()
    {
        TemporalProps Props() => new()
        {
            When = Moment.UtcDateTime, Moment = Moment, Day = new DateOnly(2026, 9, 16),
            Clock = new TimeOnly(12, 30, 15), Span = TimeSpan.FromMinutes(90), Label = "t"
        };

        var ruRu = Under("ru-RU", () => HashOf(Props()));
        var enUs = Under("en-US", () => HashOf(Props()));

        ruRu.Should().Be(enUs, "a date format of the process culture must not move the hash between nodes");
    }

    [Fact]
    public void ASubSecondChange_MovesTheHash()
    {
        var baseline = HashOf(new TemporalProps { When = Moment.UtcDateTime, Moment = Moment, Label = "t" });

        HashOf(new TemporalProps { When = Moment.UtcDateTime, Moment = Moment.AddMilliseconds(100), Label = "t" })
            .Should().NotBe(baseline, "a DateTimeOffset changed by 100 ms is a different value - the save must see it");
        HashOf(new TemporalProps { When = Moment.UtcDateTime.AddMilliseconds(100), Moment = Moment, Label = "t" })
            .Should().NotBe(baseline, "a DateTime changed by 100 ms is a different value - the save must see it");
    }

    [Fact]
    public void AChangeBelowAMillisecond_KeepsTheHash()
    {
        // The contract: milliseconds are the precision every database keeps, and the hash of a saved object must
        // equal the hash of its reloaded twin. Anything finer would move the hash on every reload.
        var baseline = HashOf(new TemporalProps { When = Moment.UtcDateTime, Moment = Moment, Label = "t" });

        HashOf(new TemporalProps { When = Moment.UtcDateTime, Moment = Moment.AddTicks(4567), Label = "t" })
            .Should().Be(baseline, "456.7 microseconds do not survive the databases and are not part of the canon");
    }

    [Fact]
    public void DateTimeKind_DoesNotMoveTheHash()
    {
        // Storage takes the clock reading and stamps Kind=Utc without converting (DateTimeConverter); the canon does
        // the same, so a Local value hashes before the save as it will after the reload.
        var clock = new DateTime(2026, 9, 16, 12, 30, 15, 250);
        var utc = HashOf(new TemporalProps { When = DateTime.SpecifyKind(clock, DateTimeKind.Utc), Moment = Moment, Label = "t" });
        var local = HashOf(new TemporalProps { When = DateTime.SpecifyKind(clock, DateTimeKind.Local), Moment = Moment, Label = "t" });
        var unspecified = HashOf(new TemporalProps { When = DateTime.SpecifyKind(clock, DateTimeKind.Unspecified), Moment = Moment, Label = "t" });

        local.Should().Be(utc);
        unspecified.Should().Be(utc);
    }

    [Fact]
    public void ADateTimeOffsetWithAnotherOffset_HashesByItsInstant()
    {
        var plusThree = HashOf(new TemporalProps { When = Moment.UtcDateTime, Moment = Moment.ToOffset(TimeSpan.FromHours(3)), Label = "t" });
        var utc = HashOf(new TemporalProps { When = Moment.UtcDateTime, Moment = Moment, Label = "t" });

        plusThree.Should().Be(utc, "the databases keep the instant, not the offset it was written with");
    }
}
