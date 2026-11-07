using System.Globalization;
using System.Text.Json;
using redb.Core.Query.Mapping;
using redb.Core.Query.Utils;
using redb.Core.Serialization;
using redb.Core.Utils;

namespace redb.Tests.Integration.Tests.Unit;

/// <summary>
/// REDB's temporal contract, pinned.
///
/// <para>
/// <b>DateTime carries no time zone.</b> The clock reading is the datum: 14:00 written is 14:00
/// read, on any machine, in any zone, through any read path. <see cref="DateTimeOffset"/> is the
/// opposite and deliberately untouched: it is the native .NET type and carries a real instant.
/// </para>
///
/// <para>
/// <b>Text form is invariant.</b> <see cref="DateOnly"/>, <see cref="TimeOnly"/> and
/// <see cref="TimeSpan"/> live in <c>_values._String</c>; their text form must not depend on the
/// ambient <see cref="CultureInfo"/>, or a value written on one host stops loading on another.
/// </para>
///
/// <para>
/// These are unit tests on purpose: every defect they cover sits in a pure conversion, and the
/// integration suites never reached any of them. All their DateTime values are
/// <c>DateTime.UtcNow</c> or <c>DateTimeKind.Utc</c> literals, where the zone-less and the
/// instant reading coincide exactly, and no test aggregates or projects a date at all.
/// </para>
/// </summary>
public class TemporalSemanticsTests
{
    /// <summary>Runs <paramref name="body"/> under a specific culture, restoring the previous one.</summary>
    private static void UnderCulture(string name, Action body)
    {
        var previous = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = new CultureInfo(name);
            body();
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }
    }

    private static JsonElement Json(string raw) =>
        JsonDocument.Parse(raw).RootElement;

    // ---------------------------------------------------------------------
    // DateTime: the wall-clock contract, on every read path
    // ---------------------------------------------------------------------

    /// <summary>
    /// The analytics path (Min/Max, AggregateRedbAsync, GroupBy, Window, scalar projections) goes
    /// through <see cref="JsonValueConverter"/>. It used to call a bare <c>DateTime.TryParse</c>,
    /// whose default <see cref="DateTimeStyles"/> converts a zoned ISO string INTO THE CALLER'S
    /// LOCAL ZONE and returns <see cref="DateTimeKind.Local"/>. The reading must survive intact.
    /// </summary>
    [Theory]
    [InlineData("\"2025-06-15T10:30:00Z\"")]
    [InlineData("\"2025-06-15T10:30:00+00:00\"")]
    [InlineData("\"2025-06-15T10:30:00.0000000Z\"")]
    public void AnalyticsPath_DateTime_KeepsWallClockReadingAndUtcKind(string json)
    {
        var value = (DateTime)JsonValueConverter.Convert(Json(json), typeof(DateTime))!;

        value.Should().Be(new DateTime(2025, 6, 15, 10, 30, 0, DateTimeKind.Utc));
        // Kind is the part that stayed broken even on a UTC host: DateTimeStyles.None labels the
        // result Local regardless of whether the value moved.
        value.Kind.Should().Be(DateTimeKind.Utc);
    }

    /// <summary>
    /// The same stored value read through analytics and through object materialization must be the
    /// same value. Previously the two disagreed by the reader's UTC offset.
    /// </summary>
    [Fact]
    public void AnalyticsPath_AgreesWithObjectMaterializationPath()
    {
        const string wire = "2025-06-15T10:30:00+00:00";

        var viaAnalytics = (DateTime)JsonValueConverter.Convert(Json($"\"{wire}\""), typeof(DateTime))!;
        var viaObject = JsonSerializer.Deserialize<DateTime>(
            $"\"{wire}\"", SystemTextJsonRedbSerializer.Options);

        viaAnalytics.Should().Be(viaObject);
        viaAnalytics.Kind.Should().Be(viaObject.Kind);
    }

    /// <summary>
    /// DateTimeOffset keeps native semantics: it carries an instant. Both read paths reduce it to
    /// the same UTC-based value so a caller never sees a provider's stored offset leak through.
    /// </summary>
    [Fact]
    public void AnalyticsPath_DateTimeOffset_NormalizesToUtcLikeObjectPath()
    {
        const string wire = "2025-06-15T13:30:00+03:00";

        var viaAnalytics = (DateTimeOffset)JsonValueConverter.Convert(Json($"\"{wire}\""), typeof(DateTimeOffset))!;
        var viaObject = JsonSerializer.Deserialize<DateTimeOffset>(
            $"\"{wire}\"", SystemTextJsonRedbSerializer.Options);

        viaAnalytics.Should().Be(viaObject);
        viaAnalytics.Offset.Should().Be(TimeSpan.Zero);
        viaAnalytics.UtcDateTime.Should().Be(new DateTime(2025, 6, 15, 10, 30, 0, DateTimeKind.Utc));
    }

    /// <summary>The reading must not depend on the reader's culture either.</summary>
    [Theory]
    [InlineData("ru-RU")]
    [InlineData("en-US")]
    [InlineData("de-DE")]
    public void DateTime_ReadIsCultureIndependent(string culture)
    {
        UnderCulture(culture, () =>
        {
            var viaAnalytics = (DateTime)JsonValueConverter.Convert(
                Json("\"2025-06-15T10:30:00Z\""), typeof(DateTime))!;
            var viaObject = JsonSerializer.Deserialize<DateTime>(
                "\"2025-06-15T10:30:00Z\"", SystemTextJsonRedbSerializer.Options);

            viaAnalytics.Should().Be(new DateTime(2025, 6, 15, 10, 30, 0, DateTimeKind.Utc));
            viaObject.Should().Be(new DateTime(2025, 6, 15, 10, 30, 0, DateTimeKind.Utc));
        });
    }

    // ---------------------------------------------------------------------
    // DateOnly / TimeOnly / TimeSpan: invariant text form
    // ---------------------------------------------------------------------

    /// <summary>
    /// ru-RU uses '.' as its date separator, so even the explicit pattern "yyyy-MM-dd" produced
    /// "2026.08.23" — which then failed to parse back. The wire form must be invariant.
    /// </summary>
    [Theory]
    [InlineData("ru-RU")]
    [InlineData("en-US")]
    [InlineData("de-DE")]
    public void DateOnly_RoundTripsUnderAnyCulture(string culture)
    {
        var value = new DateOnly(2026, 8, 23);

        UnderCulture(culture, () =>
        {
            var json = JsonSerializer.Serialize(value, SystemTextJsonRedbSerializer.Options);
            json.Should().Be("\"2026-08-23\"");

            JsonSerializer.Deserialize<DateOnly>(json, SystemTextJsonRedbSerializer.Options)
                .Should().Be(value);
        });
    }

    /// <summary>
    /// en-US renders a bare TimeOnly as "2:30 PM"; the DB write path used exactly that. Both the
    /// JSON form and the column form now go through the same invariant round-trip.
    /// </summary>
    [Theory]
    [InlineData("ru-RU")]
    [InlineData("en-US")]
    [InlineData("de-DE")]
    public void TimeOnly_RoundTripsUnderAnyCulture(string culture)
    {
        var value = new TimeOnly(14, 30, 15);

        UnderCulture(culture, () =>
        {
            var json = JsonSerializer.Serialize(value, SystemTextJsonRedbSerializer.Options);
            JsonSerializer.Deserialize<TimeOnly>(json, SystemTextJsonRedbSerializer.Options)
                .Should().Be(value);

            RedbTemporalFormat.TryParseTimeOnly(RedbTemporalFormat.ToText(value), out var fromColumn)
                .Should().BeTrue();
            fromColumn.Should().Be(value);
        });
    }

    /// <summary>
    /// The JSON form was @"hh\:mm\:ss", which silently drops the day component and the sign:
    /// 3 days 2 hours came back as 2 hours. "c" is the constant round-trip format.
    /// </summary>
    [Theory]
    [InlineData(3, 2, 0, 0)]
    [InlineData(0, 2, 0, 0)]
    [InlineData(400, 23, 59, 59)]
    public void TimeSpan_KeepsDayComponent(int days, int hours, int minutes, int seconds)
    {
        var value = new TimeSpan(days, hours, minutes, seconds);

        var json = JsonSerializer.Serialize(value, SystemTextJsonRedbSerializer.Options);
        JsonSerializer.Deserialize<TimeSpan>(json, SystemTextJsonRedbSerializer.Options)
            .Should().Be(value);
    }

    [Fact]
    public void TimeSpan_KeepsSign()
    {
        var value = TimeSpan.FromHours(-5);

        var json = JsonSerializer.Serialize(value, SystemTextJsonRedbSerializer.Options);
        JsonSerializer.Deserialize<TimeSpan>(json, SystemTextJsonRedbSerializer.Options)
            .Should().Be(value);
    }

    /// <summary>Legacy rows written by an earlier build under a plain culture still load.</summary>
    [Fact]
    public void LegacyTextForms_StillParse()
    {
        RedbTemporalFormat.TryParseTimeSpan("02:00:00", out var span).Should().BeTrue();
        span.Should().Be(TimeSpan.FromHours(2));

        RedbTemporalFormat.TryParseTimeOnly("14:30:00", out var time).Should().BeTrue();
        time.Should().Be(new TimeOnly(14, 30, 0));

        RedbTemporalFormat.TryParseDateOnly("2026-08-23", out var date).Should().BeTrue();
        date.Should().Be(new DateOnly(2026, 8, 23));
    }

    // ---------------------------------------------------------------------
    // Type map and dead base field
    // ---------------------------------------------------------------------

    /// <summary>
    /// DateOnly is seeded with db_type 'DateTime' and resolves to _DateTimeOffset on the SQL side,
    /// but the C# map had no case for it and threw "Unknown type ID".
    /// </summary>
    [Fact]
    public void TypeMapping_ResolvesTemporalTypes()
    {
        RedbTypeMapping.GetValueColumn(RedbTypeIds.DateOnly).Should().Be("_DateTimeOffset");
        RedbTypeMapping.GetValueColumn(RedbTypeIds.TimeOnly).Should().Be("_String");
        RedbTypeMapping.GetValueColumn(RedbTypeIds.TimeSpan).Should().Be("_String");

        RedbTypeMapping.GetObjectValueColumn(RedbTypeIds.DateOnly).Should().Be("_value_datetime");
        RedbTypeMapping.GetObjectValueColumn(RedbTypeIds.TimeOnly).Should().Be("_value_string");
        RedbTypeMapping.GetObjectValueColumn(RedbTypeIds.TimeSpan).Should().Be("_value_string");
    }

    /// <summary>
    /// _date_delete is a leftover of the removed _deleted_objects table and exists in no dialect's
    /// DDL. Claiming it as a base field let a string-addressed query compile SQL against a column
    /// that is not there.
    /// </summary>
    [Theory]
    [InlineData("DateDelete")]
    [InlineData("_date_delete")]
    [InlineData("date_delete")]
    public void BaseFieldMapper_DoesNotClaimPhantomDateDelete(string name)
    {
        BaseFieldMapper.IsBaseField(name).Should().BeFalse();
    }

    [Fact]
    public void BaseFieldMapper_StillClaimsRealDateFields()
    {
        BaseFieldMapper.IsBaseField("DateCreate").Should().BeTrue();
        BaseFieldMapper.IsBaseField("DateModify").Should().BeTrue();
        BaseFieldMapper.IsBaseField("DateBegin").Should().BeTrue();
        BaseFieldMapper.IsBaseField("DateComplete").Should().BeTrue();
        BaseFieldMapper.MapToColumn("DateCreate").Should().Be("_date_create");
    }
}
