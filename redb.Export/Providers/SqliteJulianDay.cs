namespace redb.Export.Providers;

/// <summary>
/// The stored form of a date in a redb SQLite database: a UTC Julian day number (REAL), in
/// <c>_objects._date_*</c>, <c>_objects._value_datetime</c>, <c>_users._date_*</c> and
/// <c>_values._DateTimeOffset</c>.
/// <para>
/// A copy of <c>redb.SQLite/Data/SqliteJulian.cs</c> - same epoch, same millisecond rounding - because
/// this package does not reference the provider. The two must stay identical: the provider binds
/// every date parameter of a query as this number, and in SQLite a REAL never equals, and always
/// sorts before, a TEXT. A date written in any other form is invisible to every date filter.
/// </para>
/// </summary>
internal static class SqliteJulianDay
{
    // 1899-12-30 00:00 UTC.
    private const double EpochJulianDay = 2415018.5;
    private const long EpochMillis = 599264352000000000L / TimeSpan.TicksPerMillisecond;
    private const double MillisPerDay = 86_400_000.0;

    /// <summary>The UTC instant of <paramref name="value"/> as a Julian day.</summary>
    public static double ToJulian(DateTimeOffset value)
    {
        var millis = value.UtcDateTime.Ticks / TimeSpan.TicksPerMillisecond - EpochMillis;
        return millis / MillisPerDay + EpochJulianDay;
    }

    /// <summary>A Julian day as a <see cref="DateTimeOffset"/> at +00:00, to the millisecond.</summary>
    public static DateTimeOffset FromJulian(double julian)
    {
        var millis = (long)Math.Round((julian - EpochJulianDay) * MillisPerDay, MidpointRounding.AwayFromZero);
        var ticks = (millis + EpochMillis) * TimeSpan.TicksPerMillisecond;
        return new DateTimeOffset(new DateTime(ticks, DateTimeKind.Utc), TimeSpan.Zero);
    }
}
