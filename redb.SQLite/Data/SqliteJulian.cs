using System;

namespace redb.SQLite.Data
{
    /// <summary>
    /// SQLite stores REDB datetimes as REAL <b>Julian day numbers in UTC</b> — the native
    /// SQLite datetime representation. SQLite's own date/time functions
    /// (<c>datetime()</c>, <c>strftime()</c>, <c>julianday()</c>, <c>date()</c>) consume a
    /// REAL Julian day directly, and because it is a plain number, range comparisons
    /// (<c>&lt;</c>/<c>&gt;</c>) are numeric, correct, and <b>index-friendly (sargable)</b> —
    /// unlike the previous TEXT-ISO storage whose lexical comparison broke on format drift.
    ///
    /// Offset is normalized to UTC, matching REDB's contract (stored datetimes are UTC
    /// instants — see <see cref="redb.Core.Utils.DateTimeConverter"/>) and how PostgreSQL
    /// keeps <c>timestamptz</c> in UTC.
    ///
    /// The day count is linear in the tick count, truncated to the millisecond - the precision the
    /// other providers keep and the hash canon assumes. It was computed through the OLE automation
    /// date (<c>ToOADate</c>), which is not linear: its zero stands for <see cref="DateTime.MinValue"/>,
    /// so a default date came back as 1899-12-30, and a day before its epoch is encoded as sign and
    /// magnitude, so any time of day before 1899-12-30 moved by a day - while the native extension
    /// writes through SQLite's own <c>julianday()</c>, which is linear.
    /// </summary>
    internal static class SqliteJulian
    {
        // 1899-12-30 00:00 UTC, the same epoch the OLE date used: for every instant from it on the number
        // is bit for bit what was stored before, so existing rows and equality filters keep matching.
        private const double EpochJulianDay = 2415018.5;
        private const long EpochMillis = 599264352000000000L / TimeSpan.TicksPerMillisecond;
        private const double MillisPerDay = 86_400_000.0;

        /// <summary>DateTimeOffset → UTC Julian day (REAL). Uses the true UTC instant.</summary>
        public static double ToJulian(DateTimeOffset dto) => ToJulian(dto.UtcDateTime);

        /// <summary>
        /// DateTime → UTC Julian day (REAL). The clock value is treated as UTC (REDB's
        /// contract: <see cref="redb.Core.Utils.DateTimeConverter.NormalizeForStorage"/>
        /// specifies Kind=Utc without converting), so Kind is ignored here.
        /// </summary>
        public static double ToJulian(DateTime dt)
        {
            var millis = dt.Ticks / TimeSpan.TicksPerMillisecond - EpochMillis;
            return millis / MillisPerDay + EpochJulianDay;
        }

        /// <summary>UTC Julian day (REAL) → DateTimeOffset (+00:00), to the millisecond.</summary>
        public static DateTimeOffset FromJulian(double julian)
        {
            var millis = (long)Math.Round((julian - EpochJulianDay) * MillisPerDay, MidpointRounding.AwayFromZero);
            var ticks = (millis + EpochMillis) * TimeSpan.TicksPerMillisecond;
            return new DateTimeOffset(new DateTime(ticks, DateTimeKind.Utc), TimeSpan.Zero);
        }
    }
}
