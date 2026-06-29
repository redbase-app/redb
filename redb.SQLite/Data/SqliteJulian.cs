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
    /// </summary>
    internal static class SqliteJulian
    {
        // OLE Automation date epoch (1899-12-30 00:00) expressed as a Julian day.
        // Julian = OADate + this constant. ToOADate/FromOADate are built-in and lossless
        // within the double's precision (same domain SQLite's julianday() uses).
        private const double OADateToJulianOffset = 2415018.5;

        /// <summary>DateTimeOffset → UTC Julian day (REAL). Uses the true UTC instant.</summary>
        public static double ToJulian(DateTimeOffset dto) => dto.UtcDateTime.ToOADate() + OADateToJulianOffset;

        /// <summary>
        /// DateTime → UTC Julian day (REAL). The clock value is treated as UTC (REDB's
        /// contract: <see cref="redb.Core.Utils.DateTimeConverter.NormalizeForStorage"/>
        /// specifies Kind=Utc without converting). ToOADate ignores Kind, so this matches.
        /// </summary>
        public static double ToJulian(DateTime dt) => dt.ToOADate() + OADateToJulianOffset;

        /// <summary>UTC Julian day (REAL) → DateTimeOffset (+00:00).</summary>
        public static DateTimeOffset FromJulian(double julian)
        {
            var utc = DateTime.SpecifyKind(DateTime.FromOADate(julian - OADateToJulianOffset), DateTimeKind.Utc);
            return new DateTimeOffset(utc, TimeSpan.Zero);
        }
    }
}
