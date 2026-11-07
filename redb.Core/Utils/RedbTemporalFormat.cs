using System;
using System.Globalization;

namespace redb.Core.Utils
{
    /// <summary>
    /// Invariant, round-trip text form for the zone-less temporal types
    /// (<see cref="DateOnly"/>, <see cref="TimeOnly"/>, <see cref="TimeSpan"/>).
    ///
    /// <para>
    /// Both the JSON converters and the <c>_values._String</c> write path go through here, so the
    /// two can never disagree and neither depends on the ambient <see cref="CultureInfo"/>.
    /// </para>
    ///
    /// <para>
    /// Why this exists. Three distinct culture leaks, only the first of which is obvious:
    /// <list type="number">
    /// <item>A bare <c>DateOnly.ToString()</c> / <c>TimeOnly.ToString()</c> uses the CURRENT
    /// culture's short date/time pattern: <c>"23.08.2026"</c> under ru-RU, <c>"8/23/2026"</c> under
    /// en-US, <c>"2:30 PM"</c> for a time. This is what the <c>_values._String</c> write path used
    /// to do, so a value written under one culture failed to parse, or silently mis-parsed with day
    /// and month swapped, under another.</item>
    /// <item><c>Parse</c> / <c>TryParse</c> without an explicit <see cref="IFormatProvider"/> reads
    /// back through the current culture, so the read side drifted with the write side.</item>
    /// <item>Even an explicit custom pattern is only partly safe: in a custom format string
    /// <c>':'</c> resolves to the culture's TIME SEPARATOR (<c>'-'</c> and <c>'.'</c> are literals,
    /// only <c>'/'</c> is the date separator). Most cultures use <c>':'</c>, which is exactly why
    /// this one hid.</item>
    /// </list>
    /// The standard round-trip specifiers used here (<c>"O"</c> for the two date/time types,
    /// <c>"c"</c> for <see cref="TimeSpan"/>) are culture-independent by definition.
    /// </para>
    ///
    /// <para>
    /// <see cref="TimeSpan"/> additionally used to be written as <c>@"hh\:mm\:ss"</c>, which drops
    /// the day component and the sign: <c>3.02:00:00</c> came back as <c>02:00:00</c>. <c>"c"</c>
    /// is the constant format <c>[-][d'.']hh':'mm':'ss['.'fffffff]</c> and keeps both.
    /// </para>
    ///
    /// <para>
    /// Parsing accepts the invariant form first and falls back to the current culture last, so rows
    /// written by an earlier build under a non-invariant culture keep loading on a host that still
    /// runs under that culture.
    /// </para>
    /// </summary>
    public static class RedbTemporalFormat
    {
        /// <summary>Round-trip <see cref="DateOnly"/> format: <c>yyyy-MM-dd</c>.</summary>
        public const string DateOnlyFormat = "O";

        /// <summary>Round-trip <see cref="TimeOnly"/> format: <c>HH:mm:ss.fffffff</c>.</summary>
        public const string TimeOnlyFormat = "O";

        /// <summary>Constant <see cref="TimeSpan"/> format: <c>[-][d.]hh:mm:ss[.fffffff]</c>.</summary>
        public const string TimeSpanFormat = "c";

        public static string ToText(DateOnly value) =>
            value.ToString(DateOnlyFormat, CultureInfo.InvariantCulture);

        public static string ToText(TimeOnly value) =>
            value.ToString(TimeOnlyFormat, CultureInfo.InvariantCulture);

        public static string ToText(TimeSpan value) =>
            value.ToString(TimeSpanFormat, CultureInfo.InvariantCulture);

        public static bool TryParseDateOnly(string? text, out DateOnly value)
        {
            value = default;
            if (string.IsNullOrWhiteSpace(text)) return false;

            // PostgreSQL sentinels. Npgsql maps DateTime.MinValue/MaxValue to -infinity/infinity,
            // so an unset DateOnly (0001-01-01, its default) comes back as the text "-infinity".
            // The DateTime converters have handled this for a long time; DateOnly did not, and
            // simply leaving a DateOnly property unassigned threw on load.
            if (text == "-infinity") { value = DateOnly.MinValue; return true; }
            if (text == "infinity") { value = DateOnly.MaxValue; return true; }

            if (DateOnly.TryParseExact(text, DateOnlyFormat, CultureInfo.InvariantCulture,
                    DateTimeStyles.None, out value))
                return true;

            if (DateOnly.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.None, out value))
                return true;

            // A full ISO timestamp also resolves to a date (the JSON side may carry one).
            if (DateTime.TryParse(text, CultureInfo.InvariantCulture,
                    DateTimeStyles.RoundtripKind, out var asDateTime))
            {
                value = DateOnly.FromDateTime(asDateTime);
                return true;
            }

            // Legacy: written by an earlier build through the current culture.
            return DateOnly.TryParse(text, CultureInfo.CurrentCulture, DateTimeStyles.None, out value);
        }

        public static bool TryParseTimeOnly(string? text, out TimeOnly value)
        {
            value = default;
            if (string.IsNullOrWhiteSpace(text)) return false;

            if (TimeOnly.TryParseExact(text, TimeOnlyFormat, CultureInfo.InvariantCulture,
                    DateTimeStyles.None, out value))
                return true;

            if (TimeOnly.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.None, out value))
                return true;

            // Older rows carry a TimeSpan-shaped string ("14:30:00").
            if (TryParseTimeSpan(text, out var asSpan)
                && asSpan >= TimeSpan.Zero && asSpan < TimeSpan.FromDays(1))
            {
                value = TimeOnly.FromTimeSpan(asSpan);
                return true;
            }

            return TimeOnly.TryParse(text, CultureInfo.CurrentCulture, DateTimeStyles.None, out value);
        }

        public static bool TryParseTimeSpan(string? text, out TimeSpan value)
        {
            value = default;
            if (string.IsNullOrWhiteSpace(text)) return false;

            if (TimeSpan.TryParseExact(text, TimeSpanFormat, CultureInfo.InvariantCulture, out value))
                return true;

            if (TimeSpan.TryParse(text, CultureInfo.InvariantCulture, out value))
                return true;

            return TimeSpan.TryParse(text, CultureInfo.CurrentCulture, out value);
        }
    }
}
