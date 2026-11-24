using System;

namespace redb.Core.Query.Utils;

/// <summary>
/// Optional, provider-registered decoder for the case where a <b>numeric</b> database value
/// targets a <b>temporal</b> CLR type (<see cref="DateTime"/>/<see cref="DateTimeOffset"/>/
/// <see cref="DateOnly"/>). This only arises when a backend stores datetimes as a number —
/// e.g. SQLite, which stores them as a REAL Julian-day. Object reads go through that backend's
/// own JSON projection (which already emits ISO text), but analytics paths (Min/Max,
/// AggregateRedbAsync, Window, GroupBy) select the raw column and return the bare number.
///
/// Core stays storage-agnostic: it knows nothing about Julian days or any epoch — it merely
/// asks the registered decoder. The decoder is null by default, so providers that never return
/// a number for a temporal column (PostgreSQL, SQL Server) are completely unaffected. The
/// numeric backend (redb.SQLite) registers the decoder when it is configured.
/// </summary>
public static class TemporalDecoder
{
    /// <summary>
    /// Set by the numeric-datetime backend at registration. Maps
    /// <c>(rawNumber, targetTemporalType)</c> → a <see cref="DateTime"/>/<see cref="DateTimeOffset"/>/
    /// <see cref="DateOnly"/>. Global by design (a process-wide codec), and idempotent — the same
    /// backend always registers the same function.
    /// </summary>
    public static Func<double, Type, object?>? NumericDecoder;

    /// <summary>True for the temporal target types this decoder handles.</summary>
    public static bool IsTemporal(Type t) =>
        t == typeof(DateTime) || t == typeof(DateTimeOffset) || t == typeof(DateOnly);

    /// <summary>
    /// If a decoder is registered, <paramref name="value"/> is numeric, and
    /// <paramref name="targetType"/> (or its underlying non-nullable type) is temporal, decode it
    /// and return true. Otherwise <paramref name="result"/> is null and the caller keeps its
    /// existing conversion path.
    /// </summary>
    public static bool TryDecode(object? value, Type targetType, out object? result)
    {
        result = null;
        if (NumericDecoder == null || value == null) return false;

        var t = Nullable.GetUnderlyingType(targetType) ?? targetType;
        if (!IsTemporal(t)) return false;

        double d;
        switch (value)
        {
            case double dd:   d = dd; break;
            case float f:     d = f; break;
            case decimal dec: d = (double)dec; break;
            case long l:      d = l; break;
            case int i:       d = i; break;
            case short s:     d = s; break;
            default:          return false;   // already a DateTime/string/etc — not our case
        }

        result = NumericDecoder(d, t);
        return true;
    }

    /// <summary>
    /// <see cref="System.Convert.ChangeType(object, Type)"/> that first lets a numeric value
    /// destined for a temporal type be decoded by the registered backend decoder. Non-temporal
    /// or non-numeric inputs fall straight through to <see cref="System.Convert.ChangeType(object, Type)"/>.
    /// </summary>
    public static object ChangeType(object value, Type targetType)
    {
        if (TryTicks(value, targetType, out var fromTicks))
            return fromTicks!;
        return TryDecode(value, targetType, out var decoded) && decoded != null
            ? decoded
            : redb.Core.Utils.RedbDecimal.ChangeType(value, targetType);
    }

    /// <summary>
    /// TimeSpan and TimeOnly are stored as their Ticks in _Long on every provider, so a number read back for one of them
    /// (a Min/Max, a group key, an average with a fraction, truncated) is its Ticks.
    /// </summary>
    private static bool TryTicks(object value, Type targetType, out object? result)
    {
        result = null;
        var t = Nullable.GetUnderlyingType(targetType) ?? targetType;
        if (t != typeof(TimeSpan) && t != typeof(TimeOnly)) return false;
        long ticks;
        switch (value)
        {
            case long l: ticks = l; break;
            case int i: ticks = i; break;
            case decimal m: ticks = (long)decimal.Truncate(m); break;
            case double d: ticks = (long)Math.Truncate(d); break;
            default: return false;
        }
        result = t == typeof(TimeSpan) ? TimeSpan.FromTicks(ticks) : new TimeOnly(ticks);
        return true;
    }
}
