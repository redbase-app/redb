using System.Globalization;
using System.Text.Json;
using redb.Core.Models.Entities;

namespace redb.Core.Query.Utils;

/// <summary>
/// Unified converter JsonElement → CLR types for EAV
/// Supports all types: string, numeric, bool, DateTime, Guid
/// </summary>
public static class JsonValueConverter
{
    /// <summary>
    /// Converts JsonElement to specified CLR type
    /// </summary>
    public static object? Convert(JsonElement elem, Type targetType)
    {
        if (elem.ValueKind == JsonValueKind.Null) 
            return null;
        
        var underlyingType = Nullable.GetUnderlyingType(targetType) ?? targetType;
        
        return underlyingType switch
        {
            // Strings — accept any scalar JSON kind, stringify Number/Bool fallbacks
            Type t when t == typeof(string) => elem.ValueKind == JsonValueKind.String
                ? elem.GetString()
                : (elem.ValueKind == JsonValueKind.Number || elem.ValueKind == JsonValueKind.True || elem.ValueKind == JsonValueKind.False
                    ? elem.GetRawText()
                    : null),

            // RedbListItem — projected from grouping/aggregations as scalar (Id or Value).
            // Build a minimal stub so callers can surface the key without an extra ListProvider lookup.
            Type t when t == typeof(RedbListItem) => BuildListItemStub(elem),
            
            // Integers (all mapped to _Long). Read via ReadIntegral, NOT bare TryGetInt64: MSSql renders
            // SUM/MIN/MAX of a base bigint field as numeric(38,10), so FOR JSON emits "2130762.0000000000".
            // TryGetInt64 rejects any number carrying a decimal point and would silently fall back to 0 —
            // ReadIntegral truncates the zero fraction instead. Same provider-rendering tolerance already
            // applied to bool (SQLite 0/1) and DateTime formats below.
            Type t when t == typeof(long) => (long)ReadIntegral(elem),
            Type t when t == typeof(int) => (int)ReadIntegral(elem),
            Type t when t == typeof(short) => (short)ReadIntegral(elem),
            Type t when t == typeof(byte) => (byte)ReadIntegral(elem),
            
            // Decimals
            Type t when t == typeof(decimal) => elem.TryGetDecimal(out var d) ? d : 0m,
            Type t when t == typeof(double) => elem.TryGetDouble(out var dbl) ? dbl : 0.0,
            Type t when t == typeof(float) => elem.TryGetSingle(out var f) ? f : 0f,
            
            // Boolean — PostgreSQL emits JSON true/false; SQLite has no native bool and stores it as
            // INTEGER 0/1, so a grouping/projection column arrives as a JSON Number → treat nonzero as true.
            Type t when t == typeof(bool) => elem.ValueKind == JsonValueKind.True ||
                (elem.ValueKind == JsonValueKind.Number && elem.TryGetDouble(out var bn) && bn != 0) ||
                (elem.ValueKind == JsonValueKind.String && bool.TryParse(elem.GetString(), out var bl) && bl),
            
            // DateTime / DateTimeOffset — with PostgreSQL row_to_json() format support
            Type t when t == typeof(DateTime) => ParseDateTime(elem),
            Type t when t == typeof(DateTimeOffset) => ParseDateTimeOffset(elem),
            
            // Guid
            Type t when t == typeof(Guid) => elem.TryGetGuid(out var g) ? g : Guid.Empty,
            
            // Fallback
            _ => elem.GetRawText()
        };
    }

    /// <summary>
    /// Reads a JSON scalar as an integral value, tolerating provider-specific numeric rendering.
    /// A plain integer takes the fast <see cref="JsonElement.TryGetInt64"/> path; a number with a
    /// (zero) fraction — MSSql's numeric(38,10) aggregate rendering — is read as decimal and truncated;
    /// an integer-as-string is parsed defensively. Returns 0 for a non-numeric scalar. The caller casts
    /// the returned decimal to the concrete integer type, so a value beyond that type's range overflows
    /// loudly rather than silently collapsing to 0.
    /// </summary>
    private static decimal ReadIntegral(JsonElement elem)
    {
        if (elem.ValueKind == JsonValueKind.Number)
        {
            if (elem.TryGetInt64(out var l)) return l;
            if (elem.TryGetDecimal(out var d)) return decimal.Truncate(d);
            if (elem.TryGetDouble(out var db)) return (decimal)Math.Truncate(db);
        }
        else if (elem.ValueKind == JsonValueKind.String &&
                 decimal.TryParse(elem.GetString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var ds))
        {
            return decimal.Truncate(ds);
        }
        return 0m;
    }

    /// <summary>
    /// Builds a minimal <see cref="RedbListItem"/> from a scalar JSON value.
    /// Number → Id; String → Value; Object → full deserialization of known fields.
    /// </summary>
    private static RedbListItem? BuildListItemStub(JsonElement elem)
    {
        switch (elem.ValueKind)
        {
            case JsonValueKind.Number:
                return elem.TryGetInt64(out var id) ? new RedbListItem { Id = id } : null;
            case JsonValueKind.String:
                return new RedbListItem { Value = elem.GetString() ?? string.Empty };
            case JsonValueKind.Object:
                var item = new RedbListItem();
                if (elem.TryGetProperty("Id", out var idProp) && idProp.TryGetInt64(out var idVal)) item.Id = idVal;
                if (elem.TryGetProperty("Value", out var valProp) && valProp.ValueKind == JsonValueKind.String) item.Value = valProp.GetString() ?? string.Empty;
                if (elem.TryGetProperty("Alias", out var aliasProp) && aliasProp.ValueKind == JsonValueKind.String) item.Alias = aliasProp.GetString();
                if (elem.TryGetProperty("IdList", out var listProp) && listProp.TryGetInt64(out var listVal)) item.IdList = listVal;
                return item;
            default:
                return null;
        }
    }
    
    /// <summary>
    /// Typed version
    /// </summary>
    public static T? Convert<T>(JsonElement elem) => (T?)Convert(elem, typeof(T));
    
    /// <summary>
    /// Get default value for type
    /// </summary>
    public static object? GetDefault(Type type) =>
        type.IsValueType ? Activator.CreateInstance(type) : null;
    
    /// <summary>
    /// Parses DateTime with fallback for PostgreSQL row_to_json() format.
    /// </summary>
    private static DateTime ParseDateTime(JsonElement elem)
    {
        if (elem.ValueKind == JsonValueKind.Number)
        {
            // A numeric datetime → ask the registered backend decoder (e.g. SQLite REAL Julian day).
            if (elem.TryGetDouble(out var num) && TemporalDecoder.TryDecode(num, typeof(DateTime), out var dec) && dec != null)
                return (DateTime)dec;
            // Legacy fallback: Unix timestamp seconds.
            return DateTimeOffset.FromUnixTimeSeconds(elem.GetInt64()).DateTime;
        }
        
        var str = elem.GetString();
        if (string.IsNullOrEmpty(str)) return DateTime.MinValue;

        // Parse as DateTimeOffset, then take the UTC instant — identical to the object
        // materialization path (PostgresInfinityDateTimeConverter). A bare DateTime.TryParse
        // with DateTimeStyles.None converts a zoned ISO string INTO THE CALLER'S LOCAL ZONE,
        // so the same field read through analytics (Min/Max/GroupBy/Window/projection) came
        // back shifted relative to the same field read through the object. REDB's contract is
        // that DateTime carries no zone: 14:00 written is 14:00 read, on any machine.
        if (DateTimeOffset.TryParse(str, CultureInfo.InvariantCulture,
                DateTimeStyles.RoundtripKind, out var dto))
            return Core.Utils.DateTimeConverter.DenormalizeFromStorage(dto);
        return DateTime.MinValue;
    }

    private static DateTimeOffset ParseDateTimeOffset(JsonElement elem)
    {
        if (elem.ValueKind == JsonValueKind.Number)
        {
            // A numeric datetime → ask the registered backend decoder (e.g. SQLite REAL Julian day).
            if (elem.TryGetDouble(out var num) && TemporalDecoder.TryDecode(num, typeof(DateTimeOffset), out var dec) && dec != null)
                return (DateTimeOffset)dec;
            // Legacy fallback: Unix timestamp seconds.
            return DateTimeOffset.FromUnixTimeSeconds(elem.GetInt64());
        }
        
        var str = elem.GetString();
        if (string.IsNullOrEmpty(str)) return DateTimeOffset.MinValue;

        // Invariant culture + ToUniversalTime, so the analytics path returns exactly what the
        // object materialization path returns (PostgresDateTimeOffsetConverter). DateTimeOffset
        // keeps native .NET semantics — it carries an instant, unlike DateTime.
        if (DateTimeOffset.TryParse(str, CultureInfo.InvariantCulture,
                DateTimeStyles.RoundtripKind, out var dto))
            return dto.ToUniversalTime();
        return DateTimeOffset.MinValue;
    }
}
