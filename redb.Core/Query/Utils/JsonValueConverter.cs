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
            
            // Integers (all mapped to _Long)
            Type t when t == typeof(long) => elem.TryGetInt64(out var l) ? l : 0L,
            Type t when t == typeof(int) => elem.TryGetInt32(out var i) ? i : 0,
            Type t when t == typeof(short) => elem.TryGetInt16(out var s) ? s : (short)0,
            Type t when t == typeof(byte) => elem.TryGetByte(out var b) ? b : (byte)0,
            
            // Decimals
            Type t when t == typeof(decimal) => elem.TryGetDecimal(out var d) ? d : 0m,
            Type t when t == typeof(double) => elem.TryGetDouble(out var dbl) ? dbl : 0.0,
            Type t when t == typeof(float) => elem.TryGetSingle(out var f) ? f : 0f,
            
            // Boolean
            Type t when t == typeof(bool) => elem.ValueKind == JsonValueKind.True ||
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
            // Unix timestamp
            return DateTimeOffset.FromUnixTimeSeconds(elem.GetInt64()).DateTime;
        }
        
        var str = elem.GetString();
        if (string.IsNullOrEmpty(str)) return DateTime.MinValue;
        
        if (DateTime.TryParse(str, out var dt)) return dt;
        return DateTime.MinValue;
    }

    private static DateTimeOffset ParseDateTimeOffset(JsonElement elem)
    {
        if (elem.ValueKind == JsonValueKind.Number)
        {
             return DateTimeOffset.FromUnixTimeSeconds(elem.GetInt64());
        }
        
        var str = elem.GetString();
        if (string.IsNullOrEmpty(str)) return DateTimeOffset.MinValue;
        
        if (DateTimeOffset.TryParse(str, out var dto)) return dto;
        return DateTimeOffset.MinValue;
    }
}
