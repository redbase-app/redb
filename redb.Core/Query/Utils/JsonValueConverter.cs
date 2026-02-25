using System.Text.Json;

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
            // Strings
            Type t when t == typeof(string) => elem.GetString(),
            
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
        if (elem.TryGetDateTime(out var dt))
            return dt;
        
        // Fallback: PostgreSQL row_to_json() may return non-ISO format
        var raw = elem.GetString();
        if (raw != null && DateTime.TryParse(raw, out var parsed))
            return parsed;
        
        return DateTime.MinValue;
    }
    
    /// <summary>
    /// Parses DateTimeOffset with fallback for PostgreSQL row_to_json() format.
    /// </summary>
    private static DateTimeOffset ParseDateTimeOffset(JsonElement elem)
    {
        if (elem.TryGetDateTimeOffset(out var dto))
            return dto;
        
        // Fallback: PostgreSQL row_to_json() may return non-ISO format (e.g., "2024-01-15 10:30:00+03")
        var raw = elem.GetString();
        if (raw != null && DateTimeOffset.TryParse(raw, out var parsed))
            return parsed;
        
        return DateTimeOffset.MinValue;
    }
}
