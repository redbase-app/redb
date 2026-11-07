using System.Text;
using redb.Core.Query;
using redb.Core.Utils;

namespace redb.Core.Query.Parameters;

/// <summary>
/// Base class for SQL parameter collection.
/// Provides value normalization and debug output.
/// Uses ISqlDialect for dialect-specific parameter formatting.
/// </summary>
public class SqlParameterCollectorBase
{
    private readonly ISqlDialect _dialect;
    private readonly List<object?> _parameters = new();
    private long? _schemeId;
    
    /// <summary>
    /// Creates a new parameter collector with the specified SQL dialect.
    /// </summary>
    public SqlParameterCollectorBase(ISqlDialect dialect)
    {
        _dialect = dialect ?? throw new ArgumentNullException(nameof(dialect));
    }
    
    /// <summary>
    /// Number of parameters added.
    /// </summary>
    public int Count => _parameters.Count;
    
    /// <summary>
    /// Sets SchemeId for debug output.
    /// </summary>
    public void SetSchemeId(long schemeId) => _schemeId = schemeId;
    
    /// <summary>
    /// Adds a parameter and returns the dialect-specific placeholder.
    /// </summary>
    /// <param name="value">Parameter value (will be normalized)</param>
    /// <returns>Parameter placeholder (e.g., "$1" for PostgreSQL, "@p0" for MSSQL)</returns>
    public string AddParameter(object? value)
    {
        var index = _parameters.Count;
        _parameters.Add(NormalizeValue(value));
        return _dialect.FormatParameter(index);
    }
    
    /// <summary>
    /// Adds a parameter with a custom index offset.
    /// Useful when building complex queries with multiple parameter sets.
    /// </summary>
    public string AddParameterWithOffset(object? value, int offset)
    {
        var index = _parameters.Count + offset;
        _parameters.Add(NormalizeValue(value));
        return _dialect.FormatParameter(index);
    }
    
    /// <summary>
    /// Returns parameters as array for use with FromSqlRaw/SqlQueryRaw.
    /// </summary>
    public object?[] ToArray() => _parameters.ToArray();
    
    /// <summary>
    /// Generates debug comment with parameter values (EF Core style).
    /// </summary>
    public string GetDebugComment()
    {
        var sb = new StringBuilder();
        sb.AppendLine("-- REDB Query Parameters:");
        
        if (_schemeId.HasValue)
            sb.AppendLine($"-- SchemeId: {_schemeId.Value}");
        
        for (int i = 0; i < _parameters.Count; i++)
        {
            var p = _parameters[i];
            var type = p?.GetType().Name ?? "null";
            var val = FormatForComment(p);
            sb.AppendLine($"-- {_dialect.FormatParameter(i)}={val} ({type})");
        }
        
        return sb.ToString();
    }
    
    /// <summary>
    /// Normalizes value before adding to parameters.
    /// Handles DateTime → UTC, object[] → typed array conversion.
    /// </summary>
    protected virtual object? NormalizeValue(object? value)
    {
        return value switch
        {
            // DateTime always converts to UTC
            DateTime dt => DateTimeConverter.NormalizeForStorage(dt),
            // DateTimeOffset converts to UTC DateTime
            DateTimeOffset dto => dto.UtcDateTime,
            // object[] converts to typed array for EF Core
            object[] arr => ConvertToTypedArray(arr),
            // Other values as-is
            _ => value
        };
    }
    
    /// <summary>
    /// Converts object[] to typed array for EF Core.
    /// EF Core doesn't support object[] - needs long[], string[], etc.
    /// </summary>
    private static object ConvertToTypedArray(object[] arr)
    {
        if (arr.Length == 0)
            return Array.Empty<long>(); // Default to long[] for empty array
            
        var firstNonNull = arr.FirstOrDefault(x => x != null);
        if (firstNonNull == null)
            return Array.Empty<long>();
            
        return firstNonNull switch
        {
            long => arr.Select(x => x is long l ? l : Convert.ToInt64(x)).ToArray(),
            int => arr.Select(x => x is int i ? i : Convert.ToInt32(x)).ToArray(),
            string => arr.Select(x => x?.ToString() ?? "").ToArray(),
            Guid => arr.Select(x => x is Guid g ? g : Guid.Parse(x?.ToString() ?? "")).ToArray(),
            decimal => arr.Select(x => x is decimal d ? d : Convert.ToDecimal(x)).ToArray(),
            double => arr.Select(x => x is double d ? d : Convert.ToDouble(x)).ToArray(),
            bool => arr.Select(x => x is bool b ? b : Convert.ToBoolean(x)).ToArray(),
            DateTime => arr.Select(x => x is DateTime dt ? DateTimeConverter.NormalizeForStorage(dt) : DateTime.MinValue).ToArray(),
            // Fallback - try to convert to long (most common case for IDs)
            _ when IsNumericType(firstNonNull.GetType()) => arr.Select(x => Convert.ToInt64(x)).ToArray(),
            _ => arr // If can't determine type - leave as is (may fail)
        };
    }
    
    private static bool IsNumericType(Type type) => 
        type == typeof(int) || type == typeof(long) || type == typeof(short) || 
        type == typeof(byte) || type == typeof(uint) || type == typeof(ulong) ||
        type == typeof(decimal) || type == typeof(double) || type == typeof(float);
    
    /// <summary>
    /// Formats value for debug comment.
    /// </summary>
    protected virtual string FormatForComment(object? value)
    {
        return value switch
        {
            null => "NULL",
            string s => $"'{EscapeForComment(s)}'",
            bool b => b ? "TRUE" : "FALSE",
            DateTime dt => $"'{dt:yyyy-MM-ddTHH:mm:ss.ffffffZ}'",
            DateTimeOffset dto => $"'{dto:yyyy-MM-ddTHH:mm:ss.ffffffzzz}'",
            decimal d => d.ToString(System.Globalization.CultureInfo.InvariantCulture),
            double d => d.ToString(System.Globalization.CultureInfo.InvariantCulture),
            float f => f.ToString(System.Globalization.CultureInfo.InvariantCulture),
            Guid g => $"'{g}'",
            long[] arr => $"[{string.Join(",", arr)}]",
            int[] arr => $"[{string.Join(",", arr)}]",
            string[] arr => $"[{string.Join(",", arr.Select(s => $"'{s}'"))}]",
            System.Collections.IEnumerable enumerable => FormatEnumerable(enumerable),
            _ => value.ToString() ?? "NULL"
        };
    }
    
    /// <summary>
    /// Formats IEnumerable for comment.
    /// </summary>
    private static string FormatEnumerable(System.Collections.IEnumerable enumerable)
    {
        var items = new List<string>();
        foreach (var item in enumerable)
        {
            items.Add(item?.ToString() ?? "NULL");
            if (items.Count > 10) 
            {
                items.Add("...");
                break;
            }
        }
        return $"[{string.Join(",", items)}]";
    }
    
    /// <summary>
    /// Escapes string for comment (truncates long strings).
    /// </summary>
    private static string EscapeForComment(string s)
    {
        var escaped = s.Replace("'", "''");
        if (escaped.Length > 50)
        {
            escaped = escaped[..47] + "...";
        }
        return escaped;
    }
}

