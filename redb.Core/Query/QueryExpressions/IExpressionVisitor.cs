using System.Linq.Expressions;

namespace redb.Core.Query.QueryExpressions;

/// <summary>
/// Interface for processing Expression Tree
/// </summary>
public interface IExpressionVisitor<out TResult>
{
    /// <summary>
    /// Process expression and return result
    /// </summary>
    TResult Visit(Expression expression);
}

/// <summary>
/// Field information in expression
/// </summary>
/// <summary>
/// Property information for filtering
/// </summary>
/// <param name="Name">Property name</param>
/// <param name="Type">Property type</param>
/// <param name="IsBaseField">true if this is a base field of IRedbObject (Id, ParentId, ValueLong, etc.)</param>
/// <param name="Function">Optional: function applied to property (Length, Count)</param>
/// <param name="FunctionSourceType">Optional: the underlying source type the function is applied to
/// (e.g. <c>string[]</c> for <c>arr.Length</c>, or <c>string</c> for <c>name.Length</c>). Lets the
/// filter builder distinguish array-Length from string-Length when <see cref="Type"/> alone is ambiguous.</param>
public record PropertyInfo(string Name, Type Type, bool IsBaseField = false, PropertyFunction? Function = null, Type? FunctionSourceType = null);

/// <summary>
/// Functions that can be applied to property
/// </summary>
public enum PropertyFunction
{
    // Existing
    /// <summary>String length (string.Length)</summary>
    Length,
    /// <summary>Collection element count (array.Count)</summary>
    Count,
    
    // Pro: String functions
    /// <summary>Convert to lowercase (string.ToLower())</summary>
    ToLower,
    /// <summary>Convert to uppercase (string.ToUpper())</summary>
    ToUpper,
    /// <summary>Remove whitespace from both sides (string.Trim())</summary>
    Trim,
    /// <summary>Remove whitespace from start (string.TrimStart())</summary>
    TrimStart,
    /// <summary>Remove whitespace from end (string.TrimEnd())</summary>
    TrimEnd,
    /// <summary>Substring (string.Substring(start[, length])). Multi-arg.</summary>
    Substring,
    /// <summary>String replacement (string.Replace(oldValue, newValue)). Multi-arg.</summary>
    Replace,
    /// <summary>Substring search position, 0-based with -1 for not-found (string.IndexOf(value)). Multi-arg.</summary>
    IndexOf,
    /// <summary>Left-pad to width (string.PadLeft(width[, padChar])). Multi-arg.</summary>
    PadLeft,
    /// <summary>Right-pad to width (string.PadRight(width[, padChar])). Multi-arg.</summary>
    PadRight,

    // Pro: Mathematical functions
    /// <summary>Absolute value (Math.Abs())</summary>
    Abs,
    /// <summary>Rounding (Math.Round()). 1-arg via FunctionCall; 2-arg [value, digits] via Multi-arg.</summary>
    Round,
    /// <summary>Round down (Math.Floor())</summary>
    Floor,
    /// <summary>Round up (Math.Ceiling())</summary>
    Ceiling,
    /// <summary>Square root (Math.Sqrt())</summary>
    Sqrt,
    /// <summary>Sign of value: -1/0/+1 (Math.Sign())</summary>
    Sign,
    /// <summary>e raised to the power (Math.Exp())</summary>
    Exp,
    /// <summary>Natural logarithm (Math.Log() single-arg)</summary>
    Log,
    /// <summary>Logarithm with explicit base: Math.Log(value, base) / Math.Log10(value). Multi-arg [base, value] (PG LOG order).</summary>
    LogBase,
    /// <summary>Exponentiation (Math.Pow(x, y)). Multi-arg.</summary>
    Pow,
    
    // Pro: Date/time functions
    /// <summary>Year from date (DateTime.Year)</summary>
    Year,
    /// <summary>Month from date (DateTime.Month)</summary>
    Month,
    /// <summary>Day from date (DateTime.Day)</summary>
    Day,
    /// <summary>Hour from date (DateTime.Hour)</summary>
    Hour,
    /// <summary>Minute from date (DateTime.Minute)</summary>
    Minute,
    /// <summary>Second from date (DateTime.Second)</summary>
    Second,
    /// <summary>Day-of-week from date (DateTime.DayOfWeek, 0=Sunday for PG DOW).</summary>
    DayOfWeek,
    /// <summary>Day-of-year from date (DateTime.DayOfYear, 1-based).</summary>
    DayOfYear,
    /// <summary>DateTime.AddDays(n) — multi-arg [date, n].</summary>
    AddDays,
    /// <summary>DateTime.AddYears(n) — multi-arg [date, n].</summary>
    AddYears,
    /// <summary>DateTime.AddMonths(n) — multi-arg [date, n].</summary>
    AddMonths,
    /// <summary>DateTime.AddHours(n) — multi-arg [date, n].</summary>
    AddHours,
    /// <summary>DateTime.AddMinutes(n) — multi-arg [date, n].</summary>
    AddMinutes,
    /// <summary>DateTime.AddSeconds(n) — multi-arg [date, n].</summary>
    AddSeconds,
    /// <summary>Regex.Replace(input, pattern, replacement) — multi-arg [input, pattern, replacement].</summary>
    RegexReplace
}

/// <summary>
/// Arithmetic operators (Pro Only).
/// </summary>
public enum ArithmeticOperator
{
    /// <summary>Addition (+)</summary>
    Add,
    /// <summary>Subtraction (-)</summary>
    Subtract,
    /// <summary>Multiplication (*)</summary>
    Multiply,
    /// <summary>Division (/)</summary>
    Divide,
    /// <summary>Modulo (%)</summary>
    Modulo
}

/// <summary>
/// Comparison operators
/// UPDATED: Support for new paradigm with 25+ array operators
/// </summary>
public enum ComparisonOperator
{
    // 📋 Basic operators
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    Contains,
    ContainsIgnoreCase,     // Contains with case insensitive
    StartsWith,
    StartsWithIgnoreCase,   // StartsWith with case insensitive
    EndsWith,
    EndsWithIgnoreCase,     // EndsWith with case insensitive
    RegexMatch,             // POSIX regex match (Regex.IsMatch) — PG '~'
    RegexMatchIgnoreCase,   // POSIX regex match, IgnoreCase — PG '~*'
    
    // 🎯 NULL semantics  
    Exists,             // $exists - explicit field existence check
    
    // 🚀 Basic array operators
    ArrayContains,      // $arrayContains - search value in array
    ArrayAny,           // $arrayAny - check that array is not empty
    ArrayEmpty,         // $arrayEmpty - check that array is empty
    ArrayCount,         // $arrayCount - exact element count
    ArrayCountGt,       // $arrayCountGt - element count greater than N
    ArrayCountGte,      // $arrayCountGte - element count greater than or equal to N
    ArrayCountLt,       // $arrayCountLt - element count less than N
    ArrayCountLte,      // $arrayCountLte - element count less than or equal to N
    
    // 🎯 Positional array operators
    ArrayAt,            // $arrayAt - array element by index
    ArrayFirst,         // $arrayFirst - first array element
    ArrayLast,          // $arrayLast - last array element
    
    // 🔍 Search array operators (for strings)
    ArrayStartsWith,    // $arrayStartsWith - string values starting with prefix
    ArrayEndsWith,      // $arrayEndsWith - string values ending with suffix
    ArrayMatches,       // $arrayMatches - search by regular expression
    
    // 📈 Aggregation array operators
    ArraySum,           // $arraySum - sum of numeric elements
    ArrayAvg,           // $arrayAvg - arithmetic average
    ArrayMin,           // $arrayMin - minimum value
    ArrayMax            // $arrayMax - maximum value
}

/// <summary>
/// Logical operators
/// </summary>
public enum LogicalOperator
{
    And,
    Or,
    Not
}

/// <summary>
/// Sort direction
/// </summary>
public enum SortDirection
{
    Ascending,
    Descending
}
