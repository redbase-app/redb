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
public record PropertyInfo(string Name, Type Type, bool IsBaseField = false, PropertyFunction? Function = null);

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
    
    // Pro: Mathematical functions
    /// <summary>Absolute value (Math.Abs())</summary>
    Abs,
    /// <summary>Rounding (Math.Round())</summary>
    Round,
    /// <summary>Round down (Math.Floor())</summary>
    Floor,
    /// <summary>Round up (Math.Ceiling())</summary>
    Ceiling,
    
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
    Second
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
