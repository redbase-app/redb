using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using redb.Core.Models.Contracts;
using redb.Core.Query.FacetFilters;
using redb.Core.Query.QueryExpressions;

namespace redb.Core.Query;

/// <summary>
/// Builds JSON filters for search_objects_with_facets function.
/// Supports 25+ operators, nullable fields, Class fields, arrays.
/// </summary>
public class FacetFilterBuilder : IFacetFilterBuilder
{
    private readonly JsonSerializerOptions _jsonOptions;
    private readonly ILogger? _logger;

    public FacetFilterBuilder(ILogger? logger = null)
    {
        _logger = logger;
        _jsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false,
            Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
        };
    }

    public string BuildFacetFilters(FilterExpression? filter)
    {
        if (filter == null)
        {
            _logger?.LogDebug("LINQ Filter: No filter provided, returning empty filter: {{}}");
            return "{}";
        }

        var filterObject = BuildFilterObject(filter);
        var filterJson = JsonSerializer.Serialize(filterObject, _jsonOptions);
        
        _logger?.LogDebug("LINQ Filter Generated: {FilterJson}", filterJson);
        return filterJson;
    }

    public virtual string BuildOrderBy(IReadOnlyList<OrderingExpression> orderings)
    {
        if (!orderings.Any())
        {
            _logger?.LogDebug("LINQ OrderBy: No ordering provided, returning empty array: []");
            return "[]";
        }

        // Extended format: [{"field": "Name", "direction": "ASC"}]
        // Works with both PostgreSQL and MSSQL (MSSQL requires this format)
        var orderArray = orderings.Select(o => new
        {
            field = BuildFieldPath(o.Property),
            direction = o.Direction == SortDirection.Ascending ? "ASC" : "DESC"
        }).ToArray();

        var orderJson = JsonSerializer.Serialize(orderArray, _jsonOptions);
        _logger?.LogDebug("LINQ OrderBy Generated: {OrderJson}", orderJson);
        return orderJson;
    }

    public QueryParameters BuildQueryParameters(int? limit = null, int? offset = null)
    {
        return new QueryParameters(limit, offset);
    }

    private object BuildFilterObject(FilterExpression filter)
    {
        return filter switch
        {
            ComparisonExpression comparison => BuildComparisonFilter(comparison),
            LogicalExpression logical => BuildLogicalFilter(logical),
            NullCheckExpression nullCheck => BuildNullCheckFilter(nullCheck),
            InExpression inExpr => BuildInFilter(inExpr),
            _ => throw new NotSupportedException($"Filter expression type {filter.GetType().Name} is not supported")
        };
    }

    private object BuildComparisonFilter(ComparisonExpression comparison)
    {
        // Computed-expression comparison: arithmetic/function on either side.
        // Emits PVT `$expr` boolean predicate node.
        if (comparison.LeftExpression != null || comparison.RightExpression != null
            || comparison.Property.Name == "__computed")
        {
            // Special case: `arr.Length OP const` (compiled as ArrayLength UnaryExpression
            // and surfaced as FunctionCallExpression(Length, PropertyValue(arr)) by the
            // parser) cannot use the generic $expr/$length form because that expands to
            // text LENGTH(...) only. Route through the `.$length` modifier so the PVT
            // engine picks the array-aware array_length() form.
            if (TryBuildArrayLengthCountFilter(comparison, out var arrFilter))
            {
                return arrFilter!;
            }
            return BuildComputedExprFilter(comparison);
        }

        var fieldName = BuildFieldPath(comparison.Property);

        // Handling property functions: Length, Count, ToLower, ToUpper, Trim, Year, ...
        if (comparison.Property.Function.HasValue)
        {
            return BuildFunctionComparisonFilter(comparison, fieldName);
        }
        
        // ✅ FIX: Convert value to correct type based on field schema!
        var originalValue = comparison.Value;
        
        // 🎯 NEW: Handle direct ListItem comparison (fallback check)
        // Main handling happens in parser, but add protection at builder level
        if (originalValue is IRedbListItem listItem)
        {
            originalValue = listItem.Id;

            // Bare ListItem field path (e.g. "Status") defaults to .Value
            // (string) on the SQL side. Direct comparison against a
            // RedbListItem instance always means "match by id", so route
            // through the .Id (bigint) accessor.
            if (!fieldName.Contains('.', StringComparison.Ordinal)
                && IsRedbListItemType(comparison.Property.Type))
            {
                fieldName += ".Id";
            }
        }
        
        // 🔍 CRITICAL LOGGING: check what type comes in
        // _logger?.LogInformation($"🔍 BEFORE ConvertValueToFieldType:");
        // _logger?.LogInformation($"   📋 Property.Type: {comparison.Property.Type.Name}");
        // _logger?.LogInformation($"   📋 Is DateTime?: {comparison.Property.Type == typeof(DateTime)}");
        // _logger?.LogInformation($"   📋 Is Nullable<DateTime>?: {comparison.Property.Type == typeof(DateTime?)}");
        // _logger?.LogInformation($"   📋 Underlying type: {Nullable.GetUnderlyingType(comparison.Property.Type)?.Name ?? "not nullable"}");
        
        var value = ConvertValueToFieldType(originalValue, comparison.Property.Type);
        
        // 🔍 DETAILED TYPE CONVERSION LOGGING
        // _logger?.LogInformation($"🔍 TYPE CONVERSION: Field '{comparison.Property.Name}' (Type: {comparison.Property.Type.Name})");
        // _logger?.LogInformation($"   📥 Original value: {originalValue} ({originalValue?.GetType().Name ?? "null"})");
        // _logger?.LogInformation($"   📤 Converted value: {value} ({value?.GetType().Name ?? "null"})");
        
        // 🔍 DETAILED LOGGING FOR DateTime/DateTimeOffset
        // if (value is DateTime dtValue)
        // {
        //     _logger?.LogInformation($"   ⏰ DateTime.Kind: {dtValue.Kind}, Value: {dtValue:yyyy-MM-dd HH:mm:ss}");
        // }
        // else if (value is DateTimeOffset dtoValue)
        // {
        //     _logger?.LogInformation($"   ⏰ DateTimeOffset: {dtoValue:yyyy-MM-dd HH:mm:ss zzz}, UTC: {dtoValue.UtcDateTime:yyyy-MM-dd HH:mm:ss}");
        // }
        // _logger?.LogInformation($"   🎯 Operator: {comparison.Operator}");

        // 🔍 SPECIAL LOGGING FOR EQUALITY
        if (comparison.Operator == ComparisonOperator.Equal)
        {
            // ✅ FIX JSON SERIALIZATION: For Double types forcibly create decimal number  
            var finalValue = value;
            if (comparison.Property.Type == typeof(double) || comparison.Property.Type == typeof(double?))
            {
                if (value is double doubleVal && doubleVal == Math.Floor(doubleVal))
                {
                    // Convert 2000.0 to "2000.0" so SQL understands it's Double
                    finalValue = doubleVal.ToString("F1", System.Globalization.CultureInfo.InvariantCulture);  // 2000 → "2000.0"
                    _logger?.LogInformation($"   🔧 FIXED: Forcibly create double string: {finalValue}");
                }
            }
            
            var result = new Dictionary<string, object>
            {
                [fieldName] = new Dictionary<string, object?> { ["$eq"] = finalValue }
            };
            
            // 🔍 LOG FINAL JSON FILTER  
            // _logger?.LogInformation($"   📋 Generated filter: {{{fieldName}: {{\"$eq\": {finalValue}}}}}");
            return result;
        }
        
        return comparison.Operator switch
        {
            ComparisonOperator.NotEqual => new Dictionary<string, object> 
            { 
                [fieldName] = new Dictionary<string, object?> { ["$ne"] = value } 
            },
            ComparisonOperator.GreaterThan => new Dictionary<string, object> 
            { 
                [fieldName] = new Dictionary<string, object?> { ["$gt"] = value } 
            },
            ComparisonOperator.GreaterThanOrEqual => new Dictionary<string, object> 
            { 
                [fieldName] = new Dictionary<string, object?> { ["$gte"] = value } 
            },
            ComparisonOperator.LessThan => new Dictionary<string, object> 
            { 
                [fieldName] = new Dictionary<string, object?> { ["$lt"] = value } 
            },
            ComparisonOperator.LessThanOrEqual => new Dictionary<string, object> 
            { 
                [fieldName] = new Dictionary<string, object?> { ["$lte"] = value } 
            },
            ComparisonOperator.Contains => BuildContainsFilter(fieldName, value, false),
            ComparisonOperator.ContainsIgnoreCase => BuildContainsFilter(fieldName, value, true),
            ComparisonOperator.StartsWith => new Dictionary<string, object> 
            { 
                [fieldName] = new Dictionary<string, object?> { ["$startsWith"] = value } 
            },
            ComparisonOperator.StartsWithIgnoreCase => new Dictionary<string, object> 
            { 
                [fieldName] = new Dictionary<string, object?> { ["$startsWithIgnoreCase"] = value } 
            },
            ComparisonOperator.EndsWith => new Dictionary<string, object> 
            { 
                [fieldName] = new Dictionary<string, object?> { ["$endsWith"] = value } 
            },
            ComparisonOperator.EndsWithIgnoreCase => new Dictionary<string, object> 
            { 
                [fieldName] = new Dictionary<string, object?> { ["$endsWithIgnoreCase"] = value } 
            },
            
            // 🎯 NULL SEMANTICS
            ComparisonOperator.Exists => new Dictionary<string, object> 
            { 
                [fieldName] = new Dictionary<string, object> { ["$exists"] = value } 
            },
            // 🚀 BASIC ARRAY OPERATORS
            ComparisonOperator.ArrayContains => BuildArrayFilter(fieldName, "$arrayContains", value),
            ComparisonOperator.ArrayAny => BuildArrayFilter(fieldName, "$arrayAny", true),
            ComparisonOperator.ArrayEmpty => BuildArrayFilter(fieldName, "$arrayEmpty", true),
            ComparisonOperator.ArrayCount => BuildArrayFilter(fieldName, "$arrayCount", value),
            ComparisonOperator.ArrayCountGt => BuildArrayFilter(fieldName, "$arrayCountGt", value),
            ComparisonOperator.ArrayCountGte => BuildArrayFilter(fieldName, "$arrayCountGte", value),
            ComparisonOperator.ArrayCountLt => BuildArrayFilter(fieldName, "$arrayCountLt", value),
            ComparisonOperator.ArrayCountLte => BuildArrayFilter(fieldName, "$arrayCountLte", value),
            
            // 🎯 POSITIONAL ARRAY OPERATORS
            ComparisonOperator.ArrayAt => BuildArrayFilter(fieldName, "$arrayAt", value),
            ComparisonOperator.ArrayFirst => BuildArrayFilter(fieldName, "$arrayFirst", value),
            ComparisonOperator.ArrayLast => BuildArrayFilter(fieldName, "$arrayLast", value),
            
            // 🔍 SEARCH ARRAY OPERATORS
            ComparisonOperator.ArrayStartsWith => BuildArrayFilter(fieldName, "$arrayStartsWith", value),
            ComparisonOperator.ArrayEndsWith => BuildArrayFilter(fieldName, "$arrayEndsWith", value),
            ComparisonOperator.ArrayMatches => BuildArrayFilter(fieldName, "$arrayMatches", value),
            
            // 📈 AGGREGATION ARRAY OPERATORS
            ComparisonOperator.ArraySum => BuildArrayFilter(fieldName, "$arraySum", value),
            ComparisonOperator.ArrayAvg => BuildArrayFilter(fieldName, "$arrayAvg", value),
            ComparisonOperator.ArrayMin => BuildArrayFilter(fieldName, "$arrayMin", value),
            ComparisonOperator.ArrayMax => BuildArrayFilter(fieldName, "$arrayMax", value),
            _ => throw new NotSupportedException($"Comparison operator {comparison.Operator} is not supported")
        };
    }

    private object BuildLogicalFilter(LogicalExpression logical)
    {
        return logical.Operator switch
        {
            LogicalOperator.And => BuildAndFilter(logical.Operands),
            LogicalOperator.Or => new Dictionary<string, object> 
            { 
                ["$or"] = logical.Operands.Select(BuildFilterObject).ToArray() 
            },
            LogicalOperator.Not => new Dictionary<string, object> 
            { 
                ["$not"] = BuildFilterObject(logical.Operands.First()) 
            },
            _ => throw new NotSupportedException($"Logical operator {logical.Operator} is not supported")
        };
    }

    private object BuildAndFilter(IReadOnlyList<FilterExpression> operands)
    {
        // For AND we can merge conditions into one object if they don't conflict
        // Otherwise use $and
        var result = new Dictionary<string, object>();

        foreach (var operand in operands)
        {
            var filterObj = BuildFilterObject(operand);
            
            if (filterObj is Dictionary<string, object> dict)
            {
                foreach (var kvp in dict)
                {
                    if (result.ContainsKey(kvp.Key))
                    {
                        // Key conflict - use $and
                        return new Dictionary<string, object>
                        {
                            ["$and"] = operands.Select(BuildFilterObject).ToArray()
                        };
                    }
                    result[kvp.Key] = kvp.Value;
                }
            }
            else
            {
                // Complex object - use $and
                return new Dictionary<string, object>
                {
                    ["$and"] = operands.Select(BuildFilterObject).ToArray()
                };
            }
        }

        return result;
    }

    /// <summary>
    /// Builds filter for property functions like Length, ToLower, Year via PVT $expr form:
    /// <c>{"$expr": {"$gt": [{"$length": {"$field": "Name"}}, {"$const": 3}]}}</c>.
    /// </summary>
    private object BuildFunctionComparisonFilter(ComparisonExpression comparison, string fieldName)
    {
        var function = comparison.Property.Function!.Value;
        var opKey = MapComparisonOperatorToExprKey(comparison.Operator);

        // Length/Count on arrays/collections is polymorphic in SQL and cannot use the
        // generic $expr/$length form (which expands to text LENGTH(...) only).
        // Route through the legacy `.$length` / `.$count` modifier so the PVT SQL
        // layer picks the array-aware array_length() form based on registered field metadata.
        if ((function == QueryExpressions.PropertyFunction.Length || function == QueryExpressions.PropertyFunction.Count)
            && IsArrayOrCollectionType(comparison.Property.FunctionSourceType))
        {
            var modifier = function == QueryExpressions.PropertyFunction.Length ? ".$length" : ".$count";
            return new Dictionary<string, object?>
            {
                [fieldName + modifier] = new Dictionary<string, object?>
                {
                    [opKey] = comparison.Value
                }
            };
        }

        var funcKey = MapPropertyFunctionToExprKey(function);

        // Inner scalar node: {"$<func>": {"$field": "<fieldName>"}}
        var inner = new Dictionary<string, object?>
        {
            [funcKey] = new Dictionary<string, object?> { ["$field"] = fieldName }
        };

        var rhs = new Dictionary<string, object?> { ["$const"] = comparison.Value };

        return new Dictionary<string, object?>
        {
            ["$expr"] = new Dictionary<string, object?>
            {
                [opKey] = new object?[] { inner, rhs }
            }
        };
    }

    private static bool IsArrayOrCollectionType(Type? t)
    {
        if (t == null) return false;
        if (t == typeof(string)) return false;
        if (t.IsArray) return true;
        if (typeof(System.Collections.IEnumerable).IsAssignableFrom(t)) return true;
        return false;
    }

    /// <summary>
    /// Detects `arr.Length/.Count OP const` (array side) and rewrites the comparison
    /// as the legacy `.$length` / `.$count` modifier filter, which the PVT SQL builder
    /// translates to <c>array_length()</c> for array-typed fields.
    /// Returns false for non-matching shapes (string.Length, both sides expressions, etc.)
    /// so caller falls back to <see cref="BuildComputedExprFilter"/>.
    /// </summary>
    private bool TryBuildArrayLengthCountFilter(ComparisonExpression comparison, out object? filter)
    {
        filter = null;

        FunctionCallExpression? fc = null;
        object? constValue = null;
        ComparisonOperator op = comparison.Operator;
        bool funcOnLeft = false;

        if (comparison.LeftExpression is FunctionCallExpression lfc
            && comparison.RightExpression is ConstantValueExpression rcv)
        {
            fc = lfc;
            constValue = rcv.Value;
            funcOnLeft = true;
        }
        else if (comparison.RightExpression is FunctionCallExpression rfc
                 && comparison.LeftExpression is ConstantValueExpression lcv)
        {
            fc = rfc;
            constValue = lcv.Value;
        }
        else
        {
            return false;
        }

        if (fc.Function != QueryExpressions.PropertyFunction.Length
            && fc.Function != QueryExpressions.PropertyFunction.Count)
        {
            return false;
        }

        if (fc.Argument is not PropertyValueExpression pv) return false;
        if (!IsArrayOrCollectionType(pv.Property.Type)) return false;

        // Mirror operator if function was on the right side: `5 < arr.Length` becomes `arr.Length > 5`
        if (!funcOnLeft)
        {
            op = op switch
            {
                ComparisonOperator.GreaterThan        => ComparisonOperator.LessThan,
                ComparisonOperator.GreaterThanOrEqual => ComparisonOperator.LessThanOrEqual,
                ComparisonOperator.LessThan           => ComparisonOperator.GreaterThan,
                ComparisonOperator.LessThanOrEqual    => ComparisonOperator.GreaterThanOrEqual,
                _ => op
            };
        }

        var fieldName = BuildFieldPath(pv.Property);
        // Both arr.Length and coll.Count on an array/collection field map to the PVT
        // `.$count` modifier, which expands to COUNT(*) over the rows representing the
        // array. The `.$length` modifier is reserved for STRING length and expands to
        // LENGTH(_String), which is the wrong operation for arrays.
        var modifier = ".$count";
        var opKey = MapComparisonOperatorToExprKey(op);

        filter = new Dictionary<string, object?>
        {
            [fieldName + modifier] = new Dictionary<string, object?>
            {
                [opKey] = constValue
            }
        };
        return true;
    }

    /// <summary>
    /// Builds PVT <c>$expr</c> predicate for computed comparisons whose operands are
    /// arithmetic / function expressions (LeftExpression/RightExpression).
    /// </summary>
    private object BuildComputedExprFilter(ComparisonExpression comparison)
    {
        var opKey = MapComparisonOperatorToExprKey(comparison.Operator);

        object? lhs = comparison.LeftExpression != null
            ? BuildScalarExprNode(comparison.LeftExpression)
            : new Dictionary<string, object?> { ["$field"] = BuildFieldPath(comparison.Property) };

        object? rhs = comparison.RightExpression != null
            ? BuildScalarExprNode(comparison.RightExpression)
            : new Dictionary<string, object?> { ["$const"] = comparison.Value };

        return new Dictionary<string, object?>
        {
            ["$expr"] = new Dictionary<string, object?>
            {
                [opKey] = new object?[] { lhs, rhs }
            }
        };
    }

    private object BuildScalarExprNode(ValueExpression expr)
    {
        switch (expr)
        {
            case PropertyValueExpression pv:
                return new Dictionary<string, object?>
                {
                    ["$field"] = BuildFieldPath(pv.Property)
                };

            case ConstantValueExpression cv:
            {
                var value = cv.Value;
                if (value is IRedbListItem li) value = li.Id;
                return new Dictionary<string, object?> { ["$const"] = value };
            }

            case ArithmeticExpression ax:
            {
                // String '+' in C# is concatenation. PVT $add maps to SQL '+'
                // (numeric); for text operands we must emit $concat (SQL '||').
                var key = ax.Operator switch
                {
                    ArithmeticOperator.Add when IsStringValueExpression(ax.Left) || IsStringValueExpression(ax.Right)
                                                 => "$concat",
                    ArithmeticOperator.Add      => "$add",
                    ArithmeticOperator.Subtract => "$sub",
                    ArithmeticOperator.Multiply => "$mul",
                    ArithmeticOperator.Divide   => "$div",
                    ArithmeticOperator.Modulo   => "$mod",
                    _ => throw new NotSupportedException($"ArithmeticOperator.{ax.Operator} is not supported.")
                };
                return new Dictionary<string, object?>
                {
                    [key] = new object?[]
                    {
                        BuildScalarExprNode(ax.Left),
                        BuildScalarExprNode(ax.Right)
                    }
                };
            }

            case FunctionCallExpression fc:
            {
                var key = MapPropertyFunctionToExprKey(fc.Function);
                return new Dictionary<string, object?>
                {
                    [key] = BuildScalarExprNode(fc.Argument)
                };
            }

            case MultiArgFunctionCallExpression mfc:
            {
                // DateTime.AddX(n) -> {"$dateadd": ["<unit>", <date>, <n>]} (unit is a plain JSON
                // string literal, NOT wrapped in $const — see pvt_build_scalar_expr in 17_pvt_expr.sql).
                var unit = MapAddFunctionToUnit(mfc.Function);
                if (unit != null)
                {
                    if (mfc.Arguments.Count != 2)
                        throw new InvalidOperationException($"DateTime.{mfc.Function} expects [date, n] (2 args).");
                    return new Dictionary<string, object?>
                    {
                        ["$dateadd"] = new object?[]
                        {
                            unit,
                            BuildScalarExprNode(mfc.Arguments[0]),
                            BuildScalarExprNode(mfc.Arguments[1])
                        }
                    };
                }

                // Regex.Replace(input, pattern, replacement) -> {"$regexreplace": [input, pat, repl, "g"]}
                // Inject 'g' flag as 4th arg ($const text literal) so PVT REGEXP_REPLACE replaces
                // all matches like .NET Regex.Replace (PG default is first-match only).
                if (mfc.Function == QueryExpressions.PropertyFunction.RegexReplace)
                {
                    if (mfc.Arguments.Count != 3)
                        throw new InvalidOperationException("Regex.Replace expects [input, pattern, replacement] (3 args).");
                    return new Dictionary<string, object?>
                    {
                        ["$regexreplace"] = new object?[]
                        {
                            BuildScalarExprNode(mfc.Arguments[0]),
                            BuildScalarExprNode(mfc.Arguments[1]),
                            BuildScalarExprNode(mfc.Arguments[2]),
                            new Dictionary<string, object?> { ["$const"] = "g" }
                        }
                    };
                }

                // Multi-arg string functions (Substring/Replace/IndexOf/PadLeft/PadRight)
                // map to {"$substring": [a, b[, c]]}, {"$replace": [a, b, c]}, etc.
                // The parser already applied C#->SQL index translation (Substring start+1, IndexOf - 1).
                var key = MapPropertyFunctionToExprKey(mfc.Function);
                var args = new object?[mfc.Arguments.Count];
                for (int i = 0; i < mfc.Arguments.Count; i++)
                    args[i] = BuildScalarExprNode(mfc.Arguments[i]);
                return new Dictionary<string, object?>
                {
                    [key] = args
                };
            }

            case CustomFunctionExpression cfe:
            {
                var args = new object?[cfe.Arguments.Count];
                for (int i = 0; i < cfe.Arguments.Count; i++)
                    args[i] = BuildScalarExprNode(cfe.Arguments[i]);
                return new Dictionary<string, object?>
                {
                    ["$" + cfe.FunctionName.ToLowerInvariant()] = args
                };
            }

            case CoalesceExpression ce:
            {
                // ?? operator (n-ary) → {"$coalesce": [a, b, ...]}, handled by
                // pvt_build_scalar_expr (17_pvt_expr.sql) which emits COALESCE(a, b, ...).
                var args = new object?[ce.Arguments.Count];
                for (int i = 0; i < ce.Arguments.Count; i++)
                    args[i] = BuildScalarExprNode(ce.Arguments[i]);
                return new Dictionary<string, object?>
                {
                    ["$coalesce"] = args
                };
            }

            case ConditionalValueExpression cv:
            {
                // C# ternary cond ? a : b → {"$if": [<bool-node>, <then>, <else>]},
                // handled by pvt_build_scalar_expr ($if branch) in 17_pvt_expr.sql.
                return new Dictionary<string, object?>
                {
                    ["$if"] = new object?[]
                    {
                        BuildBoolExprNode(cv.Test),
                        BuildScalarExprNode(cv.IfTrue),
                        BuildScalarExprNode(cv.IfFalse)
                    }
                };
            }

            default:
                throw new NotSupportedException($"ValueExpression {expr.GetType().Name} is not supported in $expr.");
        }
    }

    /// <summary>
    /// Builds the inner-predicate JSON shape expected by <c>pvt_build_bool_expr</c>
    /// (see <c>17_pvt_expr.sql</c>): comparisons render as
    /// <c>{"$eq":[lhs,rhs]}</c>, logical operators as <c>{"$and"/"$or":[..]}</c> /
    /// <c>{"$not":node}</c>, null-checks as <c>{"$null":[field]}</c> /
    /// <c>{"$notnull":[field]}</c>. Used as the <c>cond</c> slot of <c>$if</c>.
    /// </summary>
    private object BuildBoolExprNode(FilterExpression filter)
    {
        switch (filter)
        {
            case ComparisonExpression cmp:
            {
                var opKey = MapComparisonOperatorToExprKey(cmp.Operator);
                var lhs = cmp.LeftExpression != null
                    ? BuildScalarExprNode(cmp.LeftExpression)
                    : new Dictionary<string, object?> { ["$field"] = BuildFieldPath(cmp.Property) };
                var rhs = cmp.RightExpression != null
                    ? BuildScalarExprNode(cmp.RightExpression)
                    : new Dictionary<string, object?> { ["$const"] = cmp.Value };
                return new Dictionary<string, object?>
                {
                    [opKey] = new object?[] { lhs, rhs }
                };
            }

            case LogicalExpression logical:
            {
                if (logical.Operator == LogicalOperator.Not)
                {
                    return new Dictionary<string, object?>
                    {
                        ["$not"] = BuildBoolExprNode(logical.Operands[0])
                    };
                }
                var key = logical.Operator == LogicalOperator.And ? "$and" : "$or";
                var parts = new object?[logical.Operands.Count];
                for (int i = 0; i < logical.Operands.Count; i++)
                    parts[i] = BuildBoolExprNode(logical.Operands[i]);
                return new Dictionary<string, object?> { [key] = parts };
            }

            case NullCheckExpression nc:
            {
                var fieldNode = new Dictionary<string, object?>
                {
                    ["$field"] = BuildFieldPath(nc.Property)
                };
                return new Dictionary<string, object?>
                {
                    [nc.IsNull ? "$null" : "$notnull"] = new object?[] { fieldNode }
                };
            }

            default:
                throw new NotSupportedException(
                    $"FilterExpression {filter.GetType().Name} is not supported inside a ternary $if condition.");
        }
    }

    /// <summary>
    /// Best-effort check whether a <see cref="ValueExpression"/> resolves to a string at runtime.
    /// Used to decide between SQL '+' (numeric $add) and '||' ($concat) for arithmetic Add.
    /// </summary>
    private static bool IsStringValueExpression(ValueExpression expr)
    {
        return expr switch
        {
            PropertyValueExpression pve => pve.Property.Type == typeof(string),
            ConstantValueExpression cve => cve.Type == typeof(string) || cve.Value is string,
            ArithmeticExpression ae when ae.Operator == ArithmeticOperator.Add
                => IsStringValueExpression(ae.Left) || IsStringValueExpression(ae.Right),
            FunctionCallExpression fce => fce.Function is QueryExpressions.PropertyFunction.ToLower
                                                or QueryExpressions.PropertyFunction.ToUpper
                                                or QueryExpressions.PropertyFunction.Trim
                                                or QueryExpressions.PropertyFunction.TrimStart
                                                or QueryExpressions.PropertyFunction.TrimEnd
                                                || IsStringValueExpression(fce.Argument),
            MultiArgFunctionCallExpression mfce => mfce.Function is QueryExpressions.PropertyFunction.Substring
                                                       or QueryExpressions.PropertyFunction.Replace
                                                       or QueryExpressions.PropertyFunction.PadLeft
                                                       or QueryExpressions.PropertyFunction.PadRight
                                                       or QueryExpressions.PropertyFunction.RegexReplace,
            _ => false
        };
    }

    private static string MapPropertyFunctionToExprKey(QueryExpressions.PropertyFunction function) => function switch
    {
        QueryExpressions.PropertyFunction.Length    => "$length",
        QueryExpressions.PropertyFunction.Count     => "$count",
        QueryExpressions.PropertyFunction.ToLower   => "$lower",
        QueryExpressions.PropertyFunction.ToUpper   => "$upper",
        QueryExpressions.PropertyFunction.Trim      => "$trim",
        QueryExpressions.PropertyFunction.TrimStart => "$trimstart",
        QueryExpressions.PropertyFunction.TrimEnd   => "$trimend",
        QueryExpressions.PropertyFunction.Substring => "$substring",
        QueryExpressions.PropertyFunction.Replace   => "$replace",
        QueryExpressions.PropertyFunction.IndexOf   => "$indexof",
        QueryExpressions.PropertyFunction.PadLeft   => "$padleft",
        QueryExpressions.PropertyFunction.PadRight  => "$padright",
        QueryExpressions.PropertyFunction.Abs       => "$abs",
        QueryExpressions.PropertyFunction.Round     => "$round",
        QueryExpressions.PropertyFunction.Floor     => "$floor",
        QueryExpressions.PropertyFunction.Ceiling   => "$ceil",
        QueryExpressions.PropertyFunction.Sqrt      => "$sqrt",
        QueryExpressions.PropertyFunction.Sign      => "$sign",
        QueryExpressions.PropertyFunction.Exp       => "$exp",
        QueryExpressions.PropertyFunction.Log       => "$ln",      // 1-arg Math.Log = natural log
        QueryExpressions.PropertyFunction.LogBase   => "$log",     // 2-arg: [base, value]
        QueryExpressions.PropertyFunction.Pow       => "$power",
        QueryExpressions.PropertyFunction.Year      => "$year",
        QueryExpressions.PropertyFunction.Month     => "$month",
        QueryExpressions.PropertyFunction.Day       => "$day",
        QueryExpressions.PropertyFunction.Hour      => "$hour",
        QueryExpressions.PropertyFunction.Minute    => "$minute",
        QueryExpressions.PropertyFunction.Second    => "$second",
        QueryExpressions.PropertyFunction.DayOfWeek => "$dayofweek",
        QueryExpressions.PropertyFunction.DayOfYear => "$dayofyear",
        QueryExpressions.PropertyFunction.RegexReplace => "$regexreplace",
        _ => throw new NotSupportedException($"PropertyFunction.{function} has no $expr mapping.")
    };

    /// <summary>
    /// Maps a <see cref="QueryExpressions.PropertyFunction"/> AddX function to the
    /// PVT <c>$dateadd</c> unit literal (lowercase). Returns <c>null</c> if the
    /// function is not a DateTime add. Used in the MultiArgFunctionCallExpression
    /// branch to emit <c>{"$dateadd": ["unit", date, n]}</c> facet shape.
    /// </summary>
    private static string? MapAddFunctionToUnit(QueryExpressions.PropertyFunction function) => function switch
    {
        QueryExpressions.PropertyFunction.AddYears   => "year",
        QueryExpressions.PropertyFunction.AddMonths  => "month",
        QueryExpressions.PropertyFunction.AddDays    => "day",
        QueryExpressions.PropertyFunction.AddHours   => "hour",
        QueryExpressions.PropertyFunction.AddMinutes => "minute",
        QueryExpressions.PropertyFunction.AddSeconds => "second",
        _ => null
    };

    private static string MapComparisonOperatorToExprKey(ComparisonOperator op) => op switch
    {
        ComparisonOperator.Equal                => "$eq",
        ComparisonOperator.NotEqual             => "$ne",
        ComparisonOperator.GreaterThan          => "$gt",
        ComparisonOperator.GreaterThanOrEqual   => "$gte",
        ComparisonOperator.LessThan             => "$lt",
        ComparisonOperator.LessThanOrEqual      => "$lte",
        ComparisonOperator.Contains             => "$contains",
        ComparisonOperator.ContainsIgnoreCase   => "$containsignorecase",
        ComparisonOperator.StartsWith           => "$startswith",
        ComparisonOperator.StartsWithIgnoreCase => "$startswithignorecase",
        ComparisonOperator.EndsWith             => "$endswith",
        ComparisonOperator.EndsWithIgnoreCase   => "$endswithignorecase",
        ComparisonOperator.RegexMatch           => "$regex",
        ComparisonOperator.RegexMatchIgnoreCase => "$iregex",
        _ => throw new NotSupportedException($"Operator {op} is not supported in $expr predicate.")
    };

    private object BuildNullCheckFilter(NullCheckExpression nullCheck)
    {
        var fieldName = BuildFieldPath(nullCheck.Property);
        
        // Target logic for base fields and EAV fields:
        // - Base fields (_objects): column always exists, check only value -> null directly
        // - EAV fields (Props): record may not exist -> $exists for "field exists" semantics
        
        if (nullCheck.Property.IsBaseField)
        {
            // Base fields: simple null / $ne null
            if (nullCheck.IsNull)
            {
                return new Dictionary<string, object?> 
                { 
                    [fieldName] = null  // Serialized as field: null
                };
            }
            else
            {
                return new Dictionary<string, object> 
                { 
                    [fieldName] = new Dictionary<string, object?> { ["$ne"] = null } 
                };
            }
        }
        else
        {
            // EAV fields: $exists for record existence semantics
            if (nullCheck.IsNull)
            {
                return new Dictionary<string, object> 
                { 
                    [fieldName] = new Dictionary<string, object> { ["$exists"] = false } 
                };
            }
            else
            {
                return new Dictionary<string, object> 
                { 
                    [fieldName] = new Dictionary<string, object?> { ["$ne"] = null } 
                };
            }
        }
    }

    private object BuildInFilter(InExpression inExpr)
    {
        var fieldName = BuildFieldPath(inExpr.Property);
        
        return new Dictionary<string, object> 
        { 
            [fieldName] = new Dictionary<string, object> { ["$in"] = inExpr.Values.ToArray() } 
        };
    }

    // ===== 🚀 NEW METHODS FOR NEW PARADIGM =====

    /// <summary>
    /// Build field path with Class fields support (Contact.Name, Contacts[].Email)
    /// 🆕 BUG FIX: For RedbObject base fields use "0$:" prefix 
    /// to distinguish them from Props fields with same names (e.g. name vs Name)
    /// Prefix "0$:" is impossible as identifier in any programming language
    /// </summary>
    private string BuildFieldPath(redb.Core.Query.QueryExpressions.PropertyInfo property)
    {
        var fieldPath = property.Name;
        
        // 🚀 RedbObject BASE FIELDS: add "0$:" prefix for explicit identification
        // SQL function _build_single_facet_condition recognizes this prefix
        if (property.IsBaseField)
        {
            // Prefix "0$:" + field name (e.g.: "0$:name", "0$:parent_id", "0$:Id")
            return "0$:" + fieldPath;
        }
        
        // 🎯 DETERMINE FIELD TYPE FOR CLASS FIELDS
        if (IsClassField(property))
        {
            // Class field: Contact.Name, Address.City
            return fieldPath; // Field already contains full path from parser
        }
        
        if (IsClassArrayField(property))
        {
            // Class array: Contacts[].Email, Addresses[].Street
            return fieldPath; // Field already contains full path from parser  
        }
        
        if (IsCollectionType(property.Type))
        {
            // Regular array: Tags[], Scores[], Categories[]
            if (!fieldPath.EndsWith("[]"))
            {
                return fieldPath + "[]";
            }
        }
        
        // Regular field: Name, Age, Status
        return fieldPath;
    }
    
    /// <summary>
    /// Normalize RedbObject base field names to SQL names
    /// C# names (snake_case/PascalCase) → _objects SQL columns with _ prefix
    /// IMPORTANT: Normalize only UNIQUE base fields that are NOT in Props!
    /// Fields like "name", "Name" are NOT normalized - they may be Props fields.
    /// SQL function _normalize_base_field_name() will handle them itself.
    /// </summary>
    private static string? NormalizeBaseFieldName(string fieldName)
    {
        return fieldName switch
        {
            // ID fields - UNIQUE, definitely base
            "id" or "Id" => "_id",
            "parent_id" or "ParentId" or "id_parent" => "_id_parent",
            "scheme_id" or "SchemeId" or "id_scheme" => "_id_scheme",
            "owner_id" or "OwnerId" => "_id_owner",
            "who_change_id" or "WhoChangeId" => "_id_who_change",
            // Value fields - UNIQUE, definitely base
            "value_long" or "ValueLong" => "_value_long",
            "value_string" or "ValueString" => "_value_string",
            "value_guid" or "ValueGuid" => "_value_guid",
            "value_bool" or "ValueBool" => "_value_bool",
            "value_double" or "ValueDouble" => "_value_double",
            "value_numeric" or "ValueNumeric" => "_value_numeric",
            "value_datetime" or "ValueDatetime" => "_value_datetime",
            "value_bytes" or "ValueBytes" => "_value_bytes",
            // DateTime fields - UNIQUE, definitely base
            "date_create" or "DateCreate" => "_date_create",
            "date_modify" or "DateModify" => "_date_modify",
            "date_begin" or "DateBegin" => "_date_begin",
            "date_complete" or "DateComplete" => "_date_complete",
            // Other UNIQUE base fields
            "key" or "Key" => "_key",
            "note" or "Note" => "_note",
            "hash" or "Hash" => "_hash",
            // NOT normalized: name, Name - they often exist in Props!
            // SQL function _normalize_base_field_name() will process them if they're base fields
            _ => null
        };
    }

    /// <summary>
    /// 🎯 CLIENT SEMANTICS: Build Contains filter with case-insensitive search support
    /// Supports: r.Article.Contains(filter, StringComparison.OrdinalIgnoreCase)
    /// </summary>
    private object BuildContainsFilter(string fieldName, object? value, bool ignoreCase)
    {
        if (ignoreCase)
        {
            // 🚀 CASE-INSENSITIVE SEARCH
            return new Dictionary<string, object> 
            { 
                [fieldName] = new Dictionary<string, object?> { ["$containsIgnoreCase"] = value } 
            };
        }
        else
        {
            // 📝 REGULAR CASE-SENSITIVE SEARCH
            return new Dictionary<string, object> 
            { 
                [fieldName] = new Dictionary<string, object?> { ["$contains"] = value } 
            };
        }
    }

    /// <summary>
    /// Build filters for arrays with nullable support
    /// </summary>
    private object BuildArrayFilter(string fieldName, string operatorName, object? value, bool isNullable = false)
    {
        // 🔧 FIX DOUBLE BRACKETS - DON'T add "[]" if path already contains "[]"
        // Example: "Roles[].Value" already contains [] and doesn't need addition
        var arrayFieldName = fieldName.Contains("[]") ? fieldName : fieldName + "[]";
        
        if (isNullable && value == null)
        {
            // Nullable array - search for array absence
            return new Dictionary<string, object> 
            { 
                [arrayFieldName] = new Dictionary<string, object> { ["$exists"] = false }
            };
        }
        
        return new Dictionary<string, object> 
        { 
            [arrayFieldName] = new Dictionary<string, object?> { [operatorName] = value } 
        };
    }

    /// <summary>
    /// 🎯 CLIENT SEMANTICS: Build filters for nullable fields
    /// Supports: r.Auction != null &amp;&amp; r.Auction.Costs &gt; 100
    /// </summary>
    private object BuildNullableFieldFilter(string fieldName, object? value, ComparisonOperator op)
    {
        if (value == null)
        {
            // Nullable field with null value
            switch (op)
            {
                case ComparisonOperator.Equal:
                    // field == null → field is absent
                    return new Dictionary<string, object> 
                    { 
                        [fieldName] = new Dictionary<string, object> { ["$exists"] = false } 
                    };
                    
                case ComparisonOperator.NotEqual:
                    // field != null → field exists with any value  
                    return new Dictionary<string, object> 
                    { 
                        [fieldName] = new Dictionary<string, object> { ["$exists"] = true } 
                    };
                    
                default:
                    throw new NotSupportedException($"Operator {op} is not supported for nullable field with null value");
            }
        }
        
        // Nullable field with real value - regular logic
        var operatorName = op switch
        {
            ComparisonOperator.Equal => "=",
            ComparisonOperator.NotEqual => "$ne", 
            ComparisonOperator.GreaterThan => "$gt",
            ComparisonOperator.GreaterThanOrEqual => "$gte",
            ComparisonOperator.LessThan => "$lt", 
            ComparisonOperator.LessThanOrEqual => "$lte",
            ComparisonOperator.Contains => "$contains",
            ComparisonOperator.StartsWith => "$startsWith",
            ComparisonOperator.EndsWith => "$endsWith",
            _ => throw new NotSupportedException($"Operator {op} is not supported for nullable field")
        };
        
        if (operatorName == "=")
        {
            return new Dictionary<string, object?> { [fieldName] = value };
        }
        
        return new Dictionary<string, object> 
        { 
            [fieldName] = new Dictionary<string, object?> { [operatorName] = value } 
        };
    }

    /// <summary>
    /// Check for nullable field type
    /// </summary>
    private bool IsNullableType(Type type)
    {
        return type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>);
    }

    /// <summary>
    /// Returns true when the type is <see cref="IRedbListItem"/> (or a
    /// nullable / collection wrapper around it).
    /// </summary>
    private static bool IsRedbListItemType(Type type)
    {
        var t = Nullable.GetUnderlyingType(type) ?? type;
        if (typeof(IRedbListItem).IsAssignableFrom(t)) return true;
        if (t.IsGenericType)
        {
            var arg = t.GetGenericArguments().FirstOrDefault();
            if (arg != null) return IsRedbListItemType(arg);
        }
        if (t.IsArray)
        {
            var elem = t.GetElementType();
            if (elem != null) return IsRedbListItemType(elem);
        }
        return false;
    }

    /// <summary>
    /// Check for array/collection
    /// </summary>
    private bool IsCollectionType(Type type)
    {
        if (type == typeof(string)) return false; // string is not collection for our purposes
        
        return type.IsArray || 
               (type.IsGenericType && (
                   type.GetGenericTypeDefinition() == typeof(List<>) ||
                   type.GetGenericTypeDefinition() == typeof(IList<>) ||
                   type.GetGenericTypeDefinition() == typeof(ICollection<>) ||
                   type.GetGenericTypeDefinition() == typeof(IEnumerable<>)
               ));
    }

    /// <summary>
    /// Check for Class field (Contact.Name, Address.City)
    /// </summary>
    private bool IsClassField(redb.Core.Query.QueryExpressions.PropertyInfo property)
    {
        // Class field is determined by presence of dot in name and absence of [] 
        return property.Name.Contains('.') && !property.Name.Contains("[]");
    }

    /// <summary>
    /// Check for Class array (Contacts[].Email, Addresses[].Street)
    /// </summary>
    private bool IsClassArrayField(redb.Core.Query.QueryExpressions.PropertyInfo property)
    {
        // Class array is determined by presence of both dot and [] in name
        return property.Name.Contains('.') && property.Name.Contains("[]");
    }

    /// <summary>
    /// Check for business class (not primitive and not collection)
    /// </summary>
    private bool IsBusinessClass(Type type)
    {
        // Business class: not primitive, not string, not collection, not nullable primitive
        if (type.IsPrimitive || type == typeof(string) || type == typeof(decimal) ||
            type == typeof(DateTime) || type == typeof(DateTimeOffset) || type == typeof(DateOnly) || type == typeof(TimeOnly) || type == typeof(TimeSpan) || type == typeof(Guid))
            return false;
            
        if (IsNullableType(type))
        {
            var underlyingType = Nullable.GetUnderlyingType(type)!;
            return IsBusinessClass(underlyingType);
        }
        
        if (IsCollectionType(type))
            return false;
            
        // This is business class (Address, Contact, etc.)
        return type.IsClass;
    }
    
    /// <summary>
    /// ✅ FIX FOR PROBLEM #4: Convert value to correct field type
    /// Solves problem when Price (double) is searched as integer value
    /// </summary>
    private object? ConvertValueToFieldType(object? value, Type fieldType)
    {
        if (value == null) return null;
        
        // Remove Nullable wrapper if present
        var targetType = Nullable.GetUnderlyingType(fieldType) ?? fieldType;
        
        // 🔧 CRITICAL FIX: Process DateTime/DateTimeOffset BEFORE type checking!
        // Problem: DateTime without explicit Kind serializes WITHOUT timezone ('2025-11-16T00:00:00')
        // PostgreSQL interprets this as SERVER LOCAL time (not client!)
        // Solution: ALWAYS convert to UTC for explicit zone indication ('2025-11-16T00:00:00Z')
        if (value is DateTime dt && targetType == typeof(DateTime))
        {
            // ✅ Use centralized converter: DateTime → UTC
            // Unspecified is treated as UTC (NOT as Local!)
            return Core.Utils.DateTimeConverter.NormalizeForStorage(dt);
        }
        
        // If types already match (NOT DateTime!) - return as is
        if (value.GetType() == targetType)
            return value;
            
        try
        {
            // ✅ NUMERIC TYPES - main cause of the problem!
            if (targetType == typeof(double))
            {
                return Convert.ToDouble(value);  // 2000 → 2000.0
            }
            else if (targetType == typeof(float))
            {
                return Convert.ToSingle(value);
            }
            else if (targetType == typeof(decimal))
            {
                return Convert.ToDecimal(value);
            }
            else if (targetType == typeof(long))
            {
                return Convert.ToInt64(value);
            }
            else if (targetType == typeof(int))
            {
                return Convert.ToInt32(value);
            }
            else if (targetType == typeof(short))
            {
                return Convert.ToInt16(value);
            }
            else if (targetType == typeof(byte))
            {
                return Convert.ToByte(value);
            }
            
            // ✅ BOOLEAN TYPES
            else if (targetType == typeof(bool))
            {
                return Convert.ToBoolean(value);
            }

            // ✅ DATE-TIME (DateTime processed EARLIER at method start!)
            else if (targetType == typeof(DateTimeOffset))
            {
                // 🔧 CORRECT DateTimeOffset HANDLING:
                // DateTimeOffset already contains offset (timezone), therefore:
                // 1. If value is already DateTimeOffset - use it
                // 2. If DateTime - create DateTimeOffset explicitly
                // 3. If string - parse
                // 4. Convert to UTC for uniformity (PostgreSQL timestamptz stores in UTC)
                
                DateTimeOffset dtofs;
                if (value is DateTimeOffset existingOffset)
                {
                    // Already DateTimeOffset - use directly
                    dtofs = existingOffset;
                    _logger?.LogInformation($"   🔍 DateTimeOffset from existing: {dtofs:yyyy-MM-dd HH:mm:ss zzz}");
                }
                else if (value is DateTime dtValue)
                {
                    _logger?.LogInformation($"   🔍 DateTimeOffset from DateTime: {dtValue:yyyy-MM-dd HH:mm:ss} (Kind: {dtValue.Kind})");
                    // DateTime → DateTimeOffset
                    // Consider Kind for correct conversion
                    if (dtValue.Kind == DateTimeKind.Utc)
                    {
                        dtofs = new DateTimeOffset(dtValue, TimeSpan.Zero);
                    }
                    else if (dtValue.Kind == DateTimeKind.Local)
                    {
                        dtofs = new DateTimeOffset(dtValue);
                    }
                    else // Unspecified
                    {
                        // For Unspecified consider UTC
                        dtofs = new DateTimeOffset(dtValue, TimeSpan.Zero);
                    }
                }
                else if (value is string strValue)
                {
                    // Parse string
                    dtofs = DateTimeOffset.Parse(strValue);
                    _logger?.LogInformation($"   🔍 DateTimeOffset from string: {dtofs:yyyy-MM-dd HH:mm:ss zzz}");
                }
                else
                {
                    // Fallback - try to convert
                    dtofs = new DateTimeOffset(Convert.ToDateTime(value));
                    _logger?.LogInformation($"   🔍 DateTimeOffset from fallback: {dtofs:yyyy-MM-dd HH:mm:ss zzz}");
                }
                
                // Convert to UTC for uniformity with PostgreSQL timestamptz
                // PostgreSQL stores timestamptz in UTC and compares in UTC
                var result = dtofs.ToUniversalTime();
                _logger?.LogInformation($"   🔄 DateTimeOffset → UTC: {result:yyyy-MM-dd HH:mm:ss zzz}");
                return result;
            }

            // ✅ GUID
            else if (targetType == typeof(Guid))
            {
                if (value is string guidStr)
                    return Guid.Parse(guidStr);
                return (Guid)value;
            }
            
            // ✅ STRINGS
            else if (targetType == typeof(string))
            {
                return value.ToString();
            }
            
            // For other types return as is
            return value;
        }
        catch (Exception ex)
        {
            _logger?.LogWarning($"⚠️ Failed to convert value {value} ({value.GetType().Name}) to type {targetType.Name}: {ex.Message}");
            return value; // Fallback - return original value
        }
    }
}
