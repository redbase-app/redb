using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Core.Query.QueryExpressions;

namespace redb.Core.Query;

/// <summary>
/// Parses ordering expressions from LINQ to REDB query format.
/// Supports simple properties, ternary operators, arithmetic expressions and functions.
/// </summary>
public class OrderingExpressionParser : IOrderingExpressionParser
{
    /// <summary>
    /// Parse ordering expression for Props fields.
    /// Supports: p.Name, p.Price * 2, p.Name.ToLower(), etc.
    /// </summary>
    public OrderingExpression ParseOrdering<TProps, TKey>(Expression<Func<TProps, TKey>> keySelector, SortDirection direction) where TProps : class
    {
        return ParseOrderingInternal(keySelector.Body, direction, isBaseField: false);
    }
    
    /// <summary>
    /// Parse sorting by base IRedbObject fields (id, name, date_create, etc.)
    /// Uses IRedbObject for compile-time safety - Props not visible!
    /// </summary>
    public OrderingExpression ParseRedbOrdering<TKey>(Expression<Func<IRedbObject, TKey>> keySelector, SortDirection direction)
    {
        return ParseOrderingInternal(keySelector.Body, direction, isBaseField: true);
    }

    /// <summary>
    /// Parse multiple orderings.
    /// </summary>
    public IReadOnlyList<OrderingExpression> ParseMultipleOrderings<TProps>(IEnumerable<(LambdaExpression KeySelector, SortDirection Direction)> orderings) where TProps : class
    {
        return orderings
            .Select(o => ParseOrderingInternal(o.KeySelector.Body, o.Direction, isBaseField: false))
            .ToList();
    }

    #region Internal Parsing
    
    /// <summary>
    /// Main parsing entry point - tries complex expressions first, falls back to simple property.
    /// </summary>
    private OrderingExpression ParseOrderingInternal(Expression expression, SortDirection direction, bool isBaseField)
    {
        // Try to parse as complex expression (arithmetic, functions)
        var valueExpr = TryExtractValueExpression(expression, isBaseField);
        if (valueExpr != null)
        {
            // For complex expressions, extract primary property for backward compatibility
            var primaryProperty = ExtractPrimaryProperty(valueExpr) 
                ?? new QueryExpressions.PropertyInfo("__computed", expression.Type, isBaseField);
            return new OrderingExpression(primaryProperty, direction, valueExpr);
        }
        
        // Simple property case
        var property = ExtractProperty(expression, isBaseField);
        return new OrderingExpression(property, direction);
    }
    
    /// <summary>
    /// Try to extract ValueExpression for complex expressions (arithmetic, functions).
    /// Returns null for simple property access.
    /// </summary>
    private ValueExpression? TryExtractValueExpression(Expression expression, bool isBaseField)
    {
        return expression switch
        {
            // Arithmetic: p.Price + p.Discount, p.Quantity * p.Price
            BinaryExpression binary when IsArithmeticExpression(binary) =>
                CreateArithmeticExpression(binary, isBaseField),
            
            // Method calls: p.Name.ToLower(), p.Name.Trim()
            MethodCallExpression method when IsStringMethod(method) =>
                CreateFunctionExpression(method, isBaseField),
            
            // Unary: -p.Value (negation)
            UnaryExpression unary when unary.NodeType == ExpressionType.Negate =>
                CreateNegationExpression(unary, isBaseField),
            
            _ => null
        };
    }
    
    /// <summary>
    /// Check if expression is arithmetic (Add, Subtract, Multiply, Divide, Modulo).
    /// </summary>
    private static bool IsArithmeticExpression(BinaryExpression binary) => binary.NodeType switch
    {
        ExpressionType.Add => true,
        ExpressionType.Subtract => true,
        ExpressionType.Multiply => true,
        ExpressionType.Divide => true,
        ExpressionType.Modulo => true,
        _ => false
    };
    
    /// <summary>
    /// Check if method is supported string/function call.
    /// </summary>
    private static bool IsStringMethod(MethodCallExpression method)
    {
        var name = method.Method.Name;
        return name is "ToLower" or "ToUpper" or "Trim" or "TrimStart" or "TrimEnd";
    }
    
    /// <summary>
    /// Create ArithmeticExpression from binary expression.
    /// </summary>
    private ArithmeticExpression CreateArithmeticExpression(BinaryExpression binary, bool isBaseField)
    {
        var left = ExtractValueExpression(binary.Left, isBaseField);
        var right = ExtractValueExpression(binary.Right, isBaseField);
        var op = MapArithmeticOperator(binary.NodeType);
        
        return new ArithmeticExpression(left, op, right);
    }
    
    /// <summary>
    /// Create FunctionCallExpression from method call.
    /// </summary>
    private FunctionCallExpression CreateFunctionExpression(MethodCallExpression method, bool isBaseField)
    {
        var function = MapStringFunction(method.Method.Name);
        
        // Instance method (p.Name.ToLower()) - object is the argument
        if (method.Object != null)
        {
            var argument = ExtractValueExpression(method.Object, isBaseField);
            return new FunctionCallExpression(function, argument);
        }
        
        // Static method - first argument
        if (method.Arguments.Count > 0)
        {
            var argument = ExtractValueExpression(method.Arguments[0], isBaseField);
            return new FunctionCallExpression(function, argument);
        }
        
        throw new NotSupportedException($"Method {method.Method.Name} requires at least one argument");
    }
    
    /// <summary>
    /// Create negation expression (-p.Value).
    /// </summary>
    private ArithmeticExpression CreateNegationExpression(UnaryExpression unary, bool isBaseField)
    {
        var operand = ExtractValueExpression(unary.Operand, isBaseField);
        var zero = new ConstantValueExpression(0, unary.Type);
        return new ArithmeticExpression(zero, ArithmeticOperator.Subtract, operand);
    }
    
    /// <summary>
    /// Extract ValueExpression from any expression (recursive).
    /// </summary>
    private ValueExpression ExtractValueExpression(Expression expression, bool isBaseField)
    {
        return expression switch
        {
            // Nested arithmetic
            BinaryExpression binary when IsArithmeticExpression(binary) =>
                CreateArithmeticExpression(binary, isBaseField),
            
            // Method call
            MethodCallExpression method when IsStringMethod(method) =>
                CreateFunctionExpression(method, isBaseField),
            
            // Property access
            MemberExpression member when member.Member is System.Reflection.PropertyInfo =>
                new PropertyValueExpression(ExtractPropertyFromMember(member, isBaseField)),
            
            // Constant
            ConstantExpression constant =>
                new ConstantValueExpression(constant.Value, constant.Type),
            
            // Captured variable (closure)
            MemberExpression member when IsCapturedVariable(member) =>
                new ConstantValueExpression(EvaluateExpression(member), member.Type),
            
            // Unary conversion (e.g., int to double)
            UnaryExpression { NodeType: ExpressionType.Convert } unary =>
                ExtractValueExpression(unary.Operand, isBaseField),
            
            _ => throw new NotSupportedException($"Unsupported expression type in OrderBy: {expression.GetType().Name} ({expression.NodeType})")
        };
    }
    
    /// <summary>
    /// Extract primary property from ValueExpression (for backward compatibility).
    /// </summary>
    private static QueryExpressions.PropertyInfo? ExtractPrimaryProperty(ValueExpression expr) => expr switch
    {
        PropertyValueExpression pve => pve.Property,
        ArithmeticExpression ae => ExtractPrimaryProperty(ae.Left) ?? ExtractPrimaryProperty(ae.Right),
        FunctionCallExpression fce => ExtractPrimaryProperty(fce.Argument),
        _ => null
    };
    
    /// <summary>
    /// Map C# operator to ArithmeticOperator.
    /// </summary>
    private static ArithmeticOperator MapArithmeticOperator(ExpressionType nodeType) => nodeType switch
    {
        ExpressionType.Add => ArithmeticOperator.Add,
        ExpressionType.Subtract => ArithmeticOperator.Subtract,
        ExpressionType.Multiply => ArithmeticOperator.Multiply,
        ExpressionType.Divide => ArithmeticOperator.Divide,
        ExpressionType.Modulo => ArithmeticOperator.Modulo,
        _ => throw new NotSupportedException($"Unsupported arithmetic operator: {nodeType}")
    };
    
    /// <summary>
    /// Map string method to PropertyFunction.
    /// </summary>
    private static PropertyFunction MapStringFunction(string methodName) => methodName switch
    {
        "ToLower" => PropertyFunction.ToLower,
        "ToUpper" => PropertyFunction.ToUpper,
        "Trim" => PropertyFunction.Trim,
        "TrimStart" => PropertyFunction.TrimStart,
        "TrimEnd" => PropertyFunction.TrimEnd,
        _ => throw new NotSupportedException($"Unsupported string method in OrderBy: {methodName}")
    };
    
    /// <summary>
    /// Check if member expression is captured variable (closure).
    /// </summary>
    private static bool IsCapturedVariable(MemberExpression member)
    {
        return member.Expression is ConstantExpression 
            || (member.Expression is MemberExpression inner && IsCapturedVariable(inner));
    }
    
    /// <summary>
    /// Evaluate expression to get constant value.
    /// </summary>
    private static object? EvaluateExpression(Expression expression)
    {
        var lambda = Expression.Lambda(expression);
        var compiled = lambda.Compile();
        return compiled.DynamicInvoke();
    }
    
    #endregion

    #region Simple Property Extraction (backward compatibility)
    
    /// <summary>
    /// Extract property with ternary operators support for nullable fields.
    /// </summary>
    private QueryExpressions.PropertyInfo ExtractProperty(Expression expression, bool isBaseField)
    {
        return expression switch
        {
            // Ternary operator (r.Auction != null ? r.Auction.Baskets : 0)
            ConditionalExpression conditional => ExtractFromConditional(conditional, isBaseField),
            
            // Binary expression for comparison (r.Auction != null)
            BinaryExpression binary when !IsArithmeticExpression(binary) => 
                ExtractFromBinaryExpression(binary, isBaseField),
            
            // Regular property (r.Name) or nested (r.Auction.Baskets)
            MemberExpression member when member.Member is System.Reflection.PropertyInfo propInfo => 
                ExtractPropertyFromMember(member, isBaseField),
                
            _ => throw new ArgumentException($"Expression must be a property access, conditional, or binary comparison, got {expression.GetType().Name}")
        };
    }

    /// <summary>
    /// Extract property from binary expression (r.Auction != null, r.Field == value).
    /// </summary>
    private QueryExpressions.PropertyInfo ExtractFromBinaryExpression(BinaryExpression binary, bool isBaseField)
    {
        if (binary.Left is MemberExpression leftMember && leftMember.Member is System.Reflection.PropertyInfo)
        {
            return ExtractPropertyFromMember(leftMember, isBaseField);
        }
        
        if (binary.Right is MemberExpression rightMember && rightMember.Member is System.Reflection.PropertyInfo)
        {
            return ExtractPropertyFromMember(rightMember, isBaseField);
        }
        
        throw new ArgumentException($"Binary expression must have at least one property access side. Got: {binary.Left.GetType().Name} {binary.NodeType} {binary.Right.GetType().Name}");
    }

    /// <summary>
    /// Extract property from ternary operator (r.Auction != null ? r.Auction.Baskets : 0).
    /// </summary>
    private QueryExpressions.PropertyInfo ExtractFromConditional(ConditionalExpression conditional, bool isBaseField)
    {
        if (conditional.IfTrue is MemberExpression trueMember && 
            trueMember.Member is System.Reflection.PropertyInfo)
        {
            return ExtractPropertyFromMember(trueMember, isBaseField);
        }
        
        if (conditional.IfFalse is MemberExpression falseMember && 
            falseMember.Member is System.Reflection.PropertyInfo)
        {
            return ExtractPropertyFromMember(falseMember, isBaseField);
        }
        
        throw new ArgumentException("Conditional expression must have at least one property access branch");
    }

    /// <summary>
    /// Extract property from member expression with nested fields support.
    /// </summary>
    private QueryExpressions.PropertyInfo ExtractPropertyFromMember(MemberExpression member, bool isBaseField)
    {
        var fullPath = BuildPropertyPath(member);
        var propInfo = member.Member as System.Reflection.PropertyInfo;
        return new QueryExpressions.PropertyInfo(fullPath, propInfo?.PropertyType ?? typeof(object), isBaseField);
    }

    /// <summary>
    /// Build full property path for nested fields (Auction.Baskets).
    /// </summary>
    private static string BuildPropertyPath(MemberExpression memberExpression)
    {
        var pathParts = new List<string>();
        var current = memberExpression;

        while (current != null && current.Member is System.Reflection.PropertyInfo)
        {
            pathParts.Add(current.Member.Name);
            
            if (current.Expression is MemberExpression parentMember)
            {
                current = parentMember;
            }
            else
            {
                break;
            }
        }

        pathParts.Reverse();
        return string.Join(".", pathParts);
    }
    
    #endregion
}
