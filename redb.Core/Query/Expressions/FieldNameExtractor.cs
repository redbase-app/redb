using System.Linq.Expressions;
using redb.Core.Utils;

namespace redb.Core.Query.Expressions;

/// <summary>
/// Extracts field names from LINQ Expression tree.
/// Used for determining which fields to include in PVT.
/// Supports: simple fields, nested fields, Dictionary indexers, ContainsKey.
/// </summary>
public class FieldNameExtractor : ExpressionVisitor
{
    /// <summary>
    /// Collected field names.
    /// </summary>
    public HashSet<string> FieldNames { get; } = new();
    
    /// <summary>
    /// Extracts field names from expression.
    /// </summary>
    /// <typeparam name="T">Props type</typeparam>
    /// <param name="expression">Expression to extract from</param>
    /// <returns>Set of field names</returns>
    public static HashSet<string> Extract<T>(Expression<Func<T, bool>> expression)
    {
        var extractor = new FieldNameExtractor();
        extractor.Visit(expression.Body);
        return extractor.FieldNames;
    }
    
    /// <summary>
    /// Visits member access (e.g., p.Name, p.Address.City).
    /// </summary>
    protected override Expression VisitMember(MemberExpression node)
    {
        // Check if this is a Props property access
        if (node.Expression?.NodeType == ExpressionType.Parameter)
        {
            FieldNames.Add(node.Member.Name);
        }
        // Nested property access (p.Address.City)
        else if (node.Expression is MemberExpression parent && 
                 IsPropsAccess(parent))
        {
            var path = BuildPropertyPath(node);
            if (!string.IsNullOrEmpty(path))
                FieldNames.Add(path);
        }
        
        return base.VisitMember(node);
    }
    
    /// <summary>
    /// Visits method calls (Dictionary indexer, ContainsKey, string methods).
    /// </summary>
    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        // Handle Dictionary indexer: dict["key"]
        if (node.Method.Name == "get_Item" && node.Object is MemberExpression member)
        {
            var keyArg = node.Arguments.FirstOrDefault();
            var keyValue = ExtractConstantValue(keyArg);
            
            if (keyValue != null)
            {
                // Serialize complex keys (Tuples) using RedbKeySerializer
                var keyType = keyArg?.Type ?? typeof(string);
                var keyString = RedbKeySerializer.SerializeObject(keyValue, keyType);
                
                // Check for nested property after indexer: AddressBook["home"].City
                var parentExpr = node;
                // This will be handled in BuildPropertyPath
                
                FieldNames.Add($"{member.Member.Name}[{keyString}]");
            }
        }
        // Handle ContainsKey
        else if (node.Method.Name == "ContainsKey" && node.Object is MemberExpression dictMember)
        {
            var keyArg = node.Arguments.FirstOrDefault();
            var keyValue = ExtractConstantValue(keyArg);
            
            if (keyValue != null)
            {
                var keyType = keyArg?.Type ?? typeof(string);
                var keyString = RedbKeySerializer.SerializeObject(keyValue, keyType);
                FieldNames.Add($"{dictMember.Member.Name}[{keyString}]");
            }
        }
        // Handle string methods (Contains, StartsWith, EndsWith)
        else if (node.Method.DeclaringType == typeof(string) && node.Object is MemberExpression strMember)
        {
            if (IsPropsAccess(strMember))
            {
                FieldNames.Add(strMember.Member.Name);
            }
        }
        
        return base.VisitMethodCall(node);
    }
    
    /// <summary>
    /// Checks if member expression is a Props property access.
    /// </summary>
    private static bool IsPropsAccess(MemberExpression member)
    {
        return member.Expression?.NodeType == ExpressionType.Parameter ||
               (member.Expression is MemberExpression parent && IsPropsAccess(parent));
    }
    
    /// <summary>
    /// Builds property path from nested member expression.
    /// </summary>
    private static string BuildPropertyPath(MemberExpression member)
    {
        var parts = new List<string>();
        Expression? current = member;
        
        while (current is MemberExpression m)
        {
            parts.Insert(0, m.Member.Name);
            current = m.Expression;
        }
        
        return string.Join(".", parts);
    }
    
    /// <summary>
    /// Extracts constant value from expression (handles closures).
    /// </summary>
    private static object? ExtractConstantValue(Expression? expression)
    {
        if (expression == null) return null;
        
        switch (expression)
        {
            case ConstantExpression constant:
                return constant.Value;
                
            case MemberExpression member when member.Expression is ConstantExpression closure:
                // Handle closure: value(Closure).field
                var field = member.Member;
                return field switch
                {
                    System.Reflection.FieldInfo f => f.GetValue(closure.Value),
                    System.Reflection.PropertyInfo p => p.GetValue(closure.Value),
                    _ => null
                };
                
            default:
                // Try to compile and evaluate
                try
                {
                    var lambda = Expression.Lambda(expression);
                    var compiled = lambda.Compile();
                    return compiled.DynamicInvoke();
                }
                catch
                {
                    return null;
                }
        }
    }
}

