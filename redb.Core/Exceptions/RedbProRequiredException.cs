using System.Linq.Expressions;

namespace redb.Core.Exceptions;

/// <summary>
/// Exception thrown when a feature requires REDB Pro version.
/// 
/// Pro-only features include:
/// - Computed expressions in filters: x.A + x.B, Math.Abs(x.Value)
/// - String functions: ToLower(), Trim(), etc.
/// - SQL functions: Sql.Function()
/// - ChangeTracking save strategy
/// - DistinctRedb/DistinctBy queries
/// - Window functions combined with GroupBy
/// - Deep filter nesting (depth > 3)
/// 
/// Solution: Install-Package redb.MSSql.Pro or redb.Postgres.Pro
/// </summary>
public class RedbProRequiredException : NotSupportedException
{
    /// <summary>
    /// Feature name that requires Pro version.
    /// </summary>
    public string Feature { get; }
    
    /// <summary>
    /// Category of Pro feature.
    /// </summary>
    public ProFeatureCategory Category { get; }
    
    /// <summary>
    /// Creates exception for Pro-required feature.
    /// </summary>
    /// <param name="feature">Feature name (e.g., "DistinctRedb", "Math.Abs")</param>
    /// <param name="category">Feature category for appropriate error message</param>
    public RedbProRequiredException(string feature, ProFeatureCategory category = ProFeatureCategory.General) 
        : base(BuildMessage(feature, category))
    {
        Feature = feature;
        Category = category;
    }
    
    private static string BuildMessage(string feature, ProFeatureCategory category)
    {
        var solution = "Install-Package redb.MSSql.Pro (or redb.Postgres.Pro)";
        
        return category switch
        {
            ProFeatureCategory.ComputedExpression => 
                $"Computed expressions ({feature}) are not supported in open-source REDB. " +
                $"Arithmetic (x.A * x.B), functions (Math.Abs, x.Date.Year), string methods (ToLower, Trim) require Pro. {solution}",
            
            ProFeatureCategory.ChangeTracking =>
                $"ChangeTracking save strategy requires Pro edition. " +
                $"Use EavSaveStrategy.DeleteInsert or upgrade. {solution}",
            
            ProFeatureCategory.DistinctQuery =>
                $"{feature} requires REDB Pro version. {solution}",
            
            ProFeatureCategory.WindowFunction =>
                $"Window functions combined with GroupBy require Pro version. {solution}",
            
            ProFeatureCategory.FilterNesting =>
                $"Filter nesting depth exceeds open-source limit. {feature}. {solution}",
            
            ProFeatureCategory.SqlFunction =>
                $"Sql.Function({feature}) requires Pro version for custom SQL functions. {solution}",
            
            _ => $"Feature '{feature}' requires REDB Pro. {solution}"
        };
    }
    
    /// <summary>
    /// Throws if Expression contains Pro-only features.
    /// </summary>
    public static void ThrowIfProRequired(Expression expression, string context)
    {
        if (ComputedExpressionAnalyzer.RequiresPro(expression))
        {
            throw new RedbProRequiredException(context, ProFeatureCategory.ComputedExpression);
        }
    }
}

/// <summary>
/// Categories of Pro-only features for appropriate error messages.
/// </summary>
public enum ProFeatureCategory
{
    /// <summary>General Pro feature.</summary>
    General,
    
    /// <summary>Computed expressions: x.A + x.B, Math.Abs, ToLower, etc.</summary>
    ComputedExpression,
    
    /// <summary>ChangeTracking save strategy.</summary>
    ChangeTracking,
    
    /// <summary>DistinctRedb/DistinctBy queries.</summary>
    DistinctQuery,
    
    /// <summary>Window functions with GroupBy.</summary>
    WindowFunction,
    
    /// <summary>Deep filter nesting.</summary>
    FilterNesting,
    
    /// <summary>Custom SQL functions via Sql.Function().</summary>
    SqlFunction
}

/// <summary>
/// Expression analyzer for detecting Pro-only features.
/// </summary>
public static class ComputedExpressionAnalyzer
{
    /// <summary>
    /// Checks if Expression requires Pro version.
    /// </summary>
    public static bool RequiresPro(Expression expression)
    {
        var visitor = new ProFeatureDetector();
        visitor.Visit(expression);
        return visitor.RequiresPro;
    }
    
    private class ProFeatureDetector : ExpressionVisitor
    {
        public bool RequiresPro { get; private set; }
        
        protected override Expression VisitBinary(BinaryExpression node)
        {
            // Arithmetic operations between properties = Pro
            if (IsArithmeticBetweenProperties(node))
            {
                RequiresPro = true;
            }
            return base.VisitBinary(node);
        }
        
        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            // Math.* functions = Pro
            if (node.Method.DeclaringType == typeof(Math))
            {
                RequiresPro = true;
            }
            
            // Sql.Function = Pro
            if (node.Method.DeclaringType?.FullName == "redb.Core.Query.Sql" &&
                node.Method.Name == "Function")
            {
                RequiresPro = true;
            }
            
            return base.VisitMethodCall(node);
        }
        
        private bool IsArithmeticBetweenProperties(BinaryExpression node)
        {
            // Check arithmetic: +, -, *, /, %
            var isArithmetic = node.NodeType is 
                ExpressionType.Add or 
                ExpressionType.Subtract or 
                ExpressionType.Multiply or 
                ExpressionType.Divide or 
                ExpressionType.Modulo;
            
            if (!isArithmetic) return false;
            
            // Check that both operands are properties (not constants)
            var leftIsMember = ContainsMemberAccess(node.Left);
            var rightIsMember = ContainsMemberAccess(node.Right);
            
            // x.A + x.B = Pro (both properties)
            // x.A + 10 = possible in Open Source via SQL
            return leftIsMember && rightIsMember;
        }
        
        private bool ContainsMemberAccess(Expression expression)
        {
            return expression switch
            {
                MemberExpression member when member.Expression is ParameterExpression => true,
                UnaryExpression unary => ContainsMemberAccess(unary.Operand),
                BinaryExpression binary => ContainsMemberAccess(binary.Left) || ContainsMemberAccess(binary.Right),
                _ => false
            };
        }
    }
}

