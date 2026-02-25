using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text.Json;
using System.Threading.Tasks;
using redb.Core.Models.Contracts;
using redb.Core.Query.Aggregation;
using redb.Core.Query.Base;
using redb.Core.Query.Window;

namespace redb.Core.Query.Grouping;

/// <summary>
/// Tree-aware GroupBy + Window queryable.
/// Preserves TreeQueryContext for hierarchical queries.
/// </summary>
public class TreeGroupedWindowedQueryable<TKey, TProps> : IGroupedWindowedQueryable<TKey, TProps>
    where TProps : class, new()
{
    private readonly ITreeQueryProvider _treeProvider;
    private readonly TreeQueryContext<TProps> _treeContext;
    private readonly Expression _keySelector;
    private readonly GroupedWindowSpec<TKey, TProps> _windowSpec;

    public TreeGroupedWindowedQueryable(
        ITreeQueryProvider treeProvider,
        TreeQueryContext<TProps> treeContext,
        Expression keySelector,
        GroupedWindowSpec<TKey, TProps> windowSpec)
    {
        _treeProvider = treeProvider;
        _treeContext = treeContext;
        _keySelector = keySelector;
        _windowSpec = windowSpec;
    }

    public async Task<List<TResult>> SelectAsync<TResult>(
        Expression<Func<IRedbGrouping<TKey, TProps>, TResult>> selector)
    {
        var groupFields = ParseGroupFields();
        var aggregations = ParseAggregations(selector);
        var windowFuncs = ParseWindowFunctions(selector);
        var partitionBy = ParsePartitionBy();
        var orderBy = ParseOrderBy();

        var jsonResult = await _treeProvider.ExecuteTreeGroupedWindowQueryAsync(
            _treeContext, groupFields, aggregations, windowFuncs, partitionBy, orderBy);

        return MaterializeResults<TResult>(jsonResult, selector);
    }

    public async Task<string> ToSqlStringAsync<TResult>(
        Expression<Func<IRedbGrouping<TKey, TProps>, TResult>> selector)
    {
        var groupFields = ParseGroupFields();
        var aggregations = ParseAggregations(selector);
        var windowFuncs = ParseWindowFunctions(selector);
        var partitionBy = ParsePartitionBy();
        var orderBy = ParseOrderBy();

        return await _treeProvider.GetTreeGroupedWindowSqlPreviewAsync(
            _treeContext, groupFields, aggregations, windowFuncs, partitionBy, orderBy);
    }

    private List<GroupFieldRequest> ParseGroupFields()
    {
        var result = new List<GroupFieldRequest>();
        
        var body = _keySelector is LambdaExpression lambda ? lambda.Body : _keySelector;
        
        if (body is NewExpression newExpr && newExpr.Members != null)
        {
            for (int i = 0; i < newExpr.Members.Count; i++)
            {
                var member = newExpr.Members[i];
                var arg = newExpr.Arguments[i];
                var fieldPath = ExtractFieldPath(arg);
                result.Add(new GroupFieldRequest { FieldPath = fieldPath, Alias = member.Name });
            }
        }
        else if (body is MemberExpression memberExpr)
        {
            var fieldPath = ExtractFieldPath(memberExpr);
            result.Add(new GroupFieldRequest { FieldPath = fieldPath, Alias = memberExpr.Member.Name });
        }
        
        return result;
    }

    private List<AggregateRequest> ParseAggregations(LambdaExpression selector)
    {
        var result = new List<AggregateRequest>();
        
        if (selector.Body is not NewExpression newExpr || newExpr.Members == null)
            return result;

        for (int i = 0; i < newExpr.Arguments.Count; i++)
        {
            var arg = newExpr.Arguments[i];
            var alias = newExpr.Members[i].Name;
            
            if (arg is MethodCallExpression methodCall)
            {
                var funcName = methodCall.Method.Name;
                var declaringType = methodCall.Method.DeclaringType;
                
                // Check for Agg.Sum(g, x => x.Field) pattern - static Agg class methods
                if (declaringType?.Name == "Agg" && 
                    funcName is "Sum" or "Average" or "Min" or "Max" or "Count")
                {
                    // Agg.Sum has 2 arguments: (group, selector)
                    // Field path is in the second argument (lambda)
                    var fieldPath = methodCall.Arguments.Count > 1 
                        ? ExtractFieldPathFromLambda(methodCall.Arguments[1])
                        : null;
                    
                    var aggFunc = Enum.Parse<AggregateFunction>(funcName);
                    result.Add(new AggregateRequest 
                    { 
                        Function = aggFunc, 
                        FieldPath = fieldPath ?? "", 
                        Alias = alias 
                    });
                }
                // Check for g.Sum(x => x.Field) pattern - IRedbGrouping methods
                else if (IsGroupingMethod(methodCall) && 
                    funcName is "Sum" or "Avg" or "Min" or "Max" or "Count")
                {
                    var fieldPath = methodCall.Arguments.Count > 0 
                        ? ExtractFieldPathFromLambda(methodCall.Arguments[0])
                        : null;
                    result.Add(new AggregateRequest 
                    { 
                        Function = Enum.Parse<AggregateFunction>(funcName), 
                        FieldPath = fieldPath ?? "", 
                        Alias = alias 
                    });
                }
            }
        }
        
        return result;
    }

    private List<WindowFuncRequest> ParseWindowFunctions(LambdaExpression selector)
    {
        var result = new List<WindowFuncRequest>();
        
        if (selector.Body is not NewExpression newExpr || newExpr.Members == null)
            return result;

        for (int i = 0; i < newExpr.Arguments.Count; i++)
        {
            var arg = newExpr.Arguments[i];
            var alias = newExpr.Members[i].Name;
            
            if (arg is MethodCallExpression methodCall && 
                methodCall.Method.DeclaringType == typeof(Win))
            {
                var funcName = methodCall.Method.Name;
                string? fieldPath = null;
                
                if (methodCall.Arguments.Count > 0)
                {
                    fieldPath = ExtractFieldPathFromLambda(methodCall.Arguments[0]);
                }
                
                result.Add(new WindowFuncRequest { Func = funcName, FieldPath = fieldPath ?? "", Alias = alias });
            }
        }
        
        return result;
    }

    private List<WindowFieldRequest> ParsePartitionBy()
    {
        var result = new List<WindowFieldRequest>();
        
        foreach (var expr in _windowSpec.PartitionByExpressions)
        {
            if (expr.Body is MemberExpression memberExpr)
            {
                result.Add(new WindowFieldRequest { FieldPath = memberExpr.Member.Name, Alias = memberExpr.Member.Name });
            }
        }
        
        return result;
    }

    private List<WindowOrderRequest> ParseOrderBy()
    {
        var result = new List<WindowOrderRequest>();
        
        foreach (var (expr, desc) in _windowSpec.OrderByExpressions)
        {
            if (expr.Body is MethodCallExpression methodCall)
            {
                var funcName = methodCall.Method.Name;
                var declaringType = methodCall.Method.DeclaringType;
                string? fieldPath = null;
                
                // Agg.Sum(g, x => x.Field) - field is in Arguments[1]
                if (declaringType?.Name == "Agg" && methodCall.Arguments.Count > 1)
                {
                    fieldPath = ExtractFieldPathFromLambda(methodCall.Arguments[1]);
                }
                // g.Sum(x => x.Field) - field is in Arguments[0]  
                else if (methodCall.Arguments.Count > 0)
                {
                    fieldPath = ExtractFieldPathFromLambda(methodCall.Arguments[0]);
                }
                
                result.Add(new WindowOrderRequest { FieldPath = $"Agg_{funcName}_{fieldPath ?? "Count"}", Descending = desc });
            }
            else if (expr.Body is MemberExpression memberExpr)
            {
                result.Add(new WindowOrderRequest { FieldPath = memberExpr.Member.Name, Descending = desc });
            }
        }
        
        return result;
    }

    private bool IsGroupingMethod(MethodCallExpression methodCall)
    {
        var declaringType = methodCall.Method.DeclaringType;
        if (declaringType == null) return false;
        return declaringType.IsGenericType && 
               declaringType.GetGenericTypeDefinition() == typeof(IRedbGrouping<,>);
    }

    private string ExtractFieldPath(Expression expr)
    {
        if (expr is MemberExpression memberExpr)
        {
            var path = new List<string>();
            var current = memberExpr;
            
            while (current != null)
            {
                if (current.Member.Name != "Props")
                    path.Insert(0, current.Member.Name);
                current = current.Expression as MemberExpression;
            }
            
            return string.Join(".", path);
        }
        
        return expr.ToString();
    }

    private string? ExtractFieldPathFromLambda(Expression expr)
    {
        if (expr is UnaryExpression unary)
            expr = unary.Operand;
            
        if (expr is LambdaExpression lambda)
            return ExtractFieldPath(lambda.Body);
            
        return ExtractFieldPath(expr);
    }

    private List<TResult> MaterializeResults<TResult>(JsonDocument? jsonDoc, LambdaExpression selector)
    {
        var results = new List<TResult>();
        if (jsonDoc == null) return results;

        var resultType = typeof(TResult);
        var constructor = resultType.GetConstructors().FirstOrDefault();
        if (constructor == null) return results;

        foreach (var element in jsonDoc.RootElement.EnumerateArray())
        {
            var args = new List<object?>();
            
            if (selector.Body is NewExpression newExpr && newExpr.Members != null)
            {
                for (int i = 0; i < newExpr.Members.Count; i++)
                {
                    var memberName = newExpr.Members[i].Name;
                    var memberType = constructor.GetParameters()[i].ParameterType;
                    
                    // Try direct property lookup first
                    if (element.TryGetProperty(memberName, out var prop))
                    {
                        args.Add(ConvertJsonValue(prop, memberType));
                    }
                    else
                    {
                        // Handle g.Key.SomeField.Id pattern - look for parent field name
                        var jsonAlias = ExtractJsonAliasFromArgument(newExpr.Arguments[i]);
                        if (!string.IsNullOrEmpty(jsonAlias) && element.TryGetProperty(jsonAlias, out prop))
                        {
                            args.Add(ConvertJsonValue(prop, memberType));
                        }
                        else
                        {
                            args.Add(GetDefaultValue(memberType));
                        }
                    }
                }
            }
            
            var instance = constructor.Invoke(args.ToArray());
            results.Add((TResult)instance);
        }

        return results;
    }

    /// <summary>
    /// Extracts JSON field alias from selector argument.
    /// Handles patterns like g.Key.SomeField.Id -> returns "SomeField"
    /// </summary>
    private string? ExtractJsonAliasFromArgument(Expression arg)
    {
        // Handle g.Key.SomeField.Id -> we want "SomeField" (the parent of .Id)
        if (arg is MemberExpression member && member.Member.Name == "Id")
        {
            // Go up one level to get the parent (e.g., SizeItem)
            if (member.Expression is MemberExpression parent)
            {
                return parent.Member.Name;
            }
        }
        return null;
    }

    private object? ConvertJsonValue(JsonElement element, Type targetType)
    {
        return element.ValueKind switch
        {
            JsonValueKind.Number when targetType == typeof(long) => element.GetInt64(),
            JsonValueKind.Number when targetType == typeof(int) => element.GetInt32(),
            JsonValueKind.Number when targetType == typeof(decimal) => element.GetDecimal(),
            JsonValueKind.Number when targetType == typeof(double) => element.GetDouble(),
            JsonValueKind.String => element.GetString(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Null => null,
            _ => element.ToString()
        };
    }

    private object? GetDefaultValue(Type type) =>
        type.IsValueType ? Activator.CreateInstance(type) : null;
}
