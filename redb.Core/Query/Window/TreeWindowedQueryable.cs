using System.Linq.Expressions;
using System.Text.Json;
using redb.Core.Models.Entities;
using redb.Core.Query.Base;
using redb.Core.Query.Utils;

namespace redb.Core.Query.Window;

/// <summary>
/// Implementation of queries with window functions for TreeQueryable.
/// Preserves TreeQueryContext for tree-aware Window Functions execution.
/// </summary>
public class TreeWindowedQueryable<TProps> : IRedbWindowedQueryable<TProps> 
    where TProps : class, new()
{
    private readonly ITreeQueryProvider _treeProvider;
    private readonly TreeQueryContext<TProps> _treeContext;
    private readonly string? _filterJson;
    private readonly WindowSpec<TProps> _windowSpec;
    
    /// <summary>
    /// Creates tree-aware windowed queryable.
    /// </summary>
    /// <param name="treeProvider">Tree query provider with tree-aware execution</param>
    /// <param name="treeContext">Tree query context with rootObjectId, maxDepth, filters</param>
    /// <param name="filterJson">Filter JSON from Where clauses</param>
    /// <param name="windowSpec">Window specification (partition, order, frame)</param>
    public TreeWindowedQueryable(
        ITreeQueryProvider treeProvider,
        TreeQueryContext<TProps> treeContext,
        string? filterJson,
        WindowSpec<TProps> windowSpec)
    {
        _treeProvider = treeProvider;
        _treeContext = treeContext;
        _filterJson = filterJson;
        _windowSpec = windowSpec;
    }
    
    /// <summary>
    /// Execute window query with tree context and materialize results.
    /// </summary>
    public async Task<List<TResult>> SelectAsync<TResult>(
        Expression<Func<RedbObject<TProps>, TResult>> selector)
    {
        var (selectFields, windowFuncs) = ParseSelector(selector);
        var partitionBy = ParsePartitionBy();
        var orderBy = ParseOrderBy();
        var frameJson = SerializeFrame();
        
        // Use tree-aware execution with full context
        var jsonResult = await _treeProvider.ExecuteTreeWindowQueryAsync(
            _treeContext, selectFields, windowFuncs, partitionBy, orderBy, frameJson);
        
        return MaterializeResults<TResult>(jsonResult, selector);
    }
    
    /// <summary>
    /// Returns SQL string for debugging.
    /// </summary>
    public async Task<string> ToSqlStringAsync<TResult>(
        Expression<Func<RedbObject<TProps>, TResult>> selector)
    {
        var (selectFields, windowFuncs) = ParseSelector(selector);
        var partitionBy = ParsePartitionBy();
        var orderBy = ParseOrderBy();
        var frameJson = SerializeFrame();
        
        return await _treeProvider.GetTreeWindowSqlPreviewAsync(
            _treeContext, selectFields, windowFuncs, partitionBy, orderBy, frameJson);
    }
    
    private string? SerializeFrame()
    {
        if (_windowSpec.FrameSpec == null) return null;
        var f = _windowSpec.FrameSpec;
        return JsonSerializer.Serialize(new {
            type = f.Type.ToString().ToUpper(),
            start = new { kind = f.Start.Type.ToString().ToUpper(), offset = f.Start.Offset },
            end = new { kind = f.End.Type.ToString().ToUpper(), offset = f.End.Offset }
        });
    }
    
    private (List<WindowFieldRequest> Fields, List<WindowFuncRequest> Funcs) ParseSelector<TResult>(
        Expression<Func<RedbObject<TProps>, TResult>> selector)
    {
        var fields = new List<WindowFieldRequest>();
        var funcs = new List<WindowFuncRequest>();
        
        if (selector.Body is NewExpression newExpr)
        {
            for (int i = 0; i < newExpr.Arguments.Count; i++)
            {
                var alias = newExpr.Members?[i].Name ?? $"Col{i}";
                var arg = newExpr.Arguments[i];
                
                // Win.RowNumber(), Win.Sum(x.Props.Field), Win.Lag(x.Props.Field), etc.
                if (arg is MethodCallExpression mc && mc.Method.DeclaringType == typeof(Aggregation.Win))
                {
                    var sqlFunc = mc.Method.Name switch
                    {
                        "RowNumber" => "ROW_NUMBER",
                        "DenseRank" => "DENSE_RANK",
                        "FirstValue" => "FIRST_VALUE",
                        "LastValue" => "LAST_VALUE",
                        _ => mc.Method.Name.ToUpper()
                    };
                    
                    string fieldPath = "";
                    int? buckets = null;
                    
                    if (mc.Method.Name == "Ntile" && mc.Arguments.Count > 0)
                    {
                        if (mc.Arguments[0] is ConstantExpression ce && ce.Value is int b)
                            buckets = b;
                    }
                    else if (mc.Arguments.Count > 0)
                    {
                        fieldPath = ExtractFieldPathFromArg(mc.Arguments[0]);
                    }
                    
                    funcs.Add(new WindowFuncRequest { Func = sqlFunc, FieldPath = fieldPath, Alias = alias, Buckets = buckets });
                }
                else if (arg is MemberExpression member)
                {
                    var path = ExtractFieldPath(member);
                    if (!string.IsNullOrEmpty(path))
                    {
                        bool isBaseField = IsBaseFieldAccess(member);
                        fields.Add(new WindowFieldRequest 
                        { 
                            FieldPath = path, 
                            Alias = alias,
                            IsBaseField = isBaseField
                        });
                    }
                }
            }
        }
        
        return (fields, funcs);
    }
    
    private List<WindowFieldRequest> ParsePartitionBy()
    {
        var result = new List<WindowFieldRequest>();
        foreach (var (expr, isBaseField) in _windowSpec.PartitionByFields)
        {
            if (expr is LambdaExpression lambda && lambda.Body is MemberExpression member)
            {
                var path = ExtractFieldPath(member);
                if (!string.IsNullOrEmpty(path))
                    result.Add(new WindowFieldRequest 
                    { 
                        FieldPath = path, 
                        IsBaseField = isBaseField
                    });
            }
        }
        return result;
    }
    
    private List<WindowOrderRequest> ParseOrderBy()
    {
        var result = new List<WindowOrderRequest>();
        foreach (var (expr, desc, isBaseField) in _windowSpec.OrderByFields)
        {
            if (expr is LambdaExpression lambda && lambda.Body is MemberExpression member)
            {
                var path = ExtractFieldPath(member);
                if (!string.IsNullOrEmpty(path))
                    result.Add(new WindowOrderRequest 
                    { 
                        FieldPath = path, 
                        Descending = desc,
                        IsBaseField = isBaseField
                    });
            }
        }
        return result;
    }
    
    private string ExtractFieldPath(MemberExpression? member)
    {
        if (member == null) return string.Empty;
        var parts = new List<string>();
        Expression? current = member;
        while (current is MemberExpression m)
        {
            if (m.Member.Name != "Props")
                parts.Insert(0, m.Member.Name);
            current = m.Expression;
        }
        return string.Join(".", parts);
    }
    
    private bool IsBaseFieldAccess(MemberExpression member)
    {
        Expression? current = member;
        while (current is MemberExpression m)
        {
            if (m.Member.Name == "Props")
                return false;
            current = m.Expression;
        }
        return true;
    }
    
    private string ExtractFieldPathFromArg(Expression arg)
    {
        while (arg is UnaryExpression unary && 
               (unary.NodeType == ExpressionType.Convert || unary.NodeType == ExpressionType.ConvertChecked))
        {
            arg = unary.Operand;
        }
        
        if (arg is MemberExpression member)
        {
            return ExtractFieldPath(member);
        }
        
        return string.Empty;
    }
    
    private List<TResult> MaterializeResults<TResult>(JsonDocument? json, 
        Expression<Func<RedbObject<TProps>, TResult>> selector)
    {
        var results = new List<TResult>();
        if (json == null) return results;
        
        foreach (var element in json.RootElement.EnumerateArray())
        {
            var result = MaterializeSingle<TResult>(element, selector);
            if (result != null)
                results.Add(result);
        }
        return results;
    }
    
    private TResult? MaterializeSingle<TResult>(JsonElement element,
        Expression<Func<RedbObject<TProps>, TResult>> selector)
    {
        if (selector.Body is NewExpression newExpr && newExpr.Constructor != null)
        {
            var args = new object?[newExpr.Arguments.Count];
            for (int i = 0; i < newExpr.Arguments.Count; i++)
            {
                var alias = newExpr.Members?[i].Name ?? $"Col{i}";
                var propType = (newExpr.Members?[i] as System.Reflection.PropertyInfo)?.PropertyType ?? typeof(object);
                
                if (element.TryGetProperty(alias, out var prop))
                    args[i] = JsonValueConverter.Convert(prop, propType);
            }
            return (TResult)newExpr.Constructor.Invoke(args);
        }
        return default;
    }
}
