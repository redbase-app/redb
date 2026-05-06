using System.Linq.Expressions;
using System.Text.Json;
using redb.Core.Models.Entities;
using redb.Core.Query.QueryExpressions;
using redb.Core.Query.Utils;

namespace redb.Core.Query.Window;

/// <summary>
/// Implementation of queries with window functions
/// </summary>
public class RedbWindowedQueryable<TProps> : IRedbWindowedQueryable<TProps> 
    where TProps : class, new()
{
    private readonly IRedbQueryProvider _provider;
    private readonly long _schemeId;
    private readonly string? _filterJson;
    private readonly FilterExpression? _filter;
    private readonly WindowSpec<TProps> _windowSpec;
    private readonly int? _take;
    private readonly int? _skip;
    
    /// <summary>
    /// Constructor with filterJson (legacy compatibility).
    /// </summary>
    public RedbWindowedQueryable(
        IRedbQueryProvider provider,
        long schemeId,
        string? filterJson,
        WindowSpec<TProps> windowSpec,
        int? take = null,
        int? skip = null)
    {
        _provider = provider;
        _schemeId = schemeId;
        _filterJson = filterJson;
        _filter = null;
        _windowSpec = windowSpec;
        _take = take;
        _skip = skip;
    }
    
    /// <summary>
    /// Constructor with FilterExpression (Pro version).
    /// </summary>
    public RedbWindowedQueryable(
        IRedbQueryProvider provider,
        long schemeId,
        FilterExpression? filter,
        WindowSpec<TProps> windowSpec,
        int? take = null,
        int? skip = null)
    {
        _provider = provider;
        _schemeId = schemeId;
        _filter = filter;
        _filterJson = null;
        _windowSpec = windowSpec;
        _take = take;
        _skip = skip;
    }
    
    public async Task<List<TResult>> SelectAsync<TResult>(
        Expression<Func<RedbObject<TProps>, TResult>> selector)
    {
        // Parse SELECT fields and Win.* functions
        var (selectFields, windowFuncs) = ParseSelector(selector);
        var partitionBy = ParsePartitionBy();
        var orderBy = ParseOrderBy();
        var frameJson = SerializeFrame();
        
        // Call SQL - use FilterExpression if available, otherwise filterJson
        var jsonResult = _filter != null
            ? await _provider.ExecuteWindowQueryAsync(_schemeId, selectFields, windowFuncs, partitionBy, orderBy, _filter, frameJson, _take, _skip)
            : await _provider.ExecuteWindowQueryAsync(_schemeId, selectFields, windowFuncs, partitionBy, orderBy, _filterJson, frameJson, _take, _skip);
        
        // Materialize
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
        
        // Use filterJson for preview (Pro handles this separately)
        return await _provider.GetWindowSqlPreviewAsync(
            _schemeId, selectFields, windowFuncs, partitionBy, orderBy, _filterJson, frameJson, _take, _skip);
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
                
                // Win.RowNumber(), Win.Sum(x.Props.Field), Win.Lag(x.Props.Field), Win.Ntile(4), etc.
                if (arg is MethodCallExpression mc && mc.Method.DeclaringType == typeof(Aggregation.Win))
                {
                    // Convert method names to SQL-compatible
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
                    
                    // Win.Ntile(4) - extract buckets constant
                    if (mc.Method.Name == "Ntile" && mc.Arguments.Count > 0)
                    {
                        if (mc.Arguments[0] is ConstantExpression ce && ce.Value is int b)
                            buckets = b;
                    }
                    // Extract field for aggregates (Win.Sum(field), Win.Lag(field))
                    else if (mc.Arguments.Count > 0)
                    {
                        fieldPath = ExtractFieldPathFromArg(mc.Arguments[0]);
                    }
                    
                    funcs.Add(new WindowFuncRequest { Func = sqlFunc, FieldPath = fieldPath, Alias = alias, Buckets = buckets });
                }
                // Regular field
                else if (arg is MemberExpression member)
                {
                    var path = ExtractFieldPath(member);
                    if (!string.IsNullOrEmpty(path))
                    {
                        // 🔥 CRITICAL: Determine if this is a base field (Name, Id) or EAV (Props.Stock)
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
                        IsBaseField = isBaseField  // 🆕
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
                        IsBaseField = isBaseField  // 🆕
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
            // Filter only "Props" - for base fields leave everything as is
            if (m.Member.Name != "Props")
                parts.Insert(0, m.Member.Name);
            current = m.Expression;
        }
        return string.Join(".", parts);
    }
    
    /// <summary>
    /// 🔥 Determines if field access is base (Name, Id, SchemeId)
    /// or EAV through Props (Props.Stock, Props.Category).
    /// Base field: path does NOT contain "Props" (x.Name, x.Id)
    /// EAV field: path contains "Props" (x.Props.Stock)
    /// </summary>
    private bool IsBaseFieldAccess(MemberExpression member)
    {
        Expression? current = member;
        while (current is MemberExpression m)
        {
            if (m.Member.Name == "Props")
                return false; // Contains Props → EAV field
            current = m.Expression;
        }
        return true; // Does not contain Props → base field
    }
    
    /// <summary>
    /// Extracts field path from Win.Sum(x.Props.Field) argument
    /// </summary>
    private string ExtractFieldPathFromArg(Expression arg)
    {
        // Unwrap Convert if present (for value types)
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

public class WindowFieldRequest 
{ 
    public string FieldPath { get; set; } = ""; 
    public string Alias { get; set; } = "";
    public bool IsBaseField { get; set; } = false;  // 🆕 Base field from _objects
}

public class WindowFuncRequest 
{ 
    public string Func { get; set; } = "";       // SUM, AVG, LAG, ROW_NUMBER, NTILE...
    public string FieldPath { get; set; } = "";  // Field for aggregate (empty for ROW_NUMBER)
    public string Alias { get; set; } = "";
    public int? Buckets { get; set; }            // For NTILE(n)
    public bool IsBaseField { get; set; } = false;  // 🆕 Base field from _objects
}

public class WindowOrderRequest 
{ 
    public string FieldPath { get; set; } = ""; 
    public bool Descending { get; set; }
    public bool IsBaseField { get; set; } = false;  // 🆕 Base field from _objects
 }
