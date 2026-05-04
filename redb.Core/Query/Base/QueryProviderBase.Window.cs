using System.Text.Json;
using Microsoft.Extensions.Logging;
using redb.Core.Query.QueryExpressions;
using redb.Core.Query.Window;

namespace redb.Core.Query.Base;

/// <summary>
/// QueryProviderBase: Window Functions
/// </summary>
public abstract partial class QueryProviderBase
{
    public virtual async Task<JsonDocument?> ExecuteWindowQueryAsync(
        long schemeId,
        IEnumerable<WindowFieldRequest> selectFields,
        IEnumerable<WindowFuncRequest> windowFuncs,
        IEnumerable<WindowFieldRequest> partitionBy,
        IEnumerable<WindowOrderRequest> orderBy,
        string? filterJson = null,
        string? frameJson = null,
        int? take = null,
        int? skip = null)
    {
        var selectJson = JsonSerializer.Serialize(
            selectFields.Select(f => new { 
                field = f.IsBaseField ? $"0$:{f.FieldPath}" : f.FieldPath, 
                alias = f.Alias 
            }));
        
        var funcsJson = JsonSerializer.Serialize(
            windowFuncs.Select(f => new { 
                func = f.Func, 
                field = f.IsBaseField ? $"0$:{f.FieldPath}" : f.FieldPath, 
                alias = f.Alias, 
                buckets = f.Buckets 
            }));
        
        // 🔥 CRITICAL: Adding the "0$:" prefix for base fields!
        var partitionJson = JsonSerializer.Serialize(
            partitionBy.Select(p => new { 
                field = p.IsBaseField ? $"0$:{p.FieldPath}" : p.FieldPath 
            }));
        
        var orderJson = JsonSerializer.Serialize(
            orderBy.Select(o => new { 
                field = o.IsBaseField ? $"0$:{o.FieldPath}" : o.FieldPath, 
                dir = o.Descending ? "DESC" : "ASC" 
            }));
        
        _logger?.LogDebug("Window: select={Select}, funcs={Funcs}, partition={Partition}, order={Order}, frame={Frame}", 
            selectJson, funcsJson, partitionJson, orderJson, frameJson ?? "null");
        
        // Use take if specified, otherwise default to 1000
        var limit = take ?? 1000;
        
        var result = await _context.QueryFirstOrDefaultAsync<WindowResult>(_sql.Query_WindowSql(),
                schemeId,
                selectJson,
                funcsJson,
                partitionJson,
                orderJson,
                filterJson ?? "null",
                limit,
                frameJson ?? "null");
        
        if (result?.result == null) return null;
        return JsonDocument.Parse(result.result);
    }
    
    /// <summary>
    /// SQL preview for window query.
    /// Base implementation returns placeholder (overridden in Pro).
    /// </summary>
    public virtual Task<string> GetWindowSqlPreviewAsync(
        long schemeId,
        IEnumerable<WindowFieldRequest> selectFields,
        IEnumerable<WindowFuncRequest> windowFuncs,
        IEnumerable<WindowFieldRequest> partitionBy,
        IEnumerable<WindowOrderRequest> orderBy,
        string? filterJson = null,
        string? frameJson = null,
        int? take = null,
        int? skip = null)
    {
        // Base version uses SQL function, no direct SQL preview available
        return Task.FromResult($"-- Window SQL Preview not available in Open Source version\n-- SchemeId: {schemeId}\n-- Use Pro version for SQL preview");
    }
    
    /// <summary>
    /// Execute window query with FilterExpression (Pro version).
    /// Free fallback: converts FilterExpression to facet-JSON.
    /// </summary>
    public virtual async Task<JsonDocument?> ExecuteWindowQueryAsync(
        long schemeId,
        IEnumerable<WindowFieldRequest> selectFields,
        IEnumerable<WindowFuncRequest> windowFuncs,
        IEnumerable<WindowFieldRequest> partitionBy,
        IEnumerable<WindowOrderRequest> orderBy,
        FilterExpression? filter,
        string? frameJson = null,
        int? take = null,
        int? skip = null)
    {
        var filterJson = filter != null ? _facetBuilder.BuildFacetFilters(filter) : null;
        return await ExecuteWindowQueryAsync(schemeId, selectFields, windowFuncs, partitionBy, orderBy, filterJson, frameJson, take, skip);
    }
    
    private class WindowResult { public string? result { get; set; } }
}
