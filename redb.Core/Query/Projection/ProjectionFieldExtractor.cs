using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Core.Query.Aggregation;

namespace redb.Core.Query.Projection;

/// <summary>
/// Extracts structure_ids from Select expression to optimize _values loading.
/// ⭐ SUPPORTS: simple fields, Class fields, arrays, nested arrays (graph of any depth)
/// </summary>
public class ProjectionFieldExtractor
{
    /// <summary>
    /// Extracts HashSet of structure_ids for optimized BULK SELECT.
    /// </summary>
    /// <returns>
    /// HashSet of structure_ids or null (if parsing failed → load everything)
    /// </returns>
    public HashSet<long>? ExtractStructureIds<TProps, TResult>(
        IRedbScheme scheme,
        Expression<Func<RedbObject<TProps>, TResult>> selector)
        where TProps : class, new()
    {
        try
        {
            var result = new HashSet<long>();
            var paths = ExtractFieldPaths(selector.Body);
            
            foreach (var path in paths)
            {
                AddStructureIdsForPath(scheme, path, result);
            }
            
            return result.Count > 0 ? result : null;
        }
        catch (Exception ex)
        {
            // Parsing failed — load all fields
            return null;
        }
    }
    
    /// <summary>
    /// Extracts text field paths for SQL function search_objects_with_projection_by_paths.
    /// Format: ["Name", "AddressBook[home].City", "Items[0].Price"]
    /// </summary>
    /// <returns>
    /// List of text paths or null (if parsing failed)
    /// </returns>
    public List<string>? ExtractFieldPathStrings<TProps, TResult>(
        Expression<Func<RedbObject<TProps>, TResult>> selector)
        where TProps : class, new()
    {
        try
        {
            var paths = ExtractFieldPaths(selector.Body);
            
            if (paths.Count == 0)
                return null;
            
            var result = paths
                .Where(p => p.Segments.Count > 0)
                .Select(p => p.FullPath)
                .Distinct()
                .ToList();
            
            return result.Count > 0 ? result : null;
        }
        catch (Exception)
        {
            return null;
        }
    }
    
    /// <summary>
    /// Checks if expression contains aggregation calls (Agg.Sum, etc.)
    /// </summary>
    public bool HasAggregations<TProps, TResult>(
        Expression<Func<RedbObject<TProps>, TResult>> selector)
        where TProps : class, new()
    {
        return ContainsAggCall(selector.Body);
    }
    
    /// <summary>
    /// Extracts information about requested aggregations
    /// </summary>
    public List<AggregationInfo> ExtractAggregations<TProps, TResult>(
        Expression<Func<RedbObject<TProps>, TResult>> selector)
        where TProps : class, new()
    {
        var result = new List<AggregationInfo>();
        CollectAggregations(selector.Body, result);
        return result;
    }

    #region Private Methods - Path Extraction
    
    /// <summary>
    /// Extracts all field paths from expression
    /// </summary>
    private List<FieldPathInfo> ExtractFieldPaths(Expression expression)
    {
        var paths = new List<FieldPathInfo>();
        CollectFieldPaths(expression, paths);
        return paths;
    }
    
    /// <summary>
    /// Recursively collects field paths from expression
    /// </summary>
    private void CollectFieldPaths(Expression expression, List<FieldPathInfo> paths)
    {
        switch (expression)
        {
            case NewExpression newExpr:
                foreach (var arg in newExpr.Arguments)
                {
                    CollectFieldPaths(arg, paths);
                }
                break;
                
            case MemberInitExpression initExpr:
                foreach (var binding in initExpr.Bindings)
                {
                    if (binding is MemberAssignment assignment)
                    {
                        CollectFieldPaths(assignment.Expression, paths);
                    }
                }
                break;
                
            case MemberExpression memberExpr:
                var path = ExtractMemberPath(memberExpr);
                if (path != null)
                {
                    paths.Add(path);
                }
                break;
                
            case MethodCallExpression methodExpr:
                CollectFromMethodCall(methodExpr, paths);
                break;
                
            case UnaryExpression unaryExpr:
                CollectFieldPaths(unaryExpr.Operand, paths);
                break;
                
            case ConditionalExpression condExpr:
                CollectFieldPaths(condExpr.IfTrue, paths);
                CollectFieldPaths(condExpr.IfFalse, paths);
                break;
                
            case BinaryExpression binaryExpr:
                CollectFieldPaths(binaryExpr.Left, paths);
                CollectFieldPaths(binaryExpr.Right, paths);
                break;
        }
    }
    
    /// <summary>
    /// Extracts path from MemberExpression (x.Props.Customer.Name → ["Customer", "Name"])
    /// Supports Dictionary indexer: x.Props.AddressBook["home"].City → ["AddressBook[home]", "City"]
    /// </summary>
    private FieldPathInfo? ExtractMemberPath(MemberExpression memberExpr)
    {
        var segments = new List<string>();
        string? dictKey = null;
        int dictKeyInsertPosition = -1;
        Expression? current = memberExpr;
        
        while (current != null)
        {
            switch (current)
            {
                case MemberExpression member:
                    segments.Insert(0, member.Member.Name);
                    current = member.Expression;
                    break;
                    
                case MethodCallExpression indexerCall when indexerCall.Method.Name == "get_Item":
                    // Dictionary indexer: AddressBook["home"]
                    if (indexerCall.Arguments.Count > 0 && indexerCall.Arguments[0] is ConstantExpression keyConst)
                    {
                        dictKey = keyConst.Value?.ToString();
                        // Remember position where key needs to be inserted (after processing Object)
                        dictKeyInsertPosition = segments.Count;
                    }
                    current = indexerCall.Object;
                    break;
                    
                default:
                    current = null;
                    break;
            }
        }
        
        // Check that path starts with Props
        var propsIndex = segments.FindIndex(s => s == "Props");
        if (propsIndex < 0)
            return null;
        
        // Take segments after Props
        var propsSegments = segments.Skip(propsIndex + 1).ToList();
        if (propsSegments.Count == 0)
            return null;
        
        // If there's a dictionary key, add it to the appropriate segment
        // x.Props.AddressBook["home"].City → segments = ["Props", "AddressBook", "City"]
        // dictKeyInsertPosition shows how many segments were AFTER indexer (in this case 1 = "City")
        // So key should be added to segment at index (propsSegments.Count - dictKeyInsertPosition - 1)
        if (dictKey != null && propsSegments.Count > 0)
        {
            // Index of segment to add key to
            int targetIdx = propsSegments.Count - dictKeyInsertPosition - 1;
            if (targetIdx >= 0 && targetIdx < propsSegments.Count)
            {
                propsSegments[targetIdx] = $"{propsSegments[targetIdx]}[{dictKey}]";
            }
            else if (propsSegments.Count > 0)
            {
                // Fallback: add to first segment
                propsSegments[0] = $"{propsSegments[0]}[{dictKey}]";
            }
        }
        
        return new FieldPathInfo
        {
            Segments = propsSegments,
            IsArray = false,
            DictKey = dictKey
        };
    }
    
    /// <summary>
    /// Processes method calls: Select, Agg.Sum, Dictionary indexer, etc.
    /// </summary>
    private void CollectFromMethodCall(MethodCallExpression methodExpr, List<FieldPathInfo> paths)
    {
        var methodName = methodExpr.Method.Name;
        var declaringType = methodExpr.Method.DeclaringType;
        
        // ⭐ Dictionary indexer: x.Props.AddressBook["home"] or x.Props.AddressBook["home"].City
        if (methodName == "get_Item" && declaringType != null && declaringType.IsGenericType)
        {
            var genericDef = declaringType.GetGenericTypeDefinition();
            if (genericDef == typeof(Dictionary<,>) || genericDef == typeof(IDictionary<,>))
            {
                var dictPath = ExtractDictionaryIndexerPath(methodExpr);
                if (dictPath != null)
                {
                    paths.Add(dictPath);
                }
                return;
            }
        }
        
        // Agg.Sum, Agg.Average, etc.
        if (declaringType == typeof(Agg))
        {
            foreach (var arg in methodExpr.Arguments)
            {
                CollectFieldPaths(arg, paths);
            }
            return;
        }
        
        // LINQ Select: x.Props.Items.Select(i => i.Price)
        if (methodName == "Select" && methodExpr.Arguments.Count >= 1)
        {
            // Base collection
            CollectFieldPaths(methodExpr.Arguments[0], paths);
            
            // Lambda inside Select
            if (methodExpr.Arguments.Count >= 2 && methodExpr.Arguments[1] is LambdaExpression lambda)
            {
                CollectFieldPaths(lambda.Body, paths);
            }
            return;
        }
        
        // LINQ ToList
        if (methodName == "ToList" && methodExpr.Arguments.Count >= 1)
        {
            CollectFieldPaths(methodExpr.Arguments[0], paths);
            return;
        }
        
        // Extension methods (FirstOrDefault, etc.)
        if (methodExpr.Object != null)
        {
            CollectFieldPaths(methodExpr.Object, paths);
        }
        foreach (var arg in methodExpr.Arguments)
        {
            CollectFieldPaths(arg, paths);
        }
    }
    
    /// <summary>
    /// Extracts path from Dictionary indexer expression: x.Props.AddressBook["home"].City
    /// </summary>
    private FieldPathInfo? ExtractDictionaryIndexerPath(MethodCallExpression indexerExpr)
    {
        var segments = new List<string>();
        string? dictKey = null;
        
        // Extract dictionary key
        if (indexerExpr.Arguments.Count > 0 && indexerExpr.Arguments[0] is ConstantExpression keyConst)
        {
            dictKey = keyConst.Value?.ToString();
        }
        
        // Recursively extract path to dictionary (x.Props.AddressBook)
        Expression? current = indexerExpr.Object;
        
        while (current != null)
        {
            switch (current)
            {
                case MemberExpression member:
                    segments.Insert(0, member.Member.Name);
                    current = member.Expression;
                    break;
                    
                case MethodCallExpression nestedIndexer when nestedIndexer.Method.Name == "get_Item":
                    // Nested Dictionary indexer - not supported yet
                    current = nestedIndexer.Object;
                    break;
                    
                default:
                    current = null;
                    break;
            }
        }
        
        // Check that path starts with Props
        var propsIndex = segments.FindIndex(s => s == "Props");
        if (propsIndex < 0)
            return null;
        
        // Take segments after Props
        var propsSegments = segments.Skip(propsIndex + 1).ToList();
        if (propsSegments.Count == 0)
            return null;
        
        // Add key to dictionary name: AddressBook -> AddressBook[home]
        if (dictKey != null && propsSegments.Count > 0)
        {
            propsSegments[propsSegments.Count - 1] = $"{propsSegments[propsSegments.Count - 1]}[{dictKey}]";
        }
        
        return new FieldPathInfo
        {
            Segments = propsSegments,
            IsArray = false,
            DictKey = dictKey
        };
    }
    
    #endregion
    
    #region Private Methods - Structure IDs
    
    /// <summary>
    /// Adds structure_ids for path considering NESTED arrays
    /// </summary>
    private void AddStructureIdsForPath(IRedbScheme scheme, FieldPathInfo pathInfo, HashSet<long> result)
    {
        long? parentId = null;
        
        foreach (var segment in pathInfo.Segments)
        {
            var cleanName = CleanSegmentName(segment);
            
            var structure = scheme.Structures
                .FirstOrDefault(s => s.Name == cleanName && s.IdParent == parentId);
            
            if (structure == null) break;
            
            result.Add(structure.Id);
            
            // ⭐ KEY LOGIC: For any Class/Array add ALL children RECURSIVELY
            if (structure.CollectionType != null || IsClassField(scheme, structure))
            {
                AddAllChildStructuresRecursive(scheme, structure.Id, result);
            }
            
            parentId = structure.Id;
        }
    }
    
    /// <summary>
    /// RECURSIVELY adds all child structures at ANY DEPTH
    /// </summary>
    private void AddAllChildStructuresRecursive(IRedbScheme scheme, long parentId, HashSet<long> result)
    {
        var children = scheme.Structures.Where(s => s.IdParent == parentId);
        
        foreach (var child in children)
        {
            result.Add(child.Id);
            // Recursion for nested arrays and Class inside arrays
            AddAllChildStructuresRecursive(scheme, child.Id, result);
        }
    }
    
    /// <summary>
    /// Cleans segment name from [] and indices
    /// </summary>
    private string CleanSegmentName(string segment)
    {
        var bracketIndex = segment.IndexOf('[');
        return bracketIndex >= 0 ? segment.Substring(0, bracketIndex) : segment;
    }
    
    /// <summary>
    /// Checks if structure is a Class field (has child structures)
    /// </summary>
    private bool IsClassField(IRedbScheme scheme, IRedbStructure structure)
    {
        // Class field = has child structures and is not a collection
        if (structure.CollectionType != null) return false;
        return scheme.Structures.Any(s => s.IdParent == structure.Id);
    }
    
    #endregion
    
    #region Private Methods - Aggregation Detection
    
    /// <summary>
    /// Checks if expression contains Agg.* calls
    /// </summary>
    private bool ContainsAggCall(Expression expression)
    {
        switch (expression)
        {
            case MethodCallExpression methodExpr:
                if (methodExpr.Method.DeclaringType == typeof(Agg))
                    return true;
                return methodExpr.Arguments.Any(ContainsAggCall) || 
                       (methodExpr.Object != null && ContainsAggCall(methodExpr.Object));
                
            case NewExpression newExpr:
                return newExpr.Arguments.Any(ContainsAggCall);
                
            case MemberInitExpression initExpr:
                return initExpr.Bindings.OfType<MemberAssignment>()
                    .Any(b => ContainsAggCall(b.Expression));
                
            case UnaryExpression unaryExpr:
                return ContainsAggCall(unaryExpr.Operand);
                
            case BinaryExpression binaryExpr:
                return ContainsAggCall(binaryExpr.Left) || ContainsAggCall(binaryExpr.Right);
                
            case LambdaExpression lambdaExpr:
                return ContainsAggCall(lambdaExpr.Body);
                
            default:
                return false;
        }
    }
    
    /// <summary>
    /// Collects aggregation information
    /// </summary>
    private void CollectAggregations(Expression expression, List<AggregationInfo> result)
    {
        switch (expression)
        {
            case MethodCallExpression methodExpr when methodExpr.Method.DeclaringType == typeof(Agg):
                var aggInfo = new AggregationInfo
                {
                    Function = ParseAggFunction(methodExpr.Method.Name),
                    FieldPaths = new List<FieldPathInfo>()
                };
                foreach (var arg in methodExpr.Arguments)
                {
                    var argPaths = ExtractFieldPaths(arg);
                    aggInfo.FieldPaths.AddRange(argPaths);
                }
                result.Add(aggInfo);
                break;
                
            case NewExpression newExpr:
                foreach (var arg in newExpr.Arguments)
                    CollectAggregations(arg, result);
                break;
                
            case MemberInitExpression initExpr:
                foreach (var binding in initExpr.Bindings.OfType<MemberAssignment>())
                    CollectAggregations(binding.Expression, result);
                break;
                
            case UnaryExpression unaryExpr:
                CollectAggregations(unaryExpr.Operand, result);
                break;
                
            case BinaryExpression binaryExpr:
                CollectAggregations(binaryExpr.Left, result);
                CollectAggregations(binaryExpr.Right, result);
                break;
        }
    }
    
    private AggregateFunction ParseAggFunction(string methodName)
    {
        return methodName switch
        {
            "Sum" => AggregateFunction.Sum,
            "Average" => AggregateFunction.Average,
            "Min" => AggregateFunction.Min,
            "Max" => AggregateFunction.Max,
            "Count" => AggregateFunction.Count,
            _ => throw new NotSupportedException($"Unknown aggregation: {methodName}")
        };
    }
    
    #endregion
}

/// <summary>
/// Field path information
/// </summary>
public record FieldPathInfo
{
    /// <summary>Path segments: ["Contacts", "Email"] or ["AddressBook[home]", "City"]</summary>
    public List<string> Segments { get; init; } = new();
    
    /// <summary>Is array?</summary>
    public bool IsArray { get; init; }
    
    /// <summary>Array element index (for Tags[0])</summary>
    public int? ArrayIndex { get; init; }
    
    /// <summary>Dictionary key (for AddressBook["home"])</summary>
    public string? DictKey { get; init; }
    
    /// <summary>Full path as string</summary>
    public string FullPath => string.Join(".", Segments);
}

/// <summary>
/// Aggregation information
/// </summary>
public record AggregationInfo
{
    public AggregateFunction Function { get; init; }
    public List<FieldPathInfo> FieldPaths { get; init; } = new();
}
