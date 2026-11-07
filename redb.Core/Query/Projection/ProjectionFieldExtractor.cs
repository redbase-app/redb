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
/// SUPPORTS: simple fields, Class fields, arrays, nested arrays (graph of any depth)
/// </summary>
public class ProjectionFieldExtractor
{
    /// <summary>
    /// Extracts HashSet of structure_ids for optimized BULK SELECT.
    /// </summary>
    /// <returns>
    /// Three different answers, and the caller must not mix them up:
    /// null - the selector could not be reduced to a list of fields (a node was not recognized, the
    /// object or its Props is used whole, a field is unknown to the scheme): load everything;
    /// an empty set - the selector was read completely and touches no Props field (base fields
    /// only): nothing has to be loaded;
    /// a non-empty set - exactly these structures are read.
    /// </returns>
    public HashSet<long>? ExtractStructureIds<TProps, TResult>(
        IRedbScheme scheme,
        Expression<Func<RedbObject<TProps>, TResult>> selector)
        where TProps : class, new()
    {
        try
        {
            var result = new HashSet<long>();
            var paths = ExtractFieldPaths(selector.Body, selector.Parameters[0]);

            foreach (var path in paths)
            {
                // A member of Props the scheme does not know is not a stored field: a computed
                // property, most likely, reading fields this method cannot see.
                if (!AddStructureIdsForPath(scheme, path, result))
                    return null;
            }

            return result;
        }
        catch (NotSupportedException)
        {
            // Явный сигнал «узел не разобран» из CollectFieldPaths/CollectBindings - откат на
            // полную загрузку. Ловим ТОЛЬКО его: любая другая ошибка - баг экстрактора и обязана
            // быть громкой (решение владельца 2026-09-03: никаких глухих catch).
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
            var paths = ExtractFieldPaths(selector.Body, selector.Parameters[0]);

            if (paths.Count == 0)
                return null;

            var result = paths
                .Where(p => p.Segments.Count > 0)
                .Select(p => p.FullPath)
                .Distinct()
                .ToList();

            return result.Count > 0 ? result : null;
        }
        catch (NotSupportedException)
        {
            // Тот же явный fail-safe, что и в ExtractStructureIds; прочие ошибки - громкие.
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
    /// <param name="expression">Expression to read.</param>
    /// <param name="source">
    /// The selector's own parameter (the object). With it the walk can tell the object read whole
    /// (<c>Map(o)</c>, <c>new { O = o }</c>, <c>o.Props</c>) from a field of it, and throws
    /// NotSupportedException for the former: such a selector needs every field. Null where there is
    /// no such parameter (arguments of aggregates).
    /// </param>
    private List<FieldPathInfo> ExtractFieldPaths(Expression expression, ParameterExpression? source)
    {
        var paths = new List<FieldPathInfo>();
        CollectFieldPaths(expression, paths, source);
        return paths;
    }

    /// <summary>
    /// Recursively collects field paths from expression.
    /// FAIL-SAFE contract (ревью 2026-09-03, S-5): каждый вид узла либо явно разобран, либо
    /// метод бросает NotSupportedException - и публичные Extract* через свой try/catch отдают
    /// null, что означает «грузим объект целиком». Раньше неизвестный узел молча давал ноль
    /// путей из поддерева: при ЧАСТИЧНОМ разборе лямбды поле молча не загружалось и проекция
    /// отдавала default вместо значения.
    /// </summary>
    private void CollectFieldPaths(Expression expression, List<FieldPathInfo> paths, ParameterExpression? source)
    {
        switch (expression)
        {
            case NewExpression newExpr:
                foreach (var arg in newExpr.Arguments)
                {
                    CollectFieldPaths(arg, paths, source);
                }
                break;

            case MemberInitExpression initExpr:
                CollectFieldPaths(initExpr.NewExpression, paths, source);
                CollectBindings(initExpr.Bindings, paths, source);
                break;

            case MemberExpression memberExpr:
                CollectFromMemberChain(memberExpr, paths, source);
                break;

            case MethodCallExpression methodExpr:
                CollectFromMethodCall(methodExpr, paths, source);
                break;

            case UnaryExpression unaryExpr:
                CollectFieldPaths(unaryExpr.Operand, paths, source);
                break;

            case ConditionalExpression condExpr:
                // S-5b: условие тернарника раньше НЕ сканировалось - поле из условия молча не
                // загружалось и ветка выбиралась по default-значению.
                CollectFieldPaths(condExpr.Test, paths, source);
                CollectFieldPaths(condExpr.IfTrue, paths, source);
                CollectFieldPaths(condExpr.IfFalse, paths, source);
                break;

            case BinaryExpression binaryExpr:
                // o == null and o.Props != null read no field: the usual guard in front of a field
                // must not turn the whole selector into a full load.
                if (IsNullCheckOfSource(binaryExpr, source))
                    break;
                CollectFieldPaths(binaryExpr.Left, paths, source);
                CollectFieldPaths(binaryExpr.Right, paths, source);
                break;

            case LambdaExpression lambdaExpr:
                CollectFieldPaths(lambdaExpr.Body, paths, source);
                break;

            case NewArrayExpression newArrayExpr:
                foreach (var el in newArrayExpr.Expressions)
                    CollectFieldPaths(el, paths, source);
                break;

            case ListInitExpression listInitExpr:
                CollectFieldPaths(listInitExpr.NewExpression, paths, source);
                foreach (var init in listInitExpr.Initializers)
                    foreach (var arg in init.Arguments)
                        CollectFieldPaths(arg, paths, source);
                break;

            case InvocationExpression invocationExpr:
                CollectFieldPaths(invocationExpr.Expression, paths, source);
                foreach (var arg in invocationExpr.Arguments)
                    CollectFieldPaths(arg, paths, source);
                break;

            case TypeBinaryExpression typeBinaryExpr:
                CollectFieldPaths(typeBinaryExpr.Expression, paths, source);
                break;

            case IndexExpression indexExpr:
                if (indexExpr.Object != null)
                    CollectFieldPaths(indexExpr.Object, paths, source);
                foreach (var arg in indexExpr.Arguments)
                    CollectFieldPaths(arg, paths, source);
                break;

            case ParameterExpression parameterExpr:
                // The object itself handed on (Map(o), new { O = o }): whatever receives it may read
                // any field. Parameters of inner lambdas are plain leaves.
                if (source != null && parameterExpr == source)
                    throw new NotSupportedException(
                        "ProjectionFieldExtractor: the object is used whole - full load.");
                break;

            case ConstantExpression:
            case DefaultExpression:
                break; // листья без полей

            default:
                throw new NotSupportedException(
                    $"ProjectionFieldExtractor: unrecognized expression node {expression.NodeType} " +
                    "- falling back to full load (fail-safe).");
        }
    }

    /// <summary>
    /// Reads a member chain (<c>a.b.c</c>, indexers included) by what it hangs on.
    /// On the selector's parameter it is a field of the object: a Props path is recorded, a base
    /// field needs nothing, Props taken whole or a computed property of Props means a full load.
    /// On anything else that is an expression of its own - a call, <c>??</c>, a conditional, a cast -
    /// that expression is read in turn: <c>(o.Props.A ?? "").Length</c> reads A, and before this
    /// method existed the chain simply ended there and A was never loaded.
    /// </summary>
    private void CollectFromMemberChain(MemberExpression memberExpr, List<FieldPathInfo> paths, ParameterExpression? source)
    {
        // Same walk as ExtractMemberPath: members and indexers down to whatever carries them.
        var members = new List<MemberInfo>();
        Expression? root = memberExpr;
        while (true)
        {
            if (root is MemberExpression member)
            {
                members.Insert(0, member.Member);
                root = member.Expression;
            }
            else if (root is MethodCallExpression { Method.Name: "get_Item", Object: not null } indexer)
            {
                // The key of an indexer may itself read a field: Dict[o.Props.Key].
                foreach (var arg in indexer.Arguments)
                    CollectFieldPaths(arg, paths, source);
                root = indexer.Object;
            }
            else
            {
                break;
            }
        }

        // A cast of the object changes nothing about which field is read: ((IRedbObject)o).Name.
        var carrier = root;
        while (carrier is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked or ExpressionType.TypeAs } cast)
            carrier = cast.Operand;

        switch (carrier)
        {
            case null:               // static member
            case ConstantExpression: // captured variable
                return;

            case ParameterExpression parameter when source == null || parameter == source:
                var propsIndex = members.FindIndex(m => m.Name == "Props");
                if (propsIndex < 0)
                    return; // base field of the object

                if (source != null)
                {
                    if (propsIndex == members.Count - 1)
                        throw new NotSupportedException(
                            "ProjectionFieldExtractor: Props is used whole - full load.");

                    // A property without a setter is not stored; what it computes from is unknown here.
                    if (members[propsIndex + 1] is PropertyInfo { CanWrite: false })
                        throw new NotSupportedException(
                            $"ProjectionFieldExtractor: {members[propsIndex + 1].Name} is a computed property - full load.");

                    // Behind a cast ExtractMemberPath cannot follow the chain; rare enough to load everything.
                    if (!ReferenceEquals(carrier, root))
                        throw new NotSupportedException(
                            "ProjectionFieldExtractor: Props read through a cast of the object - full load.");
                }

                var path = ExtractMemberPath(memberExpr);
                if (path != null)
                    paths.Add(path);
                return;

            case ParameterExpression:
                return; // parameter of an inner lambda: its collection is already recorded with all children

            default:
                CollectFieldPaths(root!, paths, source);
                return;
        }
    }

    /// <summary>
    /// True for a comparison of the object or of its Props with null.
    /// </summary>
    private static bool IsNullCheckOfSource(BinaryExpression binaryExpr, ParameterExpression? source)
    {
        if (source == null || binaryExpr.NodeType is not (ExpressionType.Equal or ExpressionType.NotEqual))
            return false;

        return (IsNullConstant(binaryExpr.Right) && IsSourceOrItsProps(binaryExpr.Left))
            || (IsNullConstant(binaryExpr.Left) && IsSourceOrItsProps(binaryExpr.Right));

        static Expression StripCasts(Expression e)
        {
            while (e is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked or ExpressionType.TypeAs } cast)
                e = cast.Operand;
            return e;
        }

        static bool IsNullConstant(Expression e) => StripCasts(e) is ConstantExpression { Value: null };

        bool IsSourceOrItsProps(Expression e)
        {
            e = StripCasts(e);
            if (e == source) return true;
            return e is MemberExpression { Member.Name: "Props" } member && StripCasts(member.Expression!) == source;
        }
    }

    /// <summary>
    /// Разбор всех видов member-биндингов, включая вложенные объектные инициализаторы
    /// (MemberMemberBinding: new Dto { Sub = { X = p.Props.N } }) и списковые (MemberListBinding).
    /// </summary>
    private void CollectBindings(System.Collections.ObjectModel.ReadOnlyCollection<MemberBinding> bindings, List<FieldPathInfo> paths, ParameterExpression? source)
    {
        foreach (var binding in bindings)
        {
            switch (binding)
            {
                case MemberAssignment assignment:
                    CollectFieldPaths(assignment.Expression, paths, source);
                    break;
                case MemberMemberBinding nested:
                    CollectBindings(nested.Bindings, paths, source);
                    break;
                case MemberListBinding list:
                    foreach (var init in list.Initializers)
                        foreach (var arg in init.Arguments)
                            CollectFieldPaths(arg, paths, source);
                    break;
                default:
                    throw new NotSupportedException(
                        $"ProjectionFieldExtractor: unrecognized member binding {binding.BindingType}.");
            }
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
    private void CollectFromMethodCall(MethodCallExpression methodExpr, List<FieldPathInfo> paths, ParameterExpression? source)
    {
        var methodName = methodExpr.Method.Name;
        var declaringType = methodExpr.Method.DeclaringType;

        // Dictionary indexer: x.Props.AddressBook["home"] or x.Props.AddressBook["home"].City
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
                CollectFieldPaths(arg, paths, source);
            }
            return;
        }

        // LINQ Select: x.Props.Items.Select(i => i.Price)
        if (methodName == "Select" && methodExpr.Arguments.Count >= 1)
        {
            // Base collection
            CollectFieldPaths(methodExpr.Arguments[0], paths, source);

            // Lambda inside Select
            if (methodExpr.Arguments.Count >= 2 && methodExpr.Arguments[1] is LambdaExpression lambda)
            {
                CollectFieldPaths(lambda.Body, paths, source);
            }
            return;
        }

        // LINQ ToList
        if (methodName == "ToList" && methodExpr.Arguments.Count >= 1)
        {
            CollectFieldPaths(methodExpr.Arguments[0], paths, source);
            return;
        }

        // Extension methods (FirstOrDefault, etc.)
        if (methodExpr.Object != null)
        {
            CollectFieldPaths(methodExpr.Object, paths, source);
        }
        foreach (var arg in methodExpr.Arguments)
        {
            CollectFieldPaths(arg, paths, source);
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
    /// <returns>
    /// False when the first segment is not a structure of the scheme. Deeper segments may well be
    /// missing: Name.Length or Date.Value.Year read a member of a loaded value, not a field.
    /// </returns>
    private bool AddStructureIdsForPath(IRedbScheme scheme, FieldPathInfo pathInfo, HashSet<long> result)
    {
        long? parentId = null;

        foreach (var segment in pathInfo.Segments)
        {
            var cleanName = CleanSegmentName(segment);

            var structure = scheme.Structures
                .FirstOrDefault(s => s.Name == cleanName && s.IdParent == parentId);

            if (structure == null)
                return parentId != null;

            result.Add(structure.Id);

            // KEY LOGIC: For any Class/Array add ALL children RECURSIVELY
            if (structure.CollectionType != null || IsClassField(scheme, structure))
            {
                AddAllChildStructuresRecursive(scheme, structure.Id, result);
            }

            parentId = structure.Id;
        }

        return true;
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
                    var argPaths = ExtractFieldPaths(arg, source: null);
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
