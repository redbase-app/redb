using System.Linq.Expressions;
using redb.Core.Models.Contracts;
using redb.Core.Query.QueryExpressions;
using redb.Core.Utils;

namespace redb.Core.Query.Parsing;

/// <summary>
/// Base class for parsing LINQ expressions to FilterExpression.
/// Contains all DB-agnostic logic. DB-specific implementations override CheckProOnlyFeatures.
/// </summary>
public abstract class BaseFilterExpressionParser : IFilterExpressionParser
{
    /// <summary>
    /// Context flag: true when parsing WhereRedb (base fields), false for regular Where (Props).
    /// </summary>
    protected bool _isBaseFieldContext = false;

    /// <summary>
    /// Parse LINQ predicate to FilterExpression for Props fields.
    /// </summary>
    public virtual FilterExpression ParseFilter<TProps>(Expression<Func<TProps, bool>> predicate) where TProps : class
    {
        _isBaseFieldContext = false;
        CheckProOnlyFeatures(predicate.Body, "Where filter");
        return VisitExpression(predicate.Body);
    }
    
    /// <summary>
    /// Hook for backend-specific feature checks. No-op in OSS.
    /// Implementations may override to raise on unsupported expressions.
    /// </summary>
    protected virtual void CheckProOnlyFeatures(Expression body, string context)
    {
        // No-op: OSS allows all expression shapes; unsupported ones surface
        // later as concrete errors during SQL generation or execution.
    }

    /// <summary>
    /// Check property path nesting depth.
    /// No artificial limit — PVT engine supports up to 50 levels natively.
    /// Override in implementations if a backend-specific cap is needed.
    /// </summary>
    protected virtual void CheckPathDepth(int depth, string fullPath)
    {
        // Intentionally no-op: deep nested paths (e.g. Address.Building.Floor)
        // are fully supported by the SQL builder.
    }

    /// <summary>
    /// Parse LINQ predicate for base RedbObject fields (id, name, parent_id, etc.).
    /// </summary>
    public FilterExpression ParseRedbFilter(Expression<Func<IRedbObject, bool>> predicate)
    {
        _isBaseFieldContext = true;
        var result = VisitExpression(predicate.Body);
        _isBaseFieldContext = false;
        return result;
    }

    #region Expression Visitors

    /// <summary>
    /// Main visitor dispatcher.
    /// </summary>
    protected virtual FilterExpression VisitExpression(Expression expression)
    {
        return expression switch
        {
            BinaryExpression binary => VisitBinaryExpression(binary),
            UnaryExpression unary => VisitUnaryExpression(unary),
            MethodCallExpression method => VisitMethodCallExpression(method),
            ConstantExpression constant when constant.Type == typeof(bool) => 
                VisitConstantBooleanExpression(constant),
            // Support for .Where(p => p.IsActive) without explicit == true
            MemberExpression member when member.Type == typeof(bool) =>
                VisitBooleanMemberExpression(member, negated: false),
            _ => throw new NotSupportedException($"Expression type {expression.NodeType} is not supported")
        };
    }

    /// <summary>
    /// Handles boolean member expressions like p.IsActive or !p.IsActive.
    /// For Nullable&lt;T&gt;.HasValue, converts to IS [NOT] NULL check.
    /// Otherwise converts to field = true/false comparison.
    /// </summary>
    protected virtual FilterExpression VisitBooleanMemberExpression(MemberExpression member, bool negated)
    {
        // Nullable<T>.HasValue → IS [NOT] NULL
        if (member.Member.Name == "HasValue" && IsNullableAccessor(member))
        {
            var property = ExtractProperty(member);
            // HasValue (negated=false) → IS NOT NULL; !HasValue (negated=true) → IS NULL
            return new NullCheckExpression(property, negated);
        }
        
        var prop = ExtractProperty(member);
        return new ComparisonExpression(prop, ComparisonOperator.Equal, !negated);
    }

    /// <summary>
    /// Binary expression visitor (AND, OR, comparisons, arithmetic).
    /// </summary>
    protected virtual FilterExpression VisitBinaryExpression(BinaryExpression binary)
    {
        switch (binary.NodeType)
        {
            case ExpressionType.AndAlso:
                return new LogicalExpression(
                    LogicalOperator.And,
                    new[] { VisitExpression(binary.Left), VisitExpression(binary.Right) }
                );

            case ExpressionType.OrElse:
                return new LogicalExpression(
                    LogicalOperator.Or,
                    new[] { VisitExpression(binary.Left), VisitExpression(binary.Right) }
                );

            case ExpressionType.Equal:
                return VisitComparisonExpression(binary, ComparisonOperator.Equal);

            case ExpressionType.NotEqual:
                return VisitComparisonExpression(binary, ComparisonOperator.NotEqual);

            case ExpressionType.GreaterThan:
                return VisitComparisonExpression(binary, ComparisonOperator.GreaterThan);

            case ExpressionType.GreaterThanOrEqual:
                return VisitComparisonExpression(binary, ComparisonOperator.GreaterThanOrEqual);

            case ExpressionType.LessThan:
                return VisitComparisonExpression(binary, ComparisonOperator.LessThan);

            case ExpressionType.LessThanOrEqual:
                return VisitComparisonExpression(binary, ComparisonOperator.LessThanOrEqual);

            case ExpressionType.Add:
            case ExpressionType.Subtract:
            case ExpressionType.Multiply:
            case ExpressionType.Divide:
            case ExpressionType.Modulo:
                return VisitArithmeticComparisonExpression(binary);

            default:
                throw new NotSupportedException($"Binary operator {binary.NodeType} is not supported");
        }
    }
    
    /// <summary>
    /// Arithmetic expression as top-level is not allowed.
    /// </summary>
    protected virtual FilterExpression VisitArithmeticComparisonExpression(BinaryExpression binary)
    {
        throw new NotSupportedException(
            $"Arithmetic expression ({binary.NodeType}) must be part of a comparison. " +
            "Use: Where(p => p.Price * 2 > 100), not: Where(p => p.Price * 2)");
    }

    /// <summary>
    /// Comparison expression visitor.
    /// </summary>
    protected virtual FilterExpression VisitComparisonExpression(BinaryExpression binary, ComparisonOperator op)
    {
        var leftValueExpr = TryExtractValueExpression(binary.Left);
        var rightValueExpr = TryExtractValueExpression(binary.Right);
        
        if (leftValueExpr != null || rightValueExpr != null)
        {
            return CreateExtendedComparison(binary, op, leftValueExpr, rightValueExpr);
        }
        
        var (property, value) = ExtractPropertyAndValue(binary);

        if (value is IRedbListItem listItem)
        {
            // Bare ListItem field path (e.g. "Status") defaults to .Value
            // (string) on the SQL side. Direct comparison against a
            // RedbListItem instance always means "match by id", so route
            // through the .Id (bigint) accessor.
            if (!property.Name.Contains('.'))
            {
                property = property with { Name = property.Name + ".Id", Type = typeof(long) };
            }
            return new ComparisonExpression(property, op, listItem.Id);
        }

        if (value != null && value.GetType().IsEnum)
        {
            value = value.ToString();
        }

        if (value == null)
        {
            return new NullCheckExpression(property, op == ComparisonOperator.Equal);
        }

        return new ComparisonExpression(property, op, value);
    }
    
    /// <summary>
    /// Extended comparison with ValueExpression (arithmetic/functions).
    /// </summary>
    protected virtual FilterExpression CreateExtendedComparison(
        BinaryExpression binary, 
        ComparisonOperator op,
        ValueExpression? leftValueExpr,
        ValueExpression? rightValueExpr)
    {
        if (leftValueExpr != null && rightValueExpr == null)
        {
            var value = EvaluateExpression(binary.Right);
            rightValueExpr = new ConstantValueExpression(value, binary.Right.Type);
        }
        else if (rightValueExpr != null && leftValueExpr == null)
        {
            var value = EvaluateExpression(binary.Left);
            leftValueExpr = new ConstantValueExpression(value, binary.Left.Type);
        }
        
        var dummyProperty = new QueryExpressions.PropertyInfo(
            "__computed", typeof(object), _isBaseFieldContext);
        
        return new ComparisonExpression(dummyProperty, op, null)
        {
            LeftExpression = leftValueExpr,
            RightExpression = rightValueExpr
        };
    }
    
    /// <summary>
    /// Try to extract ValueExpression from Expression (arithmetic, functions).
    /// Returns null for simple property/constant.
    /// </summary>
    protected virtual ValueExpression? TryExtractValueExpression(Expression expression)
    {
        return expression switch
        {
            BinaryExpression binary when IsArithmeticExpression(binary) =>
                CreateArithmeticExpression(binary),

            // x ?? y — Coalesce (binary, right-associative; flattened to n-ary).
            BinaryExpression binary when binary.NodeType == ExpressionType.Coalesce =>
                CreateCoalesceExpression(binary),

            // C# ternary: cond ? a : b
            System.Linq.Expressions.ConditionalExpression cond =>
                CreateConditionalValueExpression(cond),
            
            MethodCallExpression method when IsCustomFunctionCall(method) =>
                CreateCustomFunctionExpression(method),
            
            MethodCallExpression method when IsProFunctionCall(method) =>
                CreateFunctionCallExpression(method),
            
            MemberExpression member when IsDateTimePropertyAccess(member) =>
                CreateDateTimeFunctionExpression(member),
            
            MemberExpression member when member.Member.Name == "Length" && 
                                         member.Expression is MethodCallExpression innerMethod &&
                                         IsProFunctionCall(innerMethod) =>
                new FunctionCallExpression(PropertyFunction.Length, CreateFunctionCallExpression(innerMethod)),

            // (any-ValueExpression).Length — e.g. `(s ?? "n/a").Length`. Falls through
            // for simple property paths (which the legacy emitter handles via
            // PropertyFunction.Length on the property column directly).
            MemberExpression member when member.Member.Name == "Length" &&
                                         member.Expression != null &&
                                         member.Expression.Type == typeof(string) &&
                                         member.Expression is not MemberExpression &&
                                         member.Expression is not MethodCallExpression &&
                                         TryExtractValueExpression(member.Expression) is { } innerValue =>
                new FunctionCallExpression(PropertyFunction.Length, innerValue),

            // arr.Length (C# compiles array .Length as ExpressionType.ArrayLength UnaryExpression,
            // not a regular MemberExpression). ArrayLength is array-only (IL ldlen), so translate
            // to PropertyFunction.Count: Pro path emits array_length(col,1), PVT path uses .$count.
            UnaryExpression unaryArr when unaryArr.NodeType == ExpressionType.ArrayLength &&
                                          unaryArr.Operand is MemberExpression arrMember =>
                new FunctionCallExpression(
                    PropertyFunction.Count,
                    new PropertyValueExpression(ExtractPropertyFromMember(arrMember))),
            
            _ => null
        };
    }

    /// <summary>
    /// Unary expression visitor (NOT).
    /// </summary>
    protected virtual FilterExpression VisitUnaryExpression(UnaryExpression unary)
    {
        switch (unary.NodeType)
        {
            case ExpressionType.Not:
                // Special case for !p.IsActive - convert to field = false
                if (unary.Operand is MemberExpression member && member.Type == typeof(bool))
                {
                    return VisitBooleanMemberExpression(member, negated: true);
                }
                var operand = VisitExpression(unary.Operand);
                return new LogicalExpression(LogicalOperator.Not, new[] { operand });

            default:
                throw new NotSupportedException($"Unary operator {unary.NodeType} is not supported");
        }
    }

    /// <summary>
    /// Method call expression visitor.
    /// </summary>
    protected virtual FilterExpression VisitMethodCallExpression(MethodCallExpression method)
    {
        var methodName = method.Method.Name;
        var declaringType = method.Method.DeclaringType;

        if (declaringType == typeof(string))
        {
            return methodName switch
            {
                "Contains" => VisitStringMethodWithComparison(method, ComparisonOperator.Contains, ComparisonOperator.ContainsIgnoreCase),
                "StartsWith" => VisitStringMethodWithComparison(method, ComparisonOperator.StartsWith, ComparisonOperator.StartsWithIgnoreCase),
                "EndsWith" => VisitStringMethodWithComparison(method, ComparisonOperator.EndsWith, ComparisonOperator.EndsWithIgnoreCase),
                _ => throw new NotSupportedException($"String method {methodName} is not supported")
            };
        }

        // Regex.IsMatch(input, pattern[, options]) -> ComparisonExpression with
        // LeftExpression=input, RightExpression=pattern, RegexMatch / RegexMatchIgnoreCase.
        if (declaringType == typeof(System.Text.RegularExpressions.Regex)
            && methodName == "IsMatch")
        {
            return VisitRegexIsMatch(method);
        }

        if (declaringType == typeof(Enumerable))
        {
            return methodName switch
            {
                "Contains" => VisitEnumerableContains(method),
                "Any" => VisitEnumerableAny(method),
                _ => throw new NotSupportedException($"Enumerable method {methodName} is not supported")
            };
        }

        // C# 13 / .NET 9 resolves `array.Contains(x)` to the ReadOnlySpan overload
        // (System.MemoryExtensions.Contains) instead of Enumerable.Contains — same
        // 2-arg (source, value) shape, same IN translation. The source arg is a
        // ReadOnlySpan Convert over the underlying array/list; a ref struct cannot
        // be compiled by EvaluateExpression, so unwrap the conversion back to the
        // collection first.
        if (declaringType == typeof(MemoryExtensions)
            && methodName == "Contains"
            && method.Arguments.Count == 2)
        {
            // The receiver is a ReadOnlySpan<T> built from the underlying array —
            // the compiler emits the array→span conversion as a Call (op_Implicit /
            // AsSpan) or, less commonly, a Convert. A ref struct can't be compiled /
            // invoked by EvaluateExpression, so peel it back to the source collection.
            var source = method.Arguments[0] switch
            {
                UnaryExpression { NodeType: ExpressionType.Convert } u => u.Operand,
                MethodCallExpression { Arguments.Count: >= 1 } c => c.Arguments[0],
                var other => other
            };
            return VisitContainsCore(source, method.Arguments[1]);
        }

        if (methodName == "Contains" && method.Object != null)
        {
            var objectType = method.Object.Type;
            
            if (objectType.IsArray)
            {
                return VisitCollectionContains(method);
            }
            
            if (objectType.IsGenericType)
            {
                var genericDef = objectType.GetGenericTypeDefinition();
                if (genericDef == typeof(List<>) || 
                    genericDef == typeof(IList<>) ||
                    genericDef == typeof(ICollection<>) ||
                    genericDef == typeof(IEnumerable<>) ||
                    objectType.GetInterfaces().Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>)))
                {
                    return VisitCollectionContains(method);
                }
            }
        }
        
        if (methodName == "ContainsKey" && method.Object != null && IsDictionaryType(method.Object.Type))
        {
            return VisitDictionaryContainsKey(method);
        }

        throw new NotSupportedException($"Method {declaringType?.Name}.{methodName} is not supported");
    }

    /// <summary>
    /// Constant boolean expression visitor.
    /// </summary>
    protected virtual FilterExpression VisitConstantBooleanExpression(ConstantExpression constant)
    {
        var value = (bool)constant.Value!;
        var dummyProperty = new QueryExpressions.PropertyInfo("__constant", typeof(bool), false);
        return new ComparisonExpression(dummyProperty, ComparisonOperator.Equal, value);
    }

    #endregion

    #region String Methods

    /// <summary>
    /// String methods with StringComparison support.
    /// </summary>
    protected virtual FilterExpression VisitStringMethodWithComparison(
        MethodCallExpression method, 
        ComparisonOperator caseSensitiveOp, 
        ComparisonOperator ignoreCaseOp)
    {
        if (method.Object == null)
            throw new ArgumentException("String method must have an object instance");

        var op = caseSensitiveOp;
        if (method.Arguments.Count == 2)
        {
            var stringComparison = EvaluateStringComparison(method.Arguments[1]);
            op = IsIgnoreCaseComparison(stringComparison) ? ignoreCaseOp : caseSensitiveOp;
        }
        else if (method.Arguments.Count != 1)
        {
            throw new NotSupportedException($"String method with {method.Arguments.Count} arguments is not supported");
        }

        var value = EvaluateExpression(method.Arguments[0]);

        // LHS may be a computed value (string concat, ToLower over concat, Substring, etc.).
        // ExtractProperty only handles property paths and bare string-function chains;
        // anything richer (e.g. `(a + " " + b).ToLower()`) must go through the computed
        // $expr / arithmetic ComparisonExpression route. Try the property form first to
        // preserve the simple-field SQL fast path; fall back to a ValueExpression if it
        // cannot be expressed as a property reference.
        try
        {
            var property = ExtractProperty(method.Object);
            return new ComparisonExpression(property, op, value);
        }
        catch (ArgumentException)
        {
            var leftValueExpr = TryExtractValueExpression(method.Object)
                ?? throw new NotSupportedException(
                    $"Cannot translate LHS of string method '{method.Method.Name}': {method.Object}");

            var dummyProperty = new QueryExpressions.PropertyInfo(
                "__computed", typeof(bool), _isBaseFieldContext);

            return new ComparisonExpression(dummyProperty, op, null)
            {
                LeftExpression = leftValueExpr,
                RightExpression = new ConstantValueExpression(value, method.Arguments[0].Type)
            };
        }
    }

    /// <summary>
    /// Regex.IsMatch(input, pattern[, options]) — builds computed ComparisonExpression
    /// routed through PVT $expr / PG '~' '~*'.
    /// </summary>
    protected virtual FilterExpression VisitRegexIsMatch(MethodCallExpression method)
    {
        if (method.Arguments.Count < 2)
            throw new NotSupportedException("Regex.IsMatch requires (input, pattern[, options]).");

        var inputExpr   = ExtractValueExpression(method.Arguments[0]);
        var patternExpr = ExtractValueExpression(method.Arguments[1]);

        var op = ComparisonOperator.RegexMatch;
        if (method.Arguments.Count >= 3)
        {
            var optsValue = EvaluateExpression(method.Arguments[2]);
            if (optsValue is System.Text.RegularExpressions.RegexOptions opts
                && (opts & System.Text.RegularExpressions.RegexOptions.IgnoreCase) != 0)
            {
                op = ComparisonOperator.RegexMatchIgnoreCase;
            }
        }

        var dummyProperty = new QueryExpressions.PropertyInfo(
            "__computed", typeof(bool), _isBaseFieldContext);

        return new ComparisonExpression(dummyProperty, op, null)
        {
            LeftExpression = inputExpr,
            RightExpression = patternExpr
        };
    }

    protected StringComparison EvaluateStringComparison(Expression comparisonExpression)
    {
        var value = EvaluateExpression(comparisonExpression);
        return value is StringComparison comparison ? comparison : StringComparison.Ordinal;
    }

    protected bool IsIgnoreCaseComparison(StringComparison comparison)
    {
        return comparison is 
            StringComparison.CurrentCultureIgnoreCase or 
            StringComparison.InvariantCultureIgnoreCase or 
            StringComparison.OrdinalIgnoreCase;
    }

    #endregion

    #region Collection Methods

    /// <summary>
    /// Enumerable.Contains visitor.
    /// </summary>
    protected virtual FilterExpression VisitEnumerableContains(MethodCallExpression method)
    {
        if (method.Arguments.Count != 2)
            throw new ArgumentException("Contains method must have exactly 2 arguments");

        return VisitContainsCore(method.Arguments[0], method.Arguments[1]);
    }

    /// <summary>
    /// Core of a 2-arg <c>Contains(source, value)</c> translation — shared by
    /// <see cref="VisitEnumerableContains"/> (<c>Enumerable.Contains</c>) and the
    /// <c>MemoryExtensions.Contains</c> span overload that C# 13 / .NET 9 binds
    /// <c>array.Contains(x)</c> to. Produces an IN-clause (value is a row property,
    /// source is a constant collection) or an array-contains (source is the row's
    /// array property).
    /// </summary>
    protected FilterExpression VisitContainsCore(Expression sourceExpression, Expression valueExpression)
    {
        if (IsPropertyAccess(valueExpression))
        {
            var property = ExtractProperty(valueExpression);
            var values = EvaluateExpression(sourceExpression);

            if (values is System.Collections.IEnumerable enumerable)
            {
                var valuesList = enumerable.Cast<object>()
                    .Select(v => v is IRedbListItem li ? (object)li.Id : v)
                    .ToList();
                if (valuesList.Count > 0
                    && enumerable.Cast<object>().Any(v => v is IRedbListItem)
                    && !property.Name.Contains('.'))
                {
                    property = property with { Name = property.Name + ".Id", Type = typeof(long) };
                }
                return new InExpression(property, valuesList);
            }
        }
        else if (IsPropertyAccess(sourceExpression))
        {
            var arrayProperty = ExtractProperty(sourceExpression);
            var value = EvaluateExpression(valueExpression);
            if (value is IRedbListItem listItem) value = listItem.Id;
            return new ComparisonExpression(arrayProperty, ComparisonOperator.ArrayContains, value);
        }

        throw new NotSupportedException("Unsupported Contains expression structure");
    }

    /// <summary>
    /// Enumerable.Any visitor.
    /// </summary>
    protected virtual FilterExpression VisitEnumerableAny(MethodCallExpression method)
    {
        // Enumerable.Any(source) — 1-arg form. Translate to: cardinality(source) > 0
        // i.e. ComparisonExpression with PropertyFunction.Length > 0, which the
        // facet builder serializes as {"$expr":{"$gt":[{"$length":{"$field":"X"}},{"$const":0}]}}.
        if (method.Arguments.Count == 1)
        {
            var only = method.Arguments[0];
            if (!IsPropertyAccess(only))
                throw new NotSupportedException("Any() must be called on a property collection");

            var prop = ExtractProperty(only);
            var lengthProp = prop with { Function = PropertyFunction.Length };
            return new ComparisonExpression(lengthProp, ComparisonOperator.GreaterThan, 0);
        }

        if (method.Arguments.Count != 2)
            throw new ArgumentException("Any method with predicate must have exactly 2 arguments");

        var sourceExpression = method.Arguments[0];
        var predicateExpression = method.Arguments[1];

        if (!IsPropertyAccess(sourceExpression))
            throw new NotSupportedException("Any() must be called on a property collection");

        var collectionProperty = ExtractProperty(sourceExpression);

        if (predicateExpression is LambdaExpression lambda)
        {
            if (lambda.Body is BinaryExpression binaryBody)
            {
                var left = binaryBody.Left;
                var right = binaryBody.Right;

                QueryExpressions.PropertyInfo? itemProperty = null;
                object? value = null;

                if (left is MemberExpression leftMember && leftMember.Expression == lambda.Parameters[0])
                {
                    itemProperty = new QueryExpressions.PropertyInfo(
                        leftMember.Member.Name, leftMember.Type, _isBaseFieldContext);
                    value = EvaluateExpression(right);
                }
                else if (right is MemberExpression rightMember && rightMember.Expression == lambda.Parameters[0])
                {
                    itemProperty = new QueryExpressions.PropertyInfo(
                        rightMember.Member.Name, rightMember.Type, _isBaseFieldContext);
                    value = EvaluateExpression(left);
                }
                else
                {
                    throw new NotSupportedException("Any() predicate must compare an item property with a value");
                }

                if (value is IRedbListItem listItem)
                {
                    value = listItem.Id;
                }

                var arrayFieldPath = $"{collectionProperty.Name}[].{itemProperty.Name}";
                var arrayProperty = new QueryExpressions.PropertyInfo(
                    arrayFieldPath, itemProperty.Type, _isBaseFieldContext);

                // For nested fields (like Roles[].Value), use Equal instead of ArrayContains
                // The path already contains [] so SQL will handle array iteration
                return new ComparisonExpression(arrayProperty, ComparisonOperator.Equal, value);
            }
            else
            {
                throw new NotSupportedException("Any() predicate must be a comparison expression");
            }
        }

        throw new NotSupportedException("Any() must have a lambda predicate");
    }

    /// <summary>
    /// Collection.Contains visitor.
    /// </summary>
    protected virtual FilterExpression VisitCollectionContains(MethodCallExpression method)
    {
        if (method.Arguments.Count != 1)
            throw new ArgumentException("Collection Contains method must have exactly 1 argument");

        var collectionExpression = method.Object!;
        var valueExpression = method.Arguments[0];

        if (IsPropertyAccess(valueExpression))
        {
            var property = ExtractProperty(valueExpression);
            var values = EvaluateExpression(collectionExpression);

            if (values is System.Collections.IEnumerable enumerable)
            {
                var valuesList = enumerable.Cast<object>()
                    .Select(v => v is IRedbListItem li ? (object)li.Id : v)
                    .ToList();
                if (valuesList.Count > 0
                    && enumerable.Cast<object>().Any(v => v is IRedbListItem)
                    && !property.Name.Contains('.'))
                {
                    property = property with { Name = property.Name + ".Id", Type = typeof(long) };
                }
                return new InExpression(property, valuesList);
            }
            else
            {
                throw new ArgumentException("Collection expression must evaluate to IEnumerable");
            }
        }
        else if (IsPropertyAccess(collectionExpression))
        {
            var arrayProperty = ExtractProperty(collectionExpression);
            var value = EvaluateExpression(valueExpression);
            if (value is IRedbListItem listItem) value = listItem.Id;
            return new ComparisonExpression(arrayProperty, ComparisonOperator.ArrayContains, value);
        }
        else
        {
            throw new NotSupportedException("Unsupported Contains expression structure");
        }
    }

    #endregion

    #region Dictionary Methods

    /// <summary>
    /// Dictionary.ContainsKey visitor.
    /// </summary>
    protected virtual FilterExpression VisitDictionaryContainsKey(MethodCallExpression method)
    {
        string dictionaryPath;
        if (method.Object is MemberExpression dictMember)
        {
            var (path, _) = BuildPropertyPath(dictMember);
            dictionaryPath = path;
        }
        else
        {
            throw new ArgumentException("ContainsKey must be called on a dictionary property");
        }
        
        var keyValue = EvaluateExpression(method.Arguments[0]);
        var keyType = method.Arguments[0].Type;
        var serializedKey = RedbKeySerializer.SerializeObject(keyValue!, keyType);
        var propertyPath = $"{dictionaryPath}.ContainsKey";
        
        var property = new QueryExpressions.PropertyInfo(propertyPath, typeof(string), _isBaseFieldContext);
        return new ComparisonExpression(property, ComparisonOperator.Equal, serializedKey);
    }

    protected bool IsDictionaryType(Type? type)
    {
        if (type == null) return false;
        if (!type.IsGenericType) return false;
        var genericDef = type.GetGenericTypeDefinition();
        return genericDef == typeof(Dictionary<,>) || genericDef == typeof(IDictionary<,>);
    }

    #endregion

    #region Arithmetic & Functions

    protected bool IsArithmeticExpression(BinaryExpression binary)
    {
        return binary.NodeType is 
            ExpressionType.Add or 
            ExpressionType.Subtract or 
            ExpressionType.Multiply or 
            ExpressionType.Divide or 
            ExpressionType.Modulo;
    }
    
    protected ValueExpression CreateArithmeticExpression(BinaryExpression binary)
    {
        var op = binary.NodeType switch
        {
            ExpressionType.Add => ArithmeticOperator.Add,
            ExpressionType.Subtract => ArithmeticOperator.Subtract,
            ExpressionType.Multiply => ArithmeticOperator.Multiply,
            ExpressionType.Divide => ArithmeticOperator.Divide,
            ExpressionType.Modulo => ArithmeticOperator.Modulo,
            _ => throw new NotSupportedException($"Arithmetic operator {binary.NodeType} is not supported")
        };
        
        var left = ExtractValueExpression(binary.Left);
        var right = ExtractValueExpression(binary.Right);
        return new ArithmeticExpression(left, op, right);
    }

    /// <summary>
    /// Builds a <see cref="CoalesceExpression"/> from a chain of <c>??</c> operators.
    /// C# parses <c>a ?? b ?? c</c> right-associatively as <c>a ?? (b ?? c)</c>; this helper
    /// flattens that into a single n-ary node <c>[a, b, c]</c> so the downstream SQL emitter
    /// produces <c>COALESCE(a, b, c)</c> instead of <c>COALESCE(a, COALESCE(b, c))</c>.
    /// </summary>
    protected ValueExpression CreateCoalesceExpression(BinaryExpression binary)
    {
        var args = new List<ValueExpression>();
        void Flatten(Expression node)
        {
            if (node is BinaryExpression be && be.NodeType == ExpressionType.Coalesce)
            {
                Flatten(be.Left);
                Flatten(be.Right);
            }
            else
            {
                args.Add(ExtractValueExpression(node));
            }
        }
        Flatten(binary);
        return new CoalesceExpression(args);
    }

    /// <summary>
    /// Builds a <see cref="ConditionalValueExpression"/> from a C# ternary <c>?:</c>.
    /// Test is parsed as a full boolean <see cref="FilterExpression"/> via <see cref="VisitExpression"/>;
    /// IfTrue / IfFalse are parsed as value expressions via <see cref="ExtractValueExpression"/>.
    /// </summary>
    protected ValueExpression CreateConditionalValueExpression(System.Linq.Expressions.ConditionalExpression cond)
    {
        var test = VisitExpression(cond.Test);
        var ifTrue = ExtractValueExpression(cond.IfTrue);
        var ifFalse = ExtractValueExpression(cond.IfFalse);
        return new ConditionalValueExpression(test, ifTrue, ifFalse);
    }
    
    /// <summary>
    /// Recursively extract ValueExpression from any Expression.
    /// </summary>
    protected ValueExpression ExtractValueExpression(Expression expression)
    {
        return expression switch
        {
            BinaryExpression binary when IsArithmeticExpression(binary) =>
                CreateArithmeticExpression(binary),

            // x ?? y — Coalesce operator (binary, right-associative chained into n-ary)
            BinaryExpression binary when binary.NodeType == ExpressionType.Coalesce =>
                CreateCoalesceExpression(binary),

            // C# ternary: cond ? a : b
            System.Linq.Expressions.ConditionalExpression cond =>
                CreateConditionalValueExpression(cond),
            
            UnaryExpression unary when unary.NodeType == ExpressionType.Convert =>
                ExtractValueExpression(unary.Operand),
            
            MethodCallExpression method when IsCustomFunctionCall(method) =>
                CreateCustomFunctionExpression(method),
            
            MethodCallExpression method when IsProFunctionCall(method) =>
                CreateFunctionCallExpression(method),
            
            MemberExpression member when IsDateTimePropertyAccess(member) =>
                CreateDateTimeFunctionExpression(member),
            
            MemberExpression member when member.Member.Name == "Length" && 
                                         member.Expression is MethodCallExpression innerMethod &&
                                         IsProFunctionCall(innerMethod) =>
                new FunctionCallExpression(PropertyFunction.Length, CreateFunctionCallExpression(innerMethod)),

            // arr.Length inside an arithmetic/function chain (ArrayLength is array-only).
            UnaryExpression unaryArr when unaryArr.NodeType == ExpressionType.ArrayLength &&
                                          unaryArr.Operand is MemberExpression arrMember =>
                new FunctionCallExpression(
                    PropertyFunction.Count,
                    new PropertyValueExpression(ExtractPropertyFromMember(arrMember))),
            
            MemberExpression member when (member.Member is System.Reflection.PropertyInfo || 
                                          member.Member is System.Reflection.FieldInfo) &&
                                         ReferencesLambdaParameter(member) =>
                new PropertyValueExpression(ExtractPropertyFromMember(member)),
            
            ConstantExpression constant =>
                new ConstantValueExpression(constant.Value, constant.Type),
            
            MemberExpression member when !ReferencesLambdaParameter(member) =>
                new ConstantValueExpression(EvaluateExpression(member), member.Type),
            
            _ => throw new NotSupportedException($"Cannot extract ValueExpression from {expression.NodeType}: {expression}")
        };
    }

    protected bool IsProFunctionCall(MethodCallExpression method)
    {
        var name = method.Method.Name;
        var declaringType = method.Method.DeclaringType;
        
        if (declaringType == typeof(string))
        {
            return name is "ToLower" or "ToLowerInvariant" or 
                          "ToUpper" or "ToUpperInvariant" or 
                          "Trim" or "TrimStart" or "TrimEnd" or
                          "Substring" or "Replace" or "IndexOf" or
                          "PadLeft" or "PadRight";
        }
        
        if (declaringType == typeof(Math))
        {
            return name is "Abs" or "Round" or "Floor" or "Ceiling" or "Truncate"
                        or "Sqrt" or "Sign" or "Exp" or "Log" or "Log10" or "Pow";
        }

        if (declaringType == typeof(DateTime) || declaringType == typeof(DateTimeOffset))
        {
            // Instance arithmetic methods AddDays/AddYears/AddMonths/AddHours/AddMinutes/AddSeconds.
            return name is "AddDays" or "AddYears" or "AddMonths"
                        or "AddHours" or "AddMinutes" or "AddSeconds";
        }

        if (declaringType == typeof(System.Text.RegularExpressions.Regex))
        {
            // Regex.Replace(input, pattern, replacement) — static; returns string.
            return name == "Replace";
        }
        
        return false;
    }
    
    protected bool IsCustomFunctionCall(MethodCallExpression method)
    {
        return method.Method.DeclaringType?.FullName == "redb.Core.Query.Sql" &&
               method.Method.Name == "Function";
    }
    
    protected ValueExpression CreateCustomFunctionExpression(MethodCallExpression method)
    {
        var funcNameExpr = method.Arguments[0];
        var funcName = (string)EvaluateExpression(funcNameExpr)!;
        
        var argsExpr = method.Arguments[1];
        var arguments = new List<ValueExpression>();
        
        if (argsExpr is NewArrayExpression newArrayExpr)
        {
            foreach (var argExpr in newArrayExpr.Expressions)
            {
                arguments.Add(ExtractValueExpression(argExpr));
            }
        }
        else
        {
            throw new NotSupportedException(
                $"Sql.Function arguments must be inline (not a variable). Got: {argsExpr.NodeType} ({argsExpr.GetType().Name})");
        }
        
        return new CustomFunctionExpression(funcName, arguments);
    }
    
    protected ValueExpression CreateFunctionCallExpression(MethodCallExpression method)
    {
        var name = method.Method.Name;
        var declaringType = method.Method.DeclaringType;
        
        PropertyFunction func;
        ValueExpression argument;
        
        if (declaringType == typeof(string))
        {
            // Multi-arg string functions (Substring/Replace/IndexOf/PadLeft/PadRight)
            // use MultiArgFunctionCallExpression and handle index translation here.
            if (name is "Substring" or "Replace" or "IndexOf" or "PadLeft" or "PadRight")
            {
                return CreateMultiArgFunctionCallExpression(method);
            }

            func = name switch
            {
                "ToLower" or "ToLowerInvariant" => PropertyFunction.ToLower,
                "ToUpper" or "ToUpperInvariant" => PropertyFunction.ToUpper,
                "Trim" => PropertyFunction.Trim,
                "TrimStart" => PropertyFunction.TrimStart,
                "TrimEnd" => PropertyFunction.TrimEnd,
                _ => throw new NotSupportedException($"String method {name} is not supported")
            };
            argument = ExtractValueExpression(method.Object!);
        }
        else if (declaringType == typeof(Math))
        {
            // Multi-arg Math overloads dispatch to MultiArgFunctionCallExpression
            // (Math is static, so method.Object is null — these are handled
            // separately below in CreateMultiArgFunctionCallExpression).
            if (name == "Pow" || name == "Log10"
             || (name == "Log"   && method.Arguments.Count == 2)
             || (name == "Round" && method.Arguments.Count == 2))
            {
                return CreateMultiArgFunctionCallExpression(method);
            }

            func = name switch
            {
                "Abs"      => PropertyFunction.Abs,
                "Round"    => PropertyFunction.Round,
                "Floor"    => PropertyFunction.Floor,
                "Ceiling"  => PropertyFunction.Ceiling,
                "Truncate" => PropertyFunction.Floor,
                "Sqrt"     => PropertyFunction.Sqrt,
                "Sign"     => PropertyFunction.Sign,
                "Exp"      => PropertyFunction.Exp,
                "Log"      => PropertyFunction.Log,    // 1-arg: natural log
                _ => throw new NotSupportedException($"Math method {name} is not supported")
            };
            argument = ExtractValueExpression(method.Arguments[0]);
        }
        else if (declaringType == typeof(DateTime) || declaringType == typeof(DateTimeOffset))
        {
            // All DateTime AddX methods are 2-arg in our AST [date, n] -> dispatch to MultiArg.
            return CreateMultiArgFunctionCallExpression(method);
        }
        else if (declaringType == typeof(System.Text.RegularExpressions.Regex))
        {
            // Regex.Replace -> MultiArg with 3 args (static method, no receiver).
            return CreateMultiArgFunctionCallExpression(method);
        }
        else
        {
            throw new NotSupportedException($"Method {declaringType?.Name}.{name} is not supported");
        }
        
        return new FunctionCallExpression(func, argument);
    }

    /// <summary>
    /// Builds a <see cref="MultiArgFunctionCallExpression"/> for the multi-arg string
    /// functions <c>Substring/Replace/IndexOf/PadLeft/PadRight</c>. Performs index
    /// translation so the same AST works on both tiers (Free PVT and Pro):
    /// <list type="bullet">
    /// <item>C# <c>string.Substring(start[, length])</c> is 0-based; SQL <c>SUBSTRING</c>
    /// is 1-based -> the <c>start</c> argument is wrapped in <c>start + 1</c>.</item>
    /// <item>C# <c>string.IndexOf(needle)</c> returns -1 for not-found; SQL <c>POSITION</c>
    /// returns 0 -> the whole call is wrapped in <c>(POSITION(...) - 1)</c>.</item>
    /// </list>
    /// </summary>
    protected ValueExpression CreateMultiArgFunctionCallExpression(MethodCallExpression method)
    {
        var name = method.Method.Name;
        var declaringType = method.Method.DeclaringType;

        // DateTime.AddX(n) — instance call: receiver is the date, single arg is the delta.
        if (declaringType == typeof(DateTime) || declaringType == typeof(DateTimeOffset))
        {
            var fn = name switch
            {
                "AddDays"    => PropertyFunction.AddDays,
                "AddYears"   => PropertyFunction.AddYears,
                "AddMonths"  => PropertyFunction.AddMonths,
                "AddHours"   => PropertyFunction.AddHours,
                "AddMinutes" => PropertyFunction.AddMinutes,
                "AddSeconds" => PropertyFunction.AddSeconds,
                _ => throw new NotSupportedException($"DateTime method {name} is not supported.")
            };
            return new MultiArgFunctionCallExpression(fn, new List<ValueExpression>
            {
                ExtractValueExpression(method.Object!),
                ExtractValueExpression(method.Arguments[0]),
            });
        }

        // Regex.Replace(input, pattern, replacement) — static; ignore optional flags overloads.
        if (declaringType == typeof(System.Text.RegularExpressions.Regex) && name == "Replace")
        {
            if (method.Arguments.Count < 3)
                throw new NotSupportedException("Regex.Replace requires (input, pattern, replacement).");
            return new MultiArgFunctionCallExpression(PropertyFunction.RegexReplace, new List<ValueExpression>
            {
                ExtractValueExpression(method.Arguments[0]), // input
                ExtractValueExpression(method.Arguments[1]), // pattern
                ExtractValueExpression(method.Arguments[2]), // replacement
            });
        }

        // Math.* are static — method.Object is null. Build args directly from method.Arguments.
        if (declaringType == typeof(Math))
        {
            switch (name)
            {
                case "Pow":
                    // Math.Pow(x, y) -> POWER(x, y)
                    return new MultiArgFunctionCallExpression(PropertyFunction.Pow, new List<ValueExpression>
                    {
                        ExtractValueExpression(method.Arguments[0]),
                        ExtractValueExpression(method.Arguments[1]),
                    });
                case "Log":
                    // C# Math.Log(value, base); PG LOG(base, value) — argument SWAP at parse time.
                    return new MultiArgFunctionCallExpression(PropertyFunction.LogBase, new List<ValueExpression>
                    {
                        ExtractValueExpression(method.Arguments[1]), // base
                        ExtractValueExpression(method.Arguments[0]), // value
                    });
                case "Log10":
                    // Math.Log10(x) -> LOG(10, x). Synthesize base=10 constant.
                    return new MultiArgFunctionCallExpression(PropertyFunction.LogBase, new List<ValueExpression>
                    {
                        new ConstantValueExpression(10.0, typeof(double)),
                        ExtractValueExpression(method.Arguments[0]),
                    });
                case "Round":
                    // Math.Round(value, digits) -> ROUND(value, digits). 1-arg form goes through the
                    // single-arg FunctionCallExpression path (PropertyFunction.Round).
                    return new MultiArgFunctionCallExpression(PropertyFunction.Round, new List<ValueExpression>
                    {
                        ExtractValueExpression(method.Arguments[0]),
                        ExtractValueExpression(method.Arguments[1]),
                    });
                default:
                    throw new NotSupportedException($"Multi-arg Math method {name} is not supported.");
            }
        }

        var receiver = ExtractValueExpression(method.Object!);
        var args = new List<ValueExpression> { receiver };

        switch (name)
        {
            case "Substring":
            {
                // s.Substring(start) | s.Substring(start, length)
                var start = ExtractValueExpression(method.Arguments[0]);
                args.Add(new ArithmeticExpression(start, ArithmeticOperator.Add, new ConstantValueExpression(1, typeof(int))));
                if (method.Arguments.Count == 2)
                    args.Add(ExtractValueExpression(method.Arguments[1]));
                return new MultiArgFunctionCallExpression(PropertyFunction.Substring, args);
            }
            case "Replace":
            {
                // s.Replace(oldValue, newValue) — string-only overload supported
                if (method.Arguments.Count != 2)
                    throw new NotSupportedException("Only string.Replace(oldValue, newValue) is supported.");
                args.Add(ExtractValueExpression(method.Arguments[0]));
                args.Add(ExtractValueExpression(method.Arguments[1]));
                return new MultiArgFunctionCallExpression(PropertyFunction.Replace, args);
            }
            case "IndexOf":
            {
                // s.IndexOf(needle) — wrap with `- 1` to map POSITION(1-based, 0=miss) to C# semantics.
                if (method.Arguments.Count != 1)
                    throw new NotSupportedException("Only string.IndexOf(value) (single argument) is supported.");
                args.Add(ExtractValueExpression(method.Arguments[0]));
                var inner = new MultiArgFunctionCallExpression(PropertyFunction.IndexOf, args);
                return new ArithmeticExpression(inner, ArithmeticOperator.Subtract, new ConstantValueExpression(1, typeof(int)));
            }
            case "PadLeft":
            case "PadRight":
            {
                // s.PadLeft(width) | s.PadLeft(width, padChar)
                args.Add(ExtractValueExpression(method.Arguments[0]));
                if (method.Arguments.Count == 2)
                {
                    // PadChar (char) — coerce to single-char string so PG LPAD/RPAD accept it.
                    var padArg = method.Arguments[1];
                    if (padArg is ConstantExpression ce && ce.Value is char ch)
                        args.Add(new ConstantValueExpression(ch.ToString(), typeof(string)));
                    else
                        args.Add(ExtractValueExpression(padArg));
                }
                var fn = name == "PadLeft" ? PropertyFunction.PadLeft : PropertyFunction.PadRight;
                return new MultiArgFunctionCallExpression(fn, args);
            }
            default:
                throw new NotSupportedException($"Multi-arg string method {name} is not supported.");
        }
    }

    protected bool IsDateTimePropertyAccess(MemberExpression member)
    {
        var exprType = member.Expression?.Type;
        
        if (exprType != typeof(DateTime) && 
            exprType != typeof(DateTime?) &&
            exprType != typeof(DateTimeOffset) &&
            exprType != typeof(DateTimeOffset?))
            return false;
        
        return member.Member.Name is "Year" or "Month" or "Day" or 
                                     "Hour" or "Minute" or "Second" or
                                     "DayOfWeek" or "DayOfYear";
    }
    
    protected ValueExpression CreateDateTimeFunctionExpression(MemberExpression member)
    {
        var func = member.Member.Name switch
        {
            "Year" => PropertyFunction.Year,
            "Month" => PropertyFunction.Month,
            "Day" => PropertyFunction.Day,
            "Hour" => PropertyFunction.Hour,
            "Minute" => PropertyFunction.Minute,
            "Second" => PropertyFunction.Second,
            "DayOfWeek" => PropertyFunction.DayOfWeek,
            "DayOfYear" => PropertyFunction.DayOfYear,
            _ => throw new NotSupportedException($"DateTime property {member.Member.Name} is not supported")
        };
        
        var argument = ExtractValueExpression(member.Expression!);
        return new FunctionCallExpression(func, argument);
    }

    #endregion

    #region Property Extraction

    /// <summary>
    /// Check if expression references lambda parameter.
    /// </summary>
    protected bool ReferencesLambdaParameter(Expression? expr)
    {
        while (expr != null)
        {
            if (expr is ParameterExpression)
                return true;
            if (expr is MemberExpression member)
                expr = member.Expression;
            else if (expr is UnaryExpression unary)
                expr = unary.Operand;
            else
                break;
        }
        return false;
    }

    /// <summary>
    /// Extract property and value from binary expression.
    /// </summary>
    protected (QueryExpressions.PropertyInfo Property, object? Value) ExtractPropertyAndValue(BinaryExpression binary)
    {
        if (IsPropertyAccess(binary.Left))
        {
            var property = ExtractProperty(binary.Left);
            var value = EvaluateExpression(binary.Right);
            return (property, value);
        }
        else if (IsPropertyAccess(binary.Right))
        {
            var property = ExtractProperty(binary.Right);
            var value = EvaluateExpression(binary.Left);
            return (property, value);
        }
        else
        {
            throw new NotSupportedException("At least one side of comparison must be a property access");
        }
    }

    /// <summary>
    /// Check if expression is property access.
    /// </summary>
    protected bool IsPropertyAccess(Expression expression)
    {
        if (expression is MemberExpression member && member.Member is System.Reflection.PropertyInfo)
            return true;
        
        if (expression is MethodCallExpression methodCall && 
            methodCall.Method.Name == "get_Item" &&
            IsDictionaryType(methodCall.Object?.Type))
            return true;
        
        return false;
    }

    /// <summary>
    /// Extract property info from expression.
    /// Supports: property access, Dictionary indexer, string functions (ToLower, ToUpper, Trim).
    /// </summary>
    protected QueryExpressions.PropertyInfo ExtractProperty(Expression expression)
    {
        if (expression is MethodCallExpression methodCall && 
            methodCall.Method.Name == "get_Item" &&
            IsDictionaryType(methodCall.Object?.Type))
        {
            return ExtractDictionaryIndexerProperty(methodCall);
        }
        
        // Support chain calls: property.ToLower(), property.ToUpper(), property.Trim()
        if (expression is MethodCallExpression stringMethodCall && 
            stringMethodCall.Method.DeclaringType == typeof(string) &&
            stringMethodCall.Object != null)
        {
            var func = stringMethodCall.Method.Name switch
            {
                "ToLower" or "ToLowerInvariant" => PropertyFunction.ToLower,
                "ToUpper" or "ToUpperInvariant" => PropertyFunction.ToUpper,
                "Trim" => PropertyFunction.Trim,
                "TrimStart" => PropertyFunction.TrimStart,
                "TrimEnd" => PropertyFunction.TrimEnd,
                _ => (PropertyFunction?)null
            };
            
            if (func.HasValue)
            {
                // Recursively extract property from inner expression
                var innerProperty = ExtractProperty(stringMethodCall.Object);
                return new QueryExpressions.PropertyInfo(innerProperty.Name, typeof(string), innerProperty.IsBaseField, func.Value);
            }
        }
        
        if (expression is MemberExpression member && member.Member is System.Reflection.PropertyInfo propInfo)
        {
            var (fullPath, function) = BuildPropertyPath(member);
            Type? sourceType = (function == PropertyFunction.Length || function == PropertyFunction.Count)
                ? member.Expression?.Type
                : null;
            return new QueryExpressions.PropertyInfo(fullPath, propInfo.PropertyType, _isBaseFieldContext, function, sourceType);
        }

        throw new ArgumentException($"Expression must be a property access, got {expression.GetType().Name}");
    }
    
    /// <summary>
    /// Extract property from Dictionary indexer.
    /// </summary>
    protected QueryExpressions.PropertyInfo ExtractDictionaryIndexerProperty(MethodCallExpression methodCall)
    {
        string dictionaryPath;
        if (methodCall.Object is MemberExpression dictMember)
        {
            var (path, _) = BuildPropertyPath(dictMember);
            dictionaryPath = path;
        }
        else
        {
            throw new ArgumentException("Dictionary indexer must be on a property");
        }
        
        var keyValue = EvaluateExpression(methodCall.Arguments[0]);
        var keyType = methodCall.Arguments[0].Type;
        var keyString = RedbKeySerializer.SerializeObject(keyValue!, keyType);
        var fullPath = $"{dictionaryPath}[{keyString}]";
        
        var dictType = methodCall.Object!.Type;
        var valueType = dictType.GetGenericArguments()[1];
        
        return new QueryExpressions.PropertyInfo(fullPath, valueType, _isBaseFieldContext);
    }
    
    /// <summary>
    /// Extract PropertyInfo from MemberExpression.
    /// </summary>
    protected QueryExpressions.PropertyInfo ExtractPropertyFromMember(MemberExpression member)
    {
        var memberType = member.Member switch
        {
            System.Reflection.PropertyInfo p => p.PropertyType,
            System.Reflection.FieldInfo f => f.FieldType,
            _ => typeof(object)
        };
        
        var (fullPath, function) = BuildPropertyPath(member);
        Type? sourceType = (function == PropertyFunction.Length || function == PropertyFunction.Count)
            ? member.Expression?.Type
            : null;
        return new QueryExpressions.PropertyInfo(fullPath, memberType, _isBaseFieldContext, function, sourceType);
    }

    /// <summary>
    /// Build property path from member expression chain.
    /// Handles nested properties, Length, Count, Dictionary indexers.
    /// </summary>
    protected (string Path, PropertyFunction? Function) BuildPropertyPath(MemberExpression memberExpression)
    {
        var pathParts = new List<string>();
        var current = memberExpression;
        PropertyFunction? function = null;

        while (current != null && current.Member is System.Reflection.PropertyInfo)
        {
            var memberName = current.Member.Name;
            
            // Skip .Value/.HasValue on Nullable<T> — these are C# accessors, not real properties
            if ((memberName == "Value" || memberName == "HasValue") && IsNullableAccessor(current))
            {
                if (current.Expression is MemberExpression parentMember2)
                {
                    current = parentMember2;
                    continue;
                }
                break;
            }
            
            if (memberName == "Length")
            {
                function = PropertyFunction.Length;
            }
            else if (memberName == "Count")
            {
                function = PropertyFunction.Count;
            }
            else
            {
                pathParts.Add(memberName);
            }
            
            if (current.Expression is MemberExpression parentMember)
            {
                current = parentMember;
            }
            else if (current.Expression is MethodCallExpression methodCall &&
                     methodCall.Method.Name == "get_Item" &&
                     IsDictionaryType(methodCall.Object?.Type))
            {
                string dictionaryPath;
                if (methodCall.Object is MemberExpression dictMember)
                {
                    var (dictPath, _) = BuildPropertyPath(dictMember);
                    dictionaryPath = dictPath;
                }
                else
                {
                    dictionaryPath = "Dict";
                }
                
                var keyValue = EvaluateExpression(methodCall.Arguments[0]);
                var keyString = keyValue?.ToString() ?? "";
                
                pathParts.Add($"{dictionaryPath}[{keyString}]");
                break;
            }
            else
            {
                break;
            }
        }

        pathParts.Reverse();
        var fullPath = string.Join(".", pathParts);
        
        // Check nesting depth (FREE version limits to 2 levels)
        CheckPathDepth(pathParts.Count, fullPath);
        
        return (fullPath, function);
    }

    /// <summary>
    /// Checks if member expression is a Nullable&lt;T&gt; accessor (.Value or .HasValue).
    /// </summary>
    private static bool IsNullableAccessor(MemberExpression member)
    {
        var declaringType = member.Member.DeclaringType;
        return declaringType != null 
            && declaringType.IsGenericType 
            && declaringType.GetGenericTypeDefinition() == typeof(Nullable<>);
    }

    /// <summary>
    /// Evaluate expression to get constant value.
    /// </summary>
    protected object? EvaluateExpression(Expression expression)
    {
        if (expression is ConstantExpression constant)
        {
            return constant.Value;
        }

        var lambda = Expression.Lambda(expression);
        var compiled = lambda.Compile();
        return compiled.DynamicInvoke();
    }

    #endregion
}

