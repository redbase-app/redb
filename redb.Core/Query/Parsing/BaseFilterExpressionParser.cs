using System.Linq.Expressions;
using redb.Core.Exceptions;
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
    /// DB-specific check for Pro-only features. Override in implementations.
    /// </summary>
    protected abstract void CheckProOnlyFeatures(Expression body, string context);

    /// <summary>
    /// Check property path nesting depth. FREE version limits to 2 levels.
    /// Override in Pro to allow deeper nesting.
    /// </summary>
    protected virtual void CheckPathDepth(int depth, string fullPath)
    {
        if (depth > 2)
        {
            throw new RedbProRequiredException(
                $"'{fullPath}' (depth {depth}), max allowed: 2 levels", 
                ProFeatureCategory.FilterNesting);
        }
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
    /// Converts to field = true/false comparison.
    /// </summary>
    protected virtual FilterExpression VisitBooleanMemberExpression(MemberExpression member, bool negated)
    {
        var property = ExtractProperty(member);
        return new ComparisonExpression(property, ComparisonOperator.Equal, !negated);
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

        if (declaringType == typeof(Enumerable))
        {
            return methodName switch
            {
                "Contains" => VisitEnumerableContains(method),
                "Any" => VisitEnumerableAny(method),
                _ => throw new NotSupportedException($"Enumerable method {methodName} is not supported")
            };
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

        var property = ExtractProperty(method.Object);
        var value = EvaluateExpression(method.Arguments[0]);

        if (method.Arguments.Count == 1)
        {
            return new ComparisonExpression(property, caseSensitiveOp, value);
        }
        else if (method.Arguments.Count == 2)
        {
            var comparisonArg = method.Arguments[1];
            var stringComparison = EvaluateStringComparison(comparisonArg);
            var finalOperator = IsIgnoreCaseComparison(stringComparison) ? ignoreCaseOp : caseSensitiveOp;
            return new ComparisonExpression(property, finalOperator, value);
        }
        else
        {
            throw new NotSupportedException($"String method with {method.Arguments.Count} arguments is not supported");
        }
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

        var sourceExpression = method.Arguments[0];
        var valueExpression = method.Arguments[1];

        if (IsPropertyAccess(valueExpression))
        {
            var property = ExtractProperty(valueExpression);
            var values = EvaluateExpression(sourceExpression);

            if (values is System.Collections.IEnumerable enumerable)
            {
                var valuesList = enumerable.Cast<object>().ToList();
                return new InExpression(property, valuesList);
            }
        }
        else if (IsPropertyAccess(sourceExpression))
        {
            var arrayProperty = ExtractProperty(sourceExpression);
            var value = EvaluateExpression(valueExpression);
            return new ComparisonExpression(arrayProperty, ComparisonOperator.ArrayContains, value);
        }

        throw new NotSupportedException("Unsupported Contains expression structure");
    }

    /// <summary>
    /// Enumerable.Any visitor.
    /// </summary>
    protected virtual FilterExpression VisitEnumerableAny(MethodCallExpression method)
    {
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
                var valuesList = enumerable.Cast<object>().ToList();
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
    /// Recursively extract ValueExpression from any Expression.
    /// </summary>
    protected ValueExpression ExtractValueExpression(Expression expression)
    {
        return expression switch
        {
            BinaryExpression binary when IsArithmeticExpression(binary) =>
                CreateArithmeticExpression(binary),
            
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
            
            MemberExpression member when member.Member is System.Reflection.PropertyInfo || 
                                         member.Member is System.Reflection.FieldInfo =>
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
                          "Trim" or "TrimStart" or "TrimEnd";
        }
        
        if (declaringType == typeof(Math))
        {
            return name is "Abs" or "Round" or "Floor" or "Ceiling" or "Truncate";
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
            func = name switch
            {
                "Abs" => PropertyFunction.Abs,
                "Round" => PropertyFunction.Round,
                "Floor" => PropertyFunction.Floor,
                "Ceiling" => PropertyFunction.Ceiling,
                "Truncate" => PropertyFunction.Floor,
                _ => throw new NotSupportedException($"Math method {name} is not supported")
            };
            argument = ExtractValueExpression(method.Arguments[0]);
        }
        else
        {
            throw new NotSupportedException($"Method {declaringType?.Name}.{name} is not supported");
        }
        
        return new FunctionCallExpression(func, argument);
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
                                     "Hour" or "Minute" or "Second";
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
            return new QueryExpressions.PropertyInfo(fullPath, propInfo.PropertyType, _isBaseFieldContext, function);
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
        return new QueryExpressions.PropertyInfo(fullPath, memberType, _isBaseFieldContext, function);
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

