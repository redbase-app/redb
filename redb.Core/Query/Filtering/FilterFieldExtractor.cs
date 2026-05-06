using redb.Core.Query.QueryExpressions;

namespace redb.Core.Query.Filtering;

/// <summary>
/// Extracts field information from FilterExpression tree.
/// Returns field names and whether they are base fields.
/// Handles Dictionary field normalization.
/// </summary>
public class FilterFieldExtractor
{
    /// <summary>
    /// Extract field names with IsBaseField flag from FilterExpression.
    /// Handles Dictionary fields: PhoneBook.ContainsKey("home") → PhoneBook[home]
    /// Removes Dictionary base fields when keyed versions exist (for PVT).
    /// </summary>
    /// <param name="filter">Filter expression to extract from</param>
    /// <returns>Dictionary: field name → is base field</returns>
    public Dictionary<string, bool> ExtractFieldInfos(FilterExpression? filter)
    {
        var infos = new Dictionary<string, bool>();
        var nullCheckFields = new HashSet<string>();
        
        if (filter != null)
            ExtractRecursive(filter, infos, nullCheckFields);
        
        // Remove Dictionary base fields from NullCheck
        // If we have "PhoneBook[home]" from ContainsKey, remove "PhoneBook" from NullCheck
        // because Dictionary base field doesn't exist in PVT (only keyed fields do)
        foreach (var nullField in nullCheckFields)
        {
            var isDictionary = infos.Keys.Any(k => k.StartsWith($"{nullField}["));
            if (isDictionary)
                infos.Remove(nullField);
        }
        
        return infos;
    }
    
    /// <summary>
    /// Extract only Props field names (not base fields) from FilterExpression.
    /// </summary>
    public HashSet<string> ExtractPropsFieldNames(FilterExpression? filter)
    {
        var infos = ExtractFieldInfos(filter);
        return infos.Where(kv => !kv.Value).Select(kv => kv.Key).ToHashSet();
    }
    
    /// <summary>
    /// Extract only base field names from FilterExpression.
    /// </summary>
    public HashSet<string> ExtractBaseFieldNames(FilterExpression? filter)
    {
        var infos = ExtractFieldInfos(filter);
        return infos.Where(kv => kv.Value).Select(kv => kv.Key).ToHashSet();
    }
    
    private void ExtractRecursive(FilterExpression filter, Dictionary<string, bool> infos, HashSet<string> nullCheckFields)
    {
        switch (filter)
        {
            case ComparisonExpression comparison:
                // Ignore special names and Pro ValueExpression
                if (comparison.Property.Name != "__constant" && 
                    comparison.Property.Name != "__computed")
                {
                    var fieldName = NormalizeDictionaryFieldName(comparison.Property.Name, comparison.Value);
                    infos.TryAdd(fieldName, comparison.Property.IsBaseField);
                }
                // Pro: Extract fields from ValueExpression
                if (comparison.LeftExpression != null)
                    ExtractFromValueExpression(comparison.LeftExpression, infos);
                if (comparison.RightExpression != null)
                    ExtractFromValueExpression(comparison.RightExpression, infos);
                break;
                
            case LogicalExpression logical:
                foreach (var operand in logical.Operands)
                    ExtractRecursive(operand, infos, nullCheckFields);
                break;
                
            case NullCheckExpression nullCheck:
                infos.TryAdd(nullCheck.Property.Name, nullCheck.Property.IsBaseField);
                // Track NullCheck fields to potentially remove Dictionary base fields later
                if (!nullCheck.Property.IsBaseField)
                    nullCheckFields.Add(nullCheck.Property.Name);
                break;
                
            case InExpression inExpr:
                infos.TryAdd(inExpr.Property.Name, inExpr.Property.IsBaseField);
                break;
        }
    }
    
    /// <summary>
    /// Extracts fields from Pro ValueExpression (arithmetic/functions).
    /// </summary>
    private void ExtractFromValueExpression(ValueExpression expr, Dictionary<string, bool> infos)
    {
        switch (expr)
        {
            case PropertyValueExpression propExpr:
                infos.TryAdd(propExpr.Property.Name, propExpr.Property.IsBaseField);
                break;
            case ArithmeticExpression arith:
                ExtractFromValueExpression(arith.Left, infos);
                ExtractFromValueExpression(arith.Right, infos);
                break;
            case FunctionCallExpression func:
                ExtractFromValueExpression(func.Argument, infos);
                break;
            case CustomFunctionExpression custom:
                foreach (var arg in custom.Arguments)
                    ExtractFromValueExpression(arg, infos);
                break;
            // ConstantValueExpression doesn't contribute fields
        }
    }
    
    /// <summary>
    /// Normalizes Dictionary field names:
    /// - "PhoneBook.ContainsKey" with value "home" → "PhoneBook[home]"
    /// - "PhoneBook[home]" → unchanged
    /// </summary>
    private static string NormalizeDictionaryFieldName(string fieldName, object? value)
    {
        // Handle ContainsKey: "PhoneBook.ContainsKey" with value "home" → "PhoneBook[home]"
        if (fieldName.EndsWith(".ContainsKey") && value is string keyValue)
        {
            var baseName = fieldName[..^".ContainsKey".Length];
            return $"{baseName}[{keyValue}]";
        }
        return fieldName;
    }
}

