using System.Reflection;
using redb.PropsEditor.Attributes;

namespace redb.PropsEditor.Services;

/// <summary>
/// Provides metadata about Props classes for dynamic UI generation.
/// Uses reflection to analyze property types and attributes.
/// </summary>
public class PropsMetadataService
{
    /// <summary>
    /// Gets metadata for all properties of a Props type.
    /// </summary>
    public IEnumerable<PropertyMetadata> GetProperties<TProps>() where TProps : class
    {
        return GetProperties(typeof(TProps));
    }

    /// <summary>
    /// Gets metadata for all properties of a Props type.
    /// </summary>
    public IEnumerable<PropertyMetadata> GetProperties(Type propsType)
    {
        var properties = propsType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanRead && p.CanWrite)
            .Where(p => p.GetCustomAttribute<PropsHiddenAttribute>() == null)
            .Select(CreateMetadata)
            .OrderBy(m => m.Order)
            .ThenBy(m => m.Label);

        return properties;
    }

    private PropertyMetadata CreateMetadata(PropertyInfo prop)
    {
        var displayAttr = prop.GetCustomAttribute<PropsDisplayAttribute>();
        var textAreaAttr = prop.GetCustomAttribute<PropsTextAreaAttribute>();
        var groupAttr = prop.GetCustomAttribute<PropsGroupAttribute>();
        var readOnlyAttr = prop.GetCustomAttribute<PropsReadOnlyAttribute>();

        return new PropertyMetadata
        {
            PropertyInfo = prop,
            Name = prop.Name,
            Label = displayAttr?.Label ?? FormatLabel(prop.Name),
            Placeholder = displayAttr?.Placeholder,
            Description = displayAttr?.Description,
            Order = displayAttr?.Order ?? 0,
            GroupName = groupAttr?.GroupName,
            GroupCollapsed = groupAttr?.Collapsed ?? false,
            IsReadOnly = readOnlyAttr != null,
            IsTextArea = textAreaAttr != null,
            TextAreaRows = textAreaAttr?.Rows ?? 4,
            PropertyType = prop.PropertyType,
            EditorType = DetermineEditorType(prop)
        };
    }

    private EditorType DetermineEditorType(PropertyInfo prop)
    {
        var type = prop.PropertyType;
        var underlyingType = Nullable.GetUnderlyingType(type) ?? type;

        // Check for TextArea attribute first
        if (prop.GetCustomAttribute<PropsTextAreaAttribute>() != null)
            return EditorType.TextArea;

        // String
        if (underlyingType == typeof(string))
            return EditorType.Text;

        // Boolean
        if (underlyingType == typeof(bool))
            return EditorType.Checkbox;

        // Numeric types
        if (underlyingType == typeof(int) || underlyingType == typeof(long) ||
            underlyingType == typeof(short) || underlyingType == typeof(byte))
            return EditorType.Integer;

        if (underlyingType == typeof(decimal) || underlyingType == typeof(double) ||
            underlyingType == typeof(float))
            return EditorType.Decimal;

        // DateTime
        if (underlyingType == typeof(DateTime) || underlyingType == typeof(DateTimeOffset))
            return EditorType.DateTime;

        // Enum
        if (underlyingType.IsEnum)
            return EditorType.Enum;

        // Arrays
        if (type.IsArray)
        {
            var elementType = type.GetElementType()!;
            if (elementType == typeof(string))
                return EditorType.StringArray;
            if (elementType.IsPrimitive || elementType == typeof(decimal))
                return EditorType.PrimitiveArray;
            return EditorType.ObjectArray;
        }

        // Complex object
        if (underlyingType.IsClass && underlyingType != typeof(string))
            return EditorType.Object;

        // Fallback to JSON
        return EditorType.Json;
    }

    private static string FormatLabel(string propertyName)
    {
        // Convert PascalCase to "Pascal Case"
        var chars = new List<char>();
        foreach (var c in propertyName)
        {
            if (chars.Count > 0 && char.IsUpper(c))
                chars.Add(' ');
            chars.Add(c);
        }
        return new string(chars.ToArray());
    }
}

/// <summary>
/// Metadata about a single property for UI rendering.
/// </summary>
public class PropertyMetadata
{
    public required PropertyInfo PropertyInfo { get; init; }
    public required string Name { get; init; }
    public required string Label { get; init; }
    public string? Placeholder { get; init; }
    public string? Description { get; init; }
    public int Order { get; init; }
    public string? GroupName { get; init; }
    public bool GroupCollapsed { get; init; }
    public bool IsReadOnly { get; init; }
    public bool IsTextArea { get; init; }
    public int TextAreaRows { get; init; }
    public required Type PropertyType { get; init; }
    public EditorType EditorType { get; init; }

    /// <summary>
    /// Gets the value of this property from an object.
    /// </summary>
    public object? GetValue(object obj) => PropertyInfo.GetValue(obj);

    /// <summary>
    /// Sets the value of this property on an object.
    /// </summary>
    public void SetValue(object obj, object? value) => PropertyInfo.SetValue(obj, value);
}

/// <summary>
/// Type of editor to render for a property.
/// </summary>
public enum EditorType
{
    Text,
    TextArea,
    Checkbox,
    Integer,
    Decimal,
    DateTime,
    Enum,
    StringArray,
    PrimitiveArray,
    ObjectArray,
    Object,
    Json
}
