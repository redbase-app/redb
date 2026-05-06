namespace redb.PropsEditor.Attributes;

/// <summary>
/// Specifies display settings for a property in PropsEditor.
/// </summary>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
public class PropsDisplayAttribute : Attribute
{
    /// <summary>
    /// Display label for the property.
    /// </summary>
    public string? Label { get; set; }

    /// <summary>
    /// Placeholder text for input fields.
    /// </summary>
    public string? Placeholder { get; set; }

    /// <summary>
    /// Description/help text shown below the field.
    /// </summary>
    public string? Description { get; set; }

    /// <summary>
    /// Display order within the group.
    /// </summary>
    public int Order { get; set; }

    public PropsDisplayAttribute() { }

    public PropsDisplayAttribute(string label)
    {
        Label = label;
    }
}

/// <summary>
/// Renders the property as a multiline text area.
/// </summary>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
public class PropsTextAreaAttribute : Attribute
{
    /// <summary>
    /// Number of rows for the text area.
    /// </summary>
    public int Rows { get; set; } = 4;

    public PropsTextAreaAttribute() { }

    public PropsTextAreaAttribute(int rows)
    {
        Rows = rows;
    }
}

/// <summary>
/// Hides the property from the editor.
/// </summary>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
public class PropsHiddenAttribute : Attribute { }

/// <summary>
/// Groups properties together in a collapsible section.
/// </summary>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
public class PropsGroupAttribute : Attribute
{
    /// <summary>
    /// Name of the group.
    /// </summary>
    public string GroupName { get; set; }

    /// <summary>
    /// Whether the group is collapsed by default.
    /// </summary>
    public bool Collapsed { get; set; }

    public PropsGroupAttribute(string groupName)
    {
        GroupName = groupName;
    }
}

/// <summary>
/// Marks the property as read-only in the editor.
/// </summary>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
public class PropsReadOnlyAttribute : Attribute { }
