namespace redb.Core.Query.Models;

/// <summary>
/// Specifies which property of ListItem is being accessed.
/// Used in PVT queries to determine JOIN requirements.
/// </summary>
public enum ListItemProperty
{
    /// <summary>Status.Id → _values._ListItem directly (no JOIN)</summary>
    Id,
    
    /// <summary>Status.Value → JOIN _list_items._value</summary>
    Value,
    
    /// <summary>Status.Alias → JOIN _list_items._alias (if available)</summary>
    Alias
}

