namespace redb.Core.Query.Grouping;

/// <summary>
/// Request for grouping field for SQL function aggregate_grouped
/// </summary>
public class GroupFieldRequest
{
    public string FieldPath { get; set; } = string.Empty;
    public string Alias { get; set; } = string.Empty;
    
    /// <summary>
    /// true = base field from _objects (scheme_id, parent_id, etc.)
    /// false = EAV field from _values (Props.Category, etc.)
    /// </summary>
    public bool IsBaseField { get; set; } = false;
}

