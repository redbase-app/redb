namespace redb.Core.Query.Grouping;

/// <summary>
/// Request for grouping field for SQL function aggregate_grouped
/// </summary>
public class GroupFieldRequest
{
    /// <summary>
    /// Path that names the element itself in an array grouping: the key of <c>GroupByArray(e =&gt; e.Skills, s =&gt; s)</c>,
    /// the argument of <c>Agg.Sum(g, h =&gt; h)</c>. Valid only for an array of simple values; a C# member name cannot
    /// start with '$', so no field can be mistaken for it.
    /// </summary>
    public const string ElementPath = "$element";

    public string FieldPath { get; set; } = string.Empty;
    public string Alias { get; set; } = string.Empty;

    /// <summary>
    /// true = base field from _objects (scheme_id, parent_id, etc.)
    /// false = Props field from _values (Props.Category, etc.)
    /// </summary>
    public bool IsBaseField { get; set; } = false;
}
