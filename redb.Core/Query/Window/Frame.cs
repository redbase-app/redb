namespace redb.Core.Query.Window;

/// <summary>
/// Window boundary type
/// </summary>
public enum FrameBoundType
{
    UnboundedPreceding,  // UNBOUNDED PRECEDING
    Preceding,           // N PRECEDING
    CurrentRow,          // CURRENT ROW
    Following,           // N FOLLOWING
    UnboundedFollowing   // UNBOUNDED FOLLOWING
}

/// <summary>
/// Frame type: ROWS or RANGE
/// </summary>
public enum FrameType
{
    Rows,   // ROWS BETWEEN
    Range   // RANGE BETWEEN
}

/// <summary>
/// Frame boundary
/// </summary>
public class FrameBound
{
    public FrameBoundType Type { get; set; }
    public int? Offset { get; set; }  // For N PRECEDING / N FOLLOWING
    
    public static FrameBound UnboundedPreceding() => new() { Type = FrameBoundType.UnboundedPreceding };
    public static FrameBound Preceding(int n) => new() { Type = FrameBoundType.Preceding, Offset = n };
    public static FrameBound CurrentRow() => new() { Type = FrameBoundType.CurrentRow };
    public static FrameBound Following(int n) => new() { Type = FrameBoundType.Following, Offset = n };
    public static FrameBound UnboundedFollowing() => new() { Type = FrameBoundType.UnboundedFollowing };
}

/// <summary>
/// Frame specification for window functions
/// </summary>
public class FrameSpec
{
    public FrameType Type { get; set; } = FrameType.Rows;
    public FrameBound Start { get; set; } = FrameBound.UnboundedPreceding();
    public FrameBound End { get; set; } = FrameBound.CurrentRow();
}

/// <summary>
/// Builder for creating FrameSpec
/// </summary>
public class FrameBuilder
{
    private readonly FrameSpec _spec = new();
    
    public FrameBuilder(FrameType type = FrameType.Rows)
    {
        _spec.Type = type;
    }
    
    /// <summary>
    /// ROWS BETWEEN n PRECEDING AND CURRENT ROW
    /// </summary>
    public FrameBuilder Preceding(int n)
    {
        _spec.Start = FrameBound.Preceding(n);
        _spec.End = FrameBound.CurrentRow();
        return this;
    }
    
    /// <summary>
    /// ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    /// </summary>
    public FrameBuilder UnboundedPreceding()
    {
        _spec.Start = FrameBound.UnboundedPreceding();
        _spec.End = FrameBound.CurrentRow();
        return this;
    }
    
    /// <summary>
    /// ... AND n FOLLOWING
    /// </summary>
    public FrameBuilder AndFollowing(int n)
    {
        _spec.End = FrameBound.Following(n);
        return this;
    }
    
    /// <summary>
    /// ... AND CURRENT ROW
    /// </summary>
    public FrameBuilder AndCurrentRow()
    {
        _spec.End = FrameBound.CurrentRow();
        return this;
    }
    
    /// <summary>
    /// ... AND UNBOUNDED FOLLOWING
    /// </summary>
    public FrameBuilder AndUnboundedFollowing()
    {
        _spec.End = FrameBound.UnboundedFollowing();
        return this;
    }
    
    public FrameSpec Build() => _spec;
    
    public static implicit operator FrameSpec(FrameBuilder builder) => builder.Build();
}

/// <summary>
/// Static helper for creating frames
/// </summary>
public static class Frame
{
    /// <summary>
    /// ROWS BETWEEN ...
    /// </summary>
    public static FrameBuilder Rows() => new(FrameType.Rows);
    
    /// <summary>
    /// ROWS BETWEEN n PRECEDING AND CURRENT ROW (sliding window)
    /// </summary>
    public static FrameBuilder Rows(int preceding) => new FrameBuilder(FrameType.Rows).Preceding(preceding);
    
    /// <summary>
    /// RANGE BETWEEN ...
    /// </summary>
    public static FrameBuilder Range() => new(FrameType.Range);
}
