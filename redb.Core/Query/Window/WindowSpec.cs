using System.Linq.Expressions;
using redb.Core.Models.Contracts;

namespace redb.Core.Query.Window;

/// <summary>
/// Implementation of window specification
/// </summary>
public class WindowSpec<TProps> : IWindowSpec<TProps> where TProps : class, new()
{
    // 🆕 CHANGED: Store IsBaseField flag for each field
    internal List<(Expression Field, bool IsBaseField)> PartitionByFields { get; } = new();
    internal List<(Expression Field, bool Descending, bool IsBaseField)> OrderByFields { get; } = new();
    internal FrameSpec? FrameSpec { get; private set; }
    
    // ===== PARTITION BY =====
    
    public IWindowSpec<TProps> PartitionBy<TKey>(Expression<Func<TProps, TKey>> keySelector)
    {
        PartitionByFields.Add((keySelector, IsBaseField: false));
        return this;
    }
    
    public IWindowSpec<TProps> PartitionByRedb<TKey>(Expression<Func<IRedbObject, TKey>> keySelector)
    {
        PartitionByFields.Add((keySelector, IsBaseField: true));
        return this;
    }
    
    // ===== ORDER BY =====
    
    public IWindowSpec<TProps> OrderBy<TKey>(Expression<Func<TProps, TKey>> keySelector)
    {
        OrderByFields.Add((keySelector, Descending: false, IsBaseField: false));
        return this;
    }
    
    public IWindowSpec<TProps> OrderByDesc<TKey>(Expression<Func<TProps, TKey>> keySelector)
    {
        OrderByFields.Add((keySelector, Descending: true, IsBaseField: false));
        return this;
    }
    
    public IWindowSpec<TProps> OrderByRedb<TKey>(Expression<Func<IRedbObject, TKey>> keySelector)
    {
        OrderByFields.Add((keySelector, Descending: false, IsBaseField: true));
        return this;
    }
    
    public IWindowSpec<TProps> OrderByDescRedb<TKey>(Expression<Func<IRedbObject, TKey>> keySelector)
    {
        OrderByFields.Add((keySelector, Descending: true, IsBaseField: true));
        return this;
    }
    
    // ===== FRAME =====
    
    public IWindowSpec<TProps> Frame(FrameSpec frame)
    {
        FrameSpec = frame;
        return this;
    }
}
