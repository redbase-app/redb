using System.Linq.Expressions;
using redb.Core.Models.Contracts;

namespace redb.Core.Query.Window;

/// <summary>
/// Window specification for Window Functions.
/// </summary>
public interface IWindowSpec<TProps> where TProps : class, new()
{
    // ===== PARTITION BY =====
    
    /// <summary>
    /// Partition by Props field.
    /// </summary>
    IWindowSpec<TProps> PartitionBy<TKey>(Expression<Func<TProps, TKey>> keySelector);
    
    /// <summary>
    /// Partition by IRedbObject base field (SchemeId, OwnerId, etc.).
    /// Uses IRedbObject for compile-time safety - Props not visible!
    /// </summary>
    IWindowSpec<TProps> PartitionByRedb<TKey>(Expression<Func<IRedbObject, TKey>> keySelector);
    
    // ===== ORDER BY =====
    
    /// <summary>
    /// Sort within window by Props field (ascending).
    /// </summary>
    IWindowSpec<TProps> OrderBy<TKey>(Expression<Func<TProps, TKey>> keySelector);
    
    /// <summary>
    /// Sort within window by Props field (descending).
    /// </summary>
    IWindowSpec<TProps> OrderByDesc<TKey>(Expression<Func<TProps, TKey>> keySelector);
    
    /// <summary>
    /// Sort within window by IRedbObject base field (ascending).
    /// Uses IRedbObject for compile-time safety - Props not visible!
    /// </summary>
    IWindowSpec<TProps> OrderByRedb<TKey>(Expression<Func<IRedbObject, TKey>> keySelector);
    
    /// <summary>
    /// Sort within window by IRedbObject base field (descending).
    /// Uses IRedbObject for compile-time safety - Props not visible!
    /// </summary>
    IWindowSpec<TProps> OrderByDescRedb<TKey>(Expression<Func<IRedbObject, TKey>> keySelector);
    
    // ===== FRAME =====
    
    /// <summary>
    /// Sets Frame (ROWS BETWEEN) for sliding windows.
    /// </summary>
    IWindowSpec<TProps> Frame(FrameSpec frame);
}

