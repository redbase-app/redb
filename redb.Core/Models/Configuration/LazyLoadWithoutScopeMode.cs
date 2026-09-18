namespace redb.Core.Models.Configuration;

/// <summary>
/// What a lazy load does when no live redb scope reads it (owner decision 2026-09-15). See
/// <see cref="RedbServiceConfiguration.LazyLoadWithoutScope"/>.
/// </summary>
public enum LazyLoadWithoutScopeMode
{
    /// <summary>
    /// Refuse with <see cref="Exceptions.RedbLazyLoadScopeEndedException"/>, naming the explicit ways (the default): a data
    /// object owns no connection, and a hidden scope per read is how connection pools run dry.
    /// </summary>
    Refuse = 0,

    /// <summary>
    /// Open a fresh scope - and a pooled connection - for each such load, outside any ambient transaction, and warn once
    /// the rate gets high. An explicit opt-in for hosts that accept the cost.
    /// </summary>
    FreshScope = 1,
}
