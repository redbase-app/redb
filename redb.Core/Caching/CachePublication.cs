using System;
using redb.Core.Data;

namespace redb.Core.Caching
{
    /// <summary>
    /// The caches publish committed state only (review after 4.0.0). A cache Set marks the graph shared, and a shared
    /// instance does not keep what one transaction saw (owner decision 2026-09-15) - so a Set inside a transaction made
    /// the writer's own loaded object forget its lazy references: an edit of <c>root.Props.Next.Props</c> followed by a
    /// save of <c>root.Props.Next</c> saved a fresh reload, and every read of a list item's object was a query. A Set
    /// inside a transaction now waits for its commit; until then the instance is the reader's own, and what a
    /// rolled-back transaction loaded never reaches the cache.
    /// </summary>
    public static class CachePublication
    {
        /// <summary>
        /// Runs <paramref name="publish"/> now, or once the transaction <paramref name="context"/> takes part in has
        /// committed - the explicit redb transaction of the context, else the ambient <see cref="Transaction"/>.
        /// </summary>
        public static void AfterCommit(IRedbContext context, Action publish)
        {
            ArgumentNullException.ThrowIfNull(context);
            ArgumentNullException.ThrowIfNull(publish);

            if (!TransactionHooks.OnCompleted(context, committed => { if (committed) publish(); }))
                publish();
        }
    }
}
