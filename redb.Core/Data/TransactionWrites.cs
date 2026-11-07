using System;
using System.Runtime.CompilerServices;

namespace redb.Core.Data
{
    /// <summary>
    /// Whether a transaction has written anything yet. Inside a transaction that has written nothing, what its
    /// connection reads is committed state, so a shared instance may keep it (the lazy-load rule, owner decision
    /// 2026-09-17); once the transaction has written, what it reads may be its own uncommitted work, and a shared
    /// instance keeps nothing of it (owner decision 2026-09-15). redb writes in few known places, which mark
    /// here: every non-query command and every bulk operation (<see cref="RedbContextBase"/>), and the provider
    /// methods that write behind a query (soft delete, purge). Kept per transaction object, weakly.
    /// </summary>
    public static class TransactionWrites
    {
        private static readonly ConditionalWeakTable<object, StrongBox<bool>> Written = new();

        /// <summary>The context's transaction has written.</summary>
        public static void Mark(IRedbContext context)
        {
            if (TransactionHooks.TransactionOf(context) is { } transaction)
                Written.GetOrCreateValue(transaction).Value = true;
        }

        /// <summary>Whether the context's transaction has written anything; false outside a transaction.</summary>
        public static bool Any(IRedbContext context)
            => TransactionHooks.TransactionOf(context) is { } transaction
               && Written.TryGetValue(transaction, out var box) && box.Value;

        /// <summary>
        /// Whether a load made now on the context's connection reads committed state: outside a transaction, or
        /// inside one that has written nothing yet.
        /// </summary>
        public static bool ReadsCommitted(IRedbContext context) => !Any(context);
    }
}
