using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace redb.Core.Data
{
    /// <summary>
    /// Callbacks that run when an explicit redb transaction (<see cref="IRedbTransaction"/>) ends, with whether it
    /// committed. The provider transaction classes report the outcome once; the props and list caches use it to
    /// publish what a transaction loaded only after its commit (<see cref="Caching.CachePublication"/>). Entries are
    /// held weakly by the transaction: a transaction nobody completes leaves nothing behind.
    /// </summary>
    public static class TransactionCompletion
    {
        private static readonly ConditionalWeakTable<IRedbTransaction, List<Action<bool>>> Pending = new();

        /// <summary>Runs <paramref name="onCompleted"/> with <c>true</c> when the transaction commits, <c>false</c> when it ends any other way.</summary>
        public static void Register(IRedbTransaction transaction, Action<bool> onCompleted)
        {
            ArgumentNullException.ThrowIfNull(transaction);
            ArgumentNullException.ThrowIfNull(onCompleted);
            var callbacks = Pending.GetOrCreateValue(transaction);
            lock (callbacks)
                callbacks.Add(onCompleted);
        }

        /// <summary>Called by the transaction once, after it committed or rolled back.</summary>
        public static void Completed(IRedbTransaction transaction, bool committed)
        {
            ArgumentNullException.ThrowIfNull(transaction);
            if (!Pending.TryGetValue(transaction, out var callbacks))
                return;
            Pending.Remove(transaction);
            Action<bool>[] snapshot;
            lock (callbacks)
                snapshot = callbacks.ToArray();
            foreach (var callback in snapshot)
                callback(committed);
        }
    }
}
