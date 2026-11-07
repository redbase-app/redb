using System;
using System.Transactions;

namespace redb.Core.Data
{
    /// <summary>
    /// The transaction a context takes part in - its explicit redb transaction, else the ambient
    /// <see cref="Transaction"/> - and a callback for its end. One place for what the cache publication and the
    /// lazy-load rule both need (review after 4.0.0).
    /// </summary>
    public static class TransactionHooks
    {
        /// <summary>The transaction object the context takes part in, or null outside any transaction.</summary>
        public static object? TransactionOf(IRedbContext context)
        {
            ArgumentNullException.ThrowIfNull(context);
            if (context.CurrentTransaction is { IsActive: true } explicitTransaction)
                return explicitTransaction;
            return Transaction.Current;
        }

        /// <summary>
        /// Runs <paramref name="onCompleted"/> with whether the transaction committed once the context's transaction
        /// ends. Returns false, running nothing, when the context is in no transaction.
        /// </summary>
        public static bool OnCompleted(IRedbContext context, Action<bool> onCompleted)
        {
            ArgumentNullException.ThrowIfNull(onCompleted);
            switch (TransactionOf(context))
            {
                case IRedbTransaction explicitTransaction:
                    TransactionCompletion.Register(explicitTransaction, onCompleted);
                    return true;
                case Transaction ambient:
                    ambient.TransactionCompleted += (_, e) =>
                        onCompleted(e.Transaction.TransactionInformation.Status == TransactionStatus.Committed);
                    return true;
                default:
                    return false;
            }
        }
    }
}
