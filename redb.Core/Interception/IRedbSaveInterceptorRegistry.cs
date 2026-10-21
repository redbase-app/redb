namespace redb.Core.Interception;

/// <summary>
/// Save interceptors added and removed while the process runs: code that cannot reach the DI container the redb
/// service was built from (a module loaded into its own AssemblyLoadContext, for instance) subscribes here for as
/// long as it lives. Registered as a singleton by <c>AddRedb</c> and <c>AddRedbPro</c> (TryAdd: an instance put into
/// the container before is kept, so several containers may share one registry); resolve it as
/// <c>IRedbSaveInterceptorRegistry</c>.
/// <para>
/// An interceptor from the registry runs after the ones registered in DI, in the order of
/// <see cref="Add"/>, with exactly the semantics of <see cref="IRedbSaveInterceptor"/> (Saving and Deleting before
/// the transaction, Saved and Deleted after the commit, everything inside an <c>ExecuteAtomicAsync</c> scope).
/// </para>
/// <para>
/// Scope of a subscription: it sees EVERY save and delete of the process made through a redb service whose container
/// holds this registry, scopes already open included, other modules' saves included, and the named instances of
/// <c>RedbInstanceFactory</c> built over the same registry. An exception thrown from <c>SavingAsync</c> or
/// <c>DeletingAsync</c> therefore cancels any save or delete of the process, not only the subscriber's own.
/// </para>
/// <para>
/// A subscription is picked up by the next save or delete; an operation already running keeps the set it started
/// with, so its Saving and Saved (Deleting and Deleted) always reach the same interceptors. Disposing the returned
/// handle removes the interceptor for every operation that starts afterwards, and the registry keeps no reference
/// to it: an AssemblyLoadContext that held the interceptor's type can unload once its running operations end.
/// </para>
/// </summary>
public interface IRedbSaveInterceptorRegistry
{
    /// <summary>
    /// Subscribes <paramref name="interceptor"/>. Dispose the result to remove it; disposing twice, or from another
    /// thread, is safe. The same instance added twice runs twice, and each handle removes one of the two.
    /// </summary>
    IDisposable Add(IRedbSaveInterceptor interceptor);

    /// <summary>The interceptors subscribed right now, in the order they were added. An immutable snapshot.</summary>
    IReadOnlyList<IRedbSaveInterceptor> Current { get; }
}

/// <summary>
/// The default <see cref="IRedbSaveInterceptorRegistry"/>: an immutable array replaced atomically on every change,
/// so a reader takes a snapshot with one read and never locks.
/// </summary>
public sealed class RedbSaveInterceptorRegistry : IRedbSaveInterceptorRegistry
{
    private IRedbSaveInterceptor[] _interceptors = [];

    /// <inheritdoc />
    public IReadOnlyList<IRedbSaveInterceptor> Current => Volatile.Read(ref _interceptors);

    /// <inheritdoc />
    public IDisposable Add(IRedbSaveInterceptor interceptor)
    {
        ArgumentNullException.ThrowIfNull(interceptor);

        while (true)
        {
            var current = Volatile.Read(ref _interceptors);
            var next = new IRedbSaveInterceptor[current.Length + 1];
            current.CopyTo(next, 0);
            next[^1] = interceptor;
            if (Interlocked.CompareExchange(ref _interceptors, next, current) == current)
                return new Subscription(this, interceptor);
        }
    }

    private void Remove(IRedbSaveInterceptor interceptor)
    {
        while (true)
        {
            var current = Volatile.Read(ref _interceptors);
            var index = Array.IndexOf(current, interceptor);
            if (index < 0)
                return;

            IRedbSaveInterceptor[] next;
            if (current.Length == 1)
            {
                next = [];
            }
            else
            {
                next = new IRedbSaveInterceptor[current.Length - 1];
                Array.Copy(current, 0, next, 0, index);
                Array.Copy(current, index + 1, next, index, current.Length - index - 1);
            }

            if (Interlocked.CompareExchange(ref _interceptors, next, current) == current)
                return;
        }
    }

    /// <summary>
    /// The handle of one subscription. Drops its own references on Dispose: a handle the subscriber forgets to
    /// release does not pin the interceptor either.
    /// </summary>
    private sealed class Subscription(RedbSaveInterceptorRegistry registry, IRedbSaveInterceptor interceptor) : IDisposable
    {
        private RedbSaveInterceptorRegistry? _registry = registry;
        private IRedbSaveInterceptor? _interceptor = interceptor;

        public void Dispose()
        {
            var interceptor = Interlocked.Exchange(ref _interceptor, null);
            if (interceptor is null)
                return;

            var registry = _registry!;
            _registry = null;
            registry.Remove(interceptor);
        }
    }
}

/// <summary>
/// The interceptors a storage provider runs: the ones registered in DI, fixed when the service is built, followed by
/// the current subscriptions of the <see cref="IRedbSaveInterceptorRegistry"/>. The provider takes one
/// <see cref="Snapshot"/> per save or delete.
/// </summary>
public sealed class RedbSaveInterceptorPipeline
{
    /// <summary>No interceptors at all: nothing in DI, no registry.</summary>
    public static RedbSaveInterceptorPipeline Empty { get; } = new(null, null);

    private readonly IRedbSaveInterceptor[] _fromDi;
    private readonly IRedbSaveInterceptorRegistry? _registry;

    public RedbSaveInterceptorPipeline(IEnumerable<IRedbSaveInterceptor>? fromDi, IRedbSaveInterceptorRegistry? registry)
    {
        _fromDi = fromDi?.ToArray() ?? [];
        _registry = registry;
    }

    /// <summary>
    /// The interceptors for one operation, DI first, then the registry in subscription order. Allocates only when
    /// both sources are non-empty; with neither it returns an empty list, and the operation collects nothing for them.
    /// </summary>
    public IReadOnlyList<IRedbSaveInterceptor> Snapshot()
    {
        var subscribed = _registry?.Current;
        if (subscribed is null || subscribed.Count == 0)
            return _fromDi;
        if (_fromDi.Length == 0)
            return subscribed;

        var all = new IRedbSaveInterceptor[_fromDi.Length + subscribed.Count];
        _fromDi.CopyTo(all, 0);
        for (var i = 0; i < subscribed.Count; i++)
            all[_fromDi.Length + i] = subscribed[i];
        return all;
    }
}
