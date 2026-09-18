using System;
using System.Threading;

namespace redb.Core;

/// <summary>
/// The redb scope a lazy load runs on (owner decision 2026-09-15, plan docs/V4/PROPS_CACHE_PROD_AND_TAILS_PLAN.md §4.1): a
/// data object owns no connection, and <see cref="Models.Entities.RedbListItem.Object"/> or the Props of a reference stub
/// load on the live scope of whoever reads them. A service makes itself current for the flow that resolved it (its
/// constructor) and for a block (<see cref="IRedbService.BeginAccess"/>); a read takes the nearest live service of its
/// database.
/// <para>
/// A frame holds its service weakly and is live while the service's context is not disposed: an ended scope never
/// answers, and a flow that outlives it simply finds the next live frame - or none. The old model opened a fresh scope and
/// pooled connection per read wherever no loader of the right scope was attached (tsum, 2026-09-09 and 2026-09-15).
/// </para>
/// </summary>
internal static class RedbAmbientScope
{
    private sealed class Frame
    {
        private readonly WeakReference<RedbServiceBase> _service;

        public Frame(RedbServiceBase service, Frame? parent)
        {
            _service = new WeakReference<RedbServiceBase>(service);
            Domain = service.CacheDomain;
            Parent = parent;
        }

        public string Domain { get; }
        public Frame? Parent { get; }

        public RedbServiceBase? Live => _service.TryGetTarget(out var service) && !service.IsScopeEnded ? service : null;
    }

    private sealed class Restore : IDisposable
    {
        private readonly Frame? _previous;
        private bool _disposed;

        public Restore(Frame? previous) => _previous = previous;

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            Current.Value = _previous;
        }
    }

    private static readonly AsyncLocal<Frame?> Current = new();

    /// <summary>Makes <paramref name="service"/> current for the rest of the calling flow.</summary>
    public static void Enter(RedbServiceBase service)
        => Current.Value = new Frame(service, FirstLive(Current.Value));

    /// <summary>Makes <paramref name="service"/> current for a block; disposing restores what was current before.</summary>
    public static IDisposable Push(RedbServiceBase service)
    {
        var previous = Current.Value;
        Current.Value = new Frame(service, FirstLive(previous));
        return new Restore(previous);
    }

    /// <summary>
    /// The nearest live service of <paramref name="domain"/> in the calling flow. With no domain - an item built in user
    /// code - the nearest live service, provided every live service in the flow reads the same database: an item of
    /// unknown origin is never read from a database it may not belong to. Null when no live redb scope reads here.
    /// </summary>
    public static RedbServiceBase? Resolve(string? domain)
    {
        RedbServiceBase? found = null;
        for (var frame = Current.Value; frame != null; frame = frame.Parent)
        {
            if (domain != null && frame.Domain != domain) continue;
            var live = frame.Live;
            if (live == null) continue;
            if (domain != null) return live;
            if (found == null)
            {
                found = live;
                continue;
            }
            if (found.CacheDomain != live.CacheDomain)
                throw new InvalidOperationException(
                    "A list item that carries no database is read while live redb scopes of two databases " +
                    $"('{found.CacheDomain}', '{live.CacheDomain}') are current, so which one it belongs to is unknown. " +
                    "Load its object explicitly on the right service: redb.LoadAsync(item.IdObject) or " +
                    "redb.LoadLinkedObjectsAsync(items).");
        }
        return found;
    }

    /// <summary>
    /// The service behind <paramref name="origin"/> - the scope that materialized an instance - if it still lives. Asked
    /// only when no scope is current for the reader, and never for a shared instance (owner decision 2026-09-15).
    /// </summary>
    public static RedbServiceBase? LiveOrigin(WeakReference<RedbServiceBase>? origin)
        => origin != null && origin.TryGetTarget(out var service) && !service.IsScopeEnded ? service : null;

    // A flow that creates a scope per iteration would otherwise chain every ended one behind the new frame.
    private static Frame? FirstLive(Frame? frame)
    {
        while (frame != null && frame.Live == null)
            frame = frame.Parent;
        return frame;
    }
}
