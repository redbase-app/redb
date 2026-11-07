using System;
using System.Threading;

namespace redb.Core.Utils
{
    /// <summary>
    /// Counts events in fixed time windows and reports the moment a window's count reaches a threshold, once per
    /// window. Lock-free and meant for diagnostics: a race at a window boundary may lose a few counts.
    /// </summary>
    internal sealed class RateWindow
    {
        private readonly long _windowMs;
        private readonly int _threshold;
        private long _windowStartMs;
        private int _count;

        public RateWindow(TimeSpan window, int threshold)
        {
            if (window <= TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(window), "Must be greater than zero");
            if (threshold <= 0)
                throw new ArgumentOutOfRangeException(nameof(threshold), "Must be greater than 0");
            _windowMs = (long)window.TotalMilliseconds;
            _threshold = threshold;
            _windowStartMs = Environment.TickCount64;
        }

        /// <summary>Adds events; returns the window's count when this call reached the threshold, otherwise 0.</summary>
        public int Add(int events = 1)
        {
            var now = Environment.TickCount64;
            var start = Interlocked.Read(ref _windowStartMs);
            if (now - start > _windowMs && Interlocked.CompareExchange(ref _windowStartMs, now, start) == start)
                Interlocked.Exchange(ref _count, 0);
            var after = Interlocked.Add(ref _count, events);
            return after >= _threshold && after - events < _threshold ? after : 0;
        }
    }
}
