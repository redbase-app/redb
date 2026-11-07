using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace redb.Tests.Integration.Fixtures;

/// <summary>
/// Records every log entry of the loggers it creates, so a test can assert that a diagnostic warning was
/// written - and written once.
/// </summary>
public sealed class CapturingLoggerProvider : ILoggerProvider
{
    private readonly ConcurrentQueue<CapturedLogEntry> _entries = new();

    public IReadOnlyList<CapturedLogEntry> Entries => _entries.ToArray();

    /// <summary>Messages of the Warning entries, in the order they were written.</summary>
    public IReadOnlyList<string> Warnings => _entries.Where(e => e.Level == LogLevel.Warning).Select(e => e.Message).ToList();

    public ILogger CreateLogger(string categoryName) => new CapturingLogger(this, categoryName);

    public void Dispose() { }

    private sealed class CapturingLogger : ILogger
    {
        private readonly CapturingLoggerProvider _owner;
        private readonly string _category;

        public CapturingLogger(CapturingLoggerProvider owner, string category)
        {
            _owner = owner;
            _category = category;
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
            => _owner._entries.Enqueue(new CapturedLogEntry(_category, logLevel, formatter(state, exception)));
    }
}

public sealed record CapturedLogEntry(string Category, LogLevel Level, string Message);
