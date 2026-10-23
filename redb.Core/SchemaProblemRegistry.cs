using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;
using redb.Core.Models;

namespace redb.Core;

/// <summary>
/// The schema problems of a database (cache domain), shared by every service of it. IRedbService is scoped: the start
/// runs in a scope of its own, and a health check or a controller gets another instance, which must see what the start
/// found. One list per domain, replaced whole, read without a lock.
/// </summary>
internal static class SchemaProblemRegistry
{
    private sealed class Entry
    {
        public readonly object Gate = new();
        public IReadOnlyList<RedbSchemaProblem> Problems = Array.Empty<RedbSchemaProblem>();
    }

    private static readonly ConcurrentDictionary<string, Entry> ByDomain = new();

    private static Entry For(string domain) => ByDomain.GetOrAdd(domain, _ => new Entry());

    /// <summary>The problems of the database as last found.</summary>
    public static IReadOnlyList<RedbSchemaProblem> Get(string domain)
        => ByDomain.TryGetValue(domain, out var entry) ? entry.Problems : Array.Empty<RedbSchemaProblem>();

    /// <summary>
    /// Replaces the list with what a look at the database found. A start logs every problem (one error per field and
    /// start); a look asked again (a health check) logs only the ones new to the list.
    /// </summary>
    public static IReadOnlyList<RedbSchemaProblem> Replace(string domain, IReadOnlyList<RedbSchemaProblem> found, bool logAll, ILogger? logger)
    {
        var entry = For(domain);
        lock (entry.Gate)
        {
            foreach (var problem in found.Where(p => logAll || !entry.Problems.Any(known => Same(known, p))))
                logger?.LogError("{Problem}", problem.Message);
            entry.Problems = found.ToArray();
            return entry.Problems;
        }
    }

    /// <summary>A problem met by a scheme sync: logged and listed unless already known for the database.</summary>
    public static void Report(string domain, RedbSchemaProblem problem, ILogger? logger)
    {
        var entry = For(domain);
        lock (entry.Gate)
        {
            if (entry.Problems.Any(known => Same(known, problem)))
                return;
            logger?.LogError("{Problem}", problem.Message);
            entry.Problems = entry.Problems.Append(problem).ToArray();
        }
    }

    private static bool Same(RedbSchemaProblem a, RedbSchemaProblem b)
        => a.Kind == b.Kind && a.SchemeId == b.SchemeId && a.Field == b.Field;
}
