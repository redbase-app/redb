using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using redb.Core.Attributes;

namespace redb.Core.Caching;

/// <summary>
/// Process-global registry of <c>schemeName → CLR Type</c>, derived from <see cref="RedbSchemeAttribute"/>.
///
/// This mapping is a <b>database-independent</b> fact: a type's scheme name comes from its attribute
/// and is identical regardless of which database/connection it is used against. Therefore it lives
/// <b>once per process</b> and is shared by every cache domain — unlike the per-domain
/// <c>scheme_id → Type</c> projection in <see cref="GlobalMetadataCache"/>, where <c>scheme_id</c> is a
/// per-database fact that must stay isolated per connection.
///
/// It is <b>self-healing</b>: assembly loads (including into plugin <c>AssemblyLoadContext</c>s) bump a
/// generation counter, and the index is rebuilt lazily on the next lookup. This removes the old
/// one-shot, per-domain snapshot that went stale whenever modules loaded after initialization.
/// </summary>
public static class ClrSchemeTypeIndex
{
    private static readonly ConcurrentDictionary<string, Type> _nameToType = new();
    private static readonly object _scanLock = new();
    private static long _assemblyGeneration;      // bumped on every assembly load in the process
    private static long _scannedGeneration = -1;  // generation captured at the last completed scan

    static ClrSchemeTypeIndex()
    {
        // Fires for assemblies loaded into ANY load context (default + plugin ALCs) in .NET Core.
        AppDomain.CurrentDomain.AssemblyLoad += static (_, _) => Interlocked.Increment(ref _assemblyGeneration);
    }

    /// <summary>
    /// Resolve a scheme name (or alias) to its CLR type, or <c>null</c> if no <see cref="RedbSchemeAttribute"/>
    /// type carries that name (a legitimately non-generic scheme).
    /// </summary>
    public static Type? Resolve(string schemeName)
    {
        if (string.IsNullOrEmpty(schemeName)) return null;
        EnsureFresh();
        return _nameToType.TryGetValue(schemeName, out var t) ? t : null;
    }

    /// <summary>
    /// Explicitly register a name → type mapping (e.g. from an authoritative scheme sync). Idempotent;
    /// last write wins. Lets types resolve even before/without an assembly scan.
    /// </summary>
    public static void Register(string schemeName, Type type)
    {
        if (!string.IsNullOrEmpty(schemeName) && type != null)
            _nameToType[schemeName] = type;
    }

    /// <summary>Rescan loaded assemblies if any have loaded since the last scan. Cheap no-op when unchanged.</summary>
    public static void EnsureFresh()
    {
        if (Interlocked.Read(ref _scannedGeneration) == Interlocked.Read(ref _assemblyGeneration))
            return;

        lock (_scanLock)
        {
            var gen = Interlocked.Read(ref _assemblyGeneration);
            if (_scannedGeneration == gen) return;   // another thread already rescanned
            Rescan();
            // If an assembly loaded mid-scan, gen < newest → next EnsureFresh rescans again (no lost update).
            Volatile.Write(ref _scannedGeneration, gen);
        }
    }

    private static void Rescan()
    {
        foreach (var type in EnumerateSchemeTypes())
        {
            var attr = type.GetCustomAttribute<RedbSchemeAttribute>();
            if (attr == null) continue;

            var name = attr.GetSchemeName(type);
            if (!string.IsNullOrEmpty(name)) _nameToType[name] = type;     // upsert; never Clear (readers stay safe)
            if (!string.IsNullOrEmpty(attr.Alias)) _nameToType[attr.Alias!] = type;
        }
    }

    /// <summary>
    /// Single source of truth for assembly enumeration: broad (every load context), reflection-load-safe.
    /// Used by the index and reusable by auto-sync so both see the same set.
    /// </summary>
    public static IEnumerable<Type> EnumerateSchemeTypes()
    {
        foreach (var asm in AppDomain.CurrentDomain.GetAssemblies())
        {
            Type?[] types;
            try
            {
                types = asm.GetTypes();
            }
            catch (ReflectionTypeLoadException ex)
            {
                types = ex.Types;
            }
            catch
            {
                continue;
            }

            foreach (var t in types)
            {
                if (t != null && t.GetCustomAttribute<RedbSchemeAttribute>() != null)
                    yield return t;
            }
        }
    }

    /// <summary>Diagnostics: number of registered name keys.</summary>
    public static int Count => _nameToType.Count;

    /// <summary>Test/reset hook — clears the index and forces a rescan on next lookup.</summary>
    public static void Clear()
    {
        lock (_scanLock)
        {
            _nameToType.Clear();
            Interlocked.Exchange(ref _scannedGeneration, -1);
        }
    }
}
