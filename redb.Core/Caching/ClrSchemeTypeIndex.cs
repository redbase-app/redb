using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using redb.Core.Attributes;
using redb.Core.Exceptions;

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
///
/// <para>
/// <b>Collectible ALC safety.</b> Entries hold <see cref="WeakReference{T}"/> to <see cref="Type"/>, never
/// a strong reference. A strong <c>Type</c> keeps its <c>AssemblyLoadContext</c> alive, so a static index
/// that held one would pin every hot-reloaded plugin module in memory forever. Dead entries are pruned
/// lazily on lookup. See docs/CACHING_FIXES_PLAN.md (C1).
/// </para>
/// </summary>
public static class ClrSchemeTypeIndex
{
    private static readonly ConcurrentDictionary<string, WeakReference<Type>> _nameToType = new();

    // Explicit [RedbScheme(Name = "...")] values claimed by more than one DISTINCT type (different
    // FullName). Two instances of the SAME FullName from different ALCs are a reload, not a conflict
    // (see Rescan). Recorded during the scan (best-effort, must not throw) and turned into a hard
    // failure by ThrowIfNameConflict at the deterministic point where the type is actually synced.
    private static readonly ConcurrentDictionary<string, (WeakReference<Type> Existing, WeakReference<Type> Conflicting)> _nameConflicts = new();

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
    /// type carries that name (a legitimately non-generic scheme). Prunes the entry if the type's ALC
    /// has been unloaded.
    /// </summary>
    public static Type? Resolve(string schemeName)
    {
        if (string.IsNullOrEmpty(schemeName)) return null;
        EnsureFresh();
        return TryResolveLive(schemeName);
    }

    private static Type? TryResolveLive(string schemeName)
    {
        if (!_nameToType.TryGetValue(schemeName, out var weak))
            return null;

        if (weak.TryGetTarget(out var type))
            return type;

        // The type's ALC was unloaded — drop the dead entry (but only if it is still THIS weak ref;
        // a concurrent Register may have replaced it with a live one).
        ((System.Collections.Generic.ICollection<KeyValuePair<string, WeakReference<Type>>>)_nameToType)
            .Remove(new KeyValuePair<string, WeakReference<Type>>(schemeName, weak));
        return null;
    }

    /// <summary>
    /// Explicitly register a name → type mapping (e.g. from an authoritative scheme sync). Idempotent;
    /// last write wins. Lets types resolve even before/without an assembly scan.
    /// </summary>
    public static void Register(string schemeName, Type type)
    {
        if (!string.IsNullOrEmpty(schemeName) && type != null)
            _nameToType[schemeName] = new WeakReference<Type>(type);
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
            if (!string.IsNullOrEmpty(name))
            {
                // Only explicit names are policed. Alias collisions have always been last-write-wins
                // (e.g. "Product" is declared in two unrelated projects) and tightening them would
                // break projects that never opted into explicit naming.
                if (!string.IsNullOrWhiteSpace(attr.Name) && TryGetLiveOwner(name, out var owner) && owner != type)
                {
                    // Same FullName from a different ALC = the SAME logical type reloaded (Tsak hot-swap),
                    // NOT a name conflict. Only distinct FullNames sharing one explicit Name are a real clash.
                    if (owner.FullName == type.FullName)
                    {
                        _nameConflicts.TryRemove(name, out _);  // clear a stale conflict left by the old instance
                    }
                    else if (owner.GetCustomAttribute<RedbSchemeAttribute>()?.Name == attr.Name)
                    {
                        _nameConflicts[name] = (new WeakReference<Type>(owner), new WeakReference<Type>(type));
                    }
                }

                _nameToType[name] = new WeakReference<Type>(type);     // upsert; never Clear (readers stay safe)
            }

            if (!string.IsNullOrEmpty(attr.Alias)) _nameToType[attr.Alias!] = new WeakReference<Type>(type);
        }
    }

    private static bool TryGetLiveOwner(string name, out Type owner)
    {
        owner = null!;
        return _nameToType.TryGetValue(name, out var weak) && weak.TryGetTarget(out owner!);
    }

    /// <summary>
    /// Throws when another <b>distinct</b> type has claimed the same explicit scheme name. Called from
    /// the sync path rather than from the scan itself: the scan is a best-effort warm-up that must not
    /// throw, but a duplicate name is fatal and has to surface deterministically, before anything is
    /// written. A conflict whose other party has since been unloaded (dead weak ref) is not reported —
    /// there is nothing to clash with anymore.
    /// </summary>
    public static void ThrowIfNameConflict(string schemeName, Type type)
    {
        EnsureFresh();

        if (!_nameConflicts.TryGetValue(schemeName, out var pair))
            return;

        // Both parties must still be alive AND one of them must be the type being synced.
        if (!pair.Existing.TryGetTarget(out var existing) || !pair.Conflicting.TryGetTarget(out var conflicting))
        {
            _nameConflicts.TryRemove(schemeName, out _);   // one side unloaded → no live conflict
            return;
        }

        if (existing == type || conflicting == type)
            throw new RedbSchemeNameConflictException(schemeName, existing, conflicting);
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

    /// <summary>Diagnostics: number of live registered name keys (dead weak refs excluded).</summary>
    public static int Count
    {
        get
        {
            var n = 0;
            foreach (var weak in _nameToType.Values)
                if (weak.TryGetTarget(out _)) n++;
            return n;
        }
    }

    /// <summary>Test/reset hook — clears the index and forces a rescan on next lookup.</summary>
    public static void Clear()
    {
        lock (_scanLock)
        {
            _nameToType.Clear();
            _nameConflicts.Clear();
            Interlocked.Exchange(ref _scannedGeneration, -1);
        }
    }
}
