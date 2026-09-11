using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using redb.Core.Models.Contracts;
using redb.Core.Providers;

namespace redb.Core.Utils;

/// <summary>
/// V4 (L.3, LAZY plan §3.5): after deserialization every UNLOADED reference stub in a Props graph
/// gets the lazy loader attached, so the first access to its <c>Props</c> loads exactly that object —
/// whose own references are again stubs (the loader passes depth 1, §4.7). The walk descends only
/// into LOADED objects via their raw Props (never through the getter, which would trigger loading),
/// mirrors the shape of <c>CacheNestedObjects</c>, and tracks visited instances against cycles.
/// </summary>
public static class LazyReferenceInstaller
{
    /// <summary>
    /// V4 (review): after deserialization, every <c>RedbObject&lt;T&gt;</c> in the graph whose Props are
    /// null is marked NOT loaded. The Props setter marks any assignment as loaded, so a stub written
    /// by a foreign serializer as <c>"properties": null</c> would otherwise come back as a loaded
    /// object with nothing in it - and a parent save would write it as such (values deleted). The
    /// reader deserializes the whole subtree with the default converter, so the rule is applied by
    /// one walk from the root; it descends through loaded objects only, via their raw Props.
    /// </summary>
    public static void MarkUnloadedWherePropsAreNull(object? root)
    {
        if (root == null) return;
        var visited = new HashSet<object>(ReferenceEqualityComparer.Instance);
        MarkInto(root, visited);
    }

    private static void MarkInto(object? node, HashSet<object> visited)
    {
        if (node == null) return;
        var type = node.GetType();
        if (type.IsPrimitive || type.IsValueType || node is string) return;
        if (IsLeafCollection(node)) return;
        // A list item is a LEAF for every reflective walk: its lazy Object getter LOADS on read,
        // so walking its properties here fired a synchronous database load per item on every
        // materialization of an object with list-item fields (~150 phantom loads a second on a
        // production stand, 2026-09-09 - with not a single explicit .Object in the application).
        if (node is Models.Contracts.IRedbListItem) return;
        if (!visited.Add(node)) return;

        if (node is Models.Entities.RedbObject)
        {
            var props = type.GetMethod("GetPropsDirectly")?.Invoke(node, null);
            if (props == null)
            {
                // The non-generic RedbObject has no such field; the probe is null-safe.
                type.GetField("_propsLoaded")?.SetValue(node, false);
                return;
            }
            MarkInto(props, visited);
            return;
        }

        if (node is IDictionary dict)
        {
            foreach (DictionaryEntry e in dict) MarkInto(e.Value, visited);
            return;
        }

        if (node is IEnumerable seq)
        {
            foreach (var item in seq) MarkInto(item, visited);
            return;
        }

        if (type.Namespace?.StartsWith("System", StringComparison.Ordinal) == true) return;
        foreach (var p in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (!p.CanRead || p.GetIndexParameters().Length > 0) continue;
            object? value;
            try { value = p.GetValue(node); } catch { continue; }
            MarkInto(value, visited);
        }
    }

    /// <summary>Attach <paramref name="loader"/> to every unloaded reference stub under <paramref name="root"/>.</summary>
    /// <summary>
    /// The installer for <see cref="Caching.GlobalPropsCache"/>.Set (tsum pool incident,
    /// 2026-09-09): every stub keeps the writer's scoped loader, wrapped so that once the
    /// writer's scope dies the load falls through to the detached loader instead of the
    /// disposed-context guard. Cheap for the writer, safe for whoever outlives it.
    /// </summary>
    public static void InstallForCacheSet(IRedbObject? root, Providers.DetachedLazyPropsLoader detached)
    {
        if (root == null) return;
        InstallInto(root, prev => prev switch
        {
            Providers.DetachedLazyPropsLoader or Providers.ScopeFallbackLazyPropsLoader => null,
            null => detached,
            _ => new Providers.ScopeFallbackLazyPropsLoader(prev, detached),
        }, new HashSet<object>(ReferenceEqualityComparer.Instance));
    }

    public static void Install(IRedbObject? root, ILazyPropsLoader? loader)
    {
        if (root == null || loader == null) return;
        var visited = new HashSet<object>(ReferenceEqualityComparer.Instance);
        InstallInto(root, loader, visited);
    }

    /// <summary>
    /// Same walk from an arbitrary node - a Props object, a collection - for callers that hold no
    /// root (the detached loader re-attaching itself to the Props it has just loaded).
    /// </summary>
    public static void InstallInto(object? node, ILazyPropsLoader? loader)
    {
        if (node == null || loader == null) return;
        var visited = new HashSet<object>(ReferenceEqualityComparer.Instance);
        InstallInto(node, loader, visited);
    }

    /// <summary>
    /// True for a collection that can hold no reference at all - an array or a System generic
    /// collection whose element types are primitives, value types or strings (a <c>byte[]</c>
    /// payload above all). Walking such a collection element by element boxes every item for
    /// nothing; both graph walkers (this installer and the Pro stub collector) skip it.
    /// </summary>
    public static bool IsLeafCollection(object node)
    {
        var type = node.GetType();
        if (type.IsArray) return IsLeafType(type.GetElementType());
        if (type.IsGenericType && type.Namespace?.StartsWith("System.Collections", StringComparison.Ordinal) == true)
        {
            foreach (var arg in type.GetGenericArguments())
                if (!IsLeafType(arg)) return false;
            return true;
        }
        return false;
    }

    private static bool IsLeafType(Type? t)
        => t != null && (t.IsPrimitive || t.IsValueType || t == typeof(string) || t == typeof(byte[]));

    private static void InstallInto(object? node, ILazyPropsLoader loader, HashSet<object> visited)
        => InstallInto(node, prev =>
            prev is Providers.DetachedLazyPropsLoader && loader is not Providers.DetachedLazyPropsLoader
                ? null                                  // a shared cached graph never regains a scoped loader
                : prev is Providers.ScopeFallbackLazyPropsLoader && loader is not Providers.DetachedLazyPropsLoader
                    ? null                              // same rule for the wrapped writer loader
                    : loader,
            visited);

    private static void InstallInto(object? node, System.Func<ILazyPropsLoader?, ILazyPropsLoader?> choose, HashSet<object> visited)
    {
        if (node == null) return;
        var type = node.GetType();
        if (type.IsPrimitive || type.IsValueType || node is string) return;
        if (IsLeafCollection(node)) return;
        // A list item is a LEAF for every reflective walk: its lazy Object getter LOADS on read,
        // so walking its properties here fired a synchronous database load per item on every
        // materialization of an object with list-item fields (~150 phantom loads a second on a
        // production stand, 2026-09-09 - with not a single explicit .Object in the application).
        if (node is Models.Contracts.IRedbListItem) return; // byte[], int[], List<string>: nothing to attach, no boxing walk
        if (!visited.Add(node)) return;

        if (node is Models.Entities.RedbObject baseObj && node is IRedbObject redbObj)
        {
            // RedbObject<T> and TreeRedbObject<T> alike: the public _lazyLoader field and
            // GetPropsDirectly are inherited, so Type.GetField/GetMethod see them on the derived
            // type. The non-generic RedbObject has neither, and both probes are null-safe.
            if (!baseObj.IsPropsLoaded && redbObj.Id > 0)
            {
                // The stub: attach the loader, do not descend — there is nothing loaded to walk.
                // A detached loader (props cache, V4 review) is never replaced by a scoped one: the
                // shared cached graph must not carry the connection of whichever scope touched it last.
                var field = type.GetField("_lazyLoader");
                if (field == null) return;
                var next = choose(field.GetValue(node) as ILazyPropsLoader);
                if (next != null)
                    field.SetValue(node, next);
                return;
            }

            // A loaded object: walk its raw Props for deeper stubs. Raw access only —
            // the Props getter of a stub is a load.
            var props = type.GetMethod("GetPropsDirectly")?.Invoke(node, null);
            InstallInto(props, choose, visited);
            return;
        }

        if (node is IDictionary dict)
        {
            foreach (DictionaryEntry e in dict) InstallInto(e.Value, choose, visited);
            return;
        }

        if (node is IEnumerable seq)
        {
            foreach (var item in seq) InstallInto(item, choose, visited);
            return;
        }

        // A business class (the Props object itself or a nested class): walk its properties.
        if (type.Namespace?.StartsWith("System", StringComparison.Ordinal) == true) return;
        foreach (var p in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (!p.CanRead || p.GetIndexParameters().Length > 0) continue;
            object? value;
            try { value = p.GetValue(node); } catch { continue; }
            InstallInto(value, choose, visited);
        }
    }
}
