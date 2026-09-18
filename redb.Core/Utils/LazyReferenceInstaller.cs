using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using redb.Core.Models.Contracts;
using redb.Core.Providers;

namespace redb.Core.Utils;

/// <summary>
/// V4 (L.3, LAZY plan §3.5): after deserialization every UNLOADED reference stub in a Props graph gets the lazy loader
/// attached, so the first access to its <c>Props</c> loads exactly that object — whose own references are again stubs (the
/// loader passes depth 1, §4.7). The walk descends only into LOADED objects via their raw Props (never through the getter,
/// which would trigger loading), mirrors the shape of <c>CacheNestedObjects</c>, and tracks visited instances against
/// cycles.
/// <para>
/// Owner decision 2026-09-15 (plan docs/V4/PROPS_CACHE_PROD_AND_TAILS_PLAN.md §4.1): a data object owns no connection. A
/// loader bound to one scope is never installed on a stub: the stub gets the scope-free loader of that loader's database,
/// which loads on the scope current for the reader, else on the materializing scope (the origin) while it lives. List items
/// are bound the same way. A cache marks its graph shared: its stubs and items lose the origin and load on the reader's
/// scope only.
/// </para>
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
        if (IsWalkLeaf(node)) return;
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
            if (!IsWalkedProperty(p)) continue;
            var value = p.GetValue(node);
            MarkInto(value, visited);
        }
    }

    /// <summary>
    /// Attach the loader of <paramref name="loader"/>'s database to every unloaded reference stub under
    /// <paramref name="root"/>, and bind every list item under it to that database and origin.
    /// </summary>
    public static void Install(IRedbObject? root, ILazyPropsLoader? loader)
    {
        if (root == null || loader == null) return;
        var visited = new HashSet<object>(ReferenceEqualityComparer.Instance);
        InstallInto(root, ForStubs(loader), visited);
    }

    /// <summary>
    /// Same walk from an arbitrary node - a Props object, a collection, a list item - for callers that hold no root.
    /// </summary>
    public static void InstallInto(object? node, ILazyPropsLoader? loader)
    {
        if (node == null || loader == null) return;
        var visited = new HashSet<object>(ReferenceEqualityComparer.Instance);
        InstallInto(node, ForStubs(loader), visited);
    }

    /// <summary>
    /// Marks a graph handed to a cache as shared by every scope: the objects, the stubs and the list items in it. A shared
    /// instance loads lazily on the reader's scope only - its stubs and items lose their origin - and, inside the reader's
    /// transaction, keeps nothing (owner decision 2026-09-15). Descends through loaded objects via their raw Props.
    /// </summary>
    public static void MarkShared(object? root)
    {
        if (root == null) return;
        MarkSharedInto(root, new HashSet<object>(ReferenceEqualityComparer.Instance));
    }

    private static void MarkSharedInto(object? node, HashSet<object> visited)
    {
        if (node == null) return;
        var type = node.GetType();
        if (type.IsPrimitive || type.IsValueType || node is string) return;
        if (node is Models.Entities.RedbListItem item)
        {
            item._isShared = true;
            item._origin = null;
            // The object the item already carries is shared with it.
            MarkSharedInto(item.LoadedObject, visited);
            return;
        }
        if (IsWalkLeaf(node)) return;
        if (!visited.Add(node)) return;

        if (node is Models.Entities.RedbObject obj)
        {
            obj._isShared = true;
            if (!obj.IsPropsLoaded)
            {
                var field = type.GetField("_lazyLoader");
                if (field?.GetValue(node) is AmbientLazyPropsLoader ambient)
                    field.SetValue(node, ambient.Shared);
                return;
            }
            MarkSharedInto(type.GetMethod("GetPropsDirectly")?.Invoke(node, null), visited);
            return;
        }

        if (node is IDictionary dict)
        {
            foreach (DictionaryEntry e in dict) MarkSharedInto(e.Value, visited);
            return;
        }

        if (node is IEnumerable seq)
        {
            foreach (var element in seq) MarkSharedInto(element, visited);
            return;
        }

        if (type.Namespace?.StartsWith("System", StringComparison.Ordinal) == true) return;
        foreach (var p in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (!IsWalkedProperty(p)) continue;
            MarkSharedInto(p.GetValue(node), visited);
        }
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

    /// <summary>
    /// True for a node no reflective walk of a loaded graph may descend into: a
    /// <see cref="IsLeafCollection"/> or a list item. A list item's lazy <c>Object</c> getter LOADS
    /// on read, so walking its properties fired a database load per linked item on every
    /// materialization of an object with list-item fields (~150 phantom loads a second on a
    /// production stand, 2026-09-09 - with not a single explicit .Object in the application).
    /// Every walker over loaded Props (this installer, the props-cache dirty guard, the cache
    /// collector, the Pro materialization walks) asks this one predicate, so the rule cannot drift.
    /// </summary>
    public static bool IsWalkLeaf(object node)
        => node is Models.Contracts.IRedbListItem || IsLeafCollection(node);

    /// <summary>
    /// True for a property a walk of a loaded graph reads: the set the scheme and the save take - public, not an
    /// indexer, not <c>[RedbIgnore]</c>. A getter's exception is not caught, as the save does not catch it (owner
    /// decision 2026-09-15); a computed property that is not data belongs under <c>[RedbIgnore]</c>.
    /// </summary>
    public static bool IsWalkedProperty(PropertyInfo property)
        => property.CanRead
           && property.GetIndexParameters().Length == 0
           && !Extensions.PropertyInfoExtensions.ShouldIgnoreForRedb(property);

    private static bool IsLeafType(Type? t)
        => t != null && (t.IsPrimitive || t.IsValueType || t == typeof(string) || t == typeof(byte[]));

    // A loader that names its database is bound to one scope: the stub gets that database's scope-free loader, with the
    // service of that scope as its origin - or the captive service that scope was lent to (RedbServiceBase.AsOrigin).
    private static ILazyPropsLoader ForStubs(ILazyPropsLoader loader)
        => loader is not AmbientLazyPropsLoader && loader.CacheDomain is { } domain
            ? AmbientLazyPropsLoader.For(domain, RedbServiceBase.ServiceOf(loader.ScopeContext)?.AsOrigin)
            : loader;

    private static void InstallInto(object? node, ILazyPropsLoader loader, HashSet<object> visited)
    {
        if (node == null) return;
        var type = node.GetType();
        if (type.IsPrimitive || type.IsValueType || node is string) return;
        if (node is Models.Entities.RedbListItem item)
        {
            // Bound once: an item never moves to another database. Its Object is never read here.
            if (loader.CacheDomain is { } domain)
                item._cacheDomain ??= domain;
            if (!item._isShared && loader is AmbientLazyPropsLoader { Origin: { } origin })
                item._origin ??= origin;
            return;
        }
        if (IsWalkLeaf(node)) return; // byte[], int[], List<string>: nothing to attach, no walk
        if (!visited.Add(node)) return;

        if (node is Models.Entities.RedbObject baseObj && node is IRedbObject redbObj)
        {
            // RedbObject<T> and TreeRedbObject<T> alike: the public _lazyLoader field and
            // GetPropsDirectly are inherited, so Type.GetField/GetMethod see them on the derived
            // type. The non-generic RedbObject has neither, and both probes are null-safe.
            if (!baseObj.IsPropsLoaded && redbObj.Id > 0)
            {
                // The stub: attach the loader, do not descend — there is nothing loaded to walk. A shared stub never
                // regains an origin.
                var installed = baseObj._isShared && loader is AmbientLazyPropsLoader ambient ? ambient.Shared : loader;
                type.GetField("_lazyLoader")?.SetValue(node, installed);
                return;
            }

            // A loaded object: walk its raw Props for deeper stubs. Raw access only —
            // the Props getter of a stub is a load.
            var props = type.GetMethod("GetPropsDirectly")?.Invoke(node, null);
            InstallInto(props, loader, visited);
            return;
        }

        if (node is IDictionary dict)
        {
            foreach (DictionaryEntry e in dict) InstallInto(e.Value, loader, visited);
            return;
        }

        if (node is IEnumerable seq)
        {
            foreach (var element in seq) InstallInto(element, loader, visited);
            return;
        }

        // A business class (the Props object itself or a nested class): walk its properties.
        if (type.Namespace?.StartsWith("System", StringComparison.Ordinal) == true) return;
        foreach (var p in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (!IsWalkedProperty(p)) continue;
            var value = p.GetValue(node);
            InstallInto(value, loader, visited);
        }
    }
}
