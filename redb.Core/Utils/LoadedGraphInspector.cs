using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;

namespace redb.Core.Utils;

/// <summary>
/// V4 (review, owner decision 2026-09-01): the props-cache dirty guard, one level deeper.
///
/// <para>
/// The parent's own hash is composed of <c>id:hash</c> of its references (L.2), so an unsaved
/// in-memory edit INSIDE a loaded nested object does not move it: the root check of the cache sees
/// a clean object and would serve the graph with the phantom edit - the SetStatus-before-Save
/// incident, one level down. This walk visits the LOADED part of a cached graph (raw Props only,
/// stubs are neither touched nor woken) and asks each nested object whether its live content hash
/// still equals its own persisted hash; after the V4 hash fixes the two are equal by construction
/// for an unmodified object, so any drift means an unsaved edit somewhere in the graph.
/// </para>
/// </summary>
public static class LoadedGraphInspector
{
    /// <summary>
    /// True when a LOADED object under <paramref name="props"/> carries a live content hash that
    /// differs from its own persisted <c>hash</c> - an unsaved in-memory edit. Stubs and objects
    /// that were never persisted (no hash) are skipped.
    /// </summary>
    public static bool HasDirtyLoadedReference(object? props)
    {
        if (props == null) return false;
        var visited = new HashSet<object>(ReferenceEqualityComparer.Instance);
        return IsDirty(props, visited);
    }

    private static bool IsDirty(object? node, HashSet<object> visited)
    {
        if (node == null) return false;
        var type = node.GetType();
        if (type.IsPrimitive || type.IsValueType || node is string) return false;
        if (LazyReferenceInstaller.IsWalkLeaf(node)) return false; // a list item's Object getter loads on read
        if (!visited.Add(node)) return false;

        if (node is RedbObject baseObj && node is IRedbObject redbObj)
        {
            // A stub is served exactly as it was stored - nothing loaded, nothing to have drifted.
            if (!baseObj.IsPropsLoaded) return false;

            var nestedProps = type.GetMethod("GetPropsDirectly")?.Invoke(node, null);
            if (nestedProps == null) return false; // an object without Props has no content to compare

            // A never-persisted object (hash null) cannot be judged - descend into it instead.
            if (redbObj.Hash is { } persisted && RedbHash.ComputeFor(redbObj) is { } live && live != persisted)
                return true;

            return IsDirty(nestedProps, visited);
        }

        if (node is IDictionary dict)
        {
            foreach (DictionaryEntry e in dict)
                if (IsDirty(e.Value, visited)) return true;
            return false;
        }

        if (node is IEnumerable seq)
        {
            foreach (var item in seq)
                if (IsDirty(item, visited)) return true;
            return false;
        }

        if (type.Namespace?.StartsWith("System", StringComparison.Ordinal) == true) return false;
        foreach (var p in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (!LazyReferenceInstaller.IsWalkedProperty(p)) continue;
            var value = p.GetValue(node);
            if (IsDirty(value, visited)) return true;
        }
        return false;
    }
}
