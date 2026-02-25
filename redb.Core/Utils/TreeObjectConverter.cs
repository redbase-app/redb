using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;

namespace redb.Core.Utils;

/// <summary>
/// Utility class for converting between RedbObject and TreeRedbObject types.
/// Provides static methods for type conversion and parent chain building.
/// </summary>
public static class TreeObjectConverter
{
    /// <summary>
    /// Convert RedbObject to TreeRedbObject preserving all properties.
    /// </summary>
    /// <typeparam name="TProps">Object properties type</typeparam>
    /// <param name="source">Source RedbObject</param>
    /// <returns>New TreeRedbObject with same data</returns>
    public static TreeRedbObject<TProps> ToTreeObject<TProps>(RedbObject<TProps> source) where TProps : class, new()
    {
        return new TreeRedbObject<TProps>
        {
            id = source.id,
            parent_id = source.parent_id,
            scheme_id = source.scheme_id,
            owner_id = source.owner_id,
            who_change_id = source.who_change_id,
            date_create = source.date_create,
            date_modify = source.date_modify,
            date_begin = source.date_begin,
            date_complete = source.date_complete,
            key = source.key,
            value_long = source.value_long,
            value_string = source.value_string,
            value_guid = source.value_guid,
            value_bool = source.value_bool,
            value_double = source.value_double,
            value_numeric = source.value_numeric,
            value_datetime = source.value_datetime,
            value_bytes = source.value_bytes,
            name = source.name,
            note = source.note,
            hash = source.hash,
            Props = source.Props
        };
    }

    /// <summary>
    /// Convert IRedbObject to TreeRedbObject (creates new instance with Props = new TProps if types don't match).
    /// </summary>
    /// <typeparam name="TProps">Target properties type</typeparam>
    /// <param name="source">Source object</param>
    /// <returns>New TreeRedbObject</returns>
    public static TreeRedbObject<TProps> ToTreeObject<TProps>(IRedbObject source) where TProps : class, new()
    {
        // If source is already RedbObject<TProps>, use direct conversion
        if (source is RedbObject<TProps> typedSource)
        {
            return ToTreeObject(typedSource);
        }

        // Otherwise copy base properties
        var result = new TreeRedbObject<TProps>
        {
            id = source.Id,
            parent_id = source.ParentId,
            scheme_id = source.SchemeId,
            owner_id = source.OwnerId,
            who_change_id = source.WhoChangeId,
            date_create = source.DateCreate,
            date_modify = source.DateModify,
            date_begin = source.DateBegin,
            date_complete = source.DateComplete,
            key = source.Key,
            value_long = source.ValueLong,
            value_string = source.ValueString,
            value_guid = source.ValueGuid,
            value_bool = source.ValueBool,
            value_double = source.ValueDouble,
            value_numeric = source.ValueNumeric,
            value_datetime = source.ValueDatetime,
            value_bytes = source.ValueBytes,
            name = source.Name,
            note = source.Note,
            hash = source.Hash,
            // Try to get Props if types match
            Props = (source as IRedbObject<TProps>)?.Props ?? new TProps()
        };

        return result;
    }

    /// <summary>
    /// Convert IRedbObject to ITreeRedbObject dynamically preserving actual Props type.
    /// Uses reflection to create correct TreeRedbObject generic type.
    /// </summary>
    /// <param name="source">Source object</param>
    /// <returns>ITreeRedbObject with correct generic type</returns>
    public static ITreeRedbObject ToTreeObjectDynamic(IRedbObject source)
    {
        var sourceType = source.GetType();

        // If source is generic RedbObject<TProps>
        if (sourceType.IsGenericType && sourceType.GetGenericTypeDefinition() == typeof(RedbObject<>))
        {
            var propsType = sourceType.GetGenericArguments()[0];
            var treeType = typeof(TreeRedbObject<>).MakeGenericType(propsType);
            var treeObj = Activator.CreateInstance(treeType) as ITreeRedbObject;

            // Copy base properties
            if (treeObj is TreeRedbObject treeBase && source is RedbObject redbBase)
            {
                CopyBaseProperties(redbBase, treeBase);
            }

            // Copy Props via reflection
            var propsProperty = sourceType.GetProperty("Props");
            var targetPropsProperty = treeType.GetProperty("Props");
            if (propsProperty != null && targetPropsProperty != null)
            {
                targetPropsProperty.SetValue(treeObj, propsProperty.GetValue(source));
            }

            return treeObj!;
        }

        // Fallback for non-generic RedbObject
        return new TreeRedbObjectWrapper(source);
    }

    /// <summary>
    /// Build Parent relationships for a collection of tree objects.
    /// Links each object's Parent property to its parent in the collection.
    /// Also populates Children collections.
    /// </summary>
    /// <typeparam name="TProps">Object properties type</typeparam>
    /// <param name="objects">Collection of tree objects</param>
    public static void BuildParentRelationships<TProps>(IEnumerable<TreeRedbObject<TProps>> objects) where TProps : class, new()
    {
        var dict = objects.ToDictionary(o => o.id, o => o);

        foreach (var obj in objects)
        {
            obj.Parent = null;
            obj.Children.Clear();

            if (obj.parent_id.HasValue && dict.TryGetValue(obj.parent_id.Value, out var parent))
            {
                obj.Parent = parent;
                parent.Children.Add(obj);
            }
        }
    }

    /// <summary>
    /// Build Parent relationships for a collection of polymorphic tree objects.
    /// Links each object's Parent property to its parent in the collection.
    /// Also populates Children collections.
    /// </summary>
    /// <param name="objects">Collection of tree objects</param>
    public static void BuildParentRelationships(IEnumerable<ITreeRedbObject> objects)
    {
        var dict = objects.ToDictionary(o => o.Id, o => o);

        foreach (var obj in objects)
        {
            obj.Parent = null;
            obj.Children.Clear();

            if (obj.ParentId.HasValue && dict.TryGetValue(obj.ParentId.Value, out var parent))
            {
                obj.Parent = parent;
                parent.Children.Add(obj);
            }
        }
    }

    /// <summary>
    /// Copy base properties from RedbObject to TreeRedbObject.
    /// </summary>
    private static void CopyBaseProperties(RedbObject source, TreeRedbObject target)
    {
        target.id = source.id;
        target.parent_id = source.parent_id;
        target.scheme_id = source.scheme_id;
        target.owner_id = source.owner_id;
        target.who_change_id = source.who_change_id;
        target.date_create = source.date_create;
        target.date_modify = source.date_modify;
        target.date_begin = source.date_begin;
        target.date_complete = source.date_complete;
        target.key = source.key;
        target.value_long = source.value_long;
        target.value_string = source.value_string;
        target.value_guid = source.value_guid;
        target.value_bool = source.value_bool;
        target.value_double = source.value_double;
        target.value_numeric = source.value_numeric;
        target.value_datetime = source.value_datetime;
        target.value_bytes = source.value_bytes;
        target.name = source.name;
        target.note = source.note;
        target.hash = source.hash;
    }

    /// <summary>
    /// Simple wrapper for non-generic IRedbObject to ITreeRedbObject.
    /// Used when source object type is unknown at compile time.
    /// </summary>
    private class TreeRedbObjectWrapper : TreeRedbObject, ITreeRedbObject
    {
        /// <summary>
        /// Original source object.
        /// </summary>
        public IRedbObject SourceObject { get; }

        public TreeRedbObjectWrapper(IRedbObject source)
        {
            SourceObject = source;
            id = source.Id;
            parent_id = source.ParentId;
            scheme_id = source.SchemeId;
            owner_id = source.OwnerId;
            who_change_id = source.WhoChangeId;
            date_create = source.DateCreate;
            date_modify = source.DateModify;
            date_begin = source.DateBegin;
            date_complete = source.DateComplete;
            key = source.Key;
            value_long = source.ValueLong;
            value_string = source.ValueString;
            value_guid = source.ValueGuid;
            value_bool = source.ValueBool;
            value_double = source.ValueDouble;
            value_numeric = source.ValueNumeric;
            value_datetime = source.ValueDatetime;
            value_bytes = source.ValueBytes;
            name = source.Name;
            note = source.Note;
            hash = source.Hash;
        }
    }
}
