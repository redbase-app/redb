using System;
using redb.Core.Models.Entities;

namespace redb.Core.Utils
{
    /// <summary>
    /// Type tests for the RedbObject family. A <see cref="TreeRedbObject{TProps}"/> is a <see cref="RedbObject{TProps}"/>
    /// with tree navigation on top; a test for the exact generic type saw it as an object without Props, so a tree node
    /// was hashed by its header alone and never matched the hash its loaded twin recomputes - the props cache never
    /// served a node saved through CreateChildAsync (props cache review, 2026-09-16).
    /// </summary>
    public static class RedbObjectTypes
    {
        /// <summary>
        /// The closed <c>RedbObject&lt;TProps&gt;</c> type of <paramref name="type"/> - the type itself or one of its base
        /// types - or null for anything else (the non-generic <see cref="RedbObject"/>, plain classes, null).
        /// </summary>
        public static Type? GenericOf(Type? type)
        {
            for (var t = type; t != null; t = t.BaseType)
                if (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(RedbObject<>))
                    return t;
            return null;
        }
    }
}
