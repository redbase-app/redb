using System;
using System.Collections.Generic;
using System.Data;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json.Serialization;

namespace redb.Core.Data
{
    /// <summary>
    /// What the row mappers of the providers (RedbRowMapper, SqlRowMapper) know about a type, built once per type:
    /// which column names reach which property, and a compiled setter per property. Each provider keeps its own
    /// conversions; this only replaces the per-cell work around them. A page of 100 objects of 86 values each read
    /// 8 600 rows of 15 columns, and per cell the mapper looked the column name up again, and set the property by
    /// reflection: about 50 of the 98 ms of the page.
    /// </summary>
    /// <remarks>
    /// Immutable once built (static readonly, initialised by the type initializer): safe to share between threads.
    /// Nothing is keyed by SQL text: the columns are bound to properties once per result set by <see cref="Bind"/>.
    /// </remarks>
    internal static class RedbRowMapping<T>
    {
        /// <summary>
        /// Column name -> property, case-insensitive, the rules of the mappers as they were: for every writable public
        /// instance property its JsonPropertyName (or snake_case name), the same with a leading underscore, the
        /// property name, its lowercase form, and the lowercase form with a leading underscore. A later property
        /// claiming a name already taken replaces the earlier one, as the dictionary assignment did before.
        /// </summary>
        public static readonly Dictionary<string, PropertyInfo> PropertyMap = BuildMap();

        private static readonly Dictionary<PropertyInfo, Slot> Slots = BuildSlots();

        /// <summary>The property of each column of <paramref name="record"/> by ordinal, null where no property takes it.</summary>
        public static Slot?[] Bind(IDataRecord record)
        {
            var slots = new Slot?[record.FieldCount];
            for (var i = 0; i < slots.Length; i++)
                if (PropertyMap.TryGetValue(record.GetName(i), out var property))
                    slots[i] = Slots[property];
            return slots;
        }

        private static Dictionary<string, PropertyInfo> BuildMap()
        {
            var map = new Dictionary<string, PropertyInfo>(StringComparer.OrdinalIgnoreCase);
            foreach (var prop in typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                if (!prop.CanWrite) continue;
                var jsonName = prop.GetCustomAttribute<JsonPropertyNameAttribute>()?.Name ?? ToSnakeCase(prop.Name);
                map[jsonName] = prop;
                map["_" + jsonName] = prop;
                map[prop.Name] = prop;
                map[prop.Name.ToLowerInvariant()] = prop;
                map["_" + prop.Name.ToLowerInvariant()] = prop;
            }
            return map;
        }

        private static Dictionary<PropertyInfo, Slot> BuildSlots()
        {
            var slots = new Dictionary<PropertyInfo, Slot>();
            foreach (var property in PropertyMap.Values)
                if (!slots.ContainsKey(property))
                    slots[property] = new Slot(property, CompileSetter(property));
            return slots;
        }

        /// <summary>
        /// (obj, value) => obj.Property = (PropertyType)value, or null where that cannot be compiled (an indexer, a
        /// setter the compiler refuses): such a property keeps the reflection setter.
        /// </summary>
        private static Action<T, object>? CompileSetter(PropertyInfo property)
        {
            var setter = property.GetSetMethod(nonPublic: true);
            if (setter == null || property.GetIndexParameters().Length > 0 || typeof(T).IsValueType)
                return null;
            try
            {
                var obj = Expression.Parameter(typeof(T), "obj");
                var value = Expression.Parameter(typeof(object), "value");
                var call = Expression.Call(obj, setter, Expression.Convert(value, property.PropertyType));
                return Expression.Lambda<Action<T, object>>(call, obj, value).Compile();
            }
            catch (Exception ex) when (ex is ArgumentException or InvalidOperationException or NotSupportedException)
            {
                return null;
            }
        }

        private static string ToSnakeCase(string pascalCase)
        {
            if (string.IsNullOrEmpty(pascalCase)) return pascalCase;
            var result = new System.Text.StringBuilder();
            for (var i = 0; i < pascalCase.Length; i++)
            {
                var c = pascalCase[i];
                if (char.IsUpper(c) && i > 0) result.Append('_');
                result.Append(char.ToLowerInvariant(c));
            }
            return result.ToString();
        }

        /// <summary>One property of <typeparamref name="T"/> as a column reaches it.</summary>
        internal sealed class Slot
        {
            private readonly Action<T, object>? _setter;

            internal Slot(PropertyInfo property, Action<T, object>? setter)
            {
                Property = property;
                TargetType = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;
                _setter = setter;
            }

            /// <summary>The property.</summary>
            public PropertyInfo Property { get; }

            /// <summary>The property type, or the underlying type of a Nullable property.</summary>
            public Type TargetType { get; }

            /// <summary>
            /// Sets the property. A value of exactly <see cref="TargetType"/> goes through the compiled setter; any other
            /// value through <see cref="PropertyInfo.SetValue(object, object)"/> as before, which keeps the conversions
            /// reflection makes on its own (a widening of a primitive, an enum from its underlying value) and its errors.
            /// </summary>
            public void Assign(T obj, object value)
            {
                if (_setter != null && value.GetType() == TargetType)
                    _setter(obj, value);
                else
                    Property.SetValue(obj, value);
            }
        }
    }
}
