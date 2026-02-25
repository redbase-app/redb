using redb.Core.Models.Entities;
using redb.Core.Models.Contracts;
using redb.Core.Attributes;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Serialization;

namespace redb.Core.Utils
{
    /// <summary>
    /// Utility for computing MD5 hash from object properties.
    /// Supports both generic RedbObject{TProps} (hash from Props) and non-generic RedbObject (hash from base value_* fields).
    /// </summary>
    public static class RedbHash
    {
        /// <summary>
        /// Compute hash from base value_* fields of IRedbObject (for Object schemes without Props).
        /// Used for non-generic RedbObject or RedbObject{TProps} with Props=null.
        /// </summary>
        public static Guid ComputeForBaseFields(IRedbObject obj)
        {
            var parts = new List<string>
            {
                obj.ValueLong?.ToString() ?? "",
                obj.ValueString ?? "",
                obj.ValueGuid?.ToString() ?? "",
                obj.ValueBool?.ToString() ?? "",
                obj.ValueDouble?.ToString(CultureInfo.InvariantCulture) ?? "",
                obj.ValueNumeric?.ToString(CultureInfo.InvariantCulture) ?? "",
                obj.ValueDatetime?.ToString("O") ?? "",
                obj.ValueBytes != null ? Convert.ToBase64String(obj.ValueBytes) : ""
            };
            
            var payload = string.Join("|", parts);
            using var md5 = MD5.Create();
            return new Guid(md5.ComputeHash(Encoding.UTF8.GetBytes(payload)));
        }
        /// <summary>
        /// Compute hash for any IRedbObject - only from business data (Props).
        /// Returns null if no Props to hash.
        /// </summary>
        public static Guid? ComputeFor(IRedbObject obj)
        {
            // Find Props property via reflection
            var propertiesProperty = obj.GetType().GetProperty("Props");
            if (propertiesProperty != null)
            {
                var propertiesValue = propertiesProperty.GetValue(obj);
                if (propertiesValue != null)
                {
                    return ComputeForObject(propertiesValue);
                }
            }
            
            // If no Props - return null
            return null;
        }

        public static Guid? ComputeFor<TProps>(RedbObject<TProps> obj) where TProps : class, new()
        {
            return ComputeForProps(obj.Props);
        }

        public static Guid? ComputeForProps<TProps>(TProps props) where TProps : class, new()
        {
            return ComputeForObject(props); 
        }

        /// <summary>
        /// Compute hash for arbitrary object via reflection.
        /// Returns null if object has no properties.
        /// </summary>
        private static Guid? ComputeForObject(object obj)
        {
            var properties = obj.GetType()
                .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => !ShouldIgnoreForHash(p))  // ✅ Filter technical properties
                .ToArray();
                
            // If no properties - no data for hashing
            if (!properties.Any())
                return null;
            
            var ordered = properties
                .OrderBy(p => p.Name, StringComparer.Ordinal)
                .Select(p => SafeGetValue(p, obj));

            var payload = string.Join("|", ordered);
            using var md5 = MD5.Create();
            var bytes = Encoding.UTF8.GetBytes(payload);
            var hash = md5.ComputeHash(bytes);
            return new Guid(hash);
        }
        
        /// <summary>
        /// Checks if property should be ignored during hash calculation.
        /// </summary>
        private static bool ShouldIgnoreForHash(PropertyInfo property)
        {
            // Only RedbIgnore affects hash calculation. JsonIgnore is for JSON serialization.
            return property.GetCustomAttributes(typeof(RedbIgnoreAttribute), false).Length > 0;
        }
        
        /// <summary>
        /// Safe property value retrieval with exception handling.
        /// 🔥 FIX: Recursively hashes nested objects and arrays!
        /// </summary>
        private static string SafeGetValue(PropertyInfo property, object obj)
        {
            try
            {
                var value = property.GetValue(obj);
                if (value == null)
                    return "";
                
                var type = value.GetType();
                
                // Primitives and simple types - just ToString
                if (IsPrimitiveOrSimple(type))
                    return value.ToString() ?? "";
                
                // Arrays and collections - hash each element
                if (value is System.Collections.IEnumerable enumerable && type != typeof(string))
                {
                    var elementHashes = new System.Collections.Generic.List<string>();
                    foreach (var item in enumerable)
                    {
                        if (item == null)
                        {
                            elementHashes.Add("null");
                        }
                        else if (IsPrimitiveOrSimple(item.GetType()))
                        {
                            elementHashes.Add(item.ToString() ?? "");
                        }
                        else
                        {
                            // 🔥 Recursively hash nested object
                            var itemHash = ComputeForObject(item);
                            elementHashes.Add(itemHash?.ToString("N") ?? "null");
                        }
                    }
                    return $"[{string.Join(",", elementHashes)}]";
                }
                
                // 🔥 Nested object (business class) - recursively hash!
                var nestedHash = ComputeForObject(value);
                return nestedHash?.ToString("N") ?? "";
            }
            catch
            {
                return "";
            }
        }
        
        /// <summary>
        /// Checks if type is primitive or simple (does not require recursion).
        /// </summary>
        private static bool IsPrimitiveOrSimple(Type type)
        {
            return type.IsPrimitive ||
                   type.IsEnum ||
                   type == typeof(string) ||
                   type == typeof(decimal) ||
                   type == typeof(DateTime) ||
                   type == typeof(DateTimeOffset) ||
                   type == typeof(TimeSpan) ||
                   type == typeof(DateOnly) ||
                   type == typeof(TimeOnly) ||
                   type == typeof(Guid) ||
                   Nullable.GetUnderlyingType(type) != null;
        }
        
        /// <summary>
        /// 🔥 Combines multiple hashes into single hash.
        /// Used for computing array hash from its element hashes.
        /// </summary>
        public static Guid CombineHashes(System.Collections.Generic.List<Guid> hashes)
        {
            if (hashes == null || !hashes.Any())
                return Guid.Empty;
            
            // Combine all hashes into string and compute MD5
            var payload = string.Join("|", hashes.Select(h => h.ToString("N")));
            using var md5 = MD5.Create();
            var bytes = Encoding.UTF8.GetBytes(payload);
            var hash = md5.ComputeHash(bytes);
            return new Guid(hash);
        }
    }
}

