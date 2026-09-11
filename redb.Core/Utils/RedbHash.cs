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
    /// Utility for computing the MD5 hash of an object. <c>_hash</c> covers the WHOLE object row:
    /// the header canon (see <see cref="HeaderCanon"/>) together with the Props content; a
    /// non-generic RedbObject (or a typed one without Props) hashes its header alone.
    /// </summary>
    public static class RedbHash
    {
        /// <summary>Canon of a null header value - distinct from an empty string.</summary>
        private const string NullMark = "\0";

        /// <summary>
        /// Canonical string of the object header: every column of the <c>_objects</c> row except
        /// the hash itself, the identity (<c>id</c>, scheme id) and the audit trio
        /// (<c>date_create</c>, <c>date_modify</c>, <c>who_change</c>). The audit trio is stamped
        /// on the database record AFTER the hash is computed and never written back into the
        /// object, so a hash carrying it could never be reproduced from a reloaded row - the
        /// cache and the ChangeTracking hash shortcut would be dead for every object. The scheme
        /// id is assigned after the batch recomputes hashes (every new object would miss its
        /// first hits), and the id is 0 until the save assigns it. Header dates canonicalise at
        /// second precision in UTC: sub-second digits do not survive every database (SQLite
        /// stores REAL Julian days, ~40 microseconds) and MSSQL keeps the offset while
        /// PostgreSQL returns UTC.
        /// </summary>
        public static string HeaderCanon(IRedbObject obj)
        {
            var inv = CultureInfo.InvariantCulture;
            var parts = new List<string>(16)
            {
                obj.ParentId?.ToString(inv) ?? NullMark,
                obj.OwnerId.ToString(inv),
                obj.Name ?? NullMark,
                obj.Note ?? NullMark,
                obj.Key?.ToString(inv) ?? NullMark,
                obj.ValueLong?.ToString(inv) ?? NullMark,
                obj.ValueString ?? NullMark,
                obj.ValueGuid?.ToString("N") ?? NullMark,
                obj.ValueBool?.ToString(inv) ?? NullMark,
                obj.ValueDouble?.ToString("R", inv) ?? NullMark,
                obj.ValueNumeric?.ToString("G29", inv) ?? NullMark,
                DateCanon(obj.ValueDatetime),
                obj.ValueBytes != null ? Convert.ToBase64String(obj.ValueBytes) : NullMark,
                obj.ValueUnique ?? NullMark,
                DateCanon(obj.DateBegin),
                DateCanon(obj.DateComplete),
            };
            return string.Join("|", parts);
        }

        private static string DateCanon(DateTimeOffset? value)
            => value?.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss", CultureInfo.InvariantCulture) ?? NullMark;

        /// <summary>
        /// Hash of the header alone (Object schemes without Props): a non-generic RedbObject or a
        /// RedbObject{TProps} with Props=null. The same canon the typed hash starts from.
        /// </summary>
        public static Guid ComputeForBaseFields(IRedbObject obj)
        {
            return new Guid(RedbMd5.ComputeHash(Encoding.UTF8.GetBytes(HeaderCanon(obj))));
        }

        /// <summary>
        /// Hash of any IRedbObject: header canon plus Props content.
        /// Returns null if there are no Props to hash (the callers fall back to
        /// <see cref="ComputeForBaseFields"/>).
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
                    return Combine(obj, ComputeForObject(propertiesValue));
                }
            }

            // If no Props - return null
            return null;
        }

        public static Guid? ComputeFor<TProps>(RedbObject<TProps> obj) where TProps : class, new()
        {
            return Combine(obj, ComputeForProps(obj.Props));
        }

        /// <summary>Header canon and the Props hash into one object hash; null Props hash stays null.</summary>
        private static Guid? Combine(IRedbObject obj, Guid? propsHash)
        {
            if (propsHash is null) return null;
            var payload = HeaderCanon(obj) + "||" + propsHash.Value.ToString("N");
            return new Guid(RedbMd5.ComputeHash(Encoding.UTF8.GetBytes(payload)));
        }

        public static Guid? ComputeForProps<TProps>(TProps props) where TProps : class, new()
        {
            return ComputeForObject(props);
        }

        /// <summary>
        /// Compute hash for arbitrary object via reflection.
        /// Returns null if object has no properties.
        /// </summary>
        private static Guid? ComputeForObject(object? obj)
        {
            // No object → no data to hash. The reflection-based ComputeFor(IRedbObject)
            // already returns null for a null Props (Props=null is a supported case —
            // see ComputeForBaseFields). This aligns the generic ComputeForProps path
            // with that contract instead of throwing NRE on obj.GetType().
            if (obj is null) return null;

            // A collection passed as the root (collection base-row hashes above all): reflecting
            // over the collection type's OWN properties degenerates the canon - List exposed only
            // Capacity|Count, long[] only Length&Co, so same-sized collections with different
            // content hashed equal (found by the F2 short-circuit, pinned by
            // ArrayBaseHash_DependsOnElementContent). Format the collection through the same
            // canon branches a collection FIELD takes, and hash that string.
            if (obj is System.Collections.IDictionary rootDict)
                return new Guid(RedbMd5.ComputeHash(Encoding.UTF8.GetBytes(FormatDictionaryCanon(rootDict))));
            if (obj is System.Collections.IEnumerable rootSeq && obj is not string)
                return new Guid(RedbMd5.ComputeHash(Encoding.UTF8.GetBytes(FormatEnumerableCanon(rootSeq))));

            var properties = obj.GetType()
                .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => !ShouldIgnoreForHash(p))  // Filter technical properties
                .ToArray();

            // If no properties - no data for hashing
            if (!properties.Any())
                return null;

            var ordered = properties
                .OrderBy(p => p.Name, StringComparer.Ordinal)
                .Select(p => SafeGetValue(p, obj));

            var payload = string.Join("|", ordered);
            var bytes = Encoding.UTF8.GetBytes(payload);
            var hash = RedbMd5.ComputeHash(bytes);
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
        /// FIX: Recursively hashes nested objects and arrays!
        /// </summary>
        private static string SafeGetValue(PropertyInfo property, object obj)
        {
            try
            {
                var value = property.GetValue(obj);
                if (value == null)
                    return "";

                var type = value.GetType();

                // Decimal: normalize to strip trailing zeros (6450.000 == 6450.000000000000000000)
                if (type == typeof(decimal))
                    return ((decimal)value).ToString("G29");

                // Primitives and simple types - just ToString
                if (IsPrimitiveOrSimple(type))
                    return value.ToString() ?? "";


                // V4 (L.2, LAZY plan §4.1): a nested RedbObject is a REFERENCE. The parent hashes
                // the reference's own persisted hash instead of walking its Props: walking would
                // (a) trigger lazy loading of the whole graph from inside a hash computation and
                // (b) make the eager and the lazy hash of the same parent differ (a stub carries
                // no Props). The hash field is present on stub and full object alike.
                if (value is IRedbObject nestedRef)
                    return $"{nestedRef.Id}:{nestedRef.Hash?.ToString("N")}"; // id = identity, hash = content-at-write

                // O-1/B-3 (owner's verdict): _values stores the dictionary item IDENTIFIER, and the
                // hash canon is the id ONLY. ListItem content lives its own life (extension objects);
                // hashing the content would move the owner's hash whenever a value in _list_items
                // is renamed.
                if (value is IRedbListItem listItemRef)
                    return listItemRef.Id.ToString();
                // Dictionaries — hash by CONTENT, order-independent. A Dictionary is an UNORDERED set of
                // key/value pairs: {a:1,b:2} equals {b:2,a:1}, so equal maps must hash equally. .NET does
                // not guarantee enumeration order (and it changes after removals), and the same logical
                // map is rebuilt in a different order when materialized from _values than when first
                // created — hashing it in enumeration order desynchronizes _objects._hash vs the cache.
                // Canonicalize by sorting "key=valueHash" pairs before hashing.
                if (value is System.Collections.IDictionary dictionary)
                    return FormatDictionaryCanon(dictionary);

                // Arrays and collections - hash each element (ORDER MATTERS here — lists/arrays are ordered)
                if (value is System.Collections.IEnumerable enumerable && type != typeof(string))
                    return FormatEnumerableCanon(enumerable);

                // Nested object (business class) - recursively hash!
                var nestedHash = ComputeForObject(value);
                return nestedHash?.ToString("N") ?? "";
            }
            catch
            {
                return "";
            }
        }

        /// <summary>
        /// Canonical string of a dictionary - order-independent (sorted "key=valueCanon" pairs).
        /// A Dictionary is an UNORDERED set of key/value pairs: {a:1,b:2} equals {b:2,a:1}, so
        /// equal maps must hash equally regardless of enumeration order.
        /// </summary>
        private static string FormatDictionaryCanon(System.Collections.IDictionary dictionary)
        {
            var entries = new System.Collections.Generic.List<string>(dictionary.Count);
            foreach (System.Collections.DictionaryEntry entry in dictionary)
            {
                var keyStr = entry.Key?.ToString() ?? "null";
                string valStr;
                if (entry.Value == null)
                    valStr = "null";
                else if (entry.Value is decimal decVal)
                    valStr = decVal.ToString("G29");
                else if (IsPrimitiveOrSimple(entry.Value.GetType()))
                    valStr = entry.Value.ToString() ?? "";
                else if (entry.Value is IRedbObject dictRef)
                    valStr = $"{dictRef.Id}:{dictRef.Hash?.ToString("N")}"; // V4 (L.2): reference by its hash
                else if (entry.Value is IRedbListItem dictListItem)
                    valStr = dictListItem.Id.ToString(); // O-1: ListItem canon is the id only
                else
                    valStr = ComputeForObject(entry.Value)?.ToString("N") ?? "null";
                entries.Add($"{keyStr}={valStr}");
            }
            entries.Sort(StringComparer.Ordinal);
            return $"{{{string.Join(",", entries)}}}";
        }

        /// <summary>
        /// Canonical string of an ordered collection - element canons joined IN ORDER
        /// (lists/arrays are ordered, so reorder changes the canon).
        /// </summary>
        private static string FormatEnumerableCanon(System.Collections.IEnumerable enumerable)
        {
            var elementHashes = new System.Collections.Generic.List<string>();
            foreach (var item in enumerable)
            {
                if (item == null)
                {
                    elementHashes.Add("null");
                }
                else if (item is decimal dec)
                {
                    elementHashes.Add(dec.ToString("G29"));
                }
                else if (IsPrimitiveOrSimple(item.GetType()))
                {
                    elementHashes.Add(item.ToString() ?? "");
                }
                else if (item is IRedbObject itemRef)
                {
                    elementHashes.Add($"{itemRef.Id}:{itemRef.Hash?.ToString("N")}"); // V4 (L.2)
                }
                else if (item is IRedbListItem itemListItem)
                {
                    elementHashes.Add(itemListItem.Id.ToString()); // O-1: ListItem canon is the id only
                }
                else
                {
                    // Recursively hash nested object
                    var itemHash = ComputeForObject(item);
                    elementHashes.Add(itemHash?.ToString("N") ?? "null");
                }
            }
            return $"[{string.Join(",", elementHashes)}]";
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
        /// Combines multiple hashes into single hash.
        /// Used for computing array hash from its element hashes.
        /// </summary>
        public static Guid CombineHashes(System.Collections.Generic.List<Guid> hashes)
        {
            if (hashes == null || !hashes.Any())
                return Guid.Empty;

            // Combine all hashes into string and compute MD5
            var payload = string.Join("|", hashes.Select(h => h.ToString("N")));
            var bytes = Encoding.UTF8.GetBytes(payload);
            var hash = RedbMd5.ComputeHash(bytes);
            return new Guid(hash);
        }
    }
}

