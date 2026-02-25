using redb.Core.Data;
using redb.Core.Utils;
using System.Text.Json.Serialization;
using System.Reflection;

namespace redb.Core.Providers.Base
{
    /// <summary>
    /// Extensions for PostgresObjectStorageProvider under the new saving paradigm
    /// </summary>
    public static class ObjectStorageProviderExtensions
    {
        // ===== ✅ NEW METHODS FOR FIELD PROCESSING UNDER THE NEW PARADIGM =====

        /// <summary>
        /// Determine if a record should be created in _values based on value and _store_null
        /// </summary>
        internal static bool ShouldCreateValueRecord(object? rawValue, bool storeNull)
        {
            // If the value is not NULL - always create a record
            if (rawValue != null) return true;
            
            // If the value is NULL - create a record only if _store_null = true
            return storeNull;
        }

        /// <summary>
        /// Check if the type is a Class type (business class, not a primitive)
        /// </summary>
        internal static bool IsClassType(string typeSemantic)
        {
            // ✅ FIXED: Class type has Type1 = "Object" (looking at TypeSemantic from _types._type)
            // Business classes are mapped to type "Class" with _type="Object"
            return typeSemantic == "Object";
        }

        /// <summary>
        /// Check if the type is a RedbObject&lt;&gt; reference
        /// </summary>
        internal static bool IsRedbObjectReference(string typeSemantic)
        {
            return typeSemantic == "RedbObjectRow";
        }
    }
}
