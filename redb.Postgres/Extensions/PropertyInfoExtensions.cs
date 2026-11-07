using System.Reflection;
using System.Text.Json.Serialization;
using redb.Core.Attributes;

namespace redb.Postgres.Extensions
{
    /// <summary>
    /// Extensions for working with PropertyInfo
    /// </summary>
    internal static class PropertyInfoExtensions
    {
        /// <summary>
        /// Checks if property should be ignored by REDB
        /// </summary>
        /// <param name="property">Property to check</param>
        /// <returns>true if property should be ignored</returns>
        public static bool ShouldIgnoreForRedb(this PropertyInfo property)
        {
            return //property.GetCustomAttributes(typeof(JsonIgnoreAttribute), false).Length > 0 ||
                   property.GetCustomAttributes(typeof(RedbIgnoreAttribute), false).Length > 0;
        }
    }
}
