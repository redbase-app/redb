using System;

namespace redb.Core.Attributes
{
    /// <summary>
    /// Attribute for configuring the REDB schema for a properties class
    /// The schema name is always equal to the class name
    /// </summary>
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public class RedbSchemeAttribute : Attribute
    {
        /// <summary>
        /// Schema alias (human-readable name)
        /// </summary>
        public string? Alias { get; set; }

        /// <summary>
        /// Parameterless constructor
        /// </summary>
        public RedbSchemeAttribute()
        {
        }

        /// <summary>
        /// Constructor with alias
        /// </summary>
        /// <param name="alias">Schema alias</param>
        public RedbSchemeAttribute(string alias)
        {
            Alias = alias ?? throw new ArgumentNullException(nameof(alias));
        }

        /// <summary>
        /// Get the schema name for the type (full name with namespace).
        /// </summary>
        /// <param name="type">Class type</param>
        /// <returns>Full class name (namespace.class) as schema name</returns>
        public string GetSchemeName(Type type)
        {
            return type.FullName ?? type.Name;
        }
    }
}
