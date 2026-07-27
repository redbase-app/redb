using System;

namespace redb.Core.Attributes
{
    /// <summary>
    /// Attribute for configuring the REDB schema for a properties class.
    /// <para>
    /// By default the schema is named after the CLR type's <see cref="Type.FullName"/>. Set
    /// <see cref="Name"/> to pin an explicit name instead, decoupling the database identity of the
    /// schema from C# namespace/class refactoring.
    /// </para>
    /// </summary>
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public class RedbSchemeAttribute : Attribute
    {
        /// <summary>
        /// Explicit schema name in the database. When null or empty, the schema is named after the
        /// CLR type's <see cref="Type.FullName"/> — the legacy default.
        /// <para>
        /// <b>Setting this on an existing project renames the schema in the database</b> on the next
        /// sync: the scheme row is looked up by the explicit name, then by FullName, then by the short
        /// type name, and the first match is renamed in place (the scheme id, and therefore all objects
        /// and structures, are untouched). Because an older binary would not find the scheme under its
        /// new name and would create a second one, every consumer of the database must be updated
        /// together. See docs/SCHEME_EXPLICIT_NAME_PLAN.md.
        /// </para>
        /// <para>
        /// Must follow C# identifier rules — Latin letters, digits, <c>_</c>, <c>.</c> and <c>+</c>,
        /// no reserved words, 128 characters max. Validated before any SQL is issued; use
        /// <see cref="Alias"/> for human-readable titles.
        /// </para>
        /// <para>
        /// <b>Multi-node note.</b> The name (and alias) live in this attribute — in code — so changing
        /// them means a redeploy, which restarts every node and rebuilds each process's static scheme
        /// caches fresh. Cross-node consistency is achieved by that restart; there is no online rename
        /// on live nodes without restart, and it is not supported. See docs/CACHING_FIXES_PLAN.md (C2).
        /// </para>
        /// </summary>
        public string? Name { get; set; }

        /// <summary>
        /// Schema alias (human-readable name). Never used as the schema name — it is free-form and may
        /// contain spaces, punctuation and non-Latin letters. Synchronised from this attribute on every
        /// sync: changing it updates the database, removing it sets the alias back to null.
        /// </summary>
        public string? Alias { get; set; }

        /// <summary>
        /// Parameterless constructor
        /// </summary>
        public RedbSchemeAttribute()
        {
        }

        /// <summary>
        /// Constructor with alias. The positional argument is <b>always</b> the alias, never the schema
        /// name — an explicit name can only be set through the <see cref="Name"/> named parameter, so
        /// existing declarations cannot change meaning by accident.
        /// </summary>
        /// <param name="alias">Schema alias</param>
        public RedbSchemeAttribute(string alias)
        {
            Alias = alias ?? throw new ArgumentNullException(nameof(alias));
        }

        /// <summary>
        /// Get the schema name for the type: the explicit <see cref="Name"/> when set, otherwise the
        /// full class name (namespace.class).
        /// </summary>
        /// <param name="type">Class type</param>
        /// <returns>Schema name</returns>
        /// <remarks>
        /// Deliberately does no validation — this sits on hot paths (assembly rescan, cache warm-up,
        /// every sync). Explicit names are validated by
        /// <see cref="SchemeNameValidator"/> at the points that write to the database.
        /// </remarks>
        public string GetSchemeName(Type type)
        {
            return !string.IsNullOrWhiteSpace(Name)
                ? Name!
                : type.FullName ?? type.Name;
        }
    }
}
