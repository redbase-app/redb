using System;

namespace redb.Core.Attributes
{
    /// <summary>
    /// Writes the free-form <c>_tags</c> marker of a scheme (on the Props class) or of a
    /// structure (on a property) at synchronisation. The column is a reserved slot for future
    /// and custom extensions: 450 characters, indexed on <c>_structures</c>, no semantics
    /// imposed by redb. WITHOUT the attribute synchronisation leaves the column untouched, so
    /// values written directly by applications and extensions survive every sync.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
    public sealed class RedbTagsAttribute : Attribute
    {
        /// <summary>The marker text, up to 450 characters (enforced in C# for provider parity).</summary>
        public string Tags { get; }

        public RedbTagsAttribute(string tags) => Tags = tags;
    }
}
