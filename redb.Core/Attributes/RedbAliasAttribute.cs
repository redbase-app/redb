using System;

namespace redb.Core.Attributes
{
    /// <summary>
    /// Attribute for specifying a human-readable alias
    /// Applied to enum values (ListItem.Alias) and Props properties (Structure.Alias)
    /// </summary>
    [AttributeUsage(AttributeTargets.Field | AttributeTargets.Property, AllowMultiple = false)]
    public class RedbAliasAttribute : Attribute
    {
        public string Alias { get; }
        
        public RedbAliasAttribute(string alias)
        {
            Alias = alias ?? throw new ArgumentNullException(nameof(alias));
        }
    }
}

