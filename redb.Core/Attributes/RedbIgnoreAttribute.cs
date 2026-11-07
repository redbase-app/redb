using System;

namespace redb.Core.Attributes
{
    /// <summary>
    /// Excludes a property from the REDB schema (but not from JSON serialization)
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class RedbIgnoreAttribute : Attribute
    {
    }
}
