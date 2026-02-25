using System;

namespace redb.Core.Models
{
    /// <summary>
    /// Model for optimized SQL query result
    /// Contains scheme_id and object JSON data in one query
    /// </summary>
    public class SchemeWithJson
    {
        /// <summary>
        /// Object scheme ID
        /// </summary>
        public long SchemeId { get; set; }

        /// <summary>
        /// Object JSON data (result of get_object_json)
        /// </summary>
        public string JsonData { get; set; } = string.Empty;
    }

    /// <summary>
    /// Model for SQL query result when loading object children
    /// Contains object_id, scheme_id and JSON data in one query
    /// </summary>
    public class ChildObjectInfo
    {
        /// <summary>
        /// Object ID
        /// </summary>
        public long ObjectId { get; set; }

        /// <summary>
        /// Object scheme ID
        /// </summary>
        public long SchemeId { get; set; }

        /// <summary>
        /// Object JSON data (result of get_object_json)
        /// </summary>
        public string JsonData { get; set; } = string.Empty;
    }
}
