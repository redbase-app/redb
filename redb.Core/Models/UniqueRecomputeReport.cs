using System.Collections.Generic;
using System.Linq;

namespace redb.Core.Models
{
    /// <summary>
    /// What a recomputation of <c>_values._unique</c> for one key structure found. Produced when a
    /// <c>[RedbUnique]</c> attribute appears on a populated structure, when the structure's encoder
    /// version is behind <c>UniqueKeyEncoder.Version</c>, when rows are found with a value but no key
    /// (a SQL-side writer touched them), and on an explicit <c>RecomputeUniqueAsync</c>.
    ///
    /// <para>
    /// Duplicates are reported, not thrown: the first row of each duplicate group (lowest value id)
    /// keeps the key, the others stay with <c>_unique IS NULL</c> — outside the index, visible here,
    /// and to a query <c>WHERE _id_structure = … AND _unique IS NULL AND &lt;column&gt; IS NOT NULL</c>.
    /// Uniqueness is enforced for every row that has a key from this point on; the losers are the
    /// application's to resolve.
    /// </para>
    /// </summary>
    public sealed class UniqueRecomputeReport
    {
        public long SchemeId { get; init; }
        public string SchemeName { get; init; } = string.Empty;
        public long StructureId { get; init; }
        public string PropertyName { get; init; } = string.Empty;

        /// <summary>Root scalar rows of the structure that were examined.</summary>
        public int TotalRows { get; init; }

        /// <summary>Rows that received a key (rows with a NULL value get none and are not counted).</summary>
        public int HashedRows { get; init; }

        /// <summary>Groups of rows sharing one key; each group's first id kept the key.</summary>
        public IReadOnlyList<UniqueDuplicateGroup> Duplicates { get; init; } = [];

        public bool HasDuplicates => Duplicates.Count > 0;

        /// <summary>Rows left without a key because another row already holds it.</summary>
        public int LosingRows => Duplicates.Sum(g => g.ValueIds.Count - 1);
    }

    /// <summary>One set of rows whose values canonicalise to the same key.</summary>
    public sealed class UniqueDuplicateGroup
    {
        /// <summary>The colliding key.</summary>
        public System.Guid Key { get; init; }

        /// <summary><c>_values._id</c> of every row in the group, lowest first; the first kept the key.</summary>
        public IReadOnlyList<long> ValueIds { get; init; } = [];

        /// <summary><c>_objects._id</c> for the same rows, in the same order.</summary>
        public IReadOnlyList<long> ObjectIds { get; init; } = [];
    }
}
