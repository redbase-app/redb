using System.Collections.Generic;

namespace redb.Core.Models
{
    /// <summary>Kinds of <see cref="RedbSchemaProblem"/>.</summary>
    public enum RedbSchemaProblemKind
    {
        /// <summary>
        /// A root field of a scheme exists more than once in <c>_structures</c>, and the copies could not be merged
        /// automatically because more than one holds values. Left by nodes that synchronized one scheme at the same
        /// moment before the schema lock (PostgreSQL, SQLite). Resolved by hand: UPGRADING.md, "Duplicate root
        /// structures".
        /// </summary>
        DuplicateRootStructure = 1,
    }

    /// <summary>
    /// A problem of the database schema that does not stop the service but needs a person: it is logged as an error at
    /// every start until it is resolved, and listed by <see cref="IRedbService.SchemaProblems"/> for health checks.
    /// </summary>
    public sealed class RedbSchemaProblem
    {
        /// <summary>Creates a problem.</summary>
        public RedbSchemaProblem(RedbSchemaProblemKind kind, long schemeId, string schemeName, string field,
            IReadOnlyList<long> structureIds, string message)
        {
            Kind = kind;
            SchemeId = schemeId;
            SchemeName = schemeName;
            Field = field;
            StructureIds = structureIds;
            Message = message;
        }

        /// <summary>What is wrong.</summary>
        public RedbSchemaProblemKind Kind { get; }

        /// <summary>Id of the scheme in <c>_schemes</c>.</summary>
        public long SchemeId { get; }

        /// <summary>Name of the scheme.</summary>
        public string SchemeName { get; }

        /// <summary>Name of the field concerned.</summary>
        public string Field { get; }

        /// <summary>Ids of the structures concerned, every copy of the field.</summary>
        public IReadOnlyList<long> StructureIds { get; }

        /// <summary>The text that is logged: what is wrong and where the fix is described.</summary>
        public string Message { get; }

        /// <inheritdoc />
        public override string ToString() => Message;
    }
}
