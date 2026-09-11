using System;

namespace redb.Core.Attributes
{
    /// <summary>
    /// Declares a Props property a unique key within its scheme: no two objects of the scheme may
    /// hold the same value in it. Enforced by the database — a unique index over
    /// <c>_values (_id_structure, _unique)</c>, where <c>_unique</c> is the 128-bit hash of the
    /// value's canonical form (<see cref="Utils.UniqueKeyEncoder"/>) — and surfaced as
    /// <see cref="Exceptions.RedbUniqueViolationException"/>.
    ///
    /// <para>
    /// Allowed on: a scalar property — <c>string</c>, integers, <c>Guid</c>, <c>bool</c>,
    /// <c>double</c>/<c>float</c>, <c>decimal</c>, <c>DateTime</c>/<c>DateTimeOffset</c>/
    /// <c>DateOnly</c>, <c>byte[]</c> and their nullable forms — of the Props class or of a nested
    /// class reached without crossing a collection (S2); and on a whole nested class, array or
    /// dictionary property, where the key is the SUBTREE content (S1). Not on enums (stored as
    /// list-item references, no canonical value form), single <c>RedbObject</c> references, or
    /// members of collection ELEMENTS — element rows share one structure across every element of
    /// every object. Scheme synchronisation rejects a misplaced attribute with
    /// <see cref="Exceptions.RedbUniqueKeyDefinitionException"/>.
    /// </para>
    ///
    /// <para>
    /// <c>null</c> never participates: any number of objects may leave the key empty.
    /// </para>
    ///
    /// <para>
    /// The value is compared exactly, after Unicode NFC normalisation for strings: no trimming, no
    /// case folding. A key is a notion of the application; an application that wants
    /// case-insensitive or trimmed keys normalises them before saving — the same contract as a unique
    /// index in any database, and what EF Core and Hibernate do.
    /// </para>
    ///
    /// <para>
    /// Putting the attribute on a property of a scheme that already has data recomputes the column
    /// for every existing object at the next synchronisation and reports duplicates
    /// (<see cref="Models.UniqueRecomputeReport"/>) instead of failing start-up: duplicate rows stay
    /// outside the index until they are resolved.
    /// </para>
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
    public sealed class RedbUniqueAttribute : Attribute
    {
        /// <summary>
        /// On a COLLECTION property switches from the subtree-content key (the default) to
        /// ELEMENT keys: <see cref="UniqueScope.Collection"/> - no duplicate elements inside one
        /// collection; <see cref="UniqueScope.Scheme"/> - element values unique across the whole
        /// scheme. Meaningless (and rejected at synchronisation) on scalars and nested classes,
        /// whose bare attribute already carries the strongest reading.
        /// </summary>
        public UniqueScope Scope { get; set; } = UniqueScope.Default;
    }
}
