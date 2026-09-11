using redb.Core.Models.Configuration;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Core.Models.Security;

namespace redb.Core.Interception;

/// <summary>
/// Save-pipeline interceptor (discussion #12, items 3-4), deliberately shaped after EF Core's
/// <c>ISaveChangesInterceptor</c> so the behaviour is familiar:
/// <list type="bullet">
///   <item><see cref="SavingAsync"/> runs after the object graph is collected and BEFORE the
///   transaction opens; an exception thrown here CANCELS the save and propagates to the caller —
///   nothing has touched the database yet.</item>
///   <item><see cref="SavedAsync"/> runs after the transaction has committed; an exception
///   propagates to the caller (exactly like EF), but the data IS saved — the save cannot be
///   un-committed from here.</item>
/// </list>
/// Register any number via DI (<c>services.AddSingleton&lt;IRedbSaveInterceptor, ...&gt;()</c>);
/// they run in registration order. With none registered the pipeline pays nothing — the change
/// feed below is not even collected.
/// <para>
/// Both hooks run INSIDE the SaveAsync call, and one scope is one save (the CT-4 guard): an
/// interceptor that writes its journal as redb objects through the SAME service scope will trip
/// that guard deterministically. Write through a separate scope (scoped IRedbService from a
/// scope factory), or buffer and flush outside the save.
/// </para>
/// <para>
/// Audit and impersonation live here by design: the application sees every object (and under
/// ChangeTracking every value-level change) next to whatever pair of real/effective users it
/// tracks, and writes its own journal - the framework does not prescribe one.
/// </para>
/// <para>
/// Atomic audit (discussion #12 follow-up): by default the hooks do not share the save's
/// transaction (Saving runs before BEGIN so a veto needs no rollback; Saved runs after COMMIT
/// so a hook can never unwind it). When the audit row must be atomic with the write, wrap the
/// call in your own transaction:
/// <code>
/// await redb.Context.ExecuteAtomicAsync(async () =>
/// {
///     await redb.SaveAsync(obj); // the whole pipeline, SavedAsync included, runs inside
/// });
/// </code>
/// Inside that scope an insert from SavedAsync via <c>redb.Context.ExecuteAsync(...)</c> (a
/// plain flat audit table is the recommended shape) joins the same transaction and commits or
/// rolls back together with the save. The same works for DeleteAsync and DeletedAsync.
/// </para>
/// </summary>
public interface IRedbSaveInterceptor
{
    Task SavingAsync(RedbSavingContext context, CancellationToken cancellationToken = default)
        => Task.CompletedTask;

    Task SavedAsync(RedbSavedContext context, CancellationToken cancellationToken = default)
        => Task.CompletedTask;

    /// <summary>
    /// Before a hard delete executes (owner decision: deletes are intercepted too). An exception
    /// cancels the delete - nothing has been removed yet. Fires for the intent: some of the ids
    /// may turn out not to exist.
    /// </summary>
    Task DeletingAsync(RedbDeletingContext context, CancellationToken cancellationToken = default)
        => Task.CompletedTask;

    /// <summary>
    /// After a hard delete committed. An exception propagates, but the rows are gone.
    /// </summary>
    Task DeletedAsync(RedbDeletedContext context, CancellationToken cancellationToken = default)
        => Task.CompletedTask;
}

/// <summary>What a hard delete is about to remove.</summary>
public sealed class RedbDeletingContext
{
    /// <summary>Ids the caller asked to delete (the intent).</summary>
    public required IReadOnlyList<long> ObjectIds { get; init; }

    /// <summary>
    /// The object instances, when the caller had them (delete-by-object overloads); empty for
    /// delete-by-id. Database-side cascades (children) are not enumerated here.
    /// </summary>
    public required IReadOnlyList<IRedbObject> KnownObjects { get; init; }

    public required IRedbUser EffectiveUser { get; init; }
}

/// <summary>What a hard delete has removed.</summary>
public sealed class RedbDeletedContext
{
    public required IReadOnlyList<long> ObjectIds { get; init; }

    public required IReadOnlyList<IRedbObject> KnownObjects { get; init; }

    public required IRedbUser EffectiveUser { get; init; }

    /// <summary>Rows the delete statement reported removed (cascades not included).</summary>
    public required int DeletedCount { get; init; }
}

/// <summary>What a save is about to do. Handed to <see cref="IRedbSaveInterceptor.SavingAsync"/>.</summary>
public sealed class RedbSavingContext
{
    /// <summary>The objects the caller passed to SaveAsync.</summary>
    public required IReadOnlyList<IRedbObject> RootObjects { get; init; }

    /// <summary>The full collected graph: roots plus nested objects reached through Props.</summary>
    public required IReadOnlyList<IRedbObject> AllObjects { get; init; }

    /// <summary>The user the save is executed as (the effective user).</summary>
    public required IRedbUser EffectiveUser { get; init; }

    public required PropsSaveStrategy Strategy { get; init; }

    /// <summary>
    /// The objects this save CREATES - the ones that arrived without an id. At this point their
    /// Id is still 0 (EF temporary-key semantics: Saving runs before ids, hashes and the
    /// unchanged-set are computed, precisely so the interceptor MAY mutate Props and every
    /// derived artefact reflects the edit); the final ids arrive in
    /// <see cref="RedbSavedContext.NewObjectIds"/>. The exotic case of a hand-assigned id that
    /// does not exist in the database is inserted by the strategy but not listed here.
    /// </summary>
    public required IReadOnlyList<IRedbObject> NewObjects { get; init; }
}

/// <summary>What a save has done. Handed to <see cref="IRedbSaveInterceptor.SavedAsync"/>.</summary>
public sealed class RedbSavedContext
{
    public required IReadOnlyList<IRedbObject> RootObjects { get; init; }

    public required IReadOnlyList<IRedbObject> AllObjects { get; init; }

    public required IRedbUser EffectiveUser { get; init; }

    public required PropsSaveStrategy Strategy { get; init; }

    /// <summary>Ids of the root objects, in the order the caller passed them.</summary>
    public required IReadOnlyList<long> SavedIds { get; init; }

    /// <summary>Final ids of the objects this save created (<see cref="RedbSavingContext.NewObjects"/>).</summary>
    public required IReadOnlySet<long> NewObjectIds { get; init; }

    /// <summary>
    /// Ids of existing objects the hash shortcut (F1) excluded from the value pipeline - their
    /// content was unchanged and no value-level work happened. Null when the shortcut found
    /// nothing or is inactive. Computed after Saving (so it reflects Saving-time edits), hence
    /// it lives here and not in the Saving context.
    /// </summary>
    public IReadOnlySet<long>? UnchangedByHash { get; init; }

    /// <summary>
    /// Value-level changes the diff actually applied — ChangeTracking only (null under
    /// DeleteInsert, which rewrites everything and diffs nothing). The diff covers EXISTING
    /// objects: values of objects this save created go straight to insert and are not listed -
    /// read creations from <see cref="NewObjectIds"/> and the objects themselves. The list holds
    /// the diff's own working rows: treat it as a read-only snapshot.
    /// </summary>
    public IReadOnlyList<RedbValueChange>? Changes { get; init; }
}

public enum RedbValueChangeType
{
    Insert,
    Update,
    Delete,
}

/// <summary>
/// One value-level change from the ChangeTracking diff, flattened onto Core types (the tree
/// machinery itself is a Pro internal). Old is null for an insert, New for a delete.
/// </summary>
public sealed class RedbValueChange
{
    public required RedbValueChangeType Type { get; init; }

    /// <summary>
    /// The change in the model's terms: the property path as declared on Props —
    /// <c>"Title"</c>, <c>"Items[1].Price"</c>, <c>"Meta[m0]"</c>. What an audit journal prints;
    /// the raw rows below are for consumers that need the storage-level detail.
    /// </summary>
    public required string PropertyPath { get; init; }

    public RedbValue? OldValue { get; init; }

    public RedbValue? NewValue { get; init; }

    public long ObjectId => (NewValue ?? OldValue)?.IdObject ?? 0;
}
