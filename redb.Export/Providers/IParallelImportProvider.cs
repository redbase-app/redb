namespace redb.Export.Providers;

/// <summary>
/// What a provider needs to offer for an import that writes over several connections at once.
///
/// The ordinary import is one connection and one transaction: it either lands whole or leaves the
/// database as it was. The parallel import trades that guarantee for speed. Rows go in over N
/// connections that commit on their own, and the secondary indexes of the large tables are dropped
/// before the load and built again after it, because maintaining them row by row is what makes a
/// large import slow. A parallel import that stops half-way leaves a partly loaded database; the
/// definitions of the dropped indexes are kept in the database itself so that the next run, or the
/// operator, can put them back.
/// </summary>
public interface IParallelImportProvider
{
    /// <summary>
    /// Opens one more connection to the same database, set up for loading: no transaction, and
    /// foreign key checks off for the whole session, since parents and children arrive in any order.
    /// </summary>
    Task<IDataProvider> OpenWorkerAsync(CancellationToken ct = default);

    /// <summary>
    /// Drops the secondary indexes of the large tables and returns the statements that create them
    /// again. The statements are stored in the database first, so a run that dies after this call
    /// loses no index definition; a later call returns the stored ones together with whatever it
    /// drops itself. Indexes that back a constraint (primary keys, UNIQUE) are left in place.
    /// </summary>
    Task<IReadOnlyList<DroppedIndex>> DropLoadIndexesAsync(CancellationToken ct = default);

    /// <summary>Creates one index from the statement <see cref="DropLoadIndexesAsync"/> returned.</summary>
    Task CreateIndexAsync(DroppedIndex index, CancellationToken ct = default);

    /// <summary>Removes the stored definitions once every index is back.</summary>
    Task ForgetDroppedIndexesAsync(CancellationToken ct = default);

    /// <summary>
    /// Switches off foreign key checks and triggers for the load where that is a setting of the
    /// database and not of a session. A provider whose workers do it for themselves in
    /// <see cref="OpenWorkerAsync"/> has nothing to do here.
    /// </summary>
    Task RelaxConstraintsForLoadAsync(CancellationToken ct = default);

    /// <summary>
    /// Undoes <see cref="RelaxConstraintsForLoadAsync"/> once the rows and the indexes are in, and
    /// has the database check the loaded rows against the foreign keys again.
    /// </summary>
    Task RestoreConstraintsAfterLoadAsync(CancellationToken ct = default);

    /// <summary>
    /// False when two index builds on the same table must not run side by side; the indexes of a
    /// table are then built one after another, and only different tables in parallel.
    /// </summary>
    bool BuildsIndexesOfOneTableConcurrently { get; }
}

/// <summary>An index dropped for the duration of a load, with the statement that creates it again.</summary>
/// <param name="Name">Index name.</param>
/// <param name="Table">Table it belongs to.</param>
/// <param name="Definition">Complete CREATE INDEX statement.</param>
public sealed record DroppedIndex(string Name, string Table, string Definition);
