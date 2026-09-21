# Changelog

All notable changes to RedBase will be documented in this file.
This changelog covers the **NuGet-published packages** only:

| Package | Edition |
|---------|---------|
| `redb.Core` | Free |
| `redb.Postgres` | Free |
| `redb.MSSql` | Free |
| `redb.SQLite` | Free |
| `redb.Export` | Free |
| `redb.Core.Pro` | Pro |
| `redb.Postgres.Pro` | Pro |
| `redb.MSSql.Pro` | Pro |
| `redb.SQLite.Pro` | Pro |
| `redb.Licensing` | Pro |
| `redb.CLI` | Tool |
| `redb.Templates` | Tool |

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [4.1.0] — 2026-09-21
### Added
- **Storage maintenance reports what a DBA needs, and marks what must never be dropped.** The
  maintenance facade grew on the request of redb.Tsak (2026-09-21), which builds a "what is going on
  in the database" page: an operator reads it and hands the findings to a DBA. Read-only by design -
  VACUUM, REINDEX and counter resets are deliberately not offered, because a button that locks a
  production table must not be one click away in a dashboard.
  - `IndexStatistics.Schema`: the schema of the table the index belongs to (null on SQLite). Two
    tables of the same name in different schemas used to be one indistinguishable row, so any
    grouping by table lied.
  - `IndexStatistics.Columns` and `IncludedColumns`: the key columns in index order, and on SQL
    Server the carried ones separately - without them a covering index is indistinguishable from a
    composite one, and "which fields is it on?" had no answer.
  - `IndexStatistics.IsSystemCritical` with `CriticalReason`, plus `IsPrimaryKey`,
    `IsUniqueConstraint`, `IsClustered`, `BacksForeignKey` and `IsRedbOwned`: an index of the redb
    schema, or one backing a key or a uniqueness constraint, must survive whatever its usage
    counters say. Some of them serve uniqueness or a key check and are never scanned, which is
    exactly how a well-meant cleanup removes them. The reason travels along, so a page can say
    which of the three it is: redb metadata, redb security, redb data.
  - `IMaintenanceProvider.GetTableStatsAsync()`: size, row estimate, the size of the table's
    indexes, when statistics were last refreshed, and on PostgreSQL dead rows and the last vacuum.
  - `IMaintenanceProvider.GetStatisticsWindowAsync()`: since when the usage counters have been
    counting (the last statistics reset, or the start of the instance) and whether the node is a
    replica. Without it "this index served no read" says nothing.
  - `IMaintenanceProvider.AnalyzeTableAsync(table, schema)`: refreshes the planner statistics of one
    table instead of the whole database, which runs for minutes on a real one. Every engine supports
    it, SQLite included. The name is checked against the catalogs and quoted by the dialect: it comes
    from a dashboard, and an identifier cannot be a query parameter.

### Fixed
- **`GetTableStatsAsync()` on a SQLite database nobody has analysed yet.** `sqlite_stat1` does not
  exist until the first ANALYZE, and a statement naming a missing table does not even prepare, so
  both forms of the query failed - including the size-less one, which named it as well. A fresh
  database is the state every quick-start image starts in, so a storage page answered an error while
  the ANALYZE button that would have fixed it sat on that same page (reported by redb.Tsak, 2026-09-21).
  The dialect now lists the forms of the query in decreasing capability - with `dbstat` and
  `sqlite_stat1`, with `sqlite_stat1` alone, with `dbstat` alone, with neither - and the first that
  runs wins. On a database never analysed the answer is what the engine knows: `EstimatedRows` null
  and `HasStatistics` false, which is exactly what that flag exists to say.

## [4.0.1] — 2026-09-18
### Added
- `IRedbScopeSource`, implemented by every `IRedbService`: `CreateScope()` opens a scope of the container the
  service came from and hands over its service of the same database; the holder disposes the scope.
  `CanCreateScope` says whether there is a container. For code that holds one service and needs one per unit
  of work (redb.Route calls outside an exchange). A container whose scopes resolve another database is refused.
- `IRedbService.BeginAccess()`: makes a service the scope lazy loads run on, for a block (UI event
  handlers, callbacks outside the flow that resolved the service). See "Lazy loads run on the reader's
  scope" under Fixed.
- `IRedbService.LoadLinkedObjectsAsync(items)`: loads the linked objects of many list items in one
  batch on the service's connection and publishes them on the items.
- `RedbServiceConfiguration.LazyLoadWithoutScope` (`Refuse` by default, or `FreshScope`): what a lazy
  load does when no live redb scope reads it.
- `ILazyPropsLoader.CacheDomain` (a default interface member): the database a scope-bound loader reads.
- The native SQLite extension is versioned like the PostgreSQL and SQL Server modules: `InitializeAsync`
  asks the loaded `redbsqlite` library for `pvt_module_version()` and refuses a build of another version,
  naming the file (`SqliteDialect.Query_PvtRequiredVersion()`, now 0.6.6). A stale library next to the
  application loaded silently and answered in an old shape.

### Fixed
- The README of the `redb.Templates` package described the options of an older template: `--db` without
  `sqlite`, and defaults of PostgreSQL and Free. The template defaults to SQLite and Pro; the README now says so
  and lists what the generated app does.
- `RedbSchemaOutdatedException` named a command the `redb` tool does not have (`redb schema upgrade-script`).
  It now names `redb schema --upgrade --provider <name>`.
- **Work inside a `TransactionScope` now belongs to that transaction on MSSQL, PostgreSQL and SQLite**
  (a redb.Route `.Transacted()` block among others). Inside a scope redb used to write past the
  transaction or fail:
  - MSSQL: a speculative `ROLLBACK` sent right after a connection opened detached it from the scope.
    Raw SQL through `redb.Context` autocommitted and survived an abandoned scope, and `SaveAsync`
    failed with "Cannot issue SAVE TRANSACTION when there is no active transaction".
  - PostgreSQL: a connection opened before the scope stayed out of it (`25P01 SAVEPOINT can only be
    used in transaction blocks`), and a second connection (the key generator, another DI scope)
    aborted the commit (`55000 prepared transactions are disabled`).
  - SQLite: the driver never takes part in `System.Transactions`, so everything autocommitted
    without an error.

  Now the transaction holds the connection. Every redb connection wrapper of the same database, in
  every DI scope, runs its commands on one connection per ambient transaction
  (`AmbientConnectionRegistry`). MSSQL and PostgreSQL open it inside the transaction and enlist. On
  SQLite redb runs `BEGIN IMMEDIATE` on it and commits or rolls it back with the scope, so the
  database write lock is held until the scope ends. Inside a scope the key generators take keys on
  that connection (a rollback leaves a gap in the sequence, never a duplicate), and the background
  key refill no longer inherits the caller's transaction. A lazy load runs on the reader's scope and
  so inside its transaction; only a load opened by `LazyLoadWithoutScope = FreshScope` reads committed
  state outside it. The speculative `ROLLBACK` is gone: the SqlClient pool resets a returned session itself.

  Behaviour to know:
  - Parallel branches in one transaction are not supported. Two commands at the same time on the
    transaction's connection (dependent clones, a parallel split) are refused with
    `InvalidOperationException`. Run the branches sequentially, or give each branch a transaction of
    its own.
  - A command after the transaction ended (rolled back because a branch failed, or timed out) is
    refused; it never runs outside the transaction.
  - An explicit redb transaction begun before the scope keeps its own connection, and
    `BeginTransactionAsync` inside a scope is still refused.
  - Two redb configurations of the same database that differ in connection parameters or session
    settings (lazy references, collation, case folding) do not share a transaction's connection: the
    first command of the second one is refused with `InvalidOperationException`, instead of running on
    a connection set up with the other configuration's settings. A connector with its own connection
    (for example redb.Route's `sql:`) to the same database in the same transaction is a second
    connection: SQL Server refuses it, PostgreSQL aborts the commit, SQLite writes outside the
    transaction. Raw SQL that belongs to the transaction goes through `redb.Context`.
  - Stores that resolve a DI scope of their own now take part in the caller's `TransactionScope`.
- **SQLite: disposing scoped services between `TransactionScope.Complete()` and `Dispose()`** no
  longer throws "The current TransactionScope is already complete".
- **`DeadlockRetryHelper` no longer retries inside an ambient `TransactionScope`.** A deadlock victim's
  transaction is already rolled back by the server (SQL Server 1205, PostgreSQL 40P01), so running the
  command again inside it failed with a second error (`25P02`, an aborted transaction) that replaced the
  deadlock itself. Inside an ambient transaction the original deadlock now leaves at once and the unit of
  work (a route's retry, the caller) runs the whole transaction again. Outside one, and under
  `TransactionScopeOption.Suppress`, the retry works as before.
- **Loading an object no longer loads the objects behind its list items.** `RedbListItem.Object` is
  lazy: reading it is a database load. Several walks over a loaded graph read every property of the
  list items they met, so each linked list item (`IdObject` set) in Props cost a load of its object
  that nobody asked for, and on Pro SQLite the load failed ("Error lazy loading Object for ListItem").
  The walks were Pro loads of Props with references (substituting the loaded references, nulling
  references to deleted objects, completing lazy reference stubs) and the props-cache check for
  unsaved edits in nested objects on every cache hit (Free and Pro, point, bulk and sync loads). A
  list item is now a leaf for every such walk, in one rule shared by all of them
  (`LazyReferenceInstaller.IsWalkLeaf`). Reading `Object` explicitly loads it as before.
- **Pro: the synchronous `Load<T>` builds the same object as `LoadAsync`, on the calling thread.** It
  used to fall back to the Free in-database JSON builder (`get_object_json`): on Pro SQLite, which has no
  such function, `Load<T>` and a first read of `RedbListItem.Object` failed ("no such function"; hidden
  whenever a Free SQLite registration in the same process had loaded the native extension), and on Pro
  MSSQL/PostgreSQL the result differed from the async load (list items lost their `IdObject`). The
  synchronous Props getter of a lazy reference stub ran its load through `Task.Run`, holding a
  thread-pool thread per touch on top of the blocked caller. Both now run the Pro materializer itself,
  with every database call synchronous on the calling thread: no `Task.Run`, no `Parallel.ForEach`, and
  a load that would go asynchronous is refused instead of blocked on. New synchronous members:
  `IRedbConnection`/`IRedbContext` `Query<T>` and `Execute`, `ISchemeSyncProvider.GetSchemeById`,
  `GlobalMetadataCache.ResolveClrType`.
- **Free: list items in Props keep `IdList` and `IdObject`.** The in-database JSON builders (PostgreSQL
  module 0.7.11, MSSQL module 0.2.16, SQLite native extension) wrote a list item with the key `idList`
  and without `id_object`, while the model reads `id_list` and `id_object`. Every Free load returned the
  list items in Props with `IdList = 0` and `IdObject = null`, so `RedbListItem.Object` had nothing to
  load. PostgreSQL and SQLite also built the whole linked object for every such item, under a key the
  deserializer ignores.

  Behaviour to know: the raw object JSON (`LoadJsonAsync`, a direct `get_object_json` call) now writes a
  list item as `{"id", "id_list", "value", "alias", "id_object"}`. `idList` is renamed and the nested
  `object` is gone; load the linked object by `id_object`. The SQLite extension must be the rebuilt one.
- **MSSQL Free: a reference to an object in the trash stays in its collection as null** (module 0.2.17).
  An array of references lost that element (`[kept, gone]` loaded as `[kept]`, shifting every index after
  it) and a dictionary of references lost the key, because the aggregate skipped the NULL returned for
  the trashed target. PostgreSQL, SQLite and Pro already returned `null` in its place; MSSQL now does too.
- **A list item's linked object loads typed on a node that has not cached its scheme yet.** On a fresh
  process or another cluster node, `RedbListItem.Object` and `GetObjectAsync` took the CLR type from the
  metadata cache only and, on a miss, returned an untyped `RedbObject<object>` built by
  `get_object_json` (on Pro SQLite, which has no such function, the load failed). The scheme is now
  resolved by its id, as the hand-out preload already did. `RedbServiceBase` implements
  `ISchemeSyncProvider.GetSchemeById`.
- **SQLite Free: a list item's linked object of a scheme without a CLR type loads.** The untyped load sent
  the PostgreSQL text of the `get_object_json` call, `::text` cast included, and SQLite failed to parse
  it. The linked-object loaders now take that SQL from the provider's dialect, like every other load.
- **Loads no longer read `[RedbIgnore]` properties or swallow a getter's exception.** The walks over a
  loaded graph (lazy-loader installation, the props-cache collector and its check for unsaved edits, the
  Pro reference substitution, the lazy reference stub collection) read every public property, including
  `[RedbIgnore]` ones, and ignored whatever a getter threw; the Props hash wrote an empty string for such
  a property. They now walk the property set the scheme and the save use: public, not an indexer, not
  `[RedbIgnore]`.

  Behaviour to know: a getter of a stored property that throws now fails the load and the hash, as it
  already failed the save. Mark a computed property that is not data with `[RedbIgnore]`.
- **A failed command no longer leaves a transaction behind on its connection** (trash review).
  When the SQL text of a command opened a transaction itself and then failed (a `SAVEPOINT` or
  `BEGIN` followed by an error, or a command timeout or cancel inside `BEGIN TRANSACTION`),
  nothing ever ended that transaction. On SQLite the pooled handle kept the database write lock,
  and every writer waited out its busy timeout ("database is locked") until the pool happened to
  hand that handle out again. On MSSQL the scope's later statements ran inside it and were rolled
  back with it: a save reported success and its row was gone. On PostgreSQL every later command of
  the scope failed with 25P02. The PostgreSQL, MSSQL and SQLite connection wrappers now roll such a
  transaction back before the exception leaves, unless a transaction of the wrapper or an ambient
  `TransactionScope` owns the connection; the original exception always propagates. SQLite also
  rolls back an unowned transaction before a handle returns to the pool.
- **Soft delete on MSSQL: `sp_mark_for_deletion` and `sp_purge_trash` run with
  `SET XACT_ABORT ON`** and open a transaction only when the caller has none (module 0.2.15). A
  timeout inside the mark used to leave `@@TRANCOUNT = 1` on the session. On SQLite the mark and
  purge batches no longer open a savepoint in their own text. On every provider the mark and each
  purge batch run inside one transaction, joining the caller's.
- **Purging a trash container whose objects are referenced** (PostgreSQL module 0.7.10, MSSQL
  module 0.2.15, SQLite dialect). A `RedbObject<T>` reference is a foreign key with no ON DELETE
  action, so a single referenced object failed the whole batch, the container stayed `running`
  and the background worker retried it every 30 minutes, forever. Now a reference held by an
  object that is itself in the trash is removed together with the purge, whatever order the
  containers are purged in. An object referenced by a live object is skipped and its reference is
  never nulled; everything else is purged, the container is marked `failed` (no longer claimed
  automatically), and `PurgeTrashAsync` throws the new `RedbObjectReferencedException` naming the
  referenced and the referencing objects. Remove or re-point those references, then purge the
  container again.
- **Background deletion worker:** a claim that fails no longer ends the poll cycle for every
  container after it, the scan and each claim use short scopes of their own, and a container that
  ends `failed` is logged as a warning and not retried.
- **An empty database is named as such at start-up.** `InitializeAsync()` without
  `ensureCreated: true` on a database that has no redb schema used to fail on the first
  start-up query with a raw driver error (`relation "_structures" does not exist`, `Cannot find
  the object "dbo._structures"`, `no such table: _schemes`) that said nothing about how to
  proceed. It now throws `RedbSchemaMissingException`, whose message names the two ways out -
  `InitializeAsync(ensureCreated: true)` / `RedbServiceConfiguration.EnsureCreated = true`, or
  the script from `GetSchemaScript()` for the schema owner - and reminds that an unexpectedly
  empty database is usually a wrong connection string. Schema creation stays opt-in by design.
  Pinned on all three providers against a throwaway empty database.
- **Props cache under load.** With `EnablePropsCache` a busy process slowed down several times while its
  idle database connections grew. Three causes:
  - Every cache hit checked the object hash and walked the object's loaded graph under one process-wide
    lock, so concurrent loads waited for each other with their connections open. The check now runs
    outside the lock.
  - An insert into a full cache sorted all entries under the write lock to evict one of them. A full cache
    now evicts the least recently used tenth of `PropsCacheMaxSize` at once, without sorting.
  - An object cached again kept the lifetime of its first insert: once `PropsCacheTtl` had passed, an
    object reloaded periodically missed on every load. A re-cached object now starts a new lifetime, and an
    expired entry is dropped when it is read.

  The service now passes its logger to the cache. The cache warns, at most once per 10 seconds, when the
  objects in use do not fit `PropsCacheMaxSize`, when checking a hit takes 50 ms or more, and when 100
  lookups find a live entry and serve none (typically a read model that does not reproduce the saved
  graph). The detached list-item load warning now counts loads per 10 seconds (200) instead of concurrent
  loads (20): loads that run one at a time drain the connection pool as well.

  Behaviour to know: the props cache no longer applies per-user quotas.
  `UserConfigurationProps.PropsCacheSize` is still stored and merged, but it does not limit the cache; the
  only limit is `PropsCacheMaxSize`.

- **SQLite: `dbVersion`, `GetDbVersionAsync()` and `dbSize` work.** The SQLite service sent the PostgreSQL
  text (`version()`, `pg_database_size(current_database())`) and failed with "no such function". It now
  returns `sqlite_version()` and the database file size in bytes (`page_count * page_size`), Free and Pro.
- **`dbSize` is in bytes on every provider.** The contract said megabytes; PostgreSQL returned bytes and
  MSSQL kilobytes. The contract now says bytes, and MSSQL multiplies its 8 KB pages by 8192.

  Behaviour to know: on MSSQL `dbSize` is now 1024 times larger than before.
- **A list of references, `List<RedbObject<T>>`, is saved the way an array of them is.** The save
  recognised arrays only and skipped the elements of a list:
  - a new object in the list was never saved, and the parent's save failed on the foreign key;
  - an edit to a loaded object in the list was silently lost when the parent was saved;
  - a reference by id got no hash, so the parent's stored hash never matched the loaded one and the
    object missed the props cache on every load.

  Any generic collection of `RedbObject` elements now counts, like an array. `AddNewObjectsAsync` also
  resolves the hashes of references by id before hashing, as `SaveAsync` does: the objects it created
  with references missed the props cache for the same reason.

  Behaviour to know: saving a parent now saves the loaded objects in its lists too, as it already did
  for arrays and single references.
- **Lazy loads run on the reader's scope: no hidden connections.** `RedbListItem.Object` and the Props
  of a reference stub used to open a fresh DI scope, and with it a pooled connection, for every read
  wherever the item or stub carried no loader of the right scope. That covered every list item
  materialized by Pro (the Pro service took a second `IListProvider` instance from DI, one without
  loaders) or from Free JSON, every reference inside an object served from the props cache, and every
  read after the loading scope ended. A data object now owns no connection: such a load runs on the live
  redb scope of whoever reads it (the scope that resolved `IRedbService` in the current flow, or a block
  in `using (redb.BeginAccess())`), on that scope's connection and transaction. Where no scope is current
  for the reader (a test fixture, a service kept in a field, a UI event handler), the scope that
  materialized the object or item answers, as long as it lives. An instance shared by a cache (the props
  cache, the list cache) is never bound to that scope, and neither is anything it loads or already carries.

  Behaviour to know:
  - When no scope is current for the reader and the materializing scope has ended, or when a shared
    instance of a cache is read with no scope current, the read throws
    `RedbLazyLoadScopeEndedException`, which names the ways out.
    `RedbServiceConfiguration.LazyLoadWithoutScope = FreshScope` opens a scope per such load instead, with
    a rate warning.
  - Code that reads objects from a cache outside the flow that resolved the service (UI event handlers in
    Blazor Server, WebAssembly, MAUI; callbacks) wraps those reads in `using (redb.BeginAccess())`.
  - An object from the props cache, or an item from the list cache, read inside a transaction sees that
    transaction's data and does not keep it, because the write behind it may roll back. Before, the read
    went to a separate connection and saw committed state.
  - Two reads of lazy data at the same time on one scope are refused, like any two concurrent commands
    on a scope.
  - An `IListProvider` injected from DI is now the service's own `ListProvider`.
- **A service resolved from the root provider lends lazy loads a fresh scope, not its connection.** Such
  a captive service (`provider.GetRequiredService<IRedbService>()` in a Program.cs, a service kept for the
  life of the process) is the reader for every flow that touches an object of the props cache or a list
  of the list cache, and parallel lazy loads of those instances all ran on its one connection: the second
  was refused as a concurrent command. Outside a transaction such a load now runs on a service of a fresh
  scope of the same container, on a pooled connection; what it reads is committed state, which a shared
  instance keeps. Inside the captive service's transaction the connection is the transaction, and a lazy
  load stays on it and reads what the transaction wrote, as before. A high rate of such loads is reported,
  as for `LazyLoadWithoutScope = FreshScope`. What a lent scope materializes belongs to the captive reader:
  the stubs under it name the captive service as their origin, not the scope that ended with the load, so
  they load the same way. `RedbServiceProviders.IsRoot(serviceProvider)` tells the root
  provider of a Microsoft DI container from a scope's.
- **Pro honours the Props depth of a query.** `WithPropsDepth(n)` on `Query<T>()` and `TreeQuery<T>()` was
  ignored by the Pro providers of all three databases: Props loaded to `DefaultMaxTreeDepth`, so references
  meant to stay stubs came back loaded, with the whole graph behind them.
- **Tree nodes keep `ValueUnique`.** Tree queries on Free returned nodes whose `ValueUnique` was null: the
  conversion of an object to a tree node dropped the key, and on PostgreSQL and SQL Server the tree query did
  not even select the column.
- **A reference without a hash loads its Props.** A reference built in code (`new RedbObject<T> { id = ... }`)
  passed to `LoadReferencesAsync` stayed without Props on Free while the props cache was on: the cache had no
  hash to match it by, and the object was dropped from the load. On Pro the same reference got empty Props
  marked as loaded, because Props are assembled by scheme and the reference carried none; its scheme is now
  read from the database.
- **`Regex.IsMatch` and `Regex.Replace` in filters on SQL Server and SQLite.** On SQL Server, Free and Pro,
  `Regex.IsMatch` was dropped from the query without a word and every row came back; `Regex.Replace` matched
  nothing on Free and was refused on Pro. They now translate to `REGEXP_LIKE` and `REGEXP_REPLACE`. On SQLite
  both failed: the Pro builder emitted PostgreSQL operators, and the Free extension refused them. The provider
  now registers `redb_regexp`, `redb_regexp_i` and `redb_regexp_replace` (.NET `Regex`) on every connection it
  opens, and both editions use them.

  Behaviour to know:
  - SQL Server needs version 2025 with compatibility level 170 for these functions; an older server refuses
    such a query by the function name. The SQL Server module version is 0.2.18.
  - A pattern runs on the engine of the database: POSIX on PostgreSQL, RE2 on SQL Server, .NET on SQLite.
  - The SQLite Free extension must be rebuilt for every platform.
- **A tree node is served from the props cache.** An object saved as `TreeRedbObject<T>` - every
  `CreateChildAsync`, every tree node saved directly - stored a header-only hash: the save recognised the
  exact generic type `RedbObject<>` and nothing derived from it, so the hash the loaded object recomputes
  (header and Props) never matched the stored one and every cache probe of such an object was a miss. The
  same test kept a tree node out of the cache after its save and out of the scheme auto-detection.

  Behaviour to know: a node saved before this fix carries the old hash until its next save; until then it
  loads from the database as before, and the next save stores the hash the cache matches.
- **A moved object is not served from the props cache with its old parent.** `MoveObjectAsync` updates the
  parent outside the save path and drops the object's hash in the database, but left the cached copy in
  place; with `SkipHashValidationOnCacheCheck` a load never asks the database and answered with the old
  parent until the entry expired.
- **Loading objects by a list of ids asks the database once, not once per id, and uses the props cache under
  `SkipHashValidationOnCacheCheck`.** The batch used to issue one `SELECT id, hash, scheme` per id before
  the cache probe, and with the flag it skipped the cache altogether and rematerialized every object. One
  query now resolves the hashes and schemes of the whole batch, in both modes. This is the road of the
  list-item preload and of `LoadLinkedObjectsAsync`.
- **The object hash sees sub-second changes and does not depend on the process culture.** The canon of a
  Props value was `ToString()`: a `DateTime` or `DateTimeOffset` lost its sub-second digits, and a `double`,
  `decimal`, `DateOnly` or `TimeOnly` took the decimal separator and date format of the current culture.
  Under the ChangeTracking save an object whose hash equals the stored one is not written, so a temporal
  property changed within the same second - a heartbeat, a "last seen" - was silently lost; and two nodes
  with different cultures hashed one object differently. The canon is now invariant: temporal values in
  UTC at millisecond precision (the precision every database keeps), numbers in the invariant culture.
  A `DateTime` hashes by its clock reading whatever its `Kind`, exactly as it is stored.

  Behaviour to know: the hash of every object with temporal or floating-point Props changes once. The
  first save of such an object after the upgrade runs the full diff (the ChangeTracking shortcut does
  not apply) and stores the new hash; the first props-cache probe of it misses. A change below one
  millisecond is not a change: the databases do not keep it.
- **SQLite: dates before 1899-12-30 and the temporal `MinValue`s round-trip.** The REAL Julian day of a
  `DateTime`, `DateTimeOffset` or `DateOnly` was computed through the OLE automation date (`ToOADate`),
  which is not linear: its zero stands for `DateTime.MinValue`, so a default date came back as
  1899-12-30, and a day before its epoch is encoded as sign and magnitude, so any time of day before
  1899-12-30 moved by a day - on the way in through the Pro writer and every filter value, and on the way
  out for rows the native extension wrote through SQLite's own `julianday()`. The day count is now linear
  in the tick count, truncated to the millisecond; for every instant from 1899-12-30 on it is bit for bit
  the number stored before, so existing rows and equality filters keep matching.
- **PostgreSQL: a trash purge no longer fails the container when another purger deleted its batch.**
  `purge_trash` marked the container 'failed' when a batch deleted nothing while objects remained, reading
  it as "every remaining object is referenced by a live object". A batch chosen here and deleted meanwhile
  by a concurrent purger - the background worker beside a caller's `PurgeTrashAsync`, another node -
  failed the container with nothing blocking it, the worker stopped claiming it, and the caller got
  `RedbObjectReferencedException` naming no referrer. 'failed' now means an empty batch; a batch that
  deleted nothing leaves the container 'running', and the purge goes on with the next one. PostgreSQL
  module 0.7.12, applied by `InitializeAsync` like every module upgrade. SQL Server counts the batch it
  chose and SQLite chooses and deletes under the write lock, so neither saw the race.
- **SQLite: a finished trash container is removed.** PostgreSQL and SQL Server remove the container in
  the purge function once it holds no objects; the SQLite purge statement ends with its result row and
  could not, so every completed container stayed in `_objects` for good, and `GetDeletionProgressAsync`
  kept answering 'completed' for it where the other providers answer null. The purge now removes it
  after the last batch.
- `RedbListItem.GetObjectAsync(cancellationToken)`: the token reaches the load. It was accepted and dropped
  on the way to the query, so a cancelled reader still ran the scheme lookup and the load.
- A scheme without a CLR type is resolved once per cache domain. Reading the object behind a list item of
  such a scheme loaded the scheme again on every read: a list of 200 items was 200 scheme queries.
- The props cache no longer takes a process-wide lock on every hit for the "found but never served"
  diagnostic; its counters are lock-free like the other diagnostics.
- Two hosts of one database that differ in `RedbServiceConfiguration.LazyLoadWithoutScope` are reported
  with a warning, once per database. The registration of the most recently constructed service wins, so
  which policy a lazy load with no live scope got depended on which host constructed a service last.
- **An edit made inside a transaction on the lazy reference of a loaded object is saved; the caches
  publish committed state only.** A props-cache Set marked the loaded graph shared at once, and a shared
  instance does not keep what one transaction saw - so inside a transaction the writer's own loaded object
  never kept its lazy references: `root.Props.Next.Props.Label = ...; SaveAsync(root.Props.Next)` saved a
  fresh reload and the edit was lost, and every read of `item.Object` of a cached list was a query. A Set
  inside a transaction (an explicit redb transaction or an ambient `TransactionScope`) now waits for its
  commit; until then the instances are the reader's own, and what a rolled-back transaction loaded never
  reaches the cache. Behaviour to know: inside one transaction the same object loaded twice is materialized
  twice - the cache answers only after the commit.
- **A saved object served from the cache loads its hand-made references.** The save cached the caller's
  graph as it was, and a reference written as `new RedbObject<T> { id = x }` carried no loader: a load
  served from the cache returned that very stub, and its `Props` came back null - no query, no exception.
  The save now gives the stubs of the saved graph the loader of its database before caching, as a load
  does.
- **A saved graph that is not a tree hashes references before their parents; a new object referenced
  twice is saved once.** A parent's hash carries "id:hash" of every reference, and the save hashed in the
  reverse of a pre-order walk that collects a target shared by two parents once, under the first: the
  second parent stored the target's stale hash for ever - a props-cache miss on every load. The same walk
  never deduplicated a new object: one instance referenced from two places was collected twice, given an
  id twice and inserted twice.
- The connection of an ambient transaction opens under a lock per transaction and database. One
  process-wide lock held the first open of every other transaction, on every database, behind a SQLite
  `BEGIN IMMEDIATE` waiting for its write lock.
- The save answers "is this a reference / a collection of references / a business class" once per type
  instead of reflecting over the type's interfaces for every property of every object, and caching a
  loaded graph no longer walks every nested object's subtree again to mark it shared - the root's Set
  marked the whole graph.
- **Saving a reference whose properties were never loaded is refused with
  `RedbUnloadedReferenceException`.** `SaveAsync(root.Props.Next)` where `Next` is a stub whose lazy load
  the getter did not keep (a shared instance inside a transaction), or a hand-written
  `new RedbObject<T> { id = x }`, saved whatever a fresh load returned - the caller's edits were lost
  silently - and cached the unloaded stub, whose validation on the next load was a lazy load inside the
  cache probe: a deadlock with the load that probes. The exception names the ways out: load the object
  (`redb.LoadAsync(id)`, `redb.LoadReferencesAsync(parent, p => p.Reference)`), edit and save the loaded
  one. Reading the `Props` of such a reference still answers null: plain System.Text.Json serialization
  of a graph reads the getter, and an exception there would fail every API response carrying a hand-made
  reference. A saved graph gives its hand-made references their loader, with or without the props cache.
  An object without Props - a scheme with no properties (a flag, a counter: the object lives in its base
  fields), an object saved with `Props = null` - is a loaded object, not a reference: what redb materializes
  as a root (`LoadAsync`, a query, a tree) is loaded by definition, on every provider; the refusal applies to
  a stub redb attached a lazy loader to (a depth boundary, a cache) that is still unloaded.
- The props cache never holds or serves an unloaded instance: a save caches loaded objects only, and a
  cache hit whose instance has no loaded Props is not served.
- **A shared instance keeps what a transaction loads while the transaction has written nothing, and
  forgets it if the transaction rolls back.** The rule "a shared instance does not keep what one
  transaction saw" guarded a narrow hazard - a transaction's own uncommitted write read back through a
  shared parent - at the price of the writer's edits in every ordinary flow: an object served from the
  cache, or loaded before the transaction, edited through its lazy reference inside the transaction
  (`root.Props.Next.Props.Label = ...; SaveAsync(root.Props.Next)`) saved a fresh reload. While the
  transaction has written nothing, what its connection reads is committed state, so the shared instance
  keeps it - stubs and list items alike - and drops it if the transaction ends without a commit. Once the
  transaction has written (a save, a delete, a tree move, a raw `Context.ExecuteAsync`, a bulk operation,
  a soft delete or purge), what it reads may be its own uncommitted work: the shared instance keeps nothing
  of it, and saving such a stub is refused with `RedbUnloadedReferenceException` instead of writing a
  fresh reload.

### Changed
- The `redb-console` template and `redb.Examples` resolve `IRedbService` from a scope (`provider.CreateAsyncScope()`)
  instead of from the container root: one service is one connection, and a scope per unit of work is the shape
  that carries over to hosts with many flows. A console works either way.
- **`Microsoft.Data.SqlClient` 7.0.3** (was 5.2.2) in `redb.MSSql` and `redb.Export`; `redb.MSSql.Pro`
  gets it through `redb.MSSql`. The 7.0 line supports .NET 8 and newer and ships `net8.0` and `net9.0`
  builds (`net10.0` uses the `net9.0` one). redb.Route, redb.Tsak and redb.Identity build and test on the
  same driver version.

  Behaviour to know:
  - Microsoft Entra ID authentication (`Authentication=Active Directory ...` in the connection string)
    is no longer in the driver package. An application that uses it adds
    `Microsoft.Data.SqlClient.Extensions.Azure`. The driver no longer brings `Azure.Core`, `Azure.Identity`
    and `Microsoft.Identity.Client`. redb does not use Entra ID itself.
  - The driver lines 5.x and 6.x are no longer supported. An application that pins
    `Microsoft.Data.SqlClient` below 7.0.3 gets a NU1605 package downgrade error and has to raise the pin.
  - The driver needs `Microsoft.IdentityModel.*` and `System.IdentityModel.Tokens.Jwt` 8.16.0 or newer.
    A lower pin of those packages in an application is a NU1605 downgrade as well. `redb.Licensing` references
    `System.IdentityModel.Tokens.Jwt` 8.16.0 (was 8.0.0), so `redb.Core.Pro`, `redb.Postgres.Pro` and
    `redb.SQLite.Pro` bring the same version without `redb.MSSql` in the graph.
- `UserConfigurationProps.PropsCacheSize` is obsolete and no longer merged into
  `EffectiveUserConfiguration`: the props cache has one process-wide limit,
  `RedbServiceConfiguration.PropsCacheMaxSize`, and no quota per user. The value is still stored with the
  configuration object; `EffectiveUserConfiguration.PropsCacheSize` reports the process limit for every
  user, sys included.
- `RedbLazyLoadScopeEndedException`: the list item form is `ForListItem(listItemId, objectId)`; the
  three-argument constructor with its `fromListItem` flag is gone.
- `ObjectStorageProviderBase.CollectAllObjectsRecursively` (protected) takes the set of instances already
  collected, next to the set of ids.
- New public helpers: `TransactionCompletion` (a provider transaction reports its outcome once; callbacks
  registered for it run with whether it committed), `CachePublication.AfterCommit` (runs a publication
  now or once the context's transaction commits).

### Removed
- `RedbListItem.SetGlobalObjectLoader`, `SetGlobalSyncObjectLoader`, `IsObjectLoaderAvailable`,
  `AttachObjectLoader`, `AttachSyncObjectLoader` and `HasObjectLoader`; `IListProvider.LinkedObjectLoader`
  and `LinkedObjectSyncLoader`; `DetachedLazyPropsLoader` and `ScopeFallbackLazyPropsLoader`; the scope
  factory parameter of `GlobalPropsCache.Initialize`. These bound items and stubs to one scope, or opened
  a fresh scope per load to make up for it (see "Lazy loads run on the reader's scope" above).
- `MemoryRedbObjectCache` constructor parameters `getUserIdFunc` and `getQuotaFunc`, its method
  `GetUserStatistics()` and the class `UserCacheStats`: the props cache has no per-user quotas any more
  (see "Props cache under load" above). The quota lookup ran synchronously on every insert, through a
  configuration service taken from the scope that initialized redb, and its failures were swallowed.
- Unreachable PostgreSQL SQL in the SQLite and Pro builders. `SqliteDialect.Query_HasAncestorTreeSql`,
  `Query_HasAncestorNormalSql` and `Query_HasDescendantSql` now refuse with `NotSupportedException`, like the
  other search functions SQLite does not have. The Pro SQL builders no longer translate `ArrayAny`,
  `ArrayEmpty` and the `ArrayCount…` comparisons, which no LINQ parser produces; such a comparison is refused
  instead of compiling to SQL (on SQLite, to PostgreSQL SQL).

## [4.0.0] — 2026-09-12

### Added
### Security
- **The default password hasher of a bare redb deployment is bcrypt now, not salted SHA256**
  (external security report). Every provider's stock user-provider factory used to hard-wire
  `SimplePasswordHasher` (SHA256+salt), and `IPasswordHasher` was not registered in DI at all,
  so `BcryptPasswordHasher` sat in the tree unreachable. Now: `AddRedb` registers
  `IPasswordHasher` as bcrypt via TryAdd (your own registration wins), the factories resolve it
  from DI, and the short user-provider constructors default to bcrypt too. Existing databases
  are safe: `BcryptPasswordHasher.VerifyPassword` recognizes both formats, so legacy SHA256+salt
  hashes keep authenticating and migrate to bcrypt naturally on the next password change.
  Pinned by PasswordHasherDefaultTests x3 (new hashes carry the `$2` bcrypt prefix; a legacy
  hash still validates and still rejects a wrong password).

### Added
- **Element-level uniqueness scopes: `[RedbUnique(Scope = ...)]` on collections** (S3 of the
  subtree-unique plan). `Scope = Collection` - no duplicate elements inside one object's
  collection (each element's canonical form is salted with the owning collection identity);
  `Scope = Scheme` - element values unique across every object of the scheme, globally
  probeable via `GetByUniqueAsync(p => p.Emails, value)`. Reference elements canonicalise by
  TARGET id - `Scope = Collection` on a collection of references reads as "no duplicate
  links". The scope lives in `_structures._unique_scope` (mirrored into the metadata cache)
  and is delivered by the versioned mechanism (PostgreSQL pvt 0.7.8, MSSQL pvt 0.2.13,
  SQLite user_version 9); the unique index itself is untouched. A scope change recomputes the
  structure's keys; `Scope` on a scalar or nested class is rejected at synchronisation.
  Pinned by eleven ElemKey scenarios in all six suites, including the change-tracking
  alignment path (a collection-scoped element added on update is re-keyed with the FINAL
  collection identity after base-row alignment - without that the salt encoded a discarded
  candidate id and the duplicate keyed past the stored element).
- **`_tags`: a free-form 450-character marker on `_schemes` and `_structures`** (reserved for
  future and custom extensions; indexed on structures, mirrored into the metadata cache,
  delivered by the same versioned step). `[RedbTags("...")]` on the Props class or a property
  writes it at synchronisation; a column WITHOUT the attribute is never touched by sync, so
  values written directly by applications survive. No semantics imposed by redb.
- **`[RedbUnique]` on a whole nested class, array or dictionary - SUBTREE keys** (S1 of the
  subtree-unique plan). The key is the CONTENT of the subtree: the canonical hash the storage
  already maintains on the base row, encoded through the same `UniqueKeyEncoder` formula as
  scalar keys, so "unique" reads as "no two objects of the scheme hold this exact content".
  Semantics follow the canonical forms: a dictionary is order-insensitive (`{a:1,b:2}` IS
  `{b:2,a:1}`), a list is ordered (`[1,2,3]` equals only `[1,2,3]`), references canonicalise
  by id (a collection of references reads as "unique SET of references"), an absent or EMPTY
  collection claims no key. `GetByUniqueAsync(p => p.Config, value)` accepts the subtree value
  itself and canonicalises it the way the save did (reference collections are outside the
  by-value lookup); `RecomputeUniqueAsync` repairs subtree keys from the stored base-row
  hashes. In classic SQL the closest shape is an indexed view or a trigger-maintained hash
  column; here it is one attribute. Pinned by seven Subtree scenarios in all six suites.
- **`[RedbUnique]` on scalars inside nested classes** (S2 of the subtree-unique plan). A key
  field of a nested class - reached from the Props root without crossing a collection - is now
  a legal unique key: same encoder, same index, same typed
  `RedbUniqueViolationException`. `GetByUniqueAsync` accepts the path both as a lambda
  (`p => p.Identity.Passport`) and as a dotted string; `RecomputeUniqueAsync` takes the same
  path for explicit backfill. A key inside an ELEMENT of a collection of classes stays
  rejected at synchronisation: those rows share one structure across every element of every
  object. The unique index `UIX__values__structure_unique` now covers keyed rows at ANY
  position (the old positional filter silently kept nested rows OUT of the index); the new
  form is delivered to existing databases by the versioned mechanism (PostgreSQL pvt 0.7.7,
  MSSQL pvt 0.2.12, SQLite user_version 8). Pinned by eight NestedKey scenarios in all six
  integration suites.
- **CancellationToken across the entire async API** (discussion #12, item 1). Every async
  method of the public contracts - storage (`IObjectStorageProvider`, ~46 methods), queries
  (`IRedbQueryable` and friends, grouped/windowed/projected included), schemes, trees, lists,
  permissions, users, roles, validation, the lazy loader, plus the `IRedbService` facade -
  now takes a trailing `CancellationToken cancellationToken = default`. Source-compatible:
  existing calls compile unchanged (binary-breaking, as everything in the V4 major). Where the
  last parameter is `params` (`LockForUpdateAsync`, the query/execute family of the contexts)
  the token rides a paired overload instead - `params` cannot be followed by anything.
  The semantics, not just the plumbing: cancellation surfaces as `OperationCanceledException`
  and nothing else; a cancelled `SaveAsync` rolls back completely (rollback and dispose always
  run on `CancellationToken.None` - an abort must land even on a cancelled token); the point of
  no return is the commit - the token is read for the LAST time right before `CommitAsync`, and
  the post-commit tail (cache updates, `SavedAsync`/`DeletedAsync` interceptors) never consults
  it again, so "cancelled" can never mean "but actually saved"; `DeadlockRetryHelper` checks the
  token before every attempt and cancels its backoff delay; the parallel ChangeTracking diff
  cancels through `ParallelOptions.CancellationToken` (one clean OCE instead of an
  `AggregateException` pile); the id generator honours the token only at the entry - a refill
  in flight feeds a shared cache and is never torn mid-way; the synchronous lazy `Props` getter
  stays token-free by design (`LoadPropsAsync` takes one). CancellationTests suite x6: every
  verb refuses a pre-cancelled token before the first write, an in-flight cancel tears down a
  server-side sleep (PG `pg_sleep`, MSSQL `WAITFOR`), and a cancelled batch save leaves zero
  rows - or commits whole, never in between.
- **`PurgeTrashAsync` joins the unified cancellation contract.** It used to return quietly on
  a cancelled token (a Cancelled progress report and a soft exit) - the one verb with its own
  cancellation dialect. Now it throws `OperationCanceledException` like everything else: the
  token gates every batch boundary and rides into the purge command itself. The farewell
  progress report with `PurgeStatus.Cancelled` still fires before the throw, so a UI knows
  where the purge stopped. Nothing is lost on cancel: each purge batch commits on its own and
  the remainder stays queued in the database - the background deletion worker (which now passes
  its stop token through, so host shutdown actually interrupts a long purge) or any retry picks
  it up.
- **`IRedbSaveInterceptor` - write-lifecycle interceptors, EF-shaped** (discussion #12,
  items 3-4; owner decision "EF behaviour is what developers know", deletes included by owner
  decision too). Four hooks with default implementations (subscribe to what you need):
  `SavingAsync` fires right after the object graph is collected and BEFORE ids, hashes and the
  unchanged-set are computed, so an interceptor MAY mutate Props (EF's SavingChanges contract;
  objects being created still show Id=0 - temporary-key semantics) and an exception CANCELS the
  whole save; `SavedAsync` fires after the commit - an exception propagates, but the data is
  saved; `DeletingAsync`/`DeletedAsync` apply the same rules around hard deletes (veto before
  the SQL, fact after; SoftDelete is not intercepted). Both save hooks run inside the SaveAsync
  call on the same scope - an audit journal written as redb objects goes through a separate
  scope (documented on the interface). Register any number via DI; they run in registration
  order; with no subscribers the pipeline pays nothing. Under ChangeTracking
  `RedbSavedContext.Changes` carries the diff the save actually applied (`RedbValueChange`:
  Insert/Update/Delete, a model-level `PropertyPath` - "Title", "Items[1].Price" - plus the old
  and new row) as a flat projection onto Core types, the tree machinery stays internal; under
  DeleteInsert it is null. Saving carries `NewObjects` (references, Id still 0), Saved their
  final `NewObjectIds` (works under both strategies) and `UnchangedByHash` from the F1 shortcut
  (computed AFTER Saving, so it reflects interceptor edits). This also closes impersonation
  tracking (item 3): the application sees the objects and the effective user next to whatever
  real/effective pair it tracks, and writes its own journal. SaveInterceptorTests suite x6
  (veto from Saving, propagation from Saved with the data kept, feed per strategy).
- **`RedbObject`: DebuggerDisplay, and `ToString()` = the canonical JSON** (discussion #12,
  item 2). The debugger shows a short "{name} (id=..., scheme=...)" line with no expanding and
  no database round-trips; `ToString()` returns the same contract `get_object_json(id)` does,
  through the same serializer - a lazy stub is never woken (base fields + `properties:null`).
  A serialization failure neither escapes ToString nor gets swallowed - it rides inside the
  fallback JSON.
- **`LoadJsonAsync(id, depth)` - the whole object as raw JSON, no CLR props type.** A public
  wrapper over the in-database materializer `get_object_json` (present on all three providers:
  a SQL function on PG/MSSQL, a C port inside the native extension on SQLite; the eager load
  path already uses it). Permission checks as in `LoadAsync` (`DefaultCheckPermissionsOnLoad`),
  not-found per `ThrowOnObjectNotFound`; the typed props cache is untouched (it is keyed by a
  CLR type, which does not exist here). Introduced for declarative Route-XML routes: a pure XML
  module with no assembly reads redb objects (`<redbGet id="…"/>`), but the method is an
  ordinary part of `IObjectStorageProvider`/`IRedbService`, callable from any code.

### Added
- **`IRedbService.Maintenance` - storage maintenance facade** (`IMaintenanceProvider`):
  `AnalyzeAsync` refreshes planner statistics with one call on every engine (PostgreSQL
  `ANALYZE`, MSSQL `sp_updatestats`, SQLite `PRAGMA analysis_limit; ANALYZE` - the limit
  comes from `MaintenanceAnalysisLimit`, default 1000), and `GetIndexStatsAsync` reads index
  health from the engine's catalogs into one honestly-nullable model (name, table,
  uniqueness, size, row estimate, usage counters, last use - null where an engine cannot
  say; SQLite reports no usage at all, MSSQL counters reset on restart, last-use needs
  PostgreSQL 16+). Explicit calls only - never a side effect of a save. Born out of the
  2026-09-10 incident where stale statistics after a bulk seed made a 30ms query run for a
  second. One implementation in the core; all provider variability lives in three dialect
  SQL texts normalized to a single column-alias contract.

### Fixed
- **`_hash` covers the whole object, and a props-cache hit no longer serves a stale header**
  (cluster review, 2026-09-11). `_hash` used to cover Props only; the header - `name`, `note`,
  `value_*`, `value_unique`, parent, owner, `date_begin/complete` - was written on every save
  and never hashed, so a save on another node could change it without moving the hash, and
  the point load (`LoadAsync` and its sync twin `Load`), answering from the cache, handed out
  a stale header with a valid hash. `RedbHash` now hashes the header canon together with the
  Props; only the hash itself, the identity (`id`, scheme id) and the audit trio
  (`date_create`, `date_modify`, `who_change`) stay out - they are stamped outside the hash
  computation and could never be reproduced from a reloaded row. Header dates canonicalise
  at second precision in UTC. The partial row updates that bypass the save path - a tree move,
  the trash - reset `_hash` to NULL so no cached copy outlives them. The cache probe stays
  the narrow `_id/_hash/_id_scheme` read and a hit still serves the cached object whole.
  Consequences: a header-only save no longer takes the ChangeTracking hash shortcut (the
  values are diffed, not rewritten); `value_unique` is now hashed - a key change invalidates
  the cache, which reverses the earlier P5 note below; hashes stored by earlier builds follow
  the old formula, so such an object misses the cache until its first save under this build
  (correctness is never at stake: a mismatch is a miss). Pinned by PropsCacheHeaderTests on
  all three providers, Free and Pro (a second ServiceProvider plays the other node), and by
  RedbHashHeaderCanonTests.
- **Tree loads carry `value_unique`** (found by the same pins). The tree conversions and
  the reflection row-to-object mapper of the Pro tree provider had not learned the V4 object
  key: `LoadTreeAsync`, `GetChildrenAsync`, `LoadPolymorphicTreeAsync` and the polymorphic
  children returned `ValueUnique = null` on every object. All row-to-object mapping now goes
  through one `RedbObjectRowExtensions` (six hand-written copies of the column list removed).
- **Scope teardown no longer races an in-flight command** (tsum garage report:
  `NpgsqlOperationInProgressException` on `ReleaseScopes()` after an HTTP → SEDA hand-off of
  `RedbListItem`; depending on timing the same race also HUNG the disposal outright, which
  the red pin caught on the un-fixed code). Every provider connection always carried a
  fail-fast guard against two concurrent commands - but teardown was exempt from it. The
  shared `CommandGate` (redb.Core) now gives commands and teardown ONE exclusion on all
  three providers: `DisposeAsync` waits for the in-flight command before touching the
  physical connection, a command entered after teardown began is refused with
  `ObjectDisposedException`, and the lazy list-item loader falls back to a detached
  (fresh-scope) load on that signal - so an item handed across an exchange boundary keeps
  working instead of dying with the scope. Pinned by ConnectionTeardownTests on all three
  providers; the reporter's workaround (passing ids instead of live items) is no longer
  required.
- **redb.Export carries the V4 unique-key hash and survives SQLite roundtrips** (owner
  finding). `_values._unique` was missing from the export entirely - an exported database
  came back with every `[RedbUnique]` key silently unenforced until the next scheme sync
  recomputed them. And on SQLite the uuid-semantic BLOB columns (`_users._hash`,
  `_schemes._structure_hash`, `_objects._hash`) were read through the driver's `GetGuid`,
  which byte-swaps an RFC-ordered BLOB, and written back as TEXT - a roundtrip corrupted
  every stored hash. All four columns now go through a provider seam (`GuidFromDb`/`GuidToDb`):
  PostgreSQL/MSSQL pass the native Guid, SQLite converts RFC-ordered BLOB(16) both ways
  (legacy TEXT still read). Genuine binary columns (`_value_bytes`, `_values._ByteArray`)
  never touch the seam. Old backups without the field import cleanly: keys stay NULL and the
  standing recompute trigger restores them on the next synchronisation. Verified by a
  SQLite-to-SQLite roundtrip: all four columns byte-identical, JSONL carries canonical guids.
- **MSSQL: bulk delete of values by list-item ids no longer scans the whole `_values` table**
  (E114 finding, 33s cold on a seeded 8.4M-row database). The `STRING_SPLIT` semi-join form
  cannot match the FILTERED index `IX__values__ListItem_not_null`, so the plan degraded to a
  full clustered scan; the method now sends a chunked parameterized `IN` list, which seeks the
  index. The other bulk deletes keep their `STRING_SPLIT` form on purpose - their indexes are
  unfiltered and seek fine. Pinned by MsSqlBulkDeletePlanTests over the actual cached plan.
- **XML documentation restored for `BeginTransactionAsync` and the isolation-level
  `ExecuteAtomicAsync` overloads** (owner finding). The BR-1 wave had inserted the
  doc comments double-spaced; a blank line splits a `///` block, so the compiler dropped the
  docs of eleven members from the shipped `redb.Core.xml` / provider XML files
  (`IRedbConnection`, `IRedbContext`, all three provider connections). Also removed two
  dangling half-blocks in `ISqlDialect` left over from deleted members.
- **Reflective walkers no longer wake `RedbListItem.Object`** (production stand, 2026-09-09).
  The lazy-loader install walk (and the nested-object cache walk) read every property of every
  business object - including the lazy `Object` of list-item fields, whose getter LOADS on
  read. Materializing anything with dictionary fields therefore fired a synchronous phantom
  load per item (~150/s on the stand, with not a single explicit `.Object` in application
  code). A list item is a leaf now: no reflective walk descends into it.
- **`NpgsqlDataSource` is owned by the container now** (factory registration instead of a
  pre-built instance). Disposing the service provider disposes the data source and closes its
  connection pool; the old form left every pool open forever, so container restarts (module
  hot-reload, test runs) stacked orphaned pools of idle sessions until the server-side pruner
  collected them minutes later.
- **FK-column indexes on `_values` (`_ListItem`, `_Object`) - partial, delivered by upgrade**
  (perf finding, 2026-09-10). Both columns carry foreign keys, and without a leading index
  every DELETE of a referenced object or list item scanned the whole table for the FK check:
  measured on an ~8.4M-row seed - SQLite 1.5-5.5s, PostgreSQL 5.9s, MSSQL 6.5-7s per check,
  milliseconds with the indexes. The partial form (`WHERE ... IS NOT NULL`) keeps them nearly
  empty - reference fields are a small fraction of rows - so writes barely pay, which is why
  the full-column ancestors of these indexes (retired for save throughput long ago) stay
  retired. Fresh databases get them from the base DDL; existing ones through each provider's
  versioned upgrade (PostgreSQL pvt 0.7.6, MSSQL pvt 0.2.11, SQLite schema version 7), pinned
  by a delivery test on all six suites.
- **`SqliteDataSource` is disposable and owned by the container now** (the SQLite counterpart
  of the `NpgsqlDataSource` fix). Microsoft.Data.Sqlite pools are static, keyed by connection
  string, and a pooled handle keeps the database FILE open - on Windows, locked. Disposing the
  service provider now clears the pool of its connection string, so a dead container (test
  host teardown, module hot-reload) releases the file instead of locking it until process
  exit.
- **`RedbListItem.Object`: the sync getter no longer seizes the process** (tsum production
  freeze, 2026-09-09; reproduced locally: 8 touches of one shared item on a starved thread
  pool froze the process for 90+ seconds with zero exceptions - even timeout continuations
  had no thread to run on). Two mechanisms removed: the lock was held ACROSS the load, so
  every toucher of a shared item (items live in the process-wide list cache) parked behind
  the first one; and Task.Run needed a SECOND pool thread per touch on top of the one blocked
  in GetAwaiter().GetResult(). The getter now loads outside any lock and without Task.Run:
  one blocked thread per touch, concurrent touchers proceed independently, the first
  published result wins (a racing duplicate load is harmless). Same scenario after the fix:
  228ms, all touches parallel. The getter remains a synchronous convenience that blocks a
  thread - hot paths should prefer `IdObject` plus an explicit load; the detached-borrow
  diagnostics point at offenders by stack.
- **`RedbListItem.Object`: the sync getter is thread-pool-free now** (the completion of the
  fix above). The getter used to block over the async load chain, so the parked thread waited
  for pool-scheduled continuations - on a saturated pool that meant degradation, and on a
  hard-capped one a total freeze. The whole lazy load now has a synchronous twin down to
  ADO.NET: sync `ExecuteScalar`/`QueryFirstOrDefault`/`ExecuteJson` on every provider
  connection, a sync `Load` on the object storage (same permission check, cache discipline and
  get_object_json path as the async one on every tier), and a sync linked-object loader wired
  through the list provider and the process-wide fallback. Loader preference is specificity
  first, then transport: item-sync, item-async, global-sync, global-async. A/B on the
  starvation stand (pool hard-capped at 4, 8 touches of one shared item): the blocking-over-
  async path freezes so completely that even its own watchdog timer never fires; the sync path
  completes in ~150ms. Custom `IRedbConnection`/`IObjectStorageProvider` implementations keep
  compiling - the new members default to blocking on the async forms.
- **ChangeTracking: a failed save no longer poisons the next saves of its scope** (Identity
  backfill report, 2026-09-10). The diff parks its DELETE/INSERT rows in pending channels on
  the provider instance and the flush cleared them only AFTER each bulk call - so a save that
  failed mid-flush (a legitimate unique violation, a deadlock retry, a cancellation) left its
  rows behind, and every following save of the scope re-sent the FAILED object's values inside
  its own batch. Observable as a serial poisoning: after one legitimate duplicate, unrelated
  saves die one after another on the failed value's unique hash while their own Props are
  intact. The channels are reset at the start of every batch save now; the same reset also
  stops a deadlock retry from doubling the channels. Affected every provider under
  ChangeTracking (DeleteInsert does not use the channels).
- **Props cache: the graph is detached at the hand-OUT, not on Set** (tsum production
  incident). Putting an object into the cache used to swap its lazy loaders to the detached
  loader immediately - on the very instance the writing request was still working with. Every
  reference touch of the writer then borrowed a fresh DI scope and a pooled connection, and a
  parallel tick drained the entire connection pool right after startup. Now Set leaves the
  writer's scoped loaders alone; a graph is switched to the detached loader once, at the moment
  the cache actually serves it to another scope (Get, GetWithoutHashValidation, the bulk
  FilterNeedToLoad), tracked per instance so a hot cache does not re-walk the graph on every
  hit. The safety invariant is unchanged: nothing served from the cache ever carries another
  scope's connection.
- **Lazy `RedbListItem.Object` no longer loads through a dead scope, and a disposed context no
  longer re-opens a connection** (production, 2026-09: idle PostgreSQL sessions grew by ~30 a day
  until a restart; 49 of them had the `_types` load that ends a materialization as their last
  statement). Two halves. (1) Every `RedbService` constructor installed the process-wide
  `RedbListItem` object loader over its OWN scoped context; the service is Scoped, so the static
  pointed at whichever scope was created last - almost always one already disposed - and reading
  `Object` ran a load through it. Serializing a list item was enough to read it: the property was
  visible to System.Text.Json, so an HTTP response or an audit record woke the loader. (2)
  `NpgsqlRedbConnection` / `SqlRedbConnection` / `SqliteRedbConnection.GetOpenConnectionAsync`
  saw the `null` that Dispose leaves behind and opened a NEW physical connection on the dead
  wrapper; the second Dispose was a no-op, so that connection was never returned, and Npgsql has
  no finalizer to rescue it - the session sat `idle` in `pg_stat_activity` for the life of the
  process. Now: items carry a loader from the provider that hands them out
  (`IListProvider.LinkedObjectLoader`, attached by `ListProviderBase` on every hand-out and by
  the Pro materializer), resolving through that scope while it lives and through a fresh scope
  from the root `IServiceScopeFactory` once it is gone; the process-wide `SetGlobalObjectLoader`
  fallback only ever borrows a fresh scope and no longer captures scoped state; `Object` is
  `[JsonIgnore]` (serialize `IdObject`, load the object explicitly where the JSON needs it); a
  context whose scope has ended throws `ObjectDisposedException` instead of re-opening. Fixed on
  the way: the loader looked `IObjectStorageProvider.LoadAsync` up by parameter list, which
  returned null (NRE at runtime) once that signature grew a `CancellationToken`; the linked
  object now loads under the configured permission policy rather than an unconditional skip.
  Breaking: `object` disappears from the System.Text.Json output of a list item. Pinned by
  DisposedScopeGuardTests x6 (every provider, both tiers), PostgresListItemObjectLoaderLeakTests
  x2 (the production sequence, with `pg_stat_activity` counting the container's own sessions
  after its data source is disposed) and RedbListItemSerializationTests.
- **SQLite: id-generator self-deadlock inside the caller's transaction** (live worker storms:
  15-30s "database is locked" waves killing Quartz and every other writer). The id cache is
  process memory and starts empty, so the first save after startup must fetch a block from the
  database - a write. When that save ran inside the caller's own BEGIN IMMEDIATE
  (`ExecuteAtomicAsync` around `SaveAsync` - the heartbeat shape), the refill went through a
  SEPARATE pooled connection and waited for the file's only write lock, held by the very
  transaction waiting for the refill; only busy_timeout x deadlock retries unwound the pair.
  Now, inside an active scope transaction, keys are allocated through that same connection
  (the write lock is already ours - no waiting), bypassing the shared cache: a rollback takes
  the sequence bump back together with the transaction, and those ids were never visible
  outside it, so no duplicates on either outcome. PostgreSQL/MSSQL are untouched - their
  sequences live outside transactions and the trap does not exist there. Pinned by
  SqliteKeygenInTransactionTests x2: the cold-cache save inside a user transaction was a 33s
  "database is locked" before the fix and is instant now; ids re-issued after a rollback do
  not collide.
- **A bare (non-generic) `RedbObject` as a Props field - a loud refusal instead of a crash.**
  The scheme sync classified it as a business class and reflected over the framework's own
  service fields in two casings (`id`/`Id`...): MSSQL died on its CI collation against UNIQUE
  `IX__structures`, PG on the reserved-name trigger (23514) - a driver error naming neither the
  field nor the cure (discussion #12, item 6). The sync now rejects such fields with the hint
  "declare RedbObject<TProps>; for raw bytes use byte[]". Pins x6 (scalar + collection).
- **`NullToDefaultConverter.Write` recursed into itself - StackOverflow on the first non-zero
  primitive serialized with the redb serializer options.** A dormant bomb: writes through these
  Options had only ever met default values, cut off by WhenWritingDefault before the converter.
  Write rewritten with direct `writer.Write*Value` calls.
- **Pro on PostgreSQL and SQLite never upgraded an existing database: a V4 start over 3.x died
  with "column _unique does not exist".** `ProRedbService` of both providers silenced
  `EnsurePvtModuleDeployedAsync` with a one-liner - on the grounds that Pro generates its
  queries in C# and needs none of the module's SQL functions. True of the functions, but the
  same pass carries the schema upgrades (V4 added `_structures._unique/_unique_version`,
  `_values._unique`, `_objects._value_unique` and their indexes), and those every tier needs.
  An existing database never received them: the full init runs only on an empty one, and the
  only catch-up path was switched off - start-up died on the very first scheme read (`_unique`
  sits in the `Structures_SelectByScheme*` column list). MSSQL Pro was not affected - it never
  skipped. Pins: `PostgresProSchemaUpgradeTests`, `SqliteProUniqueUpgradeTests`,
  `MsSqlProSchemaUpgradeTests` - the upgrade contract is now checked on all six tier-providers,
  not only Free (red-before: 4 of 6 red on PG Pro with 42703, SQLite Pro with "no such column:
  _unique").
- **Cross-database poisoning of the structure-tree cache: two services on different databases
  in one process silently broke each other's saves.** StructureTreeCache/SubtreeCache were
  static dictionaries keyed by scheme_id alone, and scheme ids coincide across databases (same
  seed): a foreign database warmed the id with ITS tree, the save walked foreign structures,
  not one property name matched - and the object was written with ZERO value rows, no error
  anywhere. Cache keys now carry the domain ((domain, schemeId)), derived from the connection
  string like every other cache. It only ever showed order-dependently (six fixtures in the
  test process: full gates went 133-468 red depending on class order); after the fix the full
  gate is 2831/2831.
- **MSSQL: base64 in the JSON projection via FOR XML instead of an XML instance - blob reads
  twice as cheap.** `CAST(N'' AS XML).value('xs:base64Binary(...)')` in four emitter sites cost
  112 ms per megabyte against 25 ms for `FOR XML PATH(''), BINARY BASE64`; the full read of an
  object with a 1 MB byte[] dropped from 336 to 134 ms server-side. Traps defused by measuring
  first: without `AS [*]` the value arrives wrapped in `<_ByteArray>...</_ByteArray>` and
  corrupts the JSON, and inside `STRING_AGG` a subquery is forbidden (Msg 130) - the array and
  dictionary branches take the value from an `OUTER APPLY`. Equivalence proven on NULL, the
  empty array, all three padding classes, all 256 byte values and a megabyte; the bytes suite
  extended with the same boundaries (78 tests on six fixtures). MSSQL bundle 0.2.10.
- **Root value_bytes on Free: Postgres returned bytea hex instead of base64, MSSQL did not emit
  the field on read at all (what was written came back null).** The tail of the
  docs/BUG_BYTES_FREE_JSON_PROJECTION.md report (§3.2/§3.3): the Б1 claim covered byte[]
  properties in Props, while the JSON projection walked around the root `_objects._value_bytes`
  column. PG 08 - encode(..., base64) without MIME line breaks (bundle 0.7.5); MSSQL 09 -
  base64 emitted via the XML trick (bundle 0.2.9). The report's manual byte matrix is now the
  permanent BytesRoundTripTests suite x6 (Props bytes + the scalar BLOB layout + the root bytes
  reaching the column, pinned against the silent NULL) - red-before: two cells red before the
  fix, 12/12 after.
- **ChangeTracking performance waves F1-F9 (docs/V4/CT_DEEP_REVIEW_PERF.md) - no semantic
  change, each wave its own commit with a benchmark and a regression run.** F1: an object whose
  recomputed hash matches the persisted one skips the whole value pipeline (id+hash pairs ride
  the existence check; a 60-object batch with 3 edits - 2-2.5x, resaving untouched up to 9x on
  SQLite; a batch-neighbour pin). F2: an equal canonical collection base hash cancels the
  element-by-element walk - and along the way the Ш4 matrix exposed a DEGENERATE collection
  base-hash canon (a List hashed as Capacity|Count, content never entered) - fixed in its own
  commit with a red-before pin. F3: deferred sweep of replaced rows in Align instead of a
  RemoveAll per replacement. F4+F5: ILookups instead of linear scans in tree building and array
  comparison (B3 -15%, B4 -20%). F6: SQLite bulk UPDATE in chunks via UPDATE FROM (VALUES).
  F7: the single-object save without the Parallel scaffolding. F9: the P7/Э2 clears only when a
  key is actually incoming. Full gate 2819/2819.
- **"null must be null" - null collection elements read back as nulls on every provider
  (В-1/В-2/В-3, owner verdict).** The Pro materializer lost nulls everywhere: plain arrays
  shortened, null references collapsed, a ListItem array vanished whole on an empty preloaded
  set, a dictionary key with a null value was dropped, a null element of a class list read back
  as an empty object. The Free JSON emitters built an empty object instead of null for class
  elements. Fixed on one shared discriminator - "a null element is a row without _Guid": five
  ProPropsMaterializer sites, PG 08 (bundle 0.7.3), MSSQL 09 - array and dictionary (bundle
  0.2.5), the SQLite native extension (0.6.4, rebuilt x3). Pins: a null in a primitive array /
  a null class in a list / a null dictionary value - green x6.
- **DTO/MemberInit in all five grouping families; post-Select predicates travel back to SQL;
  the CT pending channels are real now.** The shared selector parser (GroupSelectorMembers) is
  wired into the plain, tree, array and both window groupers plus the fifth family the tests
  discovered (standalone WithWindow, where a DTO crashed and unrecognized members silently
  yielded default). A Where after Select over simple projected props members (nested included)
  translates back into the source - the filter and the page are server-side; the untranslatable
  is filtered in memory correctly, and pushdown is refused once the source is already paged
  (call order). ChangeTracking's diff DELETE/INSERTs ride their pending channels to a single
  flush point (the DELETE -> UPDATE -> INSERT order preserved).
- **GroupBy/Having small fixes (G-4/G-5, CT-5).** Having is a copy-on-write builder in both
  groupers (branching a query used to infect the sibling branch with both predicates); a null
  constant in HAVING is a loud refusal (it used to become the string "null"); the CT diff lost
  its positional descendant pairing, and the nested-array alignment limit rose from 10 to 32
  passes.
- **ChangeTracking on SQLite joined the permanent test coverage - and the in-batch exchange of
  field unique keys got fixed (CT-1).** Investigation showed DeleteInsert in the SqlitePro
  fixture was a coverage hole, not a deliberate opt-out - under ChangeTracking the set went
  436/437 at once (the review's Julian hypothesis disproven). The single red exposed a
  bulk-layer defect: SQLite executes bulk UPDATE row by row, and swapping [RedbUnique] values
  between two objects hit the immediate index check; on PG/MSSQL the outcome depended on row
  order. Fixed x3 providers - the Э2 twin of P7: `_values._unique` of the rows about to be
  rewritten is released by one UPDATE before the bulk rewrite, in the same transaction.
  SqlitePro now runs ChangeTracking permanently: 437/437.
- **The dead ChangeTracking twin is gone (CT-3).** Next to the living Pro algorithm lay a
  second, diverging one: the single-object SavePropertiesAsync path with its own Delete/Insert
  and per-field CT - its entry point had zero callers. The core cluster, Methods.cs whole and
  two dead SQL methods across ISqlDialect and three dialects removed (-813 lines); the living
  ProcessNestedObjectsAsync kept. The twin's real risk was never its weight - it was editing
  the wrong algorithm.
- **ChangeTracking: the array base row's hash after element edits is canonical (CT-2 plus the
  bug hiding under it).** The diverging re-computation of the hash from _values rows (string
  index sort, lost Numeric/ByteArray/null elements) is gone - and the canon pin exposed the
  real root: UpdateExistingValueFields copied the column by the structure's dbType, which for a
  collection is the ELEMENT type - the canonical hash in the base row's _Guid was never copied,
  so every CT edit of array elements left the old hash behind (an eternal pseudo-diff on every
  following save). New save-invariants suite x6: a re-save is a zero diff (row _id stability
  under ChangeTracking included), point edits of arrays/dictionaries/nested classes, the hash
  canon.
- **GroupBy.SelectAsync: DTOs supported, everything unrecognized is loud (G-1/G-2/G-3, owner
  decisions).** A DTO with a member initializer (MemberInit) materializes on par with an
  anonymous type - every row used to come back null silently. A selector member that uses the
  group but is neither g.Key nor a direct Agg.* call - NotSupportedException naming the member
  (used to be a silent null); a member with no group reference is a client-side value computed
  once. A computed grouping key is a loud refusal (used to be a silently empty GROUP BY).
  Array/Tree/window groupers gained a fail-closed guard on non-anonymous selectors.
- **Select: Take/Skip called after in-memory operations now cut the filtered rows (S-4, owner
  decision 2026-09-04).** A Take issued after Where-after-Select went into SQL before the
  filter - the server returned whatever page came first and rows were silently lost (Skip
  skewed the pages). Such Take/Skip now apply in memory after the filter/sort in call order;
  Take/Skip before any in-memory operation keep the server-side LIMIT. The canonical
  Where-before-Select order stays fully server-side as it was.
- **The SaveAsync operational guard, EF-style (CT-4, owner decision 2026-09-04).** A second
  save entering the same scope before the first completes (interleaved async operations the
  connection's command guard cannot see) gets a loud InvalidOperationException in the
  EF-DbContext style - instead of silently scrambling ChangeTracking's pending state.
- **Select projections: the first e2e suite and a fix pack (review 2026-09-03).** 16 scenarios
  x6 fixtures (red-before 24/96): fixed the OrderBy-after-Select crasher on a null key
  (Comparer<object> killed the query), CountAsync lying after Distinct, Select with Agg.* now
  routed into the aggregation path (the Agg example from the XML docs only worked by
  exception), the path extractor flipped from fail-open to fail-safe (an unrecognized lambda
  node means a full load, not a silently nulled field; nested object initializers and ternary
  conditions are parsed; the catch narrowed to NotSupportedException - no deaf catches), and on
  a Pro partial load an object with zero _values rows no longer arrives with null Props under
  the lambda. Regression slice 567/567.
- **LIKE metacharacters in StartsWith/EndsWith/Contains operands are literals now (BR-7, Tsak
  report, 2026-09-02).** The sugar operators spliced their operand raw into the LIKE pattern:
  `%`/`_` acted as wildcards (a superset), `[` on MSSQL opened a character class (a WRONG SUBSET no
  post-filter can restore), and a backslash on PostgreSQL acted as LIKE's own escape character.
  Six pattern-assembly engines carried the bug and all six are fixed: the PostgreSQL plpgsql
  builders (new `pvt_like_escape()`, bundle **0.7.2**), the T-SQL builders (new
  `dbo.pvt_like_escape()` paired with `ESCAPE '\'` at every site, bundle **0.2.4**), the SQLite
  native extension (`pvtLikeEscape` + the ESCAPE clause emitted with the pattern, module 0.6.3,
  binaries rebuilt for win-x64/linux-x64/linux-arm64), and the three Pro C# builders
  (`LikeOperandSql` wraps the operand with the engines' built-in `replace()` - deliberately not the
  bundle function, so a Pro assembly never depends on a bundle version). Field, class-field, dict,
  listitem, array and expression sugar included; the raw `$like`/`$ilike`/`$arrayMatches`/`$matches`
  operators keep interpreting the caller's pattern on purpose. Red-before: 28 of 36 new tests were
  red across the six fixtures - exactly the predicted matrix (`%`/`_`/IgnoreCase everywhere, `[` on
  MSSQL only, backslash on PostgreSQL only).
- **`SaveByUniqueAsync` survives the vanish race and never masks a property collision (BR-8, Tsak
  report, 2026-09-02).** The one-shot retry onto the winner's row used to fire only when the key was
  not found at resolve time; a Remove+Set+Set interleave (row resolved, deleted, key recreated
  elsewhere) escaped to the caller. The retry is now driven by the NEW first-class discrimination:
  `RedbUniqueViolationException.Kind` says which index rejected the save - `ObjectKey`
  (`UIX__objects__scheme_unique`) or `Property` (`UIX__values__structure_unique`) - classified on
  every provider (SQLite from column names; its driver still names no key tuple, so
  StructureId/PropertyName stay null there, documented). Object-key violations retry once onto the
  current winner; property violations surface immediately - Tsak's api-key rotation used to die in
  the old catch. Red-before: the interleave is deterministic through a virtual resolve seam.
- **`LockForUpdateAsync` no longer no-ops silently on a missing id (BR-9, Tsak report, 2026-09-02).** It
  now returns how many of the requested rows exist (and are locked) - a deleted id locks nothing and
  the count is the signal. New strict `LockForUpdateRequiredAsync` throws `RedbLockNotAcquiredException`
  naming the missing ids (a CAS after a silent no-op lock plus the default `AutoSwitchToInsert` used
  to resurrect deleted objects) and refuses to run outside an active transaction. Breaking for
  implementers of `IObjectStorageProvider` (return type `Task` -> `Task<int>`); plain `await` callers
  recompile unchanged.
- **The zero-DB cache shortcut no longer applies inside a transaction (bug report п.1,
  2026-09-02).** With `EnablePropsCache` + `SkipHashValidationOnCacheCheck` a single `LoadAsync(id)`
  answered from the cache with no database query at all - even between `LockForUpdateAsync` and
  the save, where the re-read IS the read of a read-modify-write: increments committed by another
  process before the lock were silently overwritten. Inside an active transaction (explicit or
  ambient) the load now always consults the database hash; outside transactions the single-writer
  trade-off stays exactly as documented.
- **A caught unique violation no longer aborts the CALLER's transaction (bug report п.2,
  2026-09-02).** Inside a foreign transaction the batch save runs under a savepoint
  (`SAVEPOINT`/`ROLLBACK TO`; MSSQL `SAVE TRANSACTION`, not available in distributed
  transactions): on PostgreSQL the typed `RedbUniqueViolationException` used to leave the caller
  in 25P02 - every follow-up statement of a catch-and-recover pattern died, `SaveByUniqueAsync`'s
  own one-shot retry included. The rollback to the savepoint happens before the violation is
  translated (the translation itself queries the database), and the caller's transaction stays
  alive.
- **A truly fresh database failed to install from the generated `redb_init.sql` (BR-5, reported
  from redb.Tsak; review).** The 28th migration's cache resync was a top-level call that a fresh
  database planned BEFORE the 29th file had created the function - 42883 mid-init; lived-in
  databases kept the previous version of the function (00 does not drop it) and never noticed,
  which is also why every long-lived suite database stayed green. The resync moved to the tail of
  29 on both providers, where the functions exist whatever the history (dump-restored databases
  included); bundles bumped to PostgreSQL **0.7.1** / MSSQL **0.2.1**. Fixing it exposed a second
  fresh-install breaker from this very review: the deterministic sort added to the SQL
  concatenation task also re-ordered `redb_init.sql`, whose group order (main DDL first) is
  deliberate - the sort is opt-in now and applies to the pvt bundle only. Both breakers are pinned
  by the new fresh-install test (`FreshDatabase_InitializesFromScratch_AndRoundTrips`: an empty
  database, the full init, a round trip - red with 42883 on PostgreSQL before the fix).
- **Plain `StartsWith`/`Contains`/`EndsWith` keep each database's own case rules - now a documented
  contract (BR-6; owner decision 2026-09-02).** PostgreSQL: case-sensitive; SQLite: ASCII
  case-insensitive; MSSQL: whatever the collation says (CI on the default). redb does not decide
  for the programmer; explicit control is the `*IgnoreCase` forms plus `StringCollation`. Pinned
  per provider by `PlainStartsWith_IsTheDatabasesOwnSemantics_ByContract` and documented in
  COLLATION.md and llms.txt.
- **The props-cache dirty guard sees unsaved edits inside a cached graph again (review, owner
  decision 2026-09-01).** The guard exists because the cache serves the SHARED instance: it refuses
  to serve an object whose live hash drifted from the one stored at caching (the
  SetStatus-before-Save incident). After L.2 the parent's hash is id:hash of its references, so an
  unsaved in-memory edit INSIDE a loaded nested object no longer moved it and the guard served the
  graph with the phantom edit; only a direct load of the nested object itself was refused. On a
  cache hit the guard now also walks the LOADED part of the graph (raw Props, stubs neither touched
  nor woken) and compares each nested object's live content hash with its own persisted hash - the
  two are equal by construction for an unmodified object since the hash fixes above. Any drift is a
  miss: the caller gets the committed state from the database. Applies to the single and the batch
  read paths; hash semantics unchanged (the hybrid that would hash live content into the parent was
  rejected: an independently re-saved child would turn the parent into a permanent miss).
- **A reference inside a CACHED object loaded through the connection of the scope that cached it
  (review, owner question 2026-09-01: "which context does it get?").** The props cache serves one
  shared instance to every scope; the stubs inside it carried the lazy loader of the scope that
  loaded it, i.e. one connection disposed with that scope. A later request touching such a stub
  quietly resurrected a pooled connection nobody returned - or used the connection of another
  request still in flight. Now the cache hands every reference of a cached object a
  `DetachedLazyPropsLoader`: each load opens its own DI scope, takes the provider's loader from it
  (Free or Pro), loads and closes the scope; whatever a load attaches to the fresh Props is replaced
  by the detached loader again, and a scoped loader never overwrites a detached one. Such a load
  reads committed state through its own connection, not the caller's uncommitted transaction - a
  shared instance cannot belong to one caller's transaction. Without DI (no `IServiceScopeFactory`)
  the previous behaviour stays.
- **A reference that is NOT cached and outlived its scope refuses loudly instead of resurrecting a
  connection.** The scoped loaders check `IRedbContext.IsDisposed` (new, backed by the connections'
  own flag) and throw `RedbLazyLoadScopeEndedException` naming the way out: load it inside a live
  scope, or load the parent deeper while the scope is alive. The EF analogue is "attempt was made
  to lazy-load after the associated DbContext was disposed".
- **`"properties": null` in incoming JSON no longer marks a reference as loaded-with-nothing
  (review, owner decision 2026-09-01).** A stub written by a foreign serializer (ASP.NET's default
  JSON, a client app: the getter of a loader-less stub answers null) came back through the Props
  setter as a LOADED object without values, and the parent save then treated it as one: values
  deleted, nothing written - the wipe 9fa79c45 closed for our own stubs, reopened by any other
  serializer. The redb reader now treats null or absent Props as "not loaded": the nested object
  stays the reference it is. A root with null Props is saved exactly as before (an object without
  values). EF has no such trap only because a null navigation never touches the target entity;
  this restores the same rule.
- **The native SQLite extension refuses to load on a host older than 3.44.0 (review).** It reads
  its per-connection lazy flag through `sqlite3_get_clientdata` (3.44+); on an older host the
  routine-table slot lies past the end and the first `get_object_json` jumped into nothing.
  `sqlite3_redb_init` now checks `sqlite3_libversion_number()` and fails with a message naming
  the host version. .NET consumers (SQLitePCLRaw 3.50) were never affected; the `sqlite3` CLI on
  Debian 12 (3.40) / Ubuntu 22.04 (3.37) was. Binaries rebuilt for win-x64, linux-x64, linux-arm64.
- **Review of the V4 work (2026-09-01): eleven defects found by six adversarial passes over the
  commit range, every one fixed with a test that was red on the previous code.**
  - *The `byte[]` storage conversion (Б1) wiped nested payloads and blanked rows on a retry.* It
    told base rows from element rows by the NULLness of `_array_parent_id`; a `byte[]` nested in
    a class has a base row pointing at the class row, so it was classified as an element and
    deleted - every `File.Data` of a pre-V4 database vanished on the first start. A base row
    already converted (crash between the delete and the type flip, or a second node) was
    overwritten with an empty payload. Membership decides now, converted rows are kept, and the
    element delete is set-based instead of a million inlined ids.
  - *The Pro materializer cut nested classes by depth, unlike the SQL builders.* Class fields,
    array-of-class elements and dictionary-of-class values consumed a depth level; harmless while
    Pro loaded 50 levels, but after Л1 `LoadAsync(id, depth: 1)` and every first access to a stub
    came back with EMPTY nested classes on Pro. Depth counts reference hops only, on all four paths.
  - *PostgreSQL session settings did not survive the connection pool.* `redb.lazy_refs` and the
    3.7 `redb.string_collation` were set in Npgsql's physical-connection initializer; `DISCARD ALL`
    on return to the pool reset both, so only the first scope on each connection had them - in a
    web host, request 1. They are re-applied on every hand-out now, one round trip per context,
    the shape MSSQL and SQLite already used.
  - *SQLite schema upgrades reached an existing file only through `EnsureDatabaseAsync`.* The
    plain `InitializeAsync()` skipped them and failed later with `no such column: _lazy`. The
    pass now runs from the same start-up hook the PostgreSQL/MSSQL bundle block uses.
  - *SQLite soft delete kept `[RedbUnique]` keys in the trash.* Its `mark_for_deletion` nulled
    `_value_unique` but not `_values._unique`, so a repeated or cross-scheme delete tripped the
    index; the suites hid it by hard-deleting. Parity with PG/MSSQL; the suites soft-delete now.
  - *Object hashes were computed before nested objects had ids.* A parent with a nested object
    created in the same save hashed `0:` for it, and a hand-made `{ id = x }` reference hashed
    `x:` without the target's hash - the persisted `_objects._hash` was unreproducible on reload
    (a props-cache miss for ever, a hash shift on the first re-save) and the reference row's
    `_Guid` carried nothing. Hashes are computed once ids exist, children first; the persisted
    hashes of hand-made references are resolved in one query; an existing nested object re-saved
    through its parent gets its hash refreshed.
  - *With the props cache on, caching a loaded graph woke every lazy stub.* `CacheNestedObjects`
    read `Props` through the getter; with the loaders attached (Free bulk load, Pro single load)
    that was the lazy load itself, synchronous, for the whole reachable graph. The walker skips
    stubs and reads raw Props.
  - *P7 released keys only for objects taking a new key.* The object giving its key up (new key
    NULL) in the same batch was never released; the update tripped the unique index on
    PostgreSQL/SQLite depending on row order. Every existing object in the batch is released (the
    statement touches keyed rows only), and under an ambient transaction the violation surfaces as
    `RedbUniqueViolationException` like everywhere else.
  - *`LoadReferencesAsync` and the Free batch loader ignored the depth.* The batch
    `get_object_json` hard-coded 10 (Pro: 50): a batch reload pulled ten levels where a stub's
    first access pulls one, and `WithPropsDepth` was ignored on Free query pages. The depth is a
    parameter of the batch on the three dialects, the batch reload passes 1, and the Pro bulk
    load honours the requested depth like the single one.
  - *Smaller items.* Pro nested materialization dropped `value_unique` on nested and boundary
    objects; lazy-stub enrichment wrote the synthetic `Object_<id>` name into `name`; a second
    instance of one reference id in a Pro batch stayed unloaded; the Pro recursion guard was per
    loader instance (now per async flow); the reflection walkers boxed every element of `byte[]`
    and primitive collections; `Structures_SelectById` and the export/import of `_structures`
    lacked `_lazy`; `EnableLazyReferences` had inherited the `StringCollation` XML summary and was
    missing from `Clone()`; a debug `Console.WriteLine` shipped in the SQLite loader; the per-query
    `WithLazyReferences` wrap existed in three copies (one without the degrade path, all able to
    mask the query's own exception from `finally`) and is one helper now; the SQL bundle
    concatenation order depended on filesystem order; the owner namespace of a scheme is
    enforced on every adoption path - explicit name, FullName, creation-race loser - with
    `RedbSchemeNamespaceMismatchException` (owner decision), not only on the short-name fallback;
    SQLite upgrade step 6 duplicated step 5.
- **MSSQL loaded query-page Props at depth 1 where PostgreSQL and SQLite used 10.** The batch
  `get_object_json` behind LINQ results hard-coded a different depth per provider, so the same
  query materialised nested references on two providers and cut them on the third. Aligned at 10.
- **The SQLite metadata-cache WARMUP carried its own column list and silently nulled new
  columns.** `Warmup_AllMetadataCaches` (runs once per start) rebuilt the whole cache without
  `_lazy`, erasing what the per-scheme sync had just written; PostgreSQL and MSSQL delegate the
  warmup to the per-scheme function and were immune. The list now carries the marker, the schema
  upgrades repair affected caches, and updating the flag resyncs the scheme cache explicitly.
- **`_structure_hash` never reflected `AllowNotNull`, `StoreNull`, `CollectionType` or `KeyType`.**
  The hash is computed from `Structures_SelectBySchemeShort`, which carried none of those columns, so
  `SchemeHashCalculator` hashed eternal NULLs: making a field required or nullable-stored changed
  nothing, and the metadata cache kept answering from the old shape until a restart. The short select
  now carries them (and the new `_unique` flag, which must invalidate caches when a key appears or
  disappears). One-time consequence: every scheme's hash changes on the first synchronisation after
  the upgrade, rebuilding its metadata cache once.

- **Saving a parent whose reference was by id only destroyed the referenced object (all providers,
  Free and Pro).** `new RedbObject<T> { id = x }` in a Props property — or the stub a load returns
  at its depth boundary — was collected as an object to save. The save then treated it as a full
  object that happens to have no properties: the DeleteInsert strategy removed every `_values` row of
  the target, the name was reset to `Object_<id>`, the hash cleared. Silently, on the single and the
  batch path alike. The collector now recognises a reference — an id with no loaded properties,
  `RedbObject.IsPropsLoaded` — and writes the parent's `_Object` row only; the referenced object is
  not touched. A reference is not deduplicated against a loaded copy of the same object elsewhere in
  the graph, so that copy is still saved. Proven red-before/green-after by `ReferenceStubTestsBase`
  on the three providers.

- **MSSQL returned `null` for a reference at the depth boundary where PostgreSQL and SQLite return a
  stub.** `dbo.get_object_json` guarded the recursive call with `@max_depth > 0`; the other two
  builders let depth 0 produce the base fields with `hash` and no `properties`. Same employee, same
  depth: a project on two providers, none on the third. The guard is gone; the three builders agree.
  The shape — id, `scheme_id`, `hash`, no `properties`, never `null` — is now a contract test
  (`ReferenceStubTests` ×3), since lazy references are built on it.

- **The `DateOnly` correction never reached an existing database (all providers).** `DateOnly` was
  seeded with `_db_type = 'DateTime'`, a value no JSON projection branches on, so every `DateOnly`
  property materialised as `0001-01-01`. The correction for it was written, and then put in `sql/` —
  which lands only in `redb_init.sql`, applied when the tables are absent. Nothing else applied it. It
  therefore reached new databases and no existing one, however many times the package was upgraded:
  shipped, and dead on arrival.

  The general shape of the problem is worth stating, because it is not specific to this fix: a
  correction to seeded data has exactly one automatic delivery channel, the version-gated
  `sql/v2-pvt/*` bundle, and anything outside it silently applies to fresh databases only.

  PostgreSQL and MSSQL now carry the correction in that bundle
  (`sql/v2-pvt/28_migrate_dateonly_db_type.sql`), so `EnsurePvtModuleDeployedAsync` reapplies it on the
  next start. `pvt_module_version` PostgreSQL 0.6.6 → **0.6.7**, MSSQL 0.1.7 → **0.1.8**.

  SQLite has no such channel at all — no stored functions, therefore no module and no version to
  compare against — so it got an explicit idempotent step on the existing-database path of
  `EnsureDatabaseAsync`. It is a no-op once the seed is right.

  Verified against live databases rather than by inspection: the seed was rolled back to `'DateTime'` on
  both PostgreSQL and MSSQL, an ordinary service start corrected it and moved the module version; on
  SQLite the upgrade path is covered by a test that owns its own file and opens it twice, since the
  shared fixture deletes the database on every run and can only model a new one.

- **An empty `IN` set threw instead of matching nothing (`redb.Postgres.Pro`).**
  `.Where(x => wanted.Contains(x.Department))` with an empty `wanted` failed with
  `42883: operator does not exist: text = bigint`. An empty `object[]` gives Npgsql no element type
  to infer, so it sent `bigint[]`, and comparing a text column against it is not an operator that
  exists. Any code that builds its filter list dynamically could reach this with an empty selection.
  The set now compiles to `FALSE`, which is what membership in an empty set means. MSSql and SQLite
  survived the same input by accident of their spellings and are unchanged.

- **The PVT prefilter ignored membership and kept only one conjunct (`redb.Core.Pro` + all three
  Pro providers).** Three separate reasons a production filter got no prefilter at all.

  *Membership was not an expressible leaf.* `InExpression` is a node of its own, and the planner only
  ever looked at comparisons and null checks, so `.Where(x => ids.Contains(x.ShippingPoint.Id))` was
  reported as `NoAnalyzableLeaf`. It is now a branch, scored as equality minus a penalty that grows
  with the logarithm of the list: one value behaves like an equality, a few dozen still pay for
  themselves, a few hundred fall under the threshold. An empty set yields no plan, because a branch
  rendered from it would be a contradiction that takes the rest of its disjunction with it.

  *A conjunction contributed one branch, not all of them.* The rule is borrowed from a flat table,
  where pushing the single most selective predicate is enough. In a vertical layout it cannot work:
  a field named by the filter becomes a pivot column, so with two Props fields a one-branch candidate
  leaves the other column uncovered and the coverage guard refuses the whole plan. The branch was
  therefore unreachable for every multi-field conjunction. Every conjunct is now a branch; the
  candidate is scored by its weakest one, so a single broad predicate still sinks the group.

  *Nested conjunctions lost their operands.* `a && b && c` parses as a tree rather than as one node
  with three operands, and the middle node is not a leaf, so the third condition was dropped before
  scoring. Conjunctions are now flattened first.

  The three compound: the query that surfaced this filters a ListItem id against 55 values next to an
  ordinary field, and needed all three fixes to get a plan.

### Changed
- **`_objects._value_string` is an identifier column: 450 characters, indexed on MSSQL too (owner
  decision 2026-09-02).** It was NVARCHAR(MAX) there - a type SQL Server refuses to index at all
  (Msg 1919), so every filter on the column scanned while PostgreSQL and SQLite had their partial
  indexes all along. The column narrows to NVARCHAR(450) (the width `_name` already uses) and gets
  the same `IX__objects__value_string`; the limit is enforced in C# on every provider
  (`RedbValueStringTooLongException`), so the contract is one. Long text belongs in `_note` or in a
  Props field. Migration: a database holding longer values REFUSES the upgrade loudly, naming the
  offenders query - move those values to `_note`/Props and start again; nothing is truncated
  silently. MSSQL bundle **0.2.2**.
- **Pro loads honour `depth` on the bulk path as well (review).** Before Л1 Pro materialised 50
  levels on a single load and ignored `depth`; Л1 made the single load honour it (default 10)
  while `LoadAsync(ids)` and `LoadWithParentsAsync` stayed at 50. Both honour `depth` now, so
  `LoadAsync(id, depth: 2)` and `LoadAsync(new[] { id }, depth: 2)` yield the same graph.
- **The hash of a parent takes each nested reference's own `hash` instead of walking its Props
  (V4, Л1).** Walking meant that computing a parent's hash could trigger lazy loading of the
  whole graph, and that the eager and the lazy hash of the same object differed (a stub has no
  Props). Now they agree, the computation never touches the `Props` getter, and `ChangeTracking`
  sees an untouched reference as unchanged — its `_Guid` rides the same persisted hash. One
  consequence: the first re-save of an object with nested references may write a different
  `_objects._hash` than the old algorithm did; the value is stable from then on and the props
  cache heals itself on that save.
- **Serializing a `RedbObject<T>` never triggers lazy loading (V4, Л1).** `Props` is
  `[JsonIgnore]`; the `properties` JSON travels through an internal bridge that reads only what
  is loaded — an unloaded stub serializes as its base fields, the same shape the JSON builders
  emit for a reference at the depth boundary. Deserialization is unchanged.
- **The Free lazy loaders reload a reference with depth 1** (was a hard-coded 10), so the
  references under a reloaded object are stubs again; a missing target (trash) returns `null`
  instead of `InvalidOperationException` — `ILazyPropsLoader.LoadProps`/`LoadPropsAsync` are
  nullable now (V4, Л1).
- **SQLite stores its 128-bit hashes as `BLOB(16)`, not 36-character TEXT (`_objects._hash`,
  `_schemes._structure_hash`, `_users._hash`).** The TEXT form was inherited from a copy of the
  PostgreSQL script, where the column is a native `uuid`; SQLite has no uuid, and 36 bytes compared
  as text where 16 compared with `memcmp` would do is the wrong trade on the one column every cache
  check reads and the index everyone hits. The byte order is the text order (RFC 4122, left to
  right) and lives in one place, `SqliteHash`: writes go through the Guid's TEXT parameter and
  `unhex(replace($n,'-',''))` in the statement, reads convert the `byte[]` back, the native extension
  does the same in its own statements, and a filter by hash (`Where(o => o.hash == h)`) converts the
  value side so the index stays usable. `_migrations._expression_hash` is a string in the model and
  stays TEXT.

  An existing database is converted on the next open (`ApplySchemaUpgradesAsync`, the SQLite
  counterpart of the module bundle's schema step): SQLite keeps the storage class a value was written
  with whatever the column declares, so the values are rewritten in place — `WHERE typeof() = 'text'`,
  idempotent, no `ALTER` — and a reader that still meets a TEXT value understands it. The pass is
  gated by `PRAGMA user_version`, SQLite's counterpart of `pvt_module_version()`: a file stamped with
  the current schema version skips it, so the `typeof()` scan of `_objects` happens once per upgrade,
  not once per start. Needs SQLite 3.41+ for `unhex()`; the bundled library is newer. Native extension rebuilt for win-x64, linux-x64,
  linux-arm64. Covered by `SqliteHashStorageTests` on Free and Pro: byte order, conversion of a
  legacy database across three opens, filter by hash.

### Added
- **Transaction isolation level on demand (BR-1 from redb.Tsak; owner decision 2026-09-02).**
  `BeginTransactionAsync(IsolationLevel?)` and `ExecuteAtomicAsync(IsolationLevel, ...)`: the
  parameter is optional and nothing changes without it - each provider keeps its own default.
  PostgreSQL and MSSQL apply the requested level to the transaction they open; SQLite accepts it
  for portability and stays a single serial writer (BEGIN IMMEDIATE). An active or ambient
  transaction is joined as it is - its level is never changed. Under elevated levels the database
  may abort the loser (PostgreSQL 40001, MSSQL 3960/3961): the WHOLE transaction must be retried
  by its owner, and `DbErrorClassifier.IsSerializationFailure(ex)` classifies that without
  provider-specific code.
- **`LazyReferenceAccess` (Blocking | Throw) and a deadlock-free blocking getter (review, owner
  decision 2026-09-01).** The `Props` getter of an unloaded reference still loads synchronously by
  default, but the load now runs on the thread pool: a host with a `SynchronizationContext`
  (Blazor Server, WPF, WinForms, MAUI) waits for the query instead of deadlocking on its own
  continuations. `LazyReferenceAccess = Throw` makes the getter refuse with
  `RedbSynchronousLazyLoadException` naming the explicit way (`LoadPropsAsync`,
  `LoadReferencesAsync`, a larger depth) - for Blazor WebAssembly, which cannot block at all, and
  for UI hosts where a hidden query on property access is a defect. The async APIs work in both
  modes.
- **The `virtual` marker and the lazy-references option (V4, Л2).** A reference property declared
  `virtual` lands in `_structures._lazy` at synchronisation — the code is the source of truth,
  a hand-edited flag is restored, and the scheme hash includes the marker, so adding or removing
  `virtual` invalidates the caches. With `EnableLazyReferences` on, the JSON builders emit the
  base-fields stub for a marked reference REGARDLESS of depth; the option travels as a session
  flag of the context's connection (PostgreSQL GUC `redb.lazy_refs`, MSSQL `SESSION_CONTEXT`,
  a per-connection flag of the SQLite native extension — `redb_lazy_refs(1)`, readable back as
  `redb_lazy_refs()`), so not a single builder signature changed. `WithLazyReferences(bool)`
  overrides per query by riding the same flag around the execution; the Pro materializer honours
  the GLOBAL option live, but not the per-query override — a recorded Л2 boundary.
  `LoadReferencesAsync(parent, p => p.Children)` reloads a collection of stubs in one batch.
  Non-virtual references stay eager whatever the option says; with the option off behaviour is
  byte-for-byte the previous one. PostgreSQL bundle 0.7.0, MSSQL 0.2.0, SQLite user_version 6,
  native extension rebuilt for the three platforms.
- **Lazy references out of the box: `LoadAsync(depth: 1)` (V4, Л1).** A reference at the depth
  boundary is a stub (the W1 contract: base fields, no properties) that now carries a loader —
  the first access to its `Props` loads exactly that object, whose own references are stubs
  again: laziness is transitive and independent of the original depth. A collection of references
  arrives as a list of stubs, each loadable. A stub whose target left for the trash loads `null`
  Props instead of throwing. Covered by `LazyReferenceTestsBase` on the six fixtures.
- **A scheme knows which namespace owns it: `_schemes._name_space` is written on every sync (V4,
  К7).** The column existed from the start and was read into `RedbScheme.NameSpace`, but nothing
  ever wrote it. Now the CLR namespace of the Props type is recorded on creation and on the first
  synchronisation of a scheme that has none (a pre-namespace database), and a scheme has ONE
  owner on every path a type can reach it by - explicit name, FullName, the short-name fallback,
  the loser of a creation race: a foreign namespace is a hard stop before anything is renamed or
  reshaped, the typed `RedbSchemeNamespaceMismatchException` names both owners and the way out
  (move the mark in `_schemes._name_space` when the type moved, or rename one of the two) instead
  of silently attaching one project's type to another project's data - two unrelated `Order`
  classes are the textbook case. The short-name adoption of a NULL owner logs a warning.
- **`byte[]` is stored as the scalar BLOB it is: one `_values._ByteArray` value per property (V4,
  Б1).** Before, the scheme sync classified a `byte[]` property as an ARRAY of Byte — a base row
  plus one row per byte, a megabyte of payload becoming a million rows. The scalar machinery (the
  ByteArray type, the `_ByteArray` column, base64 in the JSON builders, the Pro materializer)
  existed all along and was unreachable from class models. The sync now classifies `byte[]` as a
  scalar, and a database with the old layout heals itself on the next synchronisation: the bytes
  are reassembled in index order into the base row, the element rows are deleted, the structure is
  retyped — idempotent, type flips last, so a crash half-way just converts again. `[RedbUnique]`
  on a `byte[]` property is now allowed (dedup by content is the ordinary binary-key scenario);
  the canonical form was ready in the encoder since stage 1. Two latent emit bugs surfaced by the
  first reachable ByteArray value were fixed on the way: PostgreSQL `get_object_json` decoded
  `bytea::text` (hex, not base64 — error 22023), and the MSSQL builder fetched `_ByteArray` but
  had no emit branch at all, so the property loaded as `null`. Covered on the six fixtures by
  storage-shape, legacy-conversion and megabyte-key tests.
- **The object key: `_objects._value_unique` (V4, UNIQUE stage 1).** A plain readable base field the
  application fills itself — `obj.ValueUnique = "ORD-2026-0001"` — unique per scheme under the
  partial index `UIX__objects__scheme_unique (_id_scheme, _value_unique) INCLUDE (_id)`. Full radius
  by decision: the column behaves exactly like `_value_string` — JSON projections
  (`get_object_json`, the native SQLite extension), the PVT base-field surface, LINQ
  (`WhereRedb(o => o.ValueUnique == x)`, prefix search), bulk writers, export/import. A violation is
  the same typed `RedbUniqueViolationException`. NULL never participates; comparison semantics are
  each database's own (no trimming, no case folding by redb — the application normalises its own
  keys); composite keys are the application's concatenation. The 440-character limit is enforced in
  C# on every provider, because SQLite checks no VARCHAR lengths. `_value_unique` was at first
  deliberately kept out of `_hash` (P5: the key is identity, not content); the full-object hash
  (see the Fixed entry above) reversed that - the key is header state a cluster node must see.

  Two batch semantics ride along: an in-batch key exchange passes (P7 — the batch releases every key
  it is about to retake before the row updates, inside the batch transaction, on the DeleteInsert and
  the Pro ChangeTracking paths alike), and `SaveByUniqueAsync` (P1) is the upsert by key:
  resolve-then-save with a one-shot retry onto the winner's row after a lost creation race — chosen
  over a single-statement native upsert on purpose, since ON CONFLICT/MERGE on `_objects` would
  bypass the values pipeline. Soft delete releases the object key in the same transaction
  (decision 9). Delivery: the schema-upgrades block (`00_module_init.sql`), PostgreSQL 0.6.9 →
  **0.6.10**, MSSQL 0.1.10 → **0.1.11**, SQLite `PRAGMA user_version` 2 → **3**; native extension
  rebuilt for win-x64, linux-x64, linux-arm64.

- **Unique keys on Props fields: `[RedbUnique]` (V4, UNIQUE stage 2).** A property marked with the
  attribute is unique within its scheme, enforced by the database: the value's canonical form
  (`UniqueKeyEncoder` — NFC for strings, no trimming and no case folding, one zero, UTC milliseconds
  for timestamps, `G29` for decimals, a column tag for cross-column injectivity) is hashed with
  `RedbMd5` into `_values._unique` (`uuid` / `UNIQUEIDENTIFIER` / `BLOB(16)`), guarded by the partial
  unique index `UIX__values__structure_unique` over `(_id_structure, _unique) INCLUDE (_id_object)` —
  root scalars only, NULL never participates. A violation surfaces as one typed
  `RedbUniqueViolationException` on every provider — naming the scheme and property where the driver
  reports the key tuple — instead of three driver errors. Misplaced attributes (collections,
  dictionaries, nested classes, references, enums) are rejected at scheme synchronisation with
  `RedbUniqueKeyDefinitionException`, not silently ignored at insert.

  The attribute on a populated structure recomputes the column at the next synchronisation and
  *reports* duplicates (`UniqueRecomputeReport`; losers stay outside the index with `_unique IS NULL`)
  instead of failing start-up. The same recomputation fires when `_structures._unique_version` is
  behind the encoder, when rows carry a value but no key — the trace of a SQL-side writer
  (`migrate_structure_type`, Pro data migrations and the SQLite conversion now release the keys they
  touch), and explicitly via `RecomputeUniqueAsync<TProps>(property)`. Soft delete releases keys in
  the same transaction (`_values._unique = NULL` on the way into scheme `-10`), so repeated and
  cross-scheme deletion of equal keys works and a released key is immediately reusable. Point lookup:
  `GetByUniqueAsync<TProps>(p => p.Code, value)` — one probe of the unique index.

  Delivery to existing databases opens the module bundle's new "0. Schema upgrades" block
  (`00_module_init.sql`): `_structures._unique` + `_unique_version`, `_values._unique`, the index, and
  the same columns on `_scheme_metadata_cache`; `sync_metadata_cache_for_scheme` /
  `warmup_all_metadata_caches` (they carry the cache column list) and the soft-delete functions moved
  from `sql/` into the versioned bundle (`29_metadata_cache_sync.sql`, `30_soft_delete.sql`) — outside
  it they reached fresh databases only. PostgreSQL 0.6.8 → **0.6.9**, MSSQL 0.1.9 → **0.1.10**, SQLite
  `PRAGMA user_version` 1 → **2**. Keys on `byte[]` are deferred: the scheme sync of this version
  stores a `byte[]` property as an array of bytes, not a root scalar — an open owner decision
  (`docs/V4/ROADMAP.md` §5); the encoder is ready for it.

- **A schema-upgrade contract for databases the application does not own
  (`RedbSchemaOutdatedException`, `AutoApplyDatabaseUpgrades`, `IRedbService.GetUpgradeScript()`,
  `redb schema --upgrade`).** Until now start-up against a database whose SQL module was behind the
  build had one behaviour: apply the bundle. A role the DBA had stripped of owner rights after
  installation got a raw driver error from the middle of the bundle. Now: the module is out of date
  and the role may not change the schema → `RedbSchemaOutdatedException` naming the deployed and the
  required version, with the privilege error as its cause; `AutoApplyDatabaseUpgrades = false` → the
  same exception without trying, whatever the rights; `GetUpgradeScript()` returns the versioned
  bundle as text for the DBA to apply, and the CLI exports it. SQLite has no module and reports no
  script. Driver error codes are classified in one place (`DbErrorClassifier`), replacing the two
  duck-typed copies that had grown in `DeadlockRetryHelper` and `RedbServiceBase`.

  Upgrade note for large installations (review 2026-09-01): the V4 block "0. Schema upgrades" adds
  columns and builds two partial unique indexes (`_values(_id_structure, _unique)`,
  `_objects(_id_scheme, _value_unique)`) inside the module bundle, which runs as ONE command at
  start-up. On PostgreSQL the `ALTER TABLE ... ADD COLUMN` statements hold `ACCESS EXCLUSIVE` on
  `_values`, `_objects`, `_structures` and `_scheme_metadata_cache` until the bundle ends, and the
  index build scans `_values`; on MSSQL the offline `CREATE INDEX` holds a table lock for its
  duration. Every other node blocks on those tables meanwhile, and `CONCURRENTLY` / `ONLINE = ON`
  cannot run inside the atomic bundle. On a multi-gigabyte `_values` treat the first start of 4.0
  as a maintenance step: apply `GetUpgradeScript()` (or `redb schema --upgrade`) from one node in
  a quiet window, and start the application nodes with `AutoApplyDatabaseUpgrades = false` so none
  of them races the DBA. A pre-V4 database with a small `_values` needs nothing special.

  `pvt_module_version` moved from the first file of the bundle to the last (`99_module_version.sql`),
  on both providers: a bundle that fails half-way now leaves the old version behind, and the next start
  retries instead of believing the upgrade succeeded — which is what happened on MSSQL, where every
  `GO` batch commits on its own. PostgreSQL 0.6.7 → **0.6.8**, MSSQL 0.1.8 → **0.1.9**.
  Covered by `SchemaUpgradeTestsBase` on PostgreSQL and MSSQL against a private database
  (`redb_upgrade`) and a restricted login (`redb_noddl`) the tests create themselves.

- **Nine differential tests for membership and ListItem fields (`redb.Tests.Integration`).**
  Membership over a string field, a numeric field and an empty set; a three-condition conjunction;
  and a separate suite for ListItem accessors, which are the one place where a single structure
  carries several of them and they do not behave alike: `Status.Id` lives in the row's own
  `_listitem` column and is a plain row predicate, while `Status.Value` and `Status.Alias` live in
  `_list_items` and need a join no row predicate can express. The suite pins that the first is taken,
  the other two are refused, an array of ListItem is refused, and a filter naming `.Id` and `.Value`
  together still yields one branch on the right column.

### Removed
- **The full-text index on `_values._String` (MSSQL) is gone (owner decision 2026-09-02).** It
  accelerated only `CONTAINS()`/`FREETEXT()`, which redb never generates - every string predicate
  translates to `LIKE`, and `LIKE` does not use full-text - while `CHANGE_TRACKING AUTO` paid a
  background reindex on every `_values` write. The upgrade block drops it from existing databases;
  MSSQL bundle **0.2.3**. The standing (pre-existing) parity boundary is now stated instead of
  implied: string-VALUE search over props is index-assisted on PostgreSQL only (the trigram
  `IX__values__String_pattern`); MSSQL (`NVARCHAR(MAX)`) and SQLite scan, exactly as they did with
  the full-text index in place. If a word-search operator (`$match`) ever lands, the index returns
  with it deliberately.
- **Dead save paths and a dead dialect member (review 2026-09-01).** The batch save has been the
  only live save path for a while; `ObjectStorageProviderBase.SaveAsyncNew` (public, no callers in
  the ecosystem) and everything only it reached - `PrepareValuesByStrategy`, `CommitAllChangesBatch`,
  the `IsValueChanged` / array-element change tracking of the single-object path, the
  `*ForCollection` value builders, `FindObjectInCollector`, the whole `SaveAsyncDeleteInsertBulk`
  family, `PrepareValuesWithTreeDeleteInsert` and the Pro override of `PrepareValuesByStrategy` -
  are removed after a call-graph pass over every project in the tree, including Identity,
  Route, Tsak and TGChatLmm. `ISqlDialect.Query_SqlPreviewBaseFunction` went with the Props-level
  lazy switch it served. Nothing public that had a caller changed; `AddNewObjectsAsync` stays.
- **The Props-level lazy-loading mechanism (V4, Л1 — BREAKING).** `EnableLazyLoadingForProps`
  (global and per-user), `WithLazyLoading()` on queries and tree queries, and the `lazyLoadProps`
  parameter of every `LoadAsync`/`LoadWithParentsAsync` overload are gone, along with the
  `useLazyOnDemand` branches in the query pipeline. The mechanism made the cheap part lazy (the
  scalars of one object) and kept the expensive part eager (the reference graph to depth 10), sat
  behind three switches, and had no tests. Loading the requested object is now always eager; what
  became lazy instead is the reference — see Added. Migration: delete the flag, the call and the
  parameter; the default was OFF, so unconfigured projects behave identically - with one
  exception that needs no flag: a reference beyond `depth` used to be a stub whose `Props` were
  simply null; it now carries a loader, and `Props` on it is a synchronous database round trip
  (the load runs on the thread pool, so a `SynchronizationContext` host waits rather than
  deadlocks). Blazor WebAssembly cannot block at all: set `LazyReferenceAccess = Throw` there and
  reload through `LoadReferencesAsync` / `LoadPropsAsync`; UI hosts that want no hidden queries
  on property access use the same switch.

## [3.7.2] — 2026-08-27

> **Why this release exists.** 3.7.1 shipped a regression: `array.Contains` over a nullable array
> property throws instead of translating, so a filter as ordinary as `.Where(x => x.Tags.Contains(y))`
> stops working the moment `Tags` is `T[]?` — which is what `Nullable enable` gives every optional
> array. It was introduced by the move to .NET 10 in 3.7.1, not by any change to the query parser,
> and it is the reason this is a release rather than an entry that waits for company.
>
> 3.7.1 stays listed on nuget.org and its images stay in the registry. Nothing there is unsafe; it is
> superseded, not withdrawn.

Verified on .NET 10.0.8 with the suite targeting `net10.0`: 1942 of 1944 passed, two deliberate skips,
run twice — once with the PVT prefilter on and once off, since the prefilter is a superset that must
never change a result. All six provider collections (Postgres, MsSql, Sqlite, each Free and Pro) plus
the unit tests: 1920 of 1920, no failures in either mode. The regression only reproduced on .NET 10,
so a green run on 10.0.8 is the proof that matters.

### Fixed
- **`array.Contains` over a nullable array property stopped parsing on .NET 10 (`redb.Core`).**
  A filter as ordinary as `.Where(x => x.Tags.Contains("urgent"))` threw
  `NotSupportedException: Unsupported Contains expression structure` whenever `Tags` was declared
  `T[]?`, which is what `Nullable enable` gives you for every optional array. Introduced by the move
  to .NET 10 in 3.7.1, not by any change to the parser itself.

  C# resolves `array.Contains(x)` to the `ReadOnlySpan` overload, and the parser already unwrapped
  that conversion. On .NET 10 with nullable annotations the compiler wraps the collection twice:
  `op_Implicit` around a `Convert` around the member access. Peeling only the outer layer left a
  `Convert`, which is not a `MemberExpression`, so both branches of the translation missed and the
  method fell through to its final `throw`. Conversions are now stripped in a loop from both
  operands, so the property behind them is found whatever the compiler wrapped it in.

  Covered by `PvtPrefilterEquivalenceTestsBase.ArrayContains_WithOr_SameResults` on all three
  providers, and verified separately against six shapes of `Contains`, including both `IN` forms
  over a constant collection.

- **The PVT prefilter refused three shapes it should have accepted (`redb.Core.Pro`).**

  *A disjunction nested inside a conjunction* was never taken as a candidate. The guard exists for a
  real hazard: in `(A OR B) AND C`, where `C` reads a pivot column, the prefilter may already have
  nulled that column out and the surviving conjunct then drops the object. But a sibling that only
  constrains the object's own fields is compiled into the `_objects` subquery and cannot read a pivot
  column at all. `(Name LIKE x OR Name LIKE y) AND ParentId = ANY(...)`, the ordinary shape of a
  search box scoped to a subtree, now gets its prefilter.

  *Several branches over one structure* were treated as a multi-structure plan by the guard that
  protects `ORDER BY` and `DISTINCT BY`. Coverage already guarantees that a single covered structure
  means a single pivot column, and an object either keeps its row or forms no group at all, which is
  what the authoritative filter would decide anyway. The guard now counts distinct structures.

  *`ListItem.Id`* was rejected together with `.Value` and `.Alias`. The latter two live in
  `_list_items` and need a join, which a row predicate cannot express. `.Id` is stored in the row's
  own `_listitem` column, so `Status.Id == 42` is literally `v._listitem = 42`. Its column name also
  disagreed with the rest of the resolver in casing, which alone would have scored it zero and
  dropped it silently.

- **Leaves were grouped by structure alone when merging a conjunction (`redb.Core.Pro`).**
  `Status.Id` and `Status.Value` share a structure id and differ only in column, so a merged branch
  could have spliced a string pattern onto a bigint column. Unreachable before, because every
  accepted field had a one-to-one structure-to-column mapping; reachable the moment `ListItem.Id` was
  let in, which is why it is fixed first. Grouping is now by structure and column together.

### Added
- **The prefilter planner explains itself in `ToSqlStringAsync` (`redb.Core.Pro` + all three Pro
  providers).** An applied plan lists its branches with structure, column, operator and score; a
  refusal names the guard that stopped it and what it tripped over:

  ```
  -- PVT prefilter: Row form, 2 branch(es) over 2 structure(s), score 70
  --   branch: structure 1000032, column _string, Contains, score 70
  --   branch: structure 1000034, column _string, Contains, score 70
  ```
  ```
  -- PVT prefilter: not applied, reason PivotNotCovered
  --   detail: pivot column(s) Age (structure 1000030) have no branch
  ```

  Nine reason codes, including the two decided by the provider rather than the planner: a props null
  check, and the SQLite rule about an unordered limit. Finding out why a production query got no
  prefilter used to mean patching the API to log the configuration flag and waiting for a deploy.

  The comments are built by `GetSqlPreviewAsync` and by nothing else. They must never reach the
  executed statement: a comment that varies per query is a distinct plan-cache key in both PostgreSQL
  and SQL Server, which would trade a diagnostic for a cache that never hits. Predicate values are
  not repeated either, since the parameter block above already lists them.

## [3.7.1] — 2026-08-26

> **Why 3.7.1, and what happened to 3.7.0.** 3.7.0 is withdrawn: it was built on .NET 9 and carries
> known vulnerabilities in its dependencies (see **Security** below). Every 3.7.0 package is unlisted
> on nuget.org and the `v3.7.0` releases were deleted from the public mirrors. An unlisted version
> still installs by exact number — but there is no reason to: 3.7.1 replaces it completely.
>
> A patch, not a minor: the public API surface does not change. Adding `net10.0` to the target list
> breaks nothing for existing consumers, and `net8.0` / `net9.0` are kept.

### Changed — the build moved to .NET 10

The applications and artifacts were built on net9 while the core and `redb.Route` had long
multi-targeted `net8.0;net9.0;net10.0`. The gap surfaced on 3.7.0: images and archives shipped as net9.

The `redb.Tsak.*` and `redb.Identity.*` libraries now declare `net8.0;net9.0;net10.0` — exactly like
the core and Route, so the whole ecosystem is uniform. Host applications and tests are pinned to a
single `net10.0`. Images, archives and tags are `-net10`.

.NET 8 and .NET 9 both reach end of support on **10 November 2026** — the same day, Microsoft aligned
the STS 9 date with LTS 8. .NET 10 is supported until **14 November 2028**. `net8.0` and `net9.0`
remain in the libraries' target list for now.

### Security — six high-severity advisories

Found while moving to .NET 10: changing the TFM forced a from-scratch rebuild and the NuGet audit
spoke up. It stays silent on an incremental build, which is how all of this reached 3.7.0.
- **`redb.Export` — `SQLitePCLRaw.lib.e_sqlite3` 2.1.10** ([GHSA-2m69-gcr7-jv3q], high), pulled in
  transitively through `Microsoft.Data.Sqlite` 9.0.3. `redb.SQLite` had pinned its way out of this
  long ago; `redb.Export` sat on the older `Microsoft.Data.Sqlite` and slipped past that guard.
  Bumped to 10.0.0 plus the same explicit `lib.e_sqlite3` 3.50.3 pin.
- **`redb.CLI` now requires .NET 10.** It used to target `net8.0` and install on any .NET 8 or newer
  runtime. Roll-forward only moves up, never down, so a machine carrying only .NET 8 or .NET 9 can no
  longer install the tool — install .NET 10 there.

[GHSA-2m69-gcr7-jv3q]: https://github.com/advisories/GHSA-2m69-gcr7-jv3q

## [3.7.0] — 2026-08-25

> **Why 3.7.0 and not 3.6.1.** New public surface lands across the ecosystem — two new packages
> (`redb.Route.Soap`, `redb.Identity.Grpc`, plus `redb.Identity.Management` split out of the HTTP
> facade) and new API in `redb.Route` — and new surface cannot ship as a patch. The core packages
> carry mostly fixes, but the ecosystem moves on one number.
>
> **`ExpressionSqlCache` and `CompiledQuery` are gone from `redb.Core`.** That is a public API
> removal, which strict SemVer would put in a major. It ships as a minor deliberately: both types were
> verified dead four ways before removal (see **Removed** below), nothing in REDB referenced them, and
> a major would flip `LicensePolicy.FreeThroughMajor` and start charging for Pro. A consumer who did
> reference either type directly has to drop that reference.

### Fixed
- **A property changing type could silently lose its values (all providers, Free and Pro).** Scheme
  synchronisation migrates a structure's stored values when the CLR property's type changes, then
  switches `_structures._id_type`. The migration was called with the old type's numeric **id** passed
  into a lookup keyed by **name**, so it matched nothing, fell back to the literal `"unknown"`, and
  every provider answered "unknown source type". That answer was then discarded and the type switched
  anyway — leaving the values in the old column, reading as `null`, and physically deleted by the next
  save under the default `DeleteInsert` strategy.

  Four independent layers had to line up for this to stay quiet: an id used as a name, a `?? "unknown"`
  swallowing the miss, providers reporting failure as data rather than raising, and the caller throwing
  the result away. Each is now closed: the type is resolved by id, a missing type raises, the result is
  inspected, and `_id_type` is changed only after a migration that actually completed.

  A migration that cannot complete now raises **`RedbTypeMigrationException`** and stops synchronisation.
  That is deliberate and is the whole point: the alternative outcomes are a structure whose values read
  as missing, or a CLR class and a structure that quietly disagree. The exception carries the scheme,
  property, both type names, how many values moved and how many did not, and the SQL to migrate by hand.
  A structure with no stored values is not affected — there is nothing to strand, so those type changes
  still pass.

  On SQLite this bug also disabled the provider's own safety net: its refusal to move values across
  storage columns was never reached, because the resolution failed one step earlier.

- **SQLite refused every cross-column type migration.** PostgreSQL and MSSQL convert between scalar
  types; SQLite rejected all of them wholesale — a stopgap from the fix for the missing
  `migrate_structure_type` function (GitHub #5) that swept up conversions which cannot fail. `bool` to
  `int` is the plain case: `_Boolean` and `_Long` are both `INTEGER`, so the value is copied as is.

  SQLite now implements the same matrix PostgreSQL does, for scalars: numeric and boolean moves, any
  scalar to text, and the temporal pairs (`_DateTimeOffset` holds a UTC Julian day precisely so
  SQLite's own `strftime`/`julianday` read it). Text **sources** are guarded per row rather than cast
  blindly — SQLite has affinity, not types, and `CAST('abc' AS INTEGER)` is `0`, not an error — so an
  unreadable value stays where it is and is reported, which is what PostgreSQL's regex predicate does.
  Reference columns (`_ListItem`, `_Object`) stay refused: an FK cannot be produced from a scalar.

  SQLite also reports a refusal as a result now instead of throwing `NotSupportedException`, matching
  the other two providers; the scheme-sync path turns it into the same `RedbTypeMigrationException`
  everywhere.

- **`String` to `Boolean` migration destroyed values it could not read (PostgreSQL, MSSQL).** The
  conversion mapped an unrecognised token to `NULL` through a `CASE` while the same statement cleared
  `_String`. The value was gone, and — because success is counted as rows updated — it was reported as
  a success: no error, no count, nothing to notice. Every neighbouring text conversion was already
  guarded (`~ '^-?[0-9]+$'` on PostgreSQL, `TRY_CAST(...) IS NOT NULL` on MSSQL); this one was not, on
  both providers. It is now predicated on the accepted token list, so anything else stays where it is
  and lands in `error_count`.

  PostgreSQL's `String` to `DateTimeOffset` and `String` to `Guid` were bare casts with no guard, so a
  single unparseable row raised and aborted the whole migration — every good value blocked by one bad
  one, and the failure arriving as a raw SQL error rather than a report. Both are now guarded, which is
  what MSSQL already did.

- **`migrate_structure_type` now ships in the versioned module, so fixes to it reach existing
  databases.** It lived in `sql/`, which lands only in `redb_init.sql` — applied when the tables are
  absent, i.e. to fresh databases only. Any correction to it was therefore unreachable for every
  database already in use. Moved to `sql/v2-pvt/27_migrate_structure_type.sql`, so the module version
  check redeploys it automatically on the next start like the rest of the module. PostgreSQL
  `pvt_module_version` 0.6.5 → **0.6.6**, MSSQL 0.1.6 → **0.1.7**.

### Added
- **Covering index on `_objects(_id_parent)` carrying `_hash` — now on all three providers
  (`redb.Postgres`, `redb.SQLite`).** MSSQL has had `IX__objects__id_parent` with
  `INCLUDE (_id, _hash, _id_scheme)` for some time; PostgreSQL and SQLite had no equivalent. Their
  nearest index, `IX__objects__parent_scheme_id`, stops at `(_id_parent, _id_scheme, _id)` and does
  not carry `_hash`, so a tree walk that reads the object hash — which is what the transparent cache
  does — left the index for the heap on every row.

  PostgreSQL uses `INCLUDE`, keeping `_hash` out of the key since it is returned rather than searched
  by. SQLite has no `INCLUDE`, so the columns are folded into the key, matching the indexes already
  written that way in that file. The index name is the same on all three.

  Verified on live engines rather than taken from documentation: PostgreSQL 18.1 plans
  `SELECT _id, _hash, _id_scheme WHERE _id_parent = ?` as `Index Only Scan`, SQLite 3.46.1 as
  `SEARCH ... USING COVERING INDEX`.

  New databases pick this up from the schema script. Existing ones do not: the initialisation script
  is applied only when the tables are absent, so the index has to be created by hand there.
- **PVT prefilter — a cutting step before the pivot aggregate (`redb.Core.Pro` + all three Pro
  providers).** A filter over Props compiled into a condition sitting **above** `GROUP BY`, so by the
  time it was evaluated `_values` had already been read and folded for every object in the scheme.
  No index could apply: the predicate filtered the result of an aggregate, not a column. The practical
  consequence was that query cost did not depend on selectivity at all — searching for one rare order
  number cost exactly as much as an empty search, and the trigram GIN index on `_String` sat unused.

  The prefilter narrows the object set *before* the aggregate runs. It is built as a **superset**: it
  may let extra objects through, it may never lose one, and the authoritative filter stays where it
  was. Anything the planner cannot analyse yields no prefilter and today's behaviour exactly, so the
  worst outcome is the absence of a speedup rather than a change of results.

  Opt-in through `RedbServiceConfiguration.EnablePvtPrefilter`, **off by default**.

  Measured on all three engines seeded to the same size, 100 000 objects and roughly 8.4M rows in
  `_values`, statistics refreshed, server-side time, best of three:

  | query | PG before | PG after | SQLite before | SQLite after | MSSql before | MSSql after |
  |---|---|---|---|---|---|---|
  | one needle across two string fields, as the provider emits it | 184.6 ms | 100.4 ms | 6 ms | 6 ms (rule below) | 55 ms | 17 ms |
  | the same, with an `ORDER BY` | 188.3 ms | 90.8 ms | 1150 ms | 311 ms | 55 ms | 17 ms |
  | the same, whole result, no paging | 337.9 ms | 156.4 ms | 1201 ms | 314 ms | 7037 ms | 1327 ms |
  | date range, 1.25% selective, paged | 153.7 ms | 1.5 ms | 17 ms | under 1 ms | 2 ms | 2 ms |
  | date range, whole result | | | | | 88 ms | 65 ms |

  The three engines win for three different reasons, which is worth knowing before predicting numbers
  on your own data. PostgreSQL engages the trigram GIN and reads far fewer rows: the string index
  returns 40 958 rows where the structure index returns 200 000. SQLite engages a covering partial
  index and stops going back to the table. MSSql reads the same pages and saves CPU instead, because
  `_String` is `NVARCHAR(MAX)` and the comparison carries an explicit collation. The date range shows
  the spread plainly: a hundredfold gain on PostgreSQL, sixteenfold on SQLite, nothing at all on SQL
  Server, which already streamed by `_id_object` and stopped at the hundredth group.

  An earlier tree measurement, taken on a separate PostgreSQL seed of 99 200 nodes six levels deep,
  gave 933.5 ms against 362.7 ms for the same needle.

  Scope of what actually gets a prefilter today: a top-level `OR` over selective leaves, and a range or
  equality on a single field. A top-level `AND` across *different* fields does not, because the row-level
  form can only express a disjunction and the guard against nulling out uncovered pivot columns then
  suppresses it. Filters touching arrays, dictionaries, `== null`, cross-field comparisons or computed
  expressions are recognised as unanalysable and produce no prefilter.

  **Two guards, not one.** The row form drops rows, not objects, so having a branch is not the same as
  keeping a value: a branch is a predicate, and a row failing its own branch is dropped like any other.
  With several branches an object can qualify through branch A while its branch-B row is thrown away,
  leaving column B NULL. The object set survives, which is why this went unnoticed by 1606 existing
  tests, but every value read from column B is then a lie. So on top of the coverage check a multi-branch
  plan is emitted only when nothing outside the disjunction reads those columns: no `OrderBy` or
  `DistinctBy` over Props, no projection, no `Distinct`. A single-branch plan needs no such guard, since
  an object survives only if that very row matched. For the same reason a disjunction nested inside a
  conjunction is never taken as a candidate: the sibling conjunct would read a column the prefilter had
  already nulled out, and there the objects are lost outright rather than merely misordered.

### Fixed
- **Case-insensitive search folded ASCII only, so it did not work for most of the world's text
  (`redb.Core`, `redb.Core.Pro`, all three providers).** `Contains(needle,
  OrdinalIgnoreCase)` found `HELLO` but not `ПРИВЕТ`, and the same held for Greek, Hungarian, Polish,
  Czech and French. Case folding comes from the database's own rules: on SQLite that is ASCII-only
  unconditionally (`LIKE`, `lower()`, `upper()` and even `COLLATE NOCASE`), on PostgreSQL whenever the
  database was created with `LC_CTYPE=C`, and never on SQL Server, whose default collation already
  folds every script.
  New opt-in setting `RedbServiceConfiguration.StringCollation` fixes every script whose case mapping
  is one character to one character, in one place, with no per-language work. It covers the whole
  family together — `ContainsIgnoreCase`, `StartsWithIgnoreCase`, `EndsWithIgnoreCase`, `ToLower`,
  `ToUpper` and the case-insensitive regex — so a search can never disagree with a comparison.
  Implemented per provider because the providers differ in kind: PostgreSQL attaches `COLLATE` to the
  folded operand (Pro in C#, Free through the new `pvt_fold_case()` reading a `redb.string_collation`
  GUC, which needed no change to any function signature); SQLite has nothing to attach a collation to,
  so it replaces the built-in `like`, `lower` and `upper` with Unicode-aware ones on the connection,
  the same technique SQLite's own ICU extension uses and with no native rebuild; SQL Server needs
  nothing. Unset, the generated SQL is byte for byte what it was.
  Two caveats that are documented rather than hidden: on PostgreSQL a collated operand cannot use an
  index built with the database's collation, so a trigram search degrades to a full scan until a
  matching expression index is created (the DDL is in COLLATION.md, and RedBase does not create it for
  you); and diacritics, German `ß`/`SS` and the Turkish dotted `İ` are not case folding and are not
  fixed — what the last two do differs per provider, which the test suite now pins per provider rather
  than assuming.
  See COLLATION.md.
- **Pro discarded the configured SQL dialect (`redb.Postgres.Pro`).** `ProRedbService` resolved both
  `ISqlDialect` and `ISqlDialectPro` from the container but handed only the base one to
  `ProQueryableProvider`; `ProQueryProvider` then narrowed it with `as ProPostgreSqlDialect`, a base
  instance failed that cast, and the fallback silently constructed `new ProPostgreSqlDialect()` with no
  configuration at all. Latent for as long as the dialect carried no settings — `StringCollation` was
  the first, and it vanished on every Pro query while working correctly on Free. The Pro dialect is now
  passed through explicitly and preferred over the base one wherever both are available.
- **Temporal semantics: `DateTime` keeps its clock reading on every read path, and `DateOnly` works at
  all (`redb.Core`, `redb.Core.Pro`, all three providers).**
  In REDB a `DateTime` carries no time zone: 14:00 written is 14:00 read, on any host. Object
  materialization honoured that; the analytics path did not. `JsonValueConverter` parsed a zoned ISO
  string with the default `DateTimeStyles`, which converts it *into the caller's local zone*, so
  `MinRedbAsync`, `GroupBy`, `Window` and scalar projections answered with a different value than the
  object did for the same stored field.
  Separately, `DateOnly` never round-tripped on any provider: it was seeded with
  `_db_type = 'DateTime'`, a value no `get_object_json` branch knows about, so the column was written
  correctly and dropped on the way out, and every `DateOnly` property materialized as `0001-01-01`.
  Retyped to `DateTimeOffset`, which routes it through the branch that already exists everywhere: no
  SQL function changed, no PVT version bump, no native SQLite rebuild. Migrations for existing
  databases: `redb.{Postgres,MSSql,SQLite}/sql/migrate_dateonly_db_type.sql`.
  Also fixed: `DateOnly`, `TimeOnly` and `TimeSpan` went into `_values._String` through the *current*
  culture's short pattern and were parsed back the same way, so a row written under ru-RU stopped
  loading under en-US; the `TimeSpan` JSON form dropped the day component and the sign
  (`3.02:00:00` came back as `02:00:00`); and the filter needed the same invariant spelling in both
  the Free (`FacetFilterBuilder`) and Pro (`SqlParameterCollector`) paths, or a saved row was
  unfindable by its own value. All temporal text now goes through a single `RedbTemporalFormat`.
  An unassigned `DateOnly` also threw on load, because Npgsql writes `DateTime.MinValue` as
  `-infinity` and only the `DateTime` converters recognised that marker.
  See [DATETIME.md](DATETIME.md) for the contract, the precision table and the boundaries left open.
- **Phantom `_date_delete` base field (`redb.Core`, `redb.Core.Pro`).** `BaseFieldMapper` and
  `ProSqlBuilderBase` claimed `DateDelete` as a base field and mapped it to `_date_delete`, a column
  present in none of the three DDLs and a leftover of the removed `_deleted_objects` table. Typed
  LINQ could not reach it, but a string-addressed query passed validation and compiled SQL against a
  column that is not there.
- **Ambiguous `_id` when a `== null` filter met a filter on the base `Id` (all three Pro providers).**
  A props null check switches the PVT CTE into a form whose `FROM` carries both `_objects` and
  `_values`, while the base-field filter was compiled without a table alias. `_id` is the only column
  present in both tables, so the combination produced `column reference "_id" is ambiguous` on
  PostgreSQL and its equivalents elsewhere. Every `_objects` subquery emitted by the pivot generator
  now carries the `o_src` alias and the compiled filter is qualified against it.

  Reachable from ordinary code: `.Where(p => p.Field == null).WhereRedb(o => ids.Contains(o.Id))`.
  Only `_id` was affected; `_id_parent`, `_hash`, `_value_*` and the rest do not collide.

- **`ChangePasswordAsync` threw `UnauthorizedAccessException` on a wrong current password instead of
  returning `false` (`redb.Core`, GitHub #4).** The method is `Task<bool>` documented as "true if
  changed", so a change-password form built against the contract 500'd on the most common user error.
  A wrong current password now returns `false`; precondition failures (null args, disabled/system user)
  still throw. Regression test on all six Free/Pro fixtures.

- **SQLite: any property type change crashed `InitializeAsync` with `no such table:
  migrate_structure_type` (`redb.SQLite`, GitHub #5).** The scheme-sync path runs
  `SELECT * FROM migrate_structure_type(...)`, a stored function that only exists on PostgreSQL/SQL
  Server — SQLite has none, and its `redb.SQLite/sql/migrate_structure_type.sql` is a dead PostgreSQL
  copy. `SqliteSchemeSyncProvider` now performs the migration in C#: a type change that keeps the same
  `_values` storage column (e.g. `Int` → `Long`, `DateTime` → `DateOnly`) is a clean no-op, and a
  cross-column change that actually holds data raises a clear, actionable error instead of the cryptic
  missing-table one. Regression tests on Free and Pro SQLite.

- **Docs & packaging notes from a first SQLite + Pro integration (GitHub #6).** (1) The
  `EnablePropsCache` XML summary claimed it "works only when `EnableLazyLoadingForProps = true`" —
  false; the cache is independent of lazy loading (the code gates on neither), and the summary now says
  so. (2) `redb.CLI` README's *Supported Providers* table omitted SQLite though `--provider sqlite`
  works; it is now listed. (3) `redb.CLI` targets `net8.0` only (deliberately, to avoid tripling the
  native SQLite payload) but lacked roll-forward, so a host with only a newer major runtime failed to
  start it; `RollForward=LatestMajor` now lets the net8.0 tool run on net9/net10-only machines.


### Removed
- **`ExpressionSqlCache` and `CompiledQuery` (`redb.Core`).** Both were public types that nothing
  ever used: an SQL-template cache that was never wired into any query path, and the record it
  returned. Verified dead four ways before removal — a repository-wide search found references only
  in its own file, an architecture plan under `docs/` and two documentation pages; the only assembly
  scanning in the codebase looks for types carrying `[RedbScheme]`; `CompiledQuery` had no consumer
  besides the cache; and the full solution builds with both files gone.
  This is a public API removal, so a consumer who referenced either type directly (nothing in REDB
  did) needs to drop that reference. The architecture pages on the documentation site described this
  cache as a `ConcurrentDictionary` keyed by expression string and shared for the lifetime of an
  `IRedbService`. It was none of those things: the implementation was a static `MemoryCache` singleton
  with TTL and eviction, keyed by expression structure with constants stripped. That block is gone:
  nothing caches in the SQL-compilation layer any more, and the caches that do exist keep their own
  section further down the same page.

### Changed
- **The prefilter steps aside on SQLite when a limit meets no ordering (`redb.SQLite.Pro`).**
  Without a prefilter SQLite walks `IX__values__object_structure_lookup`, so rows reach `GROUP BY`
  already ordered by `_id_object`, the aggregate streams, and `LIMIT` stops after the Nth group. A
  multi-branch prefilter makes the planner switch to MULTI-INDEX OR over `IX__values__String_not_null`:
  the rows now arrive ordered by structure and value, `GROUP BY` needs `USE TEMP B-TREE`, everything is
  materialised and the limit saves nothing.

  Measured on 100 000 objects and 8.4M rows in `_values`, one needle across two string fields, seven
  interleaved repetitions of each form after warming both: 8-12 ms without the prefilter against
  388-521 ms with it. The ranges do not overlap, and the plans differ structurally, so this is not
  timing noise.

  The rule is narrow by construction: only SQLite, only a plan with more than one branch, only a query
  that carries a limit and no ordering at all. Add any `ORDER BY` and the aggregate is materialised
  either way, so the prefilter wins again, 1150 ms against 311 ms on the same data. A single-branch
  plan never triggers MULTI-INDEX OR and keeps its own win, 16x on a date range.

  Neither of the other two engines needs this. PostgreSQL materialises the aggregate regardless and
  gains 1.8x to 2.2x on all three shapes. SQL Server cannot even produce the offending shape: its
  `OFFSET/FETCH` requires an `ORDER BY`, so every paged query carries one, and the prefilter gains
  there too, 3x paged and 5.3x on the full result. The rule therefore lives in the SQLite provider
  (`SqlitePrefilterGuards`) and not in the shared planner, which stays dialect-agnostic.

- **The pivot scan no longer re-checks the scheme (all three Pro providers).**
  Every PVT CTE carried `AND v._id_object IN (SELECT _id FROM _objects WHERE _id_scheme = @p0)`, and
  tree queries additionally joined `_objects oo` only to state `oo._id_scheme = @p0`. Both were
  redundant: `_values` rows are selected by `_id_structure`, a structure belongs to exactly one scheme,
  and the outer `SELECT ... WHERE o._id_scheme = @p0` re-applies the check anyway. The subquery and the
  join are now dropped whenever the outer statement carries that filter itself.

  Two callers must keep the check and say so explicitly. `ExecuteDeleteAsync` builds a `DELETE` whose
  only scheme filter is the one inside the CTE, so it passes `schemeCheckRequired: true`; without it a
  soft-deleted object, which moves to scheme `-10` while its `_values` keep their structures, would be
  matched and hard-deleted. The other is a filter attached directly to the object set. Tree queries have
  no delete path today; a future one must carry its own `WHERE _id_scheme`.

  Verified by comparing sorted id sets before and after on all three engines, flat and tree: identical
  everywhere. On SQLite the flat form also ran 28% faster (1209 ms → 865 ms on 100 100 objects).

- **The string value index in SQLite lost its length guard (`redb.SQLite`).**
  `IX__values__String_not_null` carried `AND length(_String) < 2000` alongside `IS NOT NULL`. That guard
  belongs to PostgreSQL, where a btree key over roughly 2700 bytes overflows the page; SQLite has no
  such limit and the condition had been copied across. Its effect there was to make the index unusable:
  SQLite does not prove implications between a query and a partial index predicate, it looks for a
  matching term, and no query states `length(_String) < 2000`.

  **Upgrade note.** `CREATE INDEX IF NOT EXISTS` does not replace an existing index, so databases
  created before this change keep the guarded version and see no benefit. Recreate it once:

  ```sql
  DROP INDEX IF EXISTS "IX__values__String_not_null";
  CREATE INDEX "IX__values__String_not_null"
      ON _values (_id_structure, _id_object, _String) WHERE _String IS NOT NULL;
  ```

## [3.6.0] — 2026-08-13

> **Why 3.6.0 and not 3.5.2.** `redb.Route` adds public API in this release —
> `.PropagateToolHeaders(...)` and the matching `?propagateToolHeaders=` endpoint option — and new
> surface cannot ship as a patch. The core packages carry only fixes, but the ecosystem moves on one
> number, and a minor is allowed to contain nothing but fixes for the packages that got none.
>
> This also re-unifies the line: 3.5.1 covered `redb.Route`, `redb.Tsak` and `redb.Identity` while the
> core stayed at 3.5.0. From 3.6.0 all four are on the same number again.
>
> **The SQLite native extension was rebuilt for every RID.** The tree-scope fix below changes
> `redb_pvt.c`, so a package carrying the previous binaries would have shipped the fix on the managed
> side and left the Free tier leaking across trees — silently, with no error. win-x64, linux-x64 and
> linux-arm64 are all rebuilt from the current source and verified byte-different from 3.5.0.
> macOS still ships no extension (it needs a macOS runner).

### Fixed
- **`SumRedbAsync` / `AverageRedbAsync` threw `InvalidOperationException` on an empty selection
  (`redb.Core`, all providers).** A `SUM`/`AVG` with no matching rows is `NULL` in SQL (an aggregate
  without `GROUP BY` still returns one row), and the result was read straight through
  `JsonElement.GetDecimal`, which requires a `Number` kind and throws on `null`. This path became
  reachable only after 3.5.0 made base-field aggregations honour the filter: before that `WhereRedb(...)`
  was dropped, so the aggregate always spanned the whole (non-empty) scheme and never produced `NULL` —
  the filter defect was masking this one. Both methods now treat a `NULL` result as `0m` (an empty sum
  is 0 — `Enumerable.Sum` semantics; a ledger with no movements balances to 0), matching the `NULL`
  guard `MinRedbAsync`/`MaxRedbAsync` already had. New empty-selection tests cover both across all six
  Free/Pro fixtures.

- **`TreeQuery(rootObj).WhereLeaves()` / `.WhereRoots()` ignored the root and scanned the whole scheme
  (all six providers — Free and Pro).** The `IsLeaf` / `IsRoot` tree branches built their query from
  `_id_scheme` alone and dropped `context.RootObjectId` / `ParentIds`, so a root-scoped leaf/root query
  returned the leaves/roots of EVERY tree in the scheme. For tree-per-entity storage this leaks across
  trees — e.g. `LoadPathAsync(leaf: null)` picked the newest leaf of another tree, attaching a fresh
  tree's first node under a foreign root. All providers now descend from the root and apply the
  leaf/root predicate over that subtree; unscoped queries keep the whole-scheme behavior (no perf
  regression). Fixed in the Pro CTE builders, and — with full Free/Pro parity — in the Free PVT layer:
  PostgreSQL plpgsql (`12_pvt_cte_builder.sql`), SQL Server (`20_pvt_build_query_sql.sql` fast-path +
  `08_pvt_tree_functions.sql` TVFs, v2-pvt module `0.1.4` → `0.1.6`; PG `0.6.3` → `0.6.4`), and the
  SQLite native extension (`redb_pvt.c`; the prebuilt Windows `redbsqlite.dll` is rebuilt — Linux/macOS
  `.so`/`.dylib` must be recompiled from source per platform). Regression test:
  `TreeTestsBase.TreeQuery_WhereLeaves_ScopedToRoot_DoesNotLeakOtherTrees` asserts isolation on all six
  fixtures. Details in `docs/TREE_SCOPED_LEAF_ISOLATION.md`.

## [3.5.0] — 2026-08-05

> **Why 3.5.0 (a minor bump) when the core packages carry only fixes.** The number is shared across the
> ecosystem, and this release adds public API surface in `redb.Route`: the Message History, XSLT and
> Routing Slip EIPs, plus `{{key}}` property placeholders in endpoint URIs. Under SemVer that is a
> minor, and a minor may legitimately contain nothing but fixes for the packages that got none —
> the reverse (shipping new public API as a patch) would not be legitimate.
>
> **The whole ecosystem moves together**, as it has since 3.3.3: the core packages (`redb.Core`,
> the three providers Free/Pro, `redb.Export`, `redb.CLI`, `redb.Templates`,
> `redb.Licensing`), all of `redb.Route`, plus `redb.Tsak` and `redb.Identity` — the latter two
> carry no code changes of their own and are rebuilt onto the new core.
>
> **Why they are not left behind on 3.4.0.** Tsak's shared-runtime layer is gated on the **minor**
> version, so a 3.4.x worker refuses to start with a 3.5.0 framework dropped into `Libs/shared` —
> "patch the framework without rebuilding the worker" holds within a minor only. Since Identity runs
> as modules inside Tsak, leaving both behind would mean the two fixes below that affect every
> consumer — the stale props-cache entry and the order-dependent `Dictionary` hash — never reach
> their users at all.

### Fixed
- **Hashing threw on Blazor WebAssembly, breaking every write (`redb.Core`, `redb.Core.Pro`).**
  The browser-wasm runtime ships no MD5 provider, so `MD5.Create()` raised
  `CryptographicException: Cryptography_UnknownHashAlgorithm, MD5` — and since object and scheme hashes
  are computed on every save, `SyncSchemeAsync` and `SaveAsync` failed outright in the browser. All MD5
  call sites now go through `RedbMd5`, which uses the platform provider where one exists and a managed
  RFC 1321 implementation where none does (today: browser-wasm only).

  **The algorithm did not change and existing databases are untouched.** Hashes are stored as `Guid`
  (16 bytes) and compared by change tracking and cache validation, so the managed implementation is
  asserted bit-for-bit identical to `System.Security.Cryptography.MD5` — RFC 1321 vectors, every block
  boundary where padding implementations go wrong (54–57, 63–65, 119–120, 127–129 bytes), and random
  buffers. On every non-browser target the executed code path is exactly what it was before.

- **Android apps shipped ~360 KB of unusable Linux binaries in every APK (`redb.SQLite`).** The Free
  tier's native loadable extension was packed into the idiomatic `runtimes/<rid>/native/` layout — but
  the .NET Android SDK harvests every `runtimes/*/native/*.so` and maps it into the APK by architecture,
  so `linux-arm64/redbsqlite.so` landed in `lib/arm64-v8a/` and `linux-x64/redbsqlite.so` in
  `lib/x86_64/`. Besides the dead weight, this raised `XA0141`: Android 16 requires 16 KB page
  alignment, so the app would fail to start there. Affected every Android consumer, including those on
  `redb.SQLite.Pro` (which depends on the Free package).

  The extension now ships under `buildTransitive/native/<rid>/`, where no platform SDK looks for it, and
  `redb.SQLite.targets` performs the delivery. The targets file previously handled only the
  no-RuntimeIdentifier case — RID-specific publish relied on NuGet flattening `runtimes/` — so it was
  extended to cover both. The output layout is unchanged (`runtimes/<rid>/native/`), so extension
  resolution behaves exactly as before on desktop and server.

- **Props cache could serve a stale ("dirty") object mutated in place after it was cached
  (`redb.Core` + all providers).** `MemoryRedbObjectCache` stores a **reference** to the object,
  not a copy. When a caller mutated a loaded object in place *before* saving it, the cache — pointing
  at the same object — instantly reflected the mutation, but its stored hash (fixed at `Set`) did not.
  A subsequent `LoadAsync` saw hash-matches-DB and returned the mutated object as if it were the
  committed state. The cache now recomputes the object's live hash on every read
  (`Get` / `GetWithoutHashValidation` / `GetInternal`) and compares it to the hash captured at `Set`;
  if they differ, the cached snapshot is dirty → treated as a MISS so the caller reloads the committed
  state from the DB. Surfaces only with `EnablePropsCache=true` (off by default). No clone/allocation
  on read — just the hash recompute.

- **`RedbHash` was order-dependent for `Dictionary` properties (`redb.Core` + all providers).** A
  `Dictionary<K,V>` was hashed in enumeration order, but .NET does not guarantee that order (it also
  changes after removals), and the same logical map is rebuilt in a different order when materialized
  from `_values` than when first created. So one logical object produced **different hashes** on the
  create path vs the load path, desynchronizing `_objects._hash` from the cache and corrupting any
  hash-based cache validation for Dictionary-bearing Props. Dictionaries are now hashed by **content**,
  order-independent (pairs canonicalized as sorted `key=valueHash`). Arrays and lists are unchanged —
  their order is significant.

- **Base-field aggregations silently ignored the query filter on Pro (`RedBase.*.Pro`).**
  `SumRedbAsync` / `AverageRedbAsync` / `MinRedbAsync` / `MaxRedbAsync` / `AggregateRedbAsync` (and the
  Props-based `GetStatisticsAsync`) routed their `Where`/`WhereRedb` filter through the legacy
  `filterJson` provider overload. The Pro providers deliberately drop `filterJson` — real filtering
  happens through the compiled `FilterExpression` overload — so those aggregations ran over the **whole
  scheme**, returning a sum/average/min/max/count as if no filter were applied. (Free was correct: it
  converts `filterJson` to facet-JSON.) These call sites now pass the `FilterExpression` directly, the
  same path `AggregateAsync` already used. Enriched aggregation tests now exercise every `*RedbAsync`
  method with a filter across all six Free/Pro × Postgres/MSSql/SQLite fixtures — the previous suite had
  zero filtered-`*RedbAsync` coverage, which is why the defect shipped.

- **Integer aggregate results collapsed to 0 on MSSql (`redb.Core`, all providers via MSSql).**
  MSSql renders `SUM`/`MIN`/`MAX` of a base `bigint` field as `numeric(38,10)`, so `FOR JSON` emits it
  with a zero fraction (`2130762.0000000000`). `JsonValueConverter` read integer targets with
  `TryGetInt64`, which rejects any JSON number carrying a decimal point, and silently fell back to `0` —
  so e.g. `AggregateRedbAsync(o => new { Sum = Agg.Sum(o.Id) })` returned 0 on MSSql while Postgres and
  SQLite (which render plain integers) were correct. The converter now reads integer targets through a
  `numeric`-tolerant path that truncates a zero fraction, mirroring the provider-rendering tolerance it
  already applies to `bool` (SQLite 0/1) and `DateTime`. A value beyond the target type's range now
  overflows loudly instead of collapsing to 0.

## [3.4.0] — 2026-07-27

> **Why 3.4.0 (a minor bump), not 3.3.4.** This release adds features, not just fixes: explicit scheme
> names (`[RedbScheme(Name = "...")]`), scheme-name validation triggers on MSSql/SQLite, and the new
> `ThrowOnSchemeMismatch` load-time guard. Under SemVer that is a minor version. The core packages
> (`redb.Core`, the three providers Free/Pro, `redb.Export`, `redb.CLI`) move together to
> **3.4.0**; `redb.Route`, `redb.Tsak` and `redb.Identity` keep their own version lines.

### Added
- **Explicit scheme names — `[RedbScheme(Name = "...")]` (`redb.Core` + all providers).** Until now
  a scheme was always named after the CLR type's `FullName`, and the string in the attribute was only
  a cosmetic `_alias`. A scheme name can now be pinned explicitly, which decouples the database
  identity of a scheme from C# namespace/class refactoring.

  The positional argument is, and stays, the **alias** — an explicit name is only settable through the
  named `Name` parameter, so not one existing declaration changes meaning. Types that declare no
  `Name` behave exactly as before.

  On the first sync a type with an explicit name has its scheme **renamed in the database**. The
  scheme is looked up along a three-step chain — explicit name, `FullName`, short type name — and the
  first match is renamed in place. The rename is a single-row `UPDATE` of `_schemes._name`: the id is
  preserved, so objects, structures and values are untouched, and polymorphic loading (which resolves
  through `scheme_id`) is unaffected.

  > **Renaming requires every consumer of that database to be updated together.** An application
  > version that predates the explicit name will not find the scheme under its new name and will
  > create a second one, splitting objects between them silently. If that has already happened, redb
  > now detects it and refuses to continue rather than picking one of the two.

  Explicit names must follow C# identifier rules (Latin letters, digits, `_`, `.`, `+`; no reserved
  words; 128 characters max) and are validated in C# before any SQL is issued, so the error names the
  offending type. Human-readable titles belong in `Alias`, which is free-form.

- **Scheme-name validation in MSSql and SQLite (`redb.MSSql`, `redb.SQLite`).** Both providers
  now carry a `_schemes` name-validation trigger mirroring the PostgreSQL `validate_scheme_name()`
  rule for rule, so a name accepted by one provider is accepted by all three. Applies to
  **newly created databases only** — `EnsureDatabaseAsync` skips initialisation when `_schemes`
  already exists. The C#-level validation covers databases of any age.

- **`RedbServiceConfiguration.ThrowOnSchemeMismatch` (`redb.Core`, default `false`).** Chooses how
  `LoadAsync<TProps>` reacts when the object's scheme does not match `TProps` (see Fixed): `false`
  returns `null` (so a soft-deleted object, scheme `-10`, reads as `null` — what soft-delete callers
  expect); `true` throws `RedbSchemeMismatchException` to surface a genuine type mistake loudly.

### Changed
- **The SQLite native extension is renamed `redb` → `redbsqlite` (`redb.SQLite`, Free tier).**
  The package now ships `runtimes/<rid>/native/redbsqlite.{dll,so}` (win-x64, linux-x64, linux-arm64)
  instead of `redb.{dll,so}`. The generic name collided with the managed `redb.*` assemblies and with
  the `redb.*` prune globs a host applies to its own bin directory — a native loadable module was
  being swept up as if it were one of ours. **The C init symbol is unchanged (`sqlite3_redb_init`)**
  and the binaries are byte-for-byte the ones shipped as `redb.*` in 3.3.3: this is a rename of the
  file, not a rebuild, and `LoadExtension` has always been called with an explicit entry point.
  - **Nothing to do** if you let the package resolve the extension (`SqliteDataSource
    .LocatePackagedExtension()`, the default for the Free DI registration) — it looks for the new name.
  - **Action required** if you pin the path yourself — `REDB_SQLITE_EXTENSION`, an explicit
    `NativeExtensionPath`, a Dockerfile `COPY`, or a deploy script that copies `redb.so` by name:
    point it at `redbsqlite.{dll,so}`. A stale path fails at connection open, not at build.

### Fixed
- **Concurrent start-up of several nodes could fail to create a scheme (`redb.Core` + all
  providers).** Reproduced in production on a three-node cluster. When several instances started
  against a database that did not yet have a given scheme, all of them missed the lookup and all
  issued `INSERT INTO _schemes`; every instance but one failed with an unhandled `UNIQUE(_name)`
  violation during initialisation.

  Scheme creation now uses a conflict-free statement per dialect (`ON CONFLICT (_name) DO NOTHING` on
  PostgreSQL and SQLite, `INSERT ... WHERE NOT EXISTS` with `UPDLOCK, HOLDLOCK` on MSSql) and the
  instance that loses the race reads back the winner's row. Catching the violation instead would not
  have worked: on PostgreSQL a failed statement inside a transaction poisons it, so the follow-up
  read would fail with `25P02`. Both creation paths are covered — typed
  (`EnsureSchemeFromTypeAsync<T>`) and untyped (`EnsureObjectSchemeAsync`).

- **Pro data migrations never ran — on any provider (`redb.Core.Pro`, `redb.SQLite`,
  `redb.MSSql`).** Four separate defects stacked on top of each other, and the feature had no test
  coverage at all, so none of them had ever surfaced:

  1. `MigrationExtensions.CreateExecutor` fetched the DB context by reflection asking for a
     **NonPublic** `Context` property, but `RedbServiceBase.Context` is public — the lookup never
     matched and every migration died with *"Cannot get IRedbContext from RedbService"*. It now uses
     `redb.Context` and `(ISchemeSyncProvider)redb` directly; both are part of `IRedbService`.
  2. The generated UPDATE was hardcoded to PostgreSQL syntax (`UPDATE _values target ...`). SQLite
     forbids aliasing an UPDATE target (`near "target": syntax error`) and T-SQL needs the alias
     bound in a trailing `FROM`. Added `ISqlDialectPro.Migration_UpdateTarget(table, alias)` with
     one implementation per provider.
  3. SQLite had no `_migrations` history table at all: PG/MSSql get it from the concatenated
     `redb_init.sql`, and SQLite has no such concatenation step, so the shared `redb_migrations.sql`
     never reached it. A SQLite-native DDL (INTEGER PK, REAL Julian `_applied_at`, INTEGER
     `_dry_run`) now ships in `redbSqlite.sql`.
  4. MSSql declared `_migrations._id` as `IDENTITY(1,1)` — the only IDENTITY in the whole MSSql
     schema — while the executor supplies ids explicitly like every other redb table, so writing
     history failed with *"Cannot insert explicit value for identity column"*. IDENTITY removed.

  Covered by a new `MigrationTestsBase` suite (apply / history row / idempotency / dry-run) running
  against all three Pro fixtures. Note: existing databases do not receive the corrected `_migrations`
  DDL, since `EnsureDatabaseAsync` skips initialisation when `_schemes` already exists — but no
  database can hold real migration history, because the feature could not complete a single run.

- **A scheme's `_alias` was never updated after creation (`redb.Core` + all providers).** It was
  written once on `INSERT` and then frozen: changing `[RedbScheme("...")]` on a class left the old
  value in the database forever. Structure aliases have always synchronised (`Structures_UpdateAlias`);
  schemes simply had no equivalent. They do now — the attribute is the source of truth, removing it
  resets `_alias` to `NULL`, and a value edited by hand in the database is overwritten on the next sync.

- **`ClrSchemeTypeIndex` pinned hot-reloaded plugin modules in memory (`redb.Core`).** The
  process-global `schemeName → Type` index held **strong** `Type` references, and a `Type` keeps its
  `AssemblyLoadContext` alive — so a collectible ALC (Tsak hot-swap) could never be collected, and after
  a reload the stale instance produced a false `RedbSchemeNameConflictException` against the fresh one,
  blocking the module from loading. Entries are now `WeakReference<Type>` (dead ones pruned on lookup),
  and two instances of the same `FullName` from different ALCs are recognised as a reload, not a name
  clash. Distinct types sharing one explicit `Name` still conflict, by design.

- **`LoadAsync<TProps>` did not verify the loaded object's scheme (`redb.Core` + all providers).**
  Loading an object of one scheme under an unrelated `TProps` deserialised garbage into the Props and —
  with `EnablePropsCache` — cached it under `objectId`, so a later `GetWithoutHashValidation` kept
  returning the garbage. `LoadAsync<TProps>` now checks the object's `_id_scheme` against the scheme
  `TProps` maps to before anything is cached. By default a mismatch returns `null` — so a soft-deleted
  object (scheme `-10`, set by `SoftDeleteAsync`) reads as `null`, which is what soft-delete callers
  expect; set `RedbServiceConfiguration.ThrowOnSchemeMismatch = true` to instead throw
  `RedbSchemeMismatchException` on a genuine type mistake. Either way garbage never reaches the cache.
  The untyped `LoadAsync(objectId)` is never affected.

- **Invalid explicit scheme names failed one at a time (`redb.Core`).** Auto-sync validates every
  `[RedbScheme(Name = "...")]` and (by design) refuses to boot on an invalid name — but it stopped at
  the first offender, so a codebase with several had to be fixed one rerun at a time. Names are now all
  validated up front and reported together in a single `AggregateException`.

### Removed
- **Dead metadata-cache interface layer (`redb.Core`).** `ICompositeMetadataCache`,
  `ISchemeMetadataCache`, `IStructureMetadataCache`, `ITypeMetadataCache`, `IStaticMetadataCache` and
  `StaticMetadataCache` — 679 lines across five files that referenced only each other. None was ever
  implemented, none was ever consumed; the live caches are `GlobalMetadataCache` (per cache domain),
  `GlobalListCache`, `GlobalPropsCache` and `ClrSchemeTypeIndex` (per process). Formally a breaking
  change since the types were public, but they could not be used for anything: the interfaces had no
  implementations. `CacheDiagnosticInfo`, `CacheHealthStatus`, `MemoryUsageInfo` and `PerformanceInfo`
  lived in the same file and are part of the live `ISchemeCacheProvider` contract — they moved
  unchanged to `redb.Core/Caching/CacheDiagnosticInfo.cs`. `redb.Core/Caching/README.md`, which
  documented only the removed layer, has been rewritten to describe the caches that actually exist.

## [3.3.3] — 2026-07-15

> **Why 3.3.3 and not 3.3.1.** The number jumps to stay in step with the rest of the ecosystem, which
> had drifted ahead: `redb.Route` and `redb.Tsak` were at **3.3.1**, and `redb.Route.Sql` / `redb.Route.Sqs`
> at **3.3.2** (a partial connector release). From 3.3.3 **every package ships one number** — redb core,
> redb.Route and redb.Tsak — so "which versions go together" stops being a question. There are no core
> releases numbered 3.3.1 or 3.3.2; the fix below is the only functional change here.
>
> `redb.Identity` keeps its own line (**1.2.2**) but is released together with this — it depends on redb
> storage, and without the rebuild its users would stay on the broken init below.

### Fixed
- **Schema init failed under a non-superuser database owner (`redb.Postgres`).** The embedded
  `redb_init.sql` carried a single `ALTER FUNCTION migrate_structure_type(...) OWNER TO postgres;`
  (a leftover from a debugging session — the only `OWNER TO` in the whole script). `EnsureCreated=true` runs
  the script as one batch, so on a least-privilege setup (app user owns the database but is not a
  member of the `postgres` role) the statement failed with *"must be able to SET ROLE postgres"*
  and rolled back the entire first-start initialization. The statement is removed: no function in
  the script is `SECURITY DEFINER`, so ownership never affected execution, and the function now
  belongs to the connecting role like every other object — which also keeps future
  `CREATE OR REPLACE` migrations working. Required app privileges are now just
  `CONNECT` + `CREATE` on the schema + DML. Note: `CREATE EXTENSION IF NOT EXISTS pg_trgm` still
  requires the extension to be preinstalled on PostgreSQL ≤ 12 (on PG 13+ `pg_trgm` is a trusted
  extension, installable by the database owner).

## [3.3.0] — 2026-07-09

### Added
- **Fail-fast concurrency guard on the provider connection (`redb.Postgres`, `redb.MSSql`,
  `redb.SQLite` + `.Pro`).** An `IRedbService` wraps a single, non-thread-safe DB connection
  (EF-DbContext model). If the same instance is entered from two threads at once, each provider now
  throws a clear `InvalidOperationException` naming the cause — instead of an opaque driver error
  (*"A command is already in progress"*, *"connection is busy"*, *"another read operation is already
  in progress"*). Lightweight `Interlocked` check with zero cost on the normal single-threaded path;
  correct scoped usage is never affected.

### Fixed
- **Query parser: `array.Contains(x)` in `WhereRedb` threw on .NET 9 / C# 13 (`redb.Core`).**
  A `string[]` (or any array) `.Contains(x)` inside a `WhereRedb(...)` predicate now binds to the
  `ReadOnlySpan` overload (`System.MemoryExtensions.Contains`) rather than `Enumerable.Contains`,
  which the filter parser rejected with `NotSupportedException`. The parser now recognises
  `MemoryExtensions.Contains`, unwraps the array→span conversion, and translates it to the same
  `IN` clause as `Enumerable.Contains` / `List.Contains`. (Refactored the two-arg `Contains`
  translation into a shared `VisitContainsCore`.)
- **`ComputeHash()` NRE on an object with `Props == null` (`redb.Core`).** `RedbHash.ComputeForObject`
  dereferenced the object before a null check, so the generic `ComputeFor<TProps>` path (used by
  `RedbObject<TProps>.ComputeHash()`) threw `NullReferenceException` when `Props` was null — even
  though null Props is a supported case (the reflection-based `ComputeFor(IRedbObject)` already
  returned null, and `ComputeForBaseFields` exists for exactly this). Added the missing guard so the
  generic path returns `null` (→ `Guid.Empty`) consistently, instead of throwing.
- **Connection-pool leak on transaction/connection dispose (`redb.Postgres`, `redb.MSSql`,
  `redb.SQLite` + `.Pro`).** Disposing the provider connection could skip returning the physical
  connection to the pool: a throw from the driver's transaction `DisposeAsync()` (possible mid
  error-storm on an already-broken connection) bypassed `_connection` disposal. Because `SaveAsync`
  runs inside an explicit transaction, every write armed this path, so under a burst of failures the
  leak was self-amplifying and eventually exhausted the pool (symptom: a healthy pool suddenly climbs
  past `MaxPoolSize` with connection-timeout errors, cleared only by a restart). The connection's
  `DisposeAsync`/`Dispose` and the transaction wrapper's `DisposeAsync` now use `try/finally`, so the
  connection is always returned and the transaction-cleanup callback always runs; the dispose fault is
  no longer swallowed — it propagates so it stays observable.

## [3.2.0] — 2026-06-29

### Added
- **SQLite provider (new): `redb.SQLite` (Free) + `redb.SQLite.Pro` (Pro).**
  RedBase now runs on SQLite — same LINQ API, same 13-table model, same
  `AddRedb(...)` wiring as Postgres/MSSql, with `Data Source=app.db`. The
  provider is swappable at the DI line; the rest of the application is unchanged.
  - **`redb.SQLite.Pro` is pure C#** (query SQL built by `ProSqlBuilder`, props
    materialized in C#, no database-side functions), so it runs anywhere
    `Microsoft.Data.Sqlite` runs — including **Blazor WebAssembly** and **mobile
    (MAUI / iOS / Android)**, where a native SQLite extension cannot be loaded. This
    is the embedded/offline/in-browser tier people asked for.
  - **`redb.SQLite` (Free)** hosts the in-DB machinery as a **native C loadable
    extension** (`redb.{dll,so,dylib}`) — the SQLite analog of the Postgres/MSSql
    server-side functions. It is the full `v2-pvt` query compiler
    (`pvt_build_query_sql` / `_aggregate_` / `_groupby_` / `_window_` /
    `_projection_` / `_array_groupby_sql`) plus the `get_object_json` materializer,
    `save_object_json`, soft-delete (`mark_for_deletion` / `purge_trash`) and the
    `v_user_permissions` view — ported from ~9k lines of PL/pgSQL to C
    (`sqlite3ext.h`). Also callable directly from non-.NET hosts (Python, the
    `sqlite3` CLI).
  - Identity uses a native `AUTOINCREMENT` table; the C extension and the C# key
    generator advance the same `sqlite_sequence` high-water mark, so ids stay
    globally unique across .NET and non-.NET callers.
  - **Minimum SQLite 3.44.0+** (`FILTER (WHERE …)`, window functions, `RETURNING`,
    JSON1, recursive CTEs). Both tiers pass the full example suite (145/145).
  - **Known limits:** the Free native extension ships for **Windows x64**,
    **Linux x64** and **Linux arm64** (`redb.dll` / `redb.so`); **macOS**
    (`osx-x64` / `osx-arm64` `.dylib`) is built from the same CMake project but
    needs a macOS runner (CI matrix next). Pro has no native dependency and runs
    everywhere today. In-memory needs `Mode=Memory;Cache=Shared` + a kept-open connection.
    `NUMERIC` maps to `REAL` (exact-via-`TEXT` is a planned config option).
- **`IUserProvider.GetUserByEmailAsync(string email)`** — new public API on
  `redb.Core.IUserProvider` for case-insensitive lookup by `_users._email`.
  Filters out soft-deleted rows (`_enabled = false`). Email is NOT enforced
  unique at the schema level; the method returns the first active match or
  `null`. Implemented in `UserProviderBase`; `Users_SelectByEmail()` SQL
  recipe added to `ISqlDialect` and to every concrete dialect:
  `PostgreSqlDialect`, `MsSqlDialect`, `SqliteDialect` (Pro variants inherit
  the base implementation, no override needed). Unblocks federation
  email-conflict detection in `redb.Identity` where the previous probe
  `GetUserByLoginAsync(email)` was effectively dead code because self-register
  forbids `@` in login.

### Changed
- **SQLite stores all datetimes as REAL Julian day (UTC) instead of TEXT ISO-8601
  (`redb.SQLite` + `redb.SQLite.Pro`).** The previous TEXT storage made range
  comparisons *lexical*, so a stored `'2024-06-15 13:45:30'` (SQLite space separator)
  never compared correctly against an ISO `'2024-06-15T…'` literal — date-range
  filters, `MinRedbAsync`/`MaxRedbAsync`, `AggregateRedbAsync`, window and group-by
  over datetime fields silently returned wrong/empty results, and a cluster heartbeat
  comparison could mark a live node dead. Every datetime column is now a REAL Julian
  number in UTC (`_objects._date_create/_modify/_begin/_complete`, `_value_datetime`,
  `_values._DateTimeOffset`, `_users._date_register/_dismiss`) — the native SQLite
  representation — so `julianday()`/`strftime()`/`datetime()`/`date()` work directly
  and range comparisons are numeric and **index-sargable**. The JSON/wire shape is
  unchanged: `get_object_json` emits ISO via `strftime`, the C# binder/reader convert
  `DateTime`/`DateTimeOffset` ↔ Julian (`ToOADate() + 2415018.5`, UTC), and the native
  `pvt` builder + Pro `ProSqlBuilder` compare against `julianday('<iso>')` on the
  **constant** side (sargable). Mirrors how PostgreSQL keeps `timestamptz` in UTC.
  **Migration:** SQLite databases created on the old TEXT schema are NOT auto-migrated
  — a fresh database (or a manual column rewrite) is required; mixing a TEXT-schema DB
  with this build yields wrong comparisons. Postgres/MSSql are unaffected.
- **Datetime analytics decode through a storage-agnostic hook (`redb.Core`).**
  `Min/Max/AggregateRedbAsync`, window and group-by select the raw datetime column
  (bypassing `get_object_json`) and hand the value to core converters
  (`JsonValueConverter`, `AggregateResult.Get<T>`, scalar `Convert.ChangeType`). To
  let SQLite's numeric Julian round-trip without teaching `redb.Core` about Julian
  days, a nullable `TemporalDecoder.NumericDecoder` extension point was added: when a
  *numeric* value targets a temporal CLR type and a decoder is registered, it is used;
  otherwise the existing path runs. `redb.SQLite`/`.Pro` register
  `SqliteJulian.FromJulian` at configure time. The hook is null for Postgres/MSSql
  (which never return a number for a temporal column), so their behavior is unchanged.
  Pro reuses the same core converters, so one hook fixes Free and Pro alike.
- **`BackgroundDeletionService` switched from in-memory channel to DB polling**
  (`redb.Core`). Earlier revisions used a `Channel<PurgeTask>` queue for
  low-latency wake-up plus a startup-only `RecoverOrphanedTasksAsync` sweep
  for crash recovery — dual-state by design (channel in memory, trash rows in
  DB). Worker force-kills always left a tail of orphaned `'pending'` rows
  that the next startup had to drain in a flood of single-item purges; a
  periodic recovery sweeper to fix that would have raced against the live
  channel reader on fresh-pending rows. Redesign: DB IS the queue.
  `ExecuteAsync` now polls `GetOrphanedDeletionTasksAsync` every 5 s,
  atomically claims each pending row via the existing cluster-safe
  `TryClaimOrphanedTaskAsync`, and purges in batches with the same
  `PurgeTrashAsync` recipe. `IBackgroundDeletionService.EnqueuePurge` is
  now a no-op (kept on the interface so manual `SoftDeleteAsync` +
  `EnqueuePurge` callers like `GroupService.AddMemberAsync` don't break —
  the trash row they wrote is picked up by the next poll). `QueueLength`
  is always 0; callers wanting the pending count should query the DB
  directly. Force-kill leaves nothing in memory because nothing was in
  memory — the next poll cycle finishes what was queued. Cleanup latency
  shifts from "milliseconds via channel" to "≤ 5 s via poll", but this
  is invisible to API consumers because objects are re-parented under
  the trash scheme synchronously by `SoftDeleteAsync` and disappear from
  queries immediately; only the physical `_values` cascade is deferred.

### Fixed
- **Pro no longer calls the Free-only `get_object_json` on the subtree-delete path
  (`redb.Core` + all `.Pro`).** `TreeProviderBase.CollectDescendantIds` (the
  `DeleteSubtreeAsync` path) lives in the shared base — Pro overrides the polymorphic
  *load* tree methods but not this one — and it used the `Tree_SelectPolymorphicChildren`
  recipe, which embeds `get_object_json`. On PostgreSQL/SQL Server that function exists
  server-side in every tier, so it ran but needlessly materialized each child's full JSON
  just to read its id; on **SQLite Pro** (no native extension) it threw
  `no such function: get_object_json`. Fixed by collecting subtree ids through a new
  id-only dialect recipe `Tree_SelectChildrenIds` (`SELECT _id … WHERE _id_parent = …`) —
  lighter for every dialect and tier. Pro source now contains zero `get_object_json` calls.
- **`DeleteSubtreeAsync` returns the real subtree size (`redb.Core`, all dialects).**
  It now returns the count of collected objects (self + descendants) instead of the raw
  `DELETE` rows-affected, which under-counts on SQLite where the `_id_parent ON DELETE
  CASCADE` FK removes child rows as a side effect (PostgreSQL/SQL Server have no such
  cascade, so the value is unchanged there).
- **Boolean keys/projections materialize correctly on SQLite (`redb.Core`, shared).**
  `JsonValueConverter` now accepts a JSON `Number` as a `bool` (nonzero → true): SQLite has
  no native boolean and stores it as `INTEGER` 0/1, so `GroupByArray`/projection columns
  arrived as numbers and always read `false`. PostgreSQL/SQL Server (which emit JSON
  `true`/`false`) are unaffected.
- **SQLite Free: `DistinctBy(field)` now deduplicates (`redb.SQLite`).** The native
  v2-pvt query builder ignored `distinct_on` (SQLite has no `DISTINCT ON`), so
  `DistinctBy` returned every row. Implemented it via `ROW_NUMBER() OVER (PARTITION BY
  <field> ORDER BY o._id)` in a chained `_ranked` CTE (`WHERE _rn = 1`), mirroring
  `redb.SQLite.Pro`. `pvt_build_query_sql` now reads the `distinct_on` argument.
- **SQLite Free: a multi-key filter no longer silently drops a `null`/text shorthand
  leaf (`redb.SQLite`).** In `pvtSplitFilter`'s multi-key (implicit-`$and`) path,
  `json_each`'s `value` column loses type for a JSON `null` (and strips quotes from text),
  so a shorthand condition like `{"0$:ParentId": null}` was rebuilt as invalid JSON and
  vanished whenever the filter had more than one key — e.g.
  `WhereRedb(o => o.ParentId == null)` combined with a `Where(...)` prop filter returned
  rows that *did* have a parent. Each value is now re-encoded as a valid JSON atom
  (type-aware) before the per-key condition is rebuilt.
- **Polymorphic `LoadAsync(IEnumerable<long>)` no longer silently returns a base,
  non-generic `RedbObject` for a scheme whose CLR type exists (`redb.Core` +
  `redb.Core.Pro`, all dialects, Free and Pro).** The `scheme_id → CLR Type`
  registry was a one-time, **per-cache-domain** snapshot built only by
  `InitializeClrTypeRegistryAsync`, which (a) used a one-shot flag and never
  re-scanned, and (b) split assembly discovery across two sources
  (`AssemblyLoadContext.Default.Assemblies` for auto-sync vs
  `AppDomain.CurrentDomain.GetAssemblies()` for the registry). In a host that loads
  modules into a plugin `AssemblyLoadContext`, or that calls `SyncSchemeAsync<T>()`
  explicitly *after* `InitializeAsync`, the type was never registered, so a
  polymorphic bulk load fell back to a non-generic `RedbObject` (top level, **silently**)
  or threw (Pro nested materializer) — and `loaded.OfType<RedbObject<TProps>>()` came
  back empty even though typed `Query<TProps>()` worked. A second, orthogonal mode:
  the registry lives inside a per-domain partition (domain = hash of the connection
  string), so two redb services on the **same** database but with slightly different
  connection strings — or a type synced under a different domain / by another cluster
  node — never shared the mapping.

  Rebuilt as two layers, each scoped to the natural lifetime of its fact:
  - **`ClrSchemeTypeIndex` (new, process-global).** `schemeName ↔ Type` from
    `[RedbScheme]` is a database-independent **code** fact, so it lives once per
    process, is shared by every cache domain, and is **self-healing**: assembly loads
    (including into plugin `AssemblyLoadContext`s) bump a generation counter and the
    index is rebuilt lazily on the next lookup. One broad assembly source for all.
  - **Per-domain `scheme_id → Type` is now a lazy cache, not a snapshot.**
    `GetClrType(long)` resolves on a miss via `scheme_id → (this domain's DB) scheme
    name → global index` and backfills; new `ResolveClrTypeAsync` adds an async cold
    path that loads the scheme by id (covers cross-domain / another node). The
    one-shot flag no longer governs correctness; `InitializeClrTypeRegistryAsync`
    became a re-runnable best-effort warm-up.
  - **Scheme sync writes the binding authoritatively.** `SyncSchemeAsync<T>()` and
    `EnsureSchemeFromTypeAsync<T>()` register `scheme.Name → typeof(T)` (global) and
    `scheme_id → typeof(T)` (this domain) at the one point where the type and a
    freshly-known `scheme_id` co-exist — so an explicit, manual per-database sync
    makes the type polymorphically loadable **regardless of `[RedbScheme]` presence,
    `InitializeAsync` ordering, plugin-ALC timing, or which node created the scheme**.

  Public API (`GetClrType`, `RegisterClrType`, `InitializeClrTypeRegistryAsync`) is
  unchanged and the happy path is still a cache hit. **`InitializeAsync` is still
  required** — it is just no longer the thing that makes the CLR registry correct. It
  also wires: the v2-pvt SQL module (`EnsurePvtModuleDeployedAsync`), the serializer
  type resolver (`SetTypeResolver`), the `RedbObject` factory + global provider
  (`RedbObjectFactory.Initialize` / `RedbObject.SetSchemeSyncProvider`), the internal
  `UserConfigurationProps` scheme, metadata/props cache warm-up, and — with
  `ensureCreated:true` — the base tables. Call it once per service/database; add a
  manual `SyncSchemeAsync<T>()` for any type not present at startup (e.g. a plugin
  module). **Known limitation (pre-existing, multi-database):** the
  `SystemTextJsonRedbSerializer` type resolver installed by `InitializeAsync` is a
  process-global static bound to one service's cache domain — with two redb databases
  in one process the last-initialized service wins it, which can mis-resolve nested
  polymorphic deserialization for the other database on the serializer path. The Pro
  `ProLazyPropsLoader` nested path is unaffected (it uses its own service's cache).
- **Soft-deleted objects no longer leak into the materializer through nested
  `RedbObject` references (`redb.Postgres`, `redb.MSSql`, `redb.SQLite`
  + all three `.Pro`).** Soft-delete is an `UPDATE` (move the row under a
  `__TRASH__*` bucket and flip `_id_scheme` to `-10`), not a `DELETE`, so an
  outbound `_values._Object` pointer FROM a surviving object TO a trashed one is
  left intact. The object→JSON materializer only checked row **existence** by
  `_id`, not scheme — so loading the surviving parent followed the dangling edge
  and re-materialized the tombstone as if it were live data (a "zombie" nested
  object). Fixed by treating `_id_scheme = -10` as non-existent on the read
  path, in every place that resolves an object by id: PG `get_object_json`,
  MSSql `dbo.get_object_json`, the SQLite Free native C extension
  (`redb_extension.c` `redbObjectJson`), and the Pro C# materializer's
  `Materialization_SelectObjectsByIds` in all three dialects. Free and Pro are
  at **parity**: both return `null` for the trashed nested reference. (Filtering
  the materializer query alone left Pro with an id-only placeholder where the
  target row used to load; `ProLazyPropsLoader` now nulls any reference whose
  target was requested but not returned — soft-deleted or hard-deleted — at any
  depth, while preserving id-only placeholders at the depth boundary and for
  cyclic references, which are never requested.) The `_values._Object` pointer
  is **not** mutated, so the nested reference reappears automatically if the
  target is restored from trash — soft-delete stays reversible. Top-level loads were already unaffected (the LINQ query
  filters by the concrete scheme, which is never `-10`); only nested-reference
  resolution leaked. Direct load-by-id (`SelectObjectById` / the entry call)
  is intentionally left unfiltered so restore/trash-admin flows can still read
  trashed rows.
- **The object→JSON materializer now auto-redeploys to existing databases on
  upgrade (`redb.Postgres`, `redb.MSSql`).** `EnsureDatabaseAsync` skips
  the full `redb_init.sql` once `_schemes` exists, re-applying only the
  versioned `v2-pvt` module — but `get_object_json` and its helpers lived in
  the core init, so a bug fix to them (like the soft-delete fix above) would
  only have reached freshly-created databases. The whole materializer
  (`get_object_json` + `get_objects_json` /
  `build_hierarchical_properties_optimized` / `build_listitem_jsonb` on PG;
  `dbo.get_object_json` + `build_properties` / `build_field_json` /
  `build_listitem_json` / `escape_json_string` on MSSql) moved from
  `redb_json_objects.sql` (now deleted) into the module
  (`v2-pvt/08_core_object_json.sql` / `09_core_object_json.sql`), and
  `pvt_module_version()` was bumped (PG `0.6.2 → 0.6.3`, MSSql `0.1.3 → 0.1.4`,
  with `Query_PvtRequiredVersion` in the dialects). A `git pull` + restart now
  re-applies the corrected functions via `EnsurePvtModuleDeployedAsync`, no
  manual `psql` / `sqlcmd` step. The module's `00_module_init` guard no longer
  treats `get_object_json` as an external prerequisite (it is module-owned).
  **SQLite Free** carries the same fix in the native C extension — it ships as
  the prebuilt `redb.{dll,so,dylib}` and must be rebuilt from
  `redb.SQLite/native` (CMake) to pick it up; the Pro tier (pure C#) needs no
  rebuild.
- **`redb.Route.Sql.SqlProducer` parameter binding now treats empty strings as
  `NULL`.** A null upstream value (e.g. an OAuth `client_id` that is absent from
  a `/connect/logout` body) is routinely serialised through string-typed plumbing
  (HTTP header → header dictionary, JSON DTO → form/body) as `string.Empty`.
  Binding that literally to a `text` / `nvarchar` audit column wrote `""`
  instead of `NULL`, so `WHERE client_id IS NULL` predicates missed those rows
  and `Event_NullFields_WrittenAsDbNull` on Postgres failed. New
  `NormalizeForDb` helper covers all four parameter-source priorities
  (explicit `.Param()`, exchange header, `Dictionary<string,object?>` body,
  `IDictionary<string,object>` body); non-string values and non-empty strings
  pass through unchanged.

- **Test infrastructure — `ProductionBootstrapFixture.WithRedb(...)` helper
  for the per-call scope pattern.** The captive `_fx.Redb` is resolved at
  fixture build time from the root `ServiceProvider`, which means any
  concurrent caller (typically a Worker-side WireTap audit pipeline still
  flushing an `INSERT INTO identity_audit_log` while the test thread resumes)
  shares the same underlying provider connection. PG surfaced this as
  `NpgsqlOperationInProgressException : A command is already in progress: INSERT INTO identity_audit_log`,
  MSSQL as `SqlConnection does not support parallel transactions`, SQLite
  as `SqliteException(SQLITE_BUSY)`. The Route DSL's parallel fan-out
  operators (`WireTap`, `Multicast`, `Splitter`, `ScatterGather`,
  `RecipientList`, `Seda`, `Vm`) already detach the
  per-exchange DI scope cache via `Exchange.Clone()` / `CreateChild()`
  skipping the `__redb_scope:` prefix and creating a brand-new
  `IServiceScope` per branch — so route-level fan-out is safe. The fixture
  is the asymmetric case: test code that bypasses the route context and
  resolves `IRedbService` from the root SP directly. New `WithRedb<T>` /
  `WithRedb` overloads open a fresh scope, resolve the per-scope
  `IRedbService`, run the action, and dispose. Failing tests in
  `SessionIntegrationTests`, `ConsentIntegrationTests`,
  `H8FederationPolishTests` migrated to the helper; the captive `Redb`
  property is retained (and documented) for bootstrap-time access where
  no Worker is processing yet.

- **`SqliteDialect` and `MsSqlDialect` `FormatCaseInsensitiveLike` now emit
  `ESCAPE '\'`.** `UserProviderBase.EscapeLikeWildcards` escapes `_`, `%`,
  `\` with a leading backslash so the user-supplied search value is matched
  literally — this depends on the dialect honouring `\` as the LIKE escape
  character. PostgreSQL does, by default. SQLite and SQL Server do NOT
  without an explicit `ESCAPE` clause, so a literal `_` in the search input
  (very common in synthetic test logins / e-mails like
  `reset_53f4f0f9@example.com`) survived as a wildcard match for ANY single
  character — a one-character mismatch from any genuine row in the table.
  Concretely: `GetUsersAsync(EmailExact = "reset_53f4f0f9@example.com")`
  searched for `_email LIKE 'reset\_53f4f0f9@…'` and returned zero matches
  because the SQLite/MSSQL engine interpreted the leading backslash as a
  literal character rather than an escape prefix. Surfaced as the
  `demo_password_reset` "no enabled user for supplied email" silent drop on
  SQLite and MSSQL (PG passed). The same engines reading the same data via
  Postgres returned the row; the rest of the lookup machinery
  (`Enabled = true`, ordering, etc.) was working correctly all along.

- **Pool-poisoning guard on all three provider connection acquires
  (`SqliteDataSource.EnsureCleanTransactionState`, new
  `SqlRedbConnection.EnsureCleanTransactionStateAsync`,
  `NpgsqlRedbTransaction` diagnostic-only) — the swallow-on-rollback path
  in every `*RedbTransaction.DisposeAsync` had quietly returned a
  driver-level connection to the pool with a still-active transaction
  on the underlying handle.** The first caller to draw that connection
  from the pool would then fail with a driver-specific message that
  obscured the real cause:
  - SQLite: `SqliteException(SQLITE_ERROR): cannot start a transaction
    within a transaction` on the next `BEGIN IMMEDIATE`.
  - SQL Server: `InvalidOperationException: SqlConnection does not
    support parallel transactions` on the next `BeginTransaction()` —
    31 of the recent MSSQL test failures took this exact stack
    (`SqlRedbConnection.BeginTransactionAsync` → `SaveAsync` BEGIN-NEW
    branch with `IsInTransaction=False` at the wrapper level).
  - PostgreSQL: usually masked because Npgsql's pool acquire runs
    `DISCARD ALL` as a built-in reset, so the leak almost never
    surfaces in practice. The fix still lands here because semantic
    correctness should not depend on driver-specific pool behaviour;
    the same `[Diag-TX-LIFECYCLE-PG]` anchors mean a future regression
    of this shape can never go silent.

  The shape of the fix is identical across providers:
  - Every freshly-opened pooled connection now runs a speculative
    `ROLLBACK` against the underlying handle right after the existing
    `ApplyPragmas` / open path. The driver-specific "no transaction is
    active" error (SQLite `SQLITE_ERROR(1)`, SQL Server error 3903)
    is the normal/clean case and is silently caught; an actual
    successful `ROLLBACK` means the pool DID hand us a dirty handle
    and is logged so the source of the leak is observable. Idiomatic
    mirror of Npgsql's built-in `DISCARD ALL` reset.
  - `CommitAsync` on every wrapper now runs the underlying
    `_transaction.CommitAsync()` inside a `try/catch`; on failure the
    wrapper speculatively rolls back so the driver-level connection
    returns clean, then re-throws so the caller still sees the
    original exception. Both the original failure and any cascading
    rollback failure emit `[Diag-TX-LIFECYCLE-{SQLITE,MSSQL,PG}]` log
    lines.
  - `RollbackAsync` and `DisposeAsync` likewise log instead of
    silently swallowing — `DisposeAsync` cannot throw (Dispose
    contract), but any leak that escapes here is now visible and is
    cleaned up by the next pool acquire's sentinel `ROLLBACK`.

- **`SqliteDialect.FormatPagination` handles the bare-`OFFSET` case
  correctly.** `OFFSET m` on its own is a SQLite parser error
  (`SQLITE_ERROR: near "OFFSET": syntax error`) — the engine only
  accepts the `LIMIT n OFFSET m` form. The dialect now emits
  `LIMIT -1 OFFSET m` for the offset-without-limit case (SQLite reads
  `-1` as unlimited); the `LIMIT n` and `LIMIT n OFFSET m` cases stay
  unchanged. Surfaced via a LINQ `.Skip(N)` chain without a matching
  `.Take(M)` — common in trim/cleanup paths (e.g. "delete everything
  older than the keep-newest-N entries"), which had been silently
  short-circuiting on SQLite for any caller that wrapped it in a
  swallow-catch.

- **All three provider `IRedbTransaction` implementations
  (`SqliteRedbTransaction` / `NpgsqlRedbTransaction` / `SqlRedbTransaction`)
  now release the connection's `_currentTransaction` slot on `CommitAsync`
  and `RollbackAsync`, not just on `DisposeAsync`.** The
  `_currentTransaction` field on every `*RedbConnection` was previously
  cleared only by the dispose callback. A code path that issued a query
  between `await tx.CommitAsync()` and `await using` scope exit would still
  see `_currentTransaction != null` and the `CreateCommand` wrapper would
  attempt `cmd.Transaction = closedTx` — Microsoft.Data.Sqlite throws
  `"The transaction object is not associated with the same connection object
  as this command."` outright, Npgsql / Microsoft.Data.SqlClient happen to
  tolerate the assignment but the semantics should not depend on driver
  tolerance. `CommitAsync` and `RollbackAsync` now invoke the same
  `_onDispose` callback `DisposeAsync` uses; the callback is a single
  `() => _currentTransaction = null` so the second invocation from
  `DisposeAsync` is a no-op. Manifested on SQLite as
  `TransactionIntegrityTests.CommitAsync_PersistsWrites` failing the
  visibility probe right after commit.
- **`SqliteRedbConnection.CreateCommand` gates `cmd.Transaction = …` on
  `_currentTransaction.IsActive`.** Defense-in-depth alongside the
  transaction-class fix above — even if some future code path forgets to
  clear `_currentTransaction`, commands fired after Commit / Rollback bind
  to no transaction (running against the autocommit connection) instead of
  throwing.
- **`SqliteDataSource.ApplyPragmas` now sets `journal_mode=WAL` and
  `synchronous=NORMAL` on every connection.** Without WAL, Microsoft.Data.Sqlite
  defaults to journal mode `DELETE` where writers block readers — concurrent
  reads during an open write tx surface as `SqliteException: database table
  is locked: <name>`, breaking redb's check-then-save patterns and any
  uncommitted-read visibility probe. WAL is the recommended production
  journal mode and matches the configuration used by ASP.NET Core
  Identity's SQLite sample plus most third-party deployments.
- **`ProducerTemplate.SendAsync` / `RequestBody` auto-start the cached
  producer.** `IProducerTemplate` overloads resolved an endpoint, cached a
  fresh `IProducer` from `endpoint.CreateProducer()`, and called
  `producer.Process(exchange)` directly. For DirectVm / Direct / Seda
  producers (which don't extend `ConnectableProducer`) this was fine; for
  every transport that does (`HttpProducer`, `KafkaProducer`,
  `AmqpProducer`, `AzureServiceBusProducer`, `MqttNetProducer`,
  `RabbitMqProducer`, `RedisProducer`, `SmtpProducer`, `LdapProducer`,
  `WmqProducer`, …) `EnsureStarted()` threw `"<name> has not been started.
  Call Start() first."` because the cached producer was never started.
  `SendAsync(IEndpoint, IMessage)`, `SendAsync(IEndpoint, object)`, and the
  two `RequestBody(IEndpoint, …)` overloads now call
  `await producer.Start(ct)` between `GetOrCreateProducer` and the first
  `Process`. The started flag short-circuits via `Interlocked.CompareExchange`
  so the extra call is a one-time setup per producer / process-lifetime
  and a no-op on every subsequent send. Surfaced when wiring outbound HTTP
  webhook delivery through `IProducerTemplate.SendAsync(url, message)` in
  `redb.Identity` (W1 / outbound webhook subscriptions). Also documented
  in `redb.Route/CHANGELOG.md`.
- **`BackgroundDeletionService` drains its queue synchronously on graceful
  shutdown** (`redb.Core`). Previously the host's `StopAsync` only
  cancelled the read loop — tasks that had been enqueued but not yet
  processed were lost; tasks mid-process left their trash containers in
  `status=running` in the DB. The next startup's `RecoverOrphanedTasksAsync`
  then drained those leftover containers one-by-one (each emitting a
  `PurgeTrash completed. Deleted=1` log line — the flood observed after
  a worker restart). Override of `StopAsync` now: marks the channel
  writer as complete, pulls every remaining task and processes it
  synchronously (no inter-batch delays), and respects the host's
  shutdown deadline (`HostOptions.ShutdownTimeout`, default 30 s for
  ASP.NET). Helps **only** when the host actually calls `StopAsync`
  (graceful shutdown via Ctrl+C / SIGTERM, `IHost.StopAsync()`); a
  hard process kill (`Stop-Process -Force` / SIGKILL) still leaves
  orphans the next startup picks up — same behavior as before.
- **`PurgeTrash completed` log line dropped from INF to DBG**
  (`redb.Core`). The line fires once per trash container processed by
  `BackgroundDeletionService`. Each high-level DELETE (e.g.
  `redb.Identity` admin/self-service user delete, DCR cleanup, federation
  provider delete) ships its ids as a single call, so almost every
  container has exactly one object inside and the log spam reads
  `Deleted=1` per item. Worker restarts compound the noise via
  `RecoverOrphanedTasksAsync` draining the accumulated backlog
  one-by-one. Operators who need per-purge visibility now enable DBG
  for the `redb.Core.Providers.Base.ObjectStorageProviderBase`
  category.
- **`UserProviderBase.DeleteUserAsync` and `Users_SoftDelete` SQL recipe no
  longer mutate `_login`** (`redb.Core`, `redb.Postgres`, `redb.MSSql`,
  `redb.SQLite`).
  Previously the soft-delete path appended a `_DEL_<timestamp>` suffix to BOTH
  `_login` and `_name`. PostgreSQL's `protect_system_users` trigger correctly
  flagged that as "Cannot change user login" — `_login` is immutable for ALL
  users by the schema contract, and conceptually "changing login" is a
  delete-and-create sequence, not an update. Fix: the SQL recipe is now
  `UPDATE _users SET _name = ?, _enabled = ?, _date_dismiss = ? WHERE _id = ?`
  (login column dropped), and the C# call passes only the suffixed name. Login
  STAYS as-is so re-registration with the same login is blocked while the
  soft-deleted row exists. Affects any caller of `IUserProvider.DeleteUserAsync` —
  most visibly `redb.Identity` admin DELETE `/users/{id}` and the new self-service
  DELETE `/me`, both of which previously returned 500 ("Database temporarily
  unavailable" wrapping the trigger violation).
- **Pro tree loading no longer calls the server-side `get_object_json` function**
  (`redb.Core.Pro`, affects `redb.Postgres.Pro` + `redb.MSSql.Pro`).
  `TreeQuery(...).ToTreeListAsync()` / `ToRootListAsync()` pull ancestor nodes via
  `TreeQueryProviderBase.LoadObjectsByIdsAsync` (both the generic and polymorphic
  overloads), which were routing through `get_object_json`. When a Pro lazy
  props loader is present, both overloads now load base `_objects` rows with a
  plain `SELECT` and materialize Props entirely in C# via the injected loader
  (`ProLazyPropsLoader` → PVT) — the same path the Pro object-storage provider
  already uses. The Free path is unchanged (still uses `get_object_json`). This
  restores the Pro invariant that the Pro engine never depends on database-side
  materialization functions. (Latent across all Pro providers; surfaced while
  bringing up the upcoming SQLite Pro provider.)
- **GroupBy / Window projection value conversion** (`redb.Core`, Free + Pro).
  `ConvertJsonValue` (grouped and tree-grouped windowed queryables) now unwraps
  `Nullable<T>` and handles JSON `Number → bool` (a boolean group key serialized
  as `0`/`1` rather than `true`/`false`), `Number → float`, and `String →
  bool`/`Guid`/`DateTimeOffset`. Previously these fell through to a string and
  threw `Object of type 'System.String' cannot be converted to type
  'System.Boolean'` when a projection member's type didn't match the JSON
  shape. PostgreSQL was unaffected because it emits native `true`/`false`.
- **MSSql Free: `DISTINCT` with paging/order no longer fails with "The multi-part
  identifier 'o._id' could not be bound"** (`redb.MSSql`, v2-pvt module
  `0.1.2 → 0.1.3`). `pvt_build_query_sql` wraps the `@distinct = 1` row-source in a
  derived table (`_dist`) that projects only `[_id]`, but appended the outer
  `ORDER BY` built with the inner alias prefix (`o.` / `_pvt_cte.`), which is not in
  scope outside the wrapper. Any `Distinct()` combined with `Take()`/`OrderBy`
  (e.g. `Query<T>().Distinct().Take(100)`) threw. The outer order now references the
  projected `[_id]` (new `@order_sql_dist`) in all three distinct branches (Shape A
  pure-base, Shape B/C pivot, tree). `EnsurePvtModuleDeployedAsync` redeploys the
  bundled module on the version bump. PostgreSQL was unaffected (it emits a single
  `SELECT DISTINCT o._id … ORDER BY o._id` with `o` in scope — no `_dist` wrapper).

## [3.0.0] — 2026-05-28

### Added
- **PG Free: full v2-pvt query engine reaches Pro-parity (0.5.x → 0.6.1)**.
  The PostgreSQL Free path got the feature-complete v2-pvt module ahead of
  MSSql Free (commits 2026-05-21 … 2026-05-28). Before this series the Free
  path was emitting `-- not available in Open Source` stubs for several
  preview surfaces and was missing several Pro-only operators. Now in Free
  on PG:
  - **Universal "no black box" SQL preview** for `GroupBy` / `Window` /
    `GroupedWindow` / `Tree-*` via two-pass compile (`pvt_build_*_sql`);
    tree previews resolve the subtree and delegate to the matching non-tree
    preview with a `-- Tree …: subtree resolved to N object(s)` header.
  - **`Sql.Function<T>` whitelist** at the SQL boundary
    ([17_pvt_expr.sql](redb.Postgres/sql/v2-pvt/17_pvt_expr.sql)) with a
    hardcoded ELSIF chain and `RAISE EXCEPTION` for non-whitelisted names;
    parser routes `Sql.Function<T>(name, args)` to
    `CustomFunctionExpression` (FREE-OVER-PRO §2.4).
  - **`ValueTuple` composite dict keys** (`Dictionary<(int,int), V>`)
    consistently encoded as Base64-JSON on both write and read sides
    (FREE-OVER-PRO §2.2).
  - **`arr.Length` / `coll.Count`** in filters via the array-aware
    `FacetFilterBuilder` (`.$count` modifier in Free); `e.Tags.Any()`
    1-arg form mapped to `<field>.$length > 0`.
  - **`Take(0)` returns empty** instead of `ArgumentException`.
  - **`HAVING` parser + `ArrayGroupBy`** with PVT agg array `unnest`
    ([19_pvt_agg_expr.sql](redb.Postgres/sql/v2-pvt/19_pvt_agg_expr.sql)) —
    fixes `42883 function sum(bigint[]) does not exist`;
    [26_pvt_array_groupby.sql](redb.Postgres/sql/v2-pvt/26_pvt_array_groupby.sql)
    added.
  - **`ListItem.Value` / `.Alias` via a single `LEFT JOIN _list_items`**
    (v2-pvt 0.6.1) — plan-shape parity with Pro; replaces correlated
    subquery per field.
  - **Nested-dict CTE pushdown** for `Field[key].Child` (FREE-OVER-PRO §2.x):
    outer `WHERE` references the already-built pivot column instead of a
    redundant `EXISTS` over `_values`.
  - **Auto-deploy of the v2-pvt bundle on version mismatch** (see the
    matching item below — same infrastructure serves both PG and MSSql).

  The MSSql Free engine described next ports this PG Free baseline; the
  parity line in the next item ("145/145 parity with PG Free") refers to
  this newly-completed PG Free feature set, not a pre-existing one.

- **MSSql Free: full v2-pvt query engine (0.1.0 → 0.1.3) — 145/145 parity
  with PG Free**. The old MSSql Free path generated a wide inline CASE WHEN
  aggregate; it is now replaced with the Pro-shape CTE: a single pass over
  `_values` using `MAX(CASE WHEN _id_structure = X AND _array_index IS NULL
  THEN ...)` and a single `LEFT JOIN _list_items`. All modes present in PG
  Free are implemented: flat/tree, scalar/array/dict fields, ListItem
  (`.Id`/`.Value`/`.Alias`), same-scheme nested POCO (compound path),
  `OrderBy`/`DistinctBy`/`Take`/`Skip`, `GroupBy`/`HAVING`, `ArrayGroupBy`
  (via `OUTER APPLY`), array aggregates (`$count`, `$sum`/`$avg`/`$min`/`$max`
  over `_Long`/`_Double`/`_Numeric`/`_DateTimeOffset`), array operators
  (`$arrayContains`, `$arrayAny`, `$arrayCount*`, `$arrayAt`,
  `$arrayStartsWith`, etc.), `Sql.Function` (whitelist), `$expr`, null
  semantics (`$exists`/`$notNull`). The SQL module is split into 27 source
  files under [redb.MSSql/sql/v2-pvt/](redb.MSSql/sql/v2-pvt/) assembled
  into a single `pvt_bundle.sql` by MSBuild. Delivery stages: Stage 1 (pivot
  CTE) → 2a (tree TVFs) → 2b (tree provider) → 2c.E (nested-dict accessor
  `Field[key].Child`) → 0.1.1 LIKE-pattern fix → 0.1.2 string `$const`
  unwrap + ListItem `$arrayContains` → 0.1.3 nested-dict CTE pushdown +
  outer `WHERE` references pivot column instead of a redundant `EXISTS`.
  **Shape parity with Pro throughout**: `_id_scheme` + `extra_where` +
  tree-filter pushed into inner `_objects` subquery, narrow-with-nested CTE
  (skips `_values` JOIN when no scalar sids), stable default `ORDER BY` when
  paging without an explicit order.

- **Auto-deploy v2-pvt bundle on version mismatch (both databases)**.
  `ISqlDialect` gained `Query_PvtRequiredVersion()` — the semver the embedded
  bundle ships. `RedbServiceBase.EnsurePvtModuleDeployedAsync` reads
  `pvt_module_version()` on `InitializeAsync()`, compares with an exact-match,
  and automatically applies the embedded `pvt_bundle.sql` resource when the
  deployed version differs. No more manual `DROP FUNCTION … CREATE FUNCTION …`
  after a SQL change. The MSBuild target `ConcatenateSqlFiles` regenerates the
  bundle whenever any `.sql` source changes (hooked to `DispatchToInnerBuilds`
  for multi-TFM builds; `EmbeddedResource` uses an explicit `LogicalName` —
  without it MSBuild silently replaces `-` with `_` in resource paths, causing
  `GetManifestResourceStream` to return `null`).

- **Pro: `GroupBy` + `HAVING` via PVT pipeline on both providers
  (Postgres.Pro + MSSql.Pro)**. `HavingAsync` existed in Free but had no Pro
  counterpart. Added full HAVING parser in the shared facet layer
  (`FacetFilterBuilder`), SQL generation in both Pro providers, and a base
  test suite in
  [GroupByHavingTestsBase](redb.Tests.Integration/Tests/Base/GroupByHavingTestsBase.cs)
  with per-dialect wrappers (PG, PG.Pro, MSSql.Pro). 33/33 HAVING + 6/6
  no-HAVING — all green.

- **Pro: `GroupBy` over array fields (`ArrayGroupBy`) — unified implementation
  for Postgres.Pro + MSSql.Pro**. PG.Pro uses an inline `GroupByArray` override
  with PVT agg array `unnest`; MSSql.Pro has its own override.
  `GroupBy(items => items.SelectMany(o => o.Skills))` with aggregates works
  on all four tiers (PG Free, PG.Pro, MSSql Free, MSSql.Pro).

- **MSSql Pro: `AggregateBatch` parity with PG.Pro — non-numeric MIN/MAX and
  inline filter subquery**. `MinAsync`/`MaxAsync` over `string`/`DateTime`/`Guid`
  fields and a `Where` filter inside a batch aggregation now produce the same
  query shape as PG.Pro (PVT CTE + outer aggregate).

- **MSSql Free: pushdown parity with Pro/PG for expression-form predicates
  and `$expr`** — the filter-splitting optimizer
  [`pvt_split_filter`](redb.MSSql/sql/v2-pvt/16_pvt_split.sql) now pushes
  top-level `$eq/$ne/$lt/$lte/$gt/$gte/$like/$ilike/$in/$nin/$between/$null/
  $notnull/$contains/$startsWith/$endsWith` expressions and arbitrary boolean
  `$expr` trees into the inner `_objects o` subquery (Shape A) when all
  `$field` references resolve to `kind='base'`. If any props field is present
  the node stays in the residual (Shape C). The new classifier
  [`pvt_expr_is_base_only`](redb.MSSql/sql/v2-pvt/17_pvt_expr.sql) makes
  this decision; the pushdown SQL itself is generated by the existing
  [`pvt_build_where_from_json`](redb.MSSql/sql/v2-pvt/14_pvt_where.sql)
  walker (extended with a `$expr` branch). Covered by 4 functional and 3
  shape-inspect tests in
  [`99_smoke_auto.sql`](redb.MSSql/sql/v2-pvt/99_smoke_auto.sql)
  (195 PASS / 0 FAIL / 1 SKIP).

### Fixed
- **Schema sync now honors `Configuration.DefaultStrictDeleteExtra`**
  (FREE-OVER-PRO §4 #1). Prior to this fix `RedbServiceConfiguration.DefaultStrictDeleteExtra`
  was set by builders, copied across configuration clones and read from
  `appsettings`, but **no execution-path code consumed it** —
  [`SchemeSyncProviderBase.SyncSchemeAsync<T>`](redb.Core/Providers/Base/SchemeSyncProviderBase.cs)
  hardcoded `strictDeleteExtra: true`, so old binaries restarting in a
  multi-version rolling deploy would unconditionally remove `_structures`
  rows added by the new binary, and every `_values` row referencing those
  structures along with them. On PostgreSQL this is done via the FK
  `_values._id_structure -> _structures._id ON DELETE CASCADE`
  ([redbPostgre.sql:215](redb.Postgres/sql/redbPostgre.sql#L215)). On MSSQL
  the same effect is produced by the `INSTEAD OF DELETE` trigger
  `TR__structures__cascade_values`
  ([redbMSSQL.sql:717](redb.MSSql/sql/redbMSSQL.sql#L717)) — the FK
  `NO ACTION` at [redbMSSQL.sql:270](redb.MSSql/sql/redbMSSQL.sql#L270)
  is a workaround for the MSSQL multiple-cascade-paths restriction, not a
  behavioral difference. `SyncSchemeAsync<T>` now reads
  now reads `Configuration.DefaultStrictDeleteExtra` instead. The default
  value is preserved (`true`) so users on the default config see no
  behavioral change. **Behavioral change**: the built-in presets
  `Development`, `HighPerformance`, and `Migration` (in
  [`PredefinedConfigurations.cs`](redb.Core/Models/Configuration/PredefinedConfigurations.cs))
  already declared `DefaultStrictDeleteExtra = false`; that setting was
  silently ignored before and now actually takes effect — apps on those
  presets will no longer auto-delete `_structures` rows missing from the
  `Props` class on startup.

 Added
  a fallback to `ROW_NUMBER() OVER (PARTITION BY <key> ORDER BY (SELECT 1))`
  + `WHERE _rn = 1` (symmetric with the Free path), plus support for
  `CoalesceExpression` in the `DistinctBy` key.

- **PG v2-pvt 0.6.1: ListItem `.Value`/`.Alias` now uses a single
  `LEFT JOIN _list_items`** instead of a correlated subquery per field —
  plan-shape parity with Pro. Additionally: nested-dict predicates in the
  outer `WHERE` now reference `_pvt_cte.[<field>]` (the already-built pivot
  column) instead of re-running a separate `EXISTS` over `_values`.

- **MSSql Free: `ORDER BY $expr` on base fields no longer produces "constant
  in ORDER BY"** — two regressions fixed: (1)
  [`pvt_collect_fields`](redb.MSSql/sql/v2-pvt/10_pvt_field_collection.sql)
  did not walk `$expr` nodes in order entries, so a field like `Age` was not
  collected, the shape was classified as A, and `pvt_b2_expr_sql` emitted
  `/*unknown-b2-field:Age*/NULL` turning `Age*2` into a constant; (2)
  [`pvt_build_order_conditions`](redb.MSSql/sql/v2-pvt/15_pvt_order.sql)
  passed a trailing-dot alias (`_pvt_cte.`) into `pvt_b2_expr_sql`, producing
  the double-dot `_pvt_cte..[_name]` for base fields inside `$expr` ORDER.
  Both sites fixed.

- **`arr.Length` / `coll.Count` in `Where` filters no longer crash on array
  PVT columns** — `e.Skills!.Length >= 3` was translated to `LENGTH(text[])`
  and raised PostgreSQL error 42883.
  [BaseFilterExpressionParser](redb.Core/Query/Parsing/BaseFilterExpressionParser.cs)
  now emits `PropertyFunction.Count` (instead of `PropertyFunction.Length`)
  for CLR `UnaryExpression(ArrayLength)` nodes. In Pro this produces
  `COALESCE(array_length(col,1), 0)`; in Free,
  [FacetFilterBuilder.TryBuildArrayLengthCountFilter](redb.Core/Query/FacetFilterBuilder.cs)
  translates the filter to the PVT modifier `.$count`. `PropertyInfo` gained
  an optional `FunctionSourceType` field so the facet builder can distinguish
  arrays from strings when choosing the modifier. Covered by
  `PropertyFunction_ArrayCount_Filters` on both tiers.

- **`Take(0)` now returns an empty result instead of `ArgumentException`** —
  validation in `RedbQueryable.Take()` and `TreeQueryableBase.Take()` relaxed
  from `count <= 0` to `count < 0` to match standard LINQ semantics
  (`Enumerable.Take(0)` → empty). Affects both tiers (Free and Pro), flat and
  tree queries. Covered by `Take_Zero_ReturnsEmpty_WithoutThrowing` and
  `Take_Zero_ReturnsEmpty_OnTreeQuery` in `PvtAuditTestsBase`.

### Tests
- `PostgresFreePvtAuditTests` moved to the shared base
  [PvtAuditTestsBase](redb.Tests.Integration/Tests/Base/PvtAuditTestsBase.cs)
  and now runs against both `PostgresFixture` (Free) and `PostgresProFixture`
  (Pro) — a regression on either tier fails immediately. Added tests for
  `Take(0)` (flat + tree), `Take(-1)` (still throws), and `DistinctBy` on a
  tree query.
- **Three audit probes from FREE-OVER-PRO §2.x confirmed working** on both
  tiers without any SQL/parser changes — tests were the only missing piece:
  - `DictTupleKey_PerformanceReviews_FiltersByCompositeKey` (§2.2) —
    `ValueTuple` dict keys are encoded by `RedbKeySerializer` to Base64-JSON
    consistently on the write side and in
    [BaseFilterExpressionParser L602](redb.Core/Query/Parsing/BaseFilterExpressionParser.cs#L602).
  - `ObjectRef_CurrentProject_NotNull_Filters` /
    `ObjectRef_CurrentProject_IsNull_Filters` (§2.3, null-check path) —
    `e.CurrentProject != null` / `== null` on `RedbObject<T>?` fields works
    via `$exists` / `$ne null`.
  - `SqlFunction_Coalesce_Filters` + `SqlFunction_UnknownName_ThrowsWhitelistViolation`
    (§2.4) — `Sql.Function<T>(name, args)` is routed by the parser to
    `CustomFunctionExpression`, `FacetFilterBuilder` emits
    `{"$<funcname>": [...]}`, and `pvt_build_scalar_expr`
    ([17_pvt_expr.sql](redb.Postgres/sql/v2-pvt/17_pvt_expr.sql)) implements
    the whitelist with a hardcoded ELSIF chain and `RAISE EXCEPTION` for
    unknown names.
  - Full PG suite (Free + Pro): **328 passed / 0 failed / 2 skipped**. The
    two remaining skips are `ObjectRef_CurrentProject_NestedField_Filters`
    (cross-scheme JOIN path, confirmed broken in both tiers — requires new
    infrastructure in both PVT and `ProQueryProvider`).
- **ListItem `.Value`/`.Alias` `OrderBy` capability gate** — PG Free PVT
  sorts by `Status.Value`/`.Alias` correctly (on par with Pro); the
  `if (IsPro)` guard in `ListItem_OrderByValue_SortsAlphabetically` /
  `ListItem_OrderByAlias_SortsAlphabetically` was overly conservative.
  Added virtual `SupportsListItemValueAliasOrdering` (default = `IsPro`) in
  `ListTestsBase`; `PostgresListTests` overrides to `true`. Result: PG Free +
  PG Pro + MsSql Pro pass with strict ordering; MsSql Free remains gated
  (insertion-order only — `ORDER BY` on a JSON expression is ignored).

### Documentation
- **New section "Schema lifecycle and multi-version deployments"** in the
  root [README.md](README.md): documents read=graceful / write=destructive,
  the `services.AddRedb(... .Configure(c => c.DefaultStrictDeleteExtra = false))`
  opt-out, the new warning log, and the equivalent cascade semantics across
  backends \u2014 PostgreSQL uses FK `ON DELETE CASCADE` on
  `_values._id_structure`, while MSSQL achieves the same effect through the
  `TR__structures__cascade_values` `INSTEAD OF DELETE` trigger (MSSQL FK is
  `NO ACTION` only to work around the multiple-cascade-paths restriction).
- Rewrote docs/FreePvtQuery/FREE-OVER-PRO.md
  §4: marked F0+F1 (this release) as done, demoted F3 (default flip) to a
  major-version task, made the cache-state-dependent nature of the Pro
  ChangeTracking destructiveness explicit (per-instance cache refresh window),
  and corrected §4.1 — the previous "obligatory `DefaultStrictDeleteExtra = false`"
  guidance was non-functional before v2.0.3 and is now actually wired.
- Updated docs/FreePvtQuery/FREE-OVER-PRO.md:
  H1 (`Take(0)`) marked fixed; H8 (tree `DistinctBy`) re-classified as
  already implemented in both tiers; §0 and §2.x updated for §2.2 /
  §2.3-null / §2.4 closures; §1 #5 (`Sql.Function`) no longer marked
  unimplemented; §2 #3, #5, #6 marked done; added §2 #6b (deferred
  nested-field cross-scheme JOIN — confirmed as a two-sided gap in Free PVT
  and Pro `SchemeFieldResolver`); added §3 #11 (MsSql Free ignores
  `OrderBy(Status.Value)`/`.Alias`).

## [2.0.2] — 2026-05-16

### Changed
- **`EavSaveStrategy` renamed to `PropsSaveStrategy`** — users frequently asked
  whether RedBase uses the EAV (Entity-Attribute-Value) pattern. RedBase's
  storage model resembles EAV in structure (`_objects` + `_values`), but
  differs in key ways: schemes are strictly typed, fields are schema-bound
  (not free-form key-value pairs), and the query layer compiles LINQ directly
  to typed SQL without generic key-value lookups. The `Eav` prefix was
  misleading. Renamed throughout:
  - `EavSaveStrategy` enum → `PropsSaveStrategy`
  - `RedbServiceConfiguration.EavSaveStrategy` property → `PropsSaveStrategy`
  - JSON/appsettings key `"EavSaveStrategy"` → `"PropsSaveStrategy"`
    (**breaking**: update `appsettings.json` / environment variables if set explicitly)
  - `Tsak:Redb:EavSaveStrategy` config key → `Tsak:Redb:PropsSaveStrategy`

## [2.0.1] — 2026-05-08

### Fixed
- **Pro Props LINQ→SQL: 80× perf regression on mixed base + props filters** —
  when a query combined a base-field predicate (`WhereRedb(o => o._id_parent == X)`,
  `parentIds.Contains(o.ParentId.Value)`, etc.) with a props predicate
  (`Where(props => ...)`), the base predicate was applied as an outer `WHERE`
  **after** the PVT CTE aggregation (`array_agg FILTER` on Postgres /
  `MAX(CASE WHEN ...)` on MSSql). PVT was built over the entire scheme
  (millions of rows) and only then filtered down — observed 894 ms vs 11 ms.
  - The base predicate is now compiled with an empty table alias and pushed
    **into** the inner `(SELECT _id FROM _objects WHERE _id_scheme = X AND <baseFilter>)`
    subquery of the PVT CTE. The outer base `WHERE` is removed when pushdown
    fires, so generated SQL contains no duplicated predicate.
  - Affects flat queries (`ToListAsync`, `CountAsync`, `ExecuteDeleteAsync`),
    aggregations (`SumAsync` etc., aggregate batch), and window functions.
  - Both providers fixed symmetrically (`redb.Postgres.Pro`, `redb.MSSql.Pro`).
  - Added new public helper `ProSqlBuilder.CompileBaseFieldsForObjectsSubquery`
    that emits base-field SQL without the `o.` alias prefix for use inside
    the `_objects` subquery.

- **Pro Props LINQ→SQL: same regression on tree queries** — the tree variant
  (`AsTreeQuery`, `AsTreeWindowQuery`) had the identical anti-pattern: base-field
  predicate was applied as outer `WHERE` after the tree pvt_cte aggregation
  (`_objects oo JOIN tree t JOIN _values v` with `array_agg FILTER` /
  `MAX(CASE WHEN)`). With large trees + selective base filter (e.g.
  `_id_parent = X`), PVT aggregated all tree members before the predicate
  narrowed the result.
  - Base predicate is now pushed into the tree pvt_cte's inner
    `WHERE oo._id_scheme = X AND <baseFilter>` (with `oo` alias to disambiguate
    `_id`/`_id_parent`/`_hash` from the joined `tree t`).
  - The Tree-Window path (`BuildTreeWindowSqlTypedAsync`) now also pushes the
    base filter into `BuildPvtSubquery`'s additional WHERE
    (`o._id = ANY(ARRAY(SELECT _id FROM tree)) AND <baseFilter>` on Postgres,
    `o._id IN (SELECT _id FROM tree) AND <baseFilter>` on MSSql).
  - Tree-Aggregation uses correlated per-row subqueries and is not affected;
    Tree-GroupBy and Tree-GroupedWindow already pushed correctly.
  - `CompileBaseFieldsForObjectsSubquery` extended with optional
    `baseTableAlias` parameter (default `""`) — backwards-compatible for the
    flat case; tree case passes `"oo"`.
  - All 134 existing tree + ParentId integration tests pass on both providers.

### Added
- **Integration tests for the pushdown contract** in
  [redb.Tests.Integration/Tests/Base/WhereTestsBase.cs](redb.Tests.Integration/Tests/Base/WhereTestsBase.cs)
  covering `WhereRedb(parentIds.Contains(...)) + Where(props)` for
  `ToListAsync`, `CountAsync`, and `OrderBy + Take`. Tests run for both
  Postgres Pro and MSSql Pro fixtures.

## [2.0.0] — 2026-05-07

### Changed
- **License changed from MIT to Apache-2.0** for all OSS packages
  (`redb.Core`, `redb.Postgres`, `redb.MSSql`, `redb.CLI`, `redb.Export`,
  `redb.Templates`, `redb.PropsEditor`).
  - Apache 2.0 adds an explicit patent grant (§ 3) and termination clause —
    stronger protection for users and contributors.
  - All previously published versions (≤ 1.3.0) on nuget.org remain under MIT.
  - Pro packages (`*.Pro`, `redb.Licensing`) are unaffected — still under the
    commercial license in `LICENSE-PRO.txt`.
  - Every nupkg now ships `LICENSE` + `NOTICE` files (Apache 2.0 § 4 attribution).
  - Contributions are now accepted under Apache-2.0; see `CONTRIBUTING.md`.
- **Strong-Name signing** is now active for all Pro assemblies
  (Public Key Token: `8e6fea371ffeb38e`). This is a binary-identity change
  for Pro consumers — assembly identity differs from previous unsigned releases.

### Why this is a major version bump
- License change is a downstream-compliance breaking change.
- Pro Strong-Name change is a binary-identity breaking change.
- No source-level API changes vs 1.3.0.

## [1.3.0] — 2026-04-18

### Fixed
- **Nullable `.Value` in `WhereRedb` resolved to wrong column** — `o.ParentId.Value == 42` generated SQL against `_id` instead of `_id_parent`. Fixed in all parsers and Pro SQL compilers.
- **`.HasValue` generated `field = true` instead of `IS NOT NULL`** — `o.ParentId.HasValue` produced type mismatch (`bigint = boolean`). Now emits `IS NOT NULL` / `IS NULL`.
- **Props cache skipped hashless objects** — `LoadPropsForManyAsync` never added objects without `_hash` to `needToLoad`, leaving their Props null.
- **Missing `_hash` in nested object SQL** — `Materialization_SelectObjectsByIds` lacked `_hash` column; nested saves corrupted existing hashes.
- **Lexicographic ArrayIndex sorting** — arrays with 10+ items sorted as `"10" < "2"`, causing false ChangeTracking updates. Now uses numeric sort.
- **Duplicate RedbObject in SaveAsync** — `CollectNestedRedbObjectsFromProperties` didn't deduplicate by ID, crashing ChangeTracking `ToDictionary`.
- **MERGE duplicate row in ChangeTracking** — `BulkUpdateValuesAsync` received duplicate `_id` values. Added `DeduplicateValueUpdates` guard.
- **Missing `ArrayParentId` for nested RedbObject refs** — `ProcessSingleIRedbObject` didn't set `_array_parent_id` inside business class arrays, breaking ChangeTracking diffs.
- **Guid field not persisted** — `SetSimpleValueByType` lacked `case "Guid"`, saving to `_String` instead of `_Guid` column.
- **`DeleteSubtreeAsync` threw "Scheme for type Object not found"** — replaced `GetDescendantsWithUserAsync<object>()` with polymorphic `CollectDescendantIds`.
- **`LoadTreeAsync(maxDepth: 1)` returned no children** — off-by-one: `maxDepth` was decremented before recursion instead of inside it.
- **MsSql Pro `Where(x => x.Field == null)`** — `CompileNullCheck` now correctly generates `IS NULL` / `IS NOT NULL` against PVT CTE columns.
- **MsSql Pro `DistinctBy` returned duplicates** — added `ROW_NUMBER() OVER (PARTITION BY ...)` CTE wrapper with `WHERE _rn = 1`.
- **MsSql DELETE stored procedures** — `SET NOCOUNT OFF` for correct affected-rows count from `ExecuteNonQueryAsync`.
- **MsSql Free WHERE null (`$exists false`)** — generates `NOT EXISTS(...)` instead of `1=0`.
- **MsSql Free OrderBy** — zero-padded numeric conversion; `ROW_NUMBER()` preserves sort through JOIN.
- **`get_object_json` / `build_field_json`** — returns `"properties": null` for objects without `_values`. Array/dict without head records return NULL instead of `[]`/`{}`.
- **KeyGenerator shared cache** — domain-isolated `KeyCacheDomain` prevents duplicate key violations across providers.
- **SaveAsync deadlocks** — `ORDER BY _id` + `ROWLOCK` (MsSql) in locking queries; consistent lock ordering.
- **MsSql reader-writer deadlocks** — init script enables `READ_COMMITTED_SNAPSHOT ON`.
- **`SchemeFieldResolver` not domain-isolated** — per-domain cache with 5-min TTL and self-heal on cache miss.
- **`SyncSchemeAsync` didn't cache scheme** — schemes cached immediately after sync, eliminating extra DB roundtrips.
- **PropsSaveStrategy ignored in Tsak Worker** — reads `Tsak:Redb:PropsSaveStrategy` from config.
- **`SimplePasswordHasher`** — replaced custom compare with `CryptographicOperations.FixedTimeEquals`.
- **GroupBy aliased keys returned null** — alias resolution for `g.Key`, `g.Key.X`, `g.Key.X.Id` patterns.
- **ListItem `Contains` passed raw objects** — `IRedbListItem` → `.Id` conversion in `VisitEnumerableContains` / `VisitCollectionContains`.
- **MsSql ListItem operators** — `$in`, `$notIn`, `$arrayContains` now use `_listitem` column.
- **Postgres `$arrayContains` for ListItem arrays** — `_listitem` column with `bigint` cast instead of `_String`.
- **Pro OrderBy `ListItem.Value` / `ListItem.Alias`** — subquery JOINs `_list_items` for text sorting; CTE bypassed for ListItem fields.
- **GroupBy/Window base field filter crash** — `WhereRedb` on base fields (`ValueString`, `Name`, etc.) combined with `GroupBy`/`Window`/`GroupedWindow` produced `pvt._value_string does not exist`. Root cause: naive `.Replace("o.", "pvt.")` put base field filter outside PVT subquery where `o.` doesn't exist. Fix: base filters now injected inside the inner subquery WHERE clause (before aggregation), props filters remain on outer `pvt`. Affected 9 sites across Postgres.Pro and MSSql.Pro (Grouping, TreeGrouping, TreeWindow, TreeGroupedWindow, GroupedWindow). Also fixes Postgres TreeGroupedWindow where base filter was silently ignored.

### Added
- **`save_object_json`** SQL functions (Postgres + MsSql) — inverse of `get_object_json`, writes JSON back via DeleteInsert.
- **`DeadlockRetryHelper`** — automatic retry with exponential backoff for deadlock exceptions.
- **`BcryptPasswordHasher`** — bcrypt (work factor 12) with backward-compatible SHA256 verification and lazy rehash.

## [1.2.14] — 2026-02-16

### Fixed
- **Free projection double-load bug** — `LazyPropsLoader.LoadPropsForManyAsync` ignored `projectedStructureIds` and reloaded full objects via `get_object_json`, overwriting partial Props returned by SQL projection. Fix: `SkipPropsLoading = true` + `UseLazyLoading = false` in `ToListWithProjectionAsync` (`RedbQueryable`, `TreeQueryableBase`). `RedbProjectedQueryable` now always sets `skipProps = true`.

### Changed
- `QueryContext.SkipPropsLoading` now also controls lazy loader assignment — prevents both eager and lazy Props post-processing during projections.
- Multi-target NuGet packages: `net8.0`, `net9.0`, `net10.0`.
- CLI: trial limit 1,024 requests per app launch (resets on restart).

## [1.2.13] — 2026-01-20

### Fixed
- Pro `DeleteAsync` uses PVT builders instead of facet functions.
- ChangeTracking: handle existing nested `RedbObject` references correctly.
- Multiple bug fixes for open-source (FREE) version.

### Added
- Tree API for hierarchical queries.
- Window functions support in LINQ queries.
- Domain-isolated caches: `GlobalMetadataCache`, `GlobalListCache`, `GlobalPropsCache`.

### Changed
- Unified Pro feature exceptions.
- Query pipeline improvements.
