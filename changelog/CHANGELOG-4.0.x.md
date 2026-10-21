# redb changelog: 4.0.x

Releases of the 4.0 line, moved here unchanged from CHANGELOG.md, which keeps the latest released line only.
Newer releases: [CHANGELOG.md](../CHANGELOG.md).

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
  the field on read at all (what was written came back null).** The rest of the
  byte[] projection report: the Б1 claim covered byte[]
  properties in Props, while the JSON projection walked around the root `_objects._value_bytes`
  column. PG 08 - encode(..., base64) without MIME line breaks (bundle 0.7.5); MSSQL 09 -
  base64 emitted via the XML trick (bundle 0.2.9). The report's manual byte matrix is now the
  permanent BytesRoundTripTests suite x6 (Props bytes + the scalar BLOB layout + the root bytes
  reaching the column, pinned against the silent NULL) - red-before: two cells red before the
  fix, 12/12 after.
- **ChangeTracking performance waves F1-F9 - no semantic
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
  deleted, nothing written - the wipe fixed earlier for our own stubs, reopened by any other
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
  stores a `byte[]` property as an array of bytes, not a root scalar — not decided yet; the encoder is ready for it.

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
