# Changelog

All notable changes to RedBase will be documented in this file.
This changelog covers the **NuGet-published packages** only:

| Package | Edition |
|---------|---------|
| `RedBase.Core` | Free |
| `RedBase.Postgres` | Free |
| `RedBase.MSSql` | Free |
| `RedBase.SQLite` | Free |
| `RedBase.Export` | Free |
| `RedBase.Core.Pro` | Pro |
| `RedBase.Postgres.Pro` | Pro |
| `RedBase.MSSql.Pro` | Pro |
| `RedBase.SQLite.Pro` | Pro |
| `RedBase.CLI` | Tool |

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
- **Schema init failed under a non-superuser database owner (`RedBase.Postgres`).** The embedded
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
- **Fail-fast concurrency guard on the provider connection (`RedBase.Postgres`, `RedBase.MSSql`,
  `RedBase.SQLite` + `.Pro`).** An `IRedbService` wraps a single, non-thread-safe DB connection
  (EF-DbContext model). If the same instance is entered from two threads at once, each provider now
  throws a clear `InvalidOperationException` naming the cause — instead of an opaque driver error
  (*"A command is already in progress"*, *"connection is busy"*, *"another read operation is already
  in progress"*). Lightweight `Interlocked` check with zero cost on the normal single-threaded path;
  correct scoped usage is never affected.

### Fixed
- **Query parser: `array.Contains(x)` in `WhereRedb` threw on .NET 9 / C# 13 (`RedBase.Core`).**
  A `string[]` (or any array) `.Contains(x)` inside a `WhereRedb(...)` predicate now binds to the
  `ReadOnlySpan` overload (`System.MemoryExtensions.Contains`) rather than `Enumerable.Contains`,
  which the filter parser rejected with `NotSupportedException`. The parser now recognises
  `MemoryExtensions.Contains`, unwraps the array→span conversion, and translates it to the same
  `IN` clause as `Enumerable.Contains` / `List.Contains`. (Refactored the two-arg `Contains`
  translation into a shared `VisitContainsCore`.)
- **`ComputeHash()` NRE on an object with `Props == null` (`RedBase.Core`).** `RedbHash.ComputeForObject`
  dereferenced the object before a null check, so the generic `ComputeFor<TProps>` path (used by
  `RedbObject<TProps>.ComputeHash()`) threw `NullReferenceException` when `Props` was null — even
  though null Props is a supported case (the reflection-based `ComputeFor(IRedbObject)` already
  returned null, and `ComputeForBaseFields` exists for exactly this). Added the missing guard so the
  generic path returns `null` (→ `Guid.Empty`) consistently, instead of throwing.
- **Connection-pool leak on transaction/connection dispose (`RedBase.Postgres`, `RedBase.MSSql`,
  `RedBase.SQLite` + `.Pro`).** Disposing the provider connection could skip returning the physical
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
- **SQLite provider (new): `RedBase.SQLite` (Free) + `RedBase.SQLite.Pro` (Pro).**
  RedBase now runs on SQLite — same LINQ API, same 13-table model, same
  `AddRedb(...)` wiring as Postgres/MSSql, with `Data Source=app.db`. The
  provider is swappable at the DI line; the rest of the application is unchanged.
  - **`RedBase.SQLite.Pro` is pure C#** (query SQL built by `ProSqlBuilder`, props
    materialized in C#, no database-side functions), so it runs anywhere
    `Microsoft.Data.Sqlite` runs — including **Blazor WebAssembly** and **mobile
    (MAUI / iOS / Android)**, where a native SQLite extension cannot be loaded. This
    is the embedded/offline/in-browser tier people asked for.
  - **`RedBase.SQLite` (Free)** hosts the in-DB machinery as a **native C loadable
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
  `RedBase.Core.IUserProvider` for case-insensitive lookup by `_users._email`.
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
  (`RedBase.SQLite` + `RedBase.SQLite.Pro`).** The previous TEXT storage made range
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
- **Datetime analytics decode through a storage-agnostic hook (`RedBase.Core`).**
  `Min/Max/AggregateRedbAsync`, window and group-by select the raw datetime column
  (bypassing `get_object_json`) and hand the value to core converters
  (`JsonValueConverter`, `AggregateResult.Get<T>`, scalar `Convert.ChangeType`). To
  let SQLite's numeric Julian round-trip without teaching `RedBase.Core` about Julian
  days, a nullable `TemporalDecoder.NumericDecoder` extension point was added: when a
  *numeric* value targets a temporal CLR type and a decoder is registered, it is used;
  otherwise the existing path runs. `RedBase.SQLite`/`.Pro` register
  `SqliteJulian.FromJulian` at configure time. The hook is null for Postgres/MSSql
  (which never return a number for a temporal column), so their behavior is unchanged.
  Pro reuses the same core converters, so one hook fixes Free and Pro alike.
- **`BackgroundDeletionService` switched from in-memory channel to DB polling**
  (`RedBase.Core`). Earlier revisions used a `Channel<PurgeTask>` queue for
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
  (`RedBase.Core` + all `.Pro`).** `TreeProviderBase.CollectDescendantIds` (the
  `DeleteSubtreeAsync` path) lives in the shared base — Pro overrides the polymorphic
  *load* tree methods but not this one — and it used the `Tree_SelectPolymorphicChildren`
  recipe, which embeds `get_object_json`. On PostgreSQL/SQL Server that function exists
  server-side in every tier, so it ran but needlessly materialized each child's full JSON
  just to read its id; on **SQLite Pro** (no native extension) it threw
  `no such function: get_object_json`. Fixed by collecting subtree ids through a new
  id-only dialect recipe `Tree_SelectChildrenIds` (`SELECT _id … WHERE _id_parent = …`) —
  lighter for every dialect and tier. Pro source now contains zero `get_object_json` calls.
- **`DeleteSubtreeAsync` returns the real subtree size (`RedBase.Core`, all dialects).**
  It now returns the count of collected objects (self + descendants) instead of the raw
  `DELETE` rows-affected, which under-counts on SQLite where the `_id_parent ON DELETE
  CASCADE` FK removes child rows as a side effect (PostgreSQL/SQL Server have no such
  cascade, so the value is unchanged there).
- **Boolean keys/projections materialize correctly on SQLite (`RedBase.Core`, shared).**
  `JsonValueConverter` now accepts a JSON `Number` as a `bool` (nonzero → true): SQLite has
  no native boolean and stores it as `INTEGER` 0/1, so `GroupByArray`/projection columns
  arrived as numbers and always read `false`. PostgreSQL/SQL Server (which emit JSON
  `true`/`false`) are unaffected.
- **SQLite Free: `DistinctBy(field)` now deduplicates (`RedBase.SQLite`).** The native
  v2-pvt query builder ignored `distinct_on` (SQLite has no `DISTINCT ON`), so
  `DistinctBy` returned every row. Implemented it via `ROW_NUMBER() OVER (PARTITION BY
  <field> ORDER BY o._id)` in a chained `_ranked` CTE (`WHERE _rn = 1`), mirroring
  `RedBase.SQLite.Pro`. `pvt_build_query_sql` now reads the `distinct_on` argument.
- **SQLite Free: a multi-key filter no longer silently drops a `null`/text shorthand
  leaf (`RedBase.SQLite`).** In `pvtSplitFilter`'s multi-key (implicit-`$and`) path,
  `json_each`'s `value` column loses type for a JSON `null` (and strips quotes from text),
  so a shorthand condition like `{"0$:ParentId": null}` was rebuilt as invalid JSON and
  vanished whenever the filter had more than one key — e.g.
  `WhereRedb(o => o.ParentId == null)` combined with a `Where(...)` prop filter returned
  rows that *did* have a parent. Each value is now re-encoded as a valid JSON atom
  (type-aware) before the per-key condition is rebuilt.
- **Polymorphic `LoadAsync(IEnumerable<long>)` no longer silently returns a base,
  non-generic `RedbObject` for a scheme whose CLR type exists (`RedBase.Core` +
  `RedBase.Core.Pro`, all dialects, Free and Pro).** The `scheme_id → CLR Type`
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
  `RedbObject` references (`RedBase.Postgres`, `RedBase.MSSql`, `RedBase.SQLite`
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
  upgrade (`RedBase.Postgres`, `RedBase.MSSql`).** `EnsureDatabaseAsync` skips
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
  shutdown** (`RedBase.Core`). Previously the host's `StopAsync` only
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
  (`RedBase.Core`). The line fires once per trash container processed by
  `BackgroundDeletionService`. Each high-level DELETE (e.g.
  `redb.Identity` admin/self-service user delete, DCR cleanup, federation
  provider delete) ships its ids as a single call, so almost every
  container has exactly one object inside and the log spam reads
  `Deleted=1` per item. Worker restarts compound the noise via
  `RecoverOrphanedTasksAsync` draining the accumulated backlog
  one-by-one. Operators who need per-purge visibility now enable DBG
  for the `RedBase.Core.Providers.Base.ObjectStorageProviderBase`
  category.
- **`UserProviderBase.DeleteUserAsync` and `Users_SoftDelete` SQL recipe no
  longer mutate `_login`** (`RedBase.Core`, `RedBase.Postgres`, `RedBase.MSSql`,
  `RedBase.SQLite`).
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
  (`RedBase.Core.Pro`, affects `RedBase.Postgres.Pro` + `RedBase.MSSql.Pro`).
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
- **GroupBy / Window projection value conversion** (`RedBase.Core`, Free + Pro).
  `ConvertJsonValue` (grouped and tree-grouped windowed queryables) now unwraps
  `Nullable<T>` and handles JSON `Number → bool` (a boolean group key serialized
  as `0`/`1` rather than `true`/`false`), `Number → float`, and `String →
  bool`/`Guid`/`DateTimeOffset`. Previously these fell through to a string and
  threw `Object of type 'System.String' cannot be converted to type
  'System.Boolean'` when a projection member's type didn't match the JSON
  shape. PostgreSQL was unaffected because it emits native `true`/`false`.
- **MSSql Free: `DISTINCT` with paging/order no longer fails with "The multi-part
  identifier 'o._id' could not be bound"** (`RedBase.MSSql`, v2-pvt module
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
- Rewrote [docs/FreePvtQuery/FREE-OVER-PRO.md](docs/FreePvtQuery/FREE-OVER-PRO.md)
  §4: marked F0+F1 (this release) as done, demoted F3 (default flip) to a
  major-version task, made the cache-state-dependent nature of the Pro
  ChangeTracking destructiveness explicit (per-instance cache refresh window),
  and corrected §4.1 — the previous "obligatory `DefaultStrictDeleteExtra = false`"
  guidance was non-functional before v2.0.3 and is now actually wired.
- Updated [docs/FreePvtQuery/FREE-OVER-PRO.md](docs/FreePvtQuery/FREE-OVER-PRO.md):
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
