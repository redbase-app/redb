# RedBase Roadmap

This document tracks what RedBase ships today, what is coming next, and the long-term direction for both the open-source (Apache 2.0) and Pro editions.

It is intentionally high-level. Day-to-day changes live in [CHANGELOG.md](CHANGELOG.md); detailed design notes for individual features live under [docs/](docs/).

> **Editions in one sentence**
>
> - **Free (Apache 2.0)** — `redb.Core`, `redb.Postgres`, `redb.MSSql`, `redb.Export`, `redb.CLI`, `redb.Templates`, `redb.PropsEditor`.
> - **Pro** — `redb.Core.Pro`, `redb.Postgres.Pro`, `redb.MSSql.Pro`. Same API surface, extra performance and tooling.

---

## Status legend

| Symbol | Meaning |
|--------|---------|
| OK | Shipping today |
| WIP | In active development |
| PLANNED | Designed, not yet implemented |
| RESEARCH | Under exploration, design not finalised |

---

## 1. What ships today

### Core data model

- OK — Code-first schemas: plain C# classes decorated with `[RedbScheme]`, auto-synced by `InitializeAsync()` and `SyncSchemeAsync<T>()`.
- OK — Strongly-typed objects: `RedbObject<TProps>`, full graph load with `LoadAsync` and `SaveAsync`.
- OK — Bare `RedbObject` (no Props) for cases where only base fields matter, and mixed-schema batches: `SaveAsync(IEnumerable<IRedbObject>)`.
- OK — Object references between schemes via `[ObjectReference]` — saved as typed links, resolved by the loader.
- OK — Partial updates with change tracking — only modified Props fields are written back.
- OK — Bulk insert and bulk delete: `BulkInsertAsync`, `DeleteAsync(ids)` for thousands of objects in one round-trip.
- OK — Primitive arrays (`int[]`, `string[]`, …) and `List<T>` of primitives.
- OK — Nested classes (composition) and arrays of classes (`Address[]`, `List<Address>`).
- OK — `Dictionary<K, V>` where the value is a primitive or a class; tuple keys (`Dictionary<(int, string), V>`) included.
- OK — Multi-level collections **via a wrapper class** (`Dictionary<K, Class>` where `Class` contains an array or dictionary).

### Trees (with polymorphism)

- OK — `TreeRedbObject<TProps>` with first-class `Parent` / `Children` navigation and `parent_id` base field.
- OK — Tree creation: `TreeCreate`, `TreeCreateBulk`, `TreeMove` (reparenting with subtree integrity checks).
- OK — Tree loading: `TreeLoad`, `TreeChildren`, `TreeDescendants`, `TreePath` (materialised path), `TreeCount`, `TreeStats`.
- OK — Traversals: `BFS`, `DFS` (pre-order and post-order), `GetLeaves`, `GetNodesAtLevel`, `IsBalanced`, `FlattenToList`.
- OK — Predicate-based search inside a loaded tree: `Find(id)`, `FindAll(predicate)`.
- OK — `TreeQuery<T>()` — LINQ-style server-side queries over trees: `WhereRoot`, `WhereLeaves`, `WhereLevel`, `WhereChildrenOf(parentId)`, multi-root, `IsDescendant`/`IsAncestor`, `GetLevel`.
- OK — **Polymorphic** ancestor/descendant filters: `WhereHasAncestor` / `WhereHasDescendant` work across **mixed Props types** in the same tree — a `Department` node and an `Employee` node coexist, and the filter traverses both.
- OK — `Parent` relationships re-built across heterogeneous result sets via `TreeObjectConverter` so navigation works after a polymorphic load.
- OK — Tree result shaping: `ToTreeList`, `ToRootList`, `ToFlatList`.
- OK — Analytics on trees: `TreeQuery` composes with `GroupBy`, window functions, `DistinctBy` / `DistinctByRedb`, `OrderBy` / `OrderByDescending`.

### Query API

- OK — LINQ-style query builder: `Where`, `OrderBy`/`OrderByDescending`/`ThenBy`, `Skip`, `Take`, `FirstOrDefault`/`Single`, async variants.
- OK — Combinators: `And`, `Or`, `Not`, chained predicates, parentheses, ranges, nullable comparisons, booleans.
- OK — String operators: `Contains`, `StartsWith`, `EndsWith`, case-insensitive (`ILIKE` on PostgreSQL, `COLLATE` on MSSQL), `ToLower`, `Trim`, `Length`, chained string ops.
- OK — Arrays in `Where`: `Contains`, multiple-condition matches, `NotContains`, `Any` / `All` over arrays, `ArrayContainsAnd` / `ArrayContainsOr`.
- OK — Dictionaries in `Where`: `ContainsKey`, indexer (`dict[key]`), filter by nested class fields inside dictionary values, tuple-key filters.
- OK — Date/time operators: `>`/`<` / range, `DateTime.Year` and similar component extraction.
- OK — Set operators: `Distinct` / `DistinctBy` / `DistinctByRedb`, `WhereIn` / `WhereInRedb`.
- OK — Predicates over base fields (id, scheme, name, owner, timestamps) and across multiple base fields in one query.
- OK — Projections: `Select(x => new { … })`, scalar selects, anonymous types, server-side projection of arithmetic expressions.
- OK — Arithmetic & SQL functions in projections and predicates: `*`, `+`, `Abs`, `Power`, nested math, `Coalesce`, custom SQL function calls.
- OK — Scalar reductions: `Count`, `Any`, `All`, `Sum`, `Avg`, `Min`, `Max`, `AggregateAsync` — both client-side and `*Redb` server-side variants, with and without filters.
- OK — Group-by: single-key, filtered, multi-key, `GroupByRedb`, `GroupByArray`, group-by combined with windowing.
- OK — Window functions: `ROW_NUMBER`, running sums, `LAG` / `LEAD`, `RANK`, `NTILE`, `FIRST_VALUE` / `LAST_VALUE`, custom frames, `PARTITION BY` / `ORDER BY` (also via `*Redb` variants), filtered windows.

### Security and validation

- OK — Built-in users, roles and permissions: `IUserProvider`, `IRoleProvider`, `IPermissionProvider`. Permissions are checked centrally inside the storage and tree providers based on the current `IRedbSecurityContext` and configuration flags (`DefaultCheckPermissionsOnLoad`, `…OnSave`, `…OnDelete`).
- OK — `IValidationProvider` runs Props-level validators (`[Required]`, `[Range]`, custom attributes) on save.
- OK — Pluggable security context for multi-user / per-request impersonation.

### Soft-delete with background purge

A first-class **Recycle Bin** model lives in `IObjectStorageProvider` and `IBackgroundDeletionService` — not a `_deleted = true` flag bolted on, but a real two-phase delete pipeline.

- OK — `SoftDeleteAsync(ids | objects, [user], [trashParentId])` — atomically creates a **trash container** object, moves the targets *and their full descendant subtrees* underneath it, **and reassigns their `_id_scheme` to the reserved `@@__deleted` scheme (id `-10`)**. Returns a `DeletionMark(TrashId, MarkedCount)`. The application can show the user a "Deleted N items — Undo" toast instantly.
- OK — **Marked rows disappear from all normal queries automatically.** Because `Query<TProps>()`, `Tree<TProps>()`, aggregations and window queries all filter by `_id_scheme = <user scheme>`, soft-deleted objects are invisible the moment marking commits — no `WHERE _deleted = false` glue in every query, no orphaned references in hot paths.
- OK — **Marking 1 000 deeply-linked objects no longer means scanning a thousand FK cascades on the request thread.** The mark step is a single SQL function (`mark_for_deletion`) that re-parents and re-schemes via CTE; physical row removal (with all the `_values` / `_permissions` / FK clean-up cost) happens later in the purge worker, in batches.
- OK — `DeleteWithPurgeAsync(ids, batchSize, IProgress<PurgeProgress>, …)` — mark + physically purge in batches with a progress callback, cancellable. Suitable for "Empty Recycle Bin" UX.
- OK — `IBackgroundDeletionService` — queue-backed fire-and-forget pipeline. Marking returns immediately; physical purge runs on a background worker against a **separate DB connection** so it never blocks the request thread. Progress (`Pending` / `Running` / `Completed`) is persisted **in the trash object itself** — survives process restarts and is **cluster-safe**: any node can pick up an in-progress purge job because all state lives in the database, not in the marking node's memory.
- OK — `GetProgressAsync(trashId)` and `GetUserActiveProgressAsync(userId)` — query live or recent deletion jobs from any node, no shared in-memory state required.
- OK — Configurable `trashParentId` — multiple parallel trash containers per tenant / per user / per workflow are supported out of the box.

This is the "elegant" path most apps want: a familiar Recycle Bin UX (mark → undo → purge → progress) with zero glue code, persistent progress, no background-job framework dependency, and — critically — **no FK-cascade latency tax on the user-facing delete call**.

### Lists

- OK — `IListProvider` — first-class named lists of objects (think tags, dictionaries, lookup catalogues) with `ListCreate`, `ListAddItems`, `ListGetByName`.
- OK — List items can wrap full `RedbObject` payloads, query them by value, by `WhereIn`, by direct field compare, by props-array `Any`.

### Storage and tooling

- OK — PostgreSQL provider (`redb.Postgres`), Microsoft SQL Server provider (`redb.MSSql`), and SQLite provider (`redb.SQLite` / `redb.SQLite.Pro` — embedded, Blazor WASM & mobile).
- OK — Database bootstrap: `InitializeAsync(ensureCreated: true)`, `EnsureDatabaseAsync()`, `GetSchemaScript()`.
- OK — Export/import as `.redb` files (JSONL/ZIP) for backup and PostgreSQL ↔ MSSQL migration.
- OK — CLI tool (`redb.CLI`) for schema init, export, import.
- OK — Visual props editor component (`redb.PropsEditor`).

### Developer experience

- OK — Project templates (`redb.Templates`): `dotnet new install redb.Templates` then `dotnet new redb -n MyApp --db postgres|mssql --pro true|false` — a ready-to-run console app with CRUD, tree hierarchy, `SearchAsync`, and optional Pro LINQ/analytics wired up.
- OK — Runnable, self-discovering examples library (`redb.Examples`): 190+ scenarios from `E000_BulkInsert` through `E195_GroupByWindowFiltered`, each tagged with category / difficulty / tier (`Free` / `Pro` / `Enterprise`) via `[ExampleMeta]`. The same project runs against PostgreSQL or MSSQL, doubles as the seed data for the docs site, and is the contract test for new operators.
- OK — Integration test harness (`redb.Tests.Integration`): xUnit + FluentAssertions suite with shared `*TestsBase` classes (CRUD, Where, Tree, PolymorphicTree, Array, Dictionary, List, Aggregation, GroupBy, Window, Ordering, ProQuery) executed against both Postgres and MSSQL via per-provider fixtures — every feature ships with a parity test across both supported databases.
- OK — Inline SQL inspection: any query exposes `ToSqlStringAsync()` so developers (and example authors) can paste the generated SQL into their query plan tool of choice.

### Connection isolation and caches

RedBase is built around process-wide metadata caches (schemes, types, CLR registry, lists, props, field resolvers). To make those caches safe when an application talks to **more than one database** — multi-tenant deployments, sharded read-replicas, parallel integration tests against several ephemeral databases, or simply two `RedbService` instances in the same DI container — every cache is partitioned by a **cache domain**.

- OK — `IRedbService.CacheDomain` — every service instance is bound to one domain.
- OK — Domain key resolution: explicit `RedbServiceConfiguration.CacheDomain`, or computed automatically as the first 16 hex chars of `SHA-256(connectionString)` with the password stripped before hashing.
- OK — Domain-isolated caches: `GlobalMetadataCache` (schemes, DB types, CLR type registry), `GlobalListCache` (lists and list items), `GlobalPropsCache` (per-object Props snapshots), `SchemeFieldResolver` (Pro).
- OK — Per-domain enable/disable and statistics — turn caching off for a single connection without affecting others; query hit/miss counters per domain.
- OK — Guarantees: a scheme cached for connection A is never returned to a query running on connection B, even when both processes share the same `_id_*` ranges or the same scheme name maps to a different `_id` in each database.

Practical consequences this unlocks today:

- Two `RedbService` instances pointing at different PostgreSQL databases in the same process share zero cache state — no cross-contamination of `schemeId`, no stale CLR-to-scheme mappings.
- Test suites can spin up N temporary databases in parallel and each gets its own cache slice.
- A future cluster / sharding layer can route requests to per-shard `IRedbService` instances without rebuilding the caching layer.

---

## 2. Near-term — next minor releases

Focus: complete the **Props-as-anything** story so any C# shape round-trips through RedBase without a wrapper class.

### Props as collection / primitive — WIP

Currently `Props` must be a class. The next milestone lets you store collections and primitives directly:

```csharp
RedbObject<Dictionary<string, decimal>>   // PLANNED
RedbObject<Order[]>                        // PLANNED
RedbObject<List<int>>                      // PLANNED
RedbPrimitive<long>                        // PLANNED
RedbPrimitive<string>                      // PLANNED
```

- SQL layer: **OK** — new `Array`, `Dictionary`, `JsonDocument`, `XDocument` types in `_types`; `_schemes._type` column added; storage hierarchy via the system `@@_value` structure. See [docs/dict/DICTIONARY_ARRAY_IMPLEMENTATION.md](docs/dict/DICTIONARY_ARRAY_IMPLEMENTATION.md).
- .NET layer: **WIP** — serializer, query builder and materialiser updates.

### Direct nested collections — PLANNED

Today, `Dictionary<K, V[]>`, `List<List<T>>`, `T[][]` and `T[,]` require a wrapper class. The plan is to lift this restriction so nested collections work out of the box.

Tracked in [docs/dict/NESTED_COLLECTIONS_LIMITATIONS.md](docs/dict/NESTED_COLLECTIONS_LIMITATIONS.md).

### Custom scheme names — PLANNED

Restore first-class support for `[RedbScheme("MyName")]` as the actual scheme identifier (currently `Alias` is cosmetic and the FQN is always used). Includes a one-shot migration that renames an existing FQN-named scheme to its custom name on first sync.

Tracked in [docs/dict/SCHEME_NAMING_ANALYSIS.md](docs/dict/SCHEME_NAMING_ANALYSIS.md).

### JSON and XML documents — PLANNED

`RedbObject<JsonDocument>` and `RedbObject<XDocument>` as first-class Props types. Stored via the same `@@_value` hierarchy as arrays and dictionaries — no opaque blobs, every node is indexable.

---

## 3. Mid-term — Path Query API

Once JSON/XML documents are first-class, RedBase will gain a **JSONPath / XPath query API** that works directly against `_values` — no `jsonb`, no `XML.value(...)`, one indexed CTE for the whole engine.

### Filters

```csharp
query.WherePath("$.store.name",   name => name == "Amazon");      // PLANNED
query.WherePath("$.price",        ComparisonOperator.GreaterThan, 100);
query.WherePath<decimal>("$.price", p => p > 100 && p < 500);

// Collections
query.WherePath("$.books[*].price",   PathOp.Any,   p => p > 100);
query.WherePath("$.items[*].status",  PathOp.All,   s => s == "active");
query.WherePath("$.orders[*]",        PathOp.Count, c => c > 5);
query.WherePath("$.items[*].price",   PathOp.Sum,   sum => sum > 1000);

// JSONPath / XPath predicates inside the path
query.WherePath("$.books[?(@.price > 100)].title");
query.WherePath("/books/book[@price > 100]/title");
```

### Projections

```csharp
var names = await query.SelectPath<string>("$.store.name").ToListAsync();          // PLANNED
var prices = await query.SelectPathArray<decimal>("$.items[*].price").ToListAsync();
var stats  = await query.SelectPath(new {
    Total = ("$.items[*].price", PathAgg.Sum),
    Count = ("$.items[*]",       PathAgg.Count),
}).ToListAsync();
```

### Algorithm

A **single CTE** with cumulative path tracking and early pruning. One algorithm covers JSON, XML, arrays and dictionaries — no per-shape special cases, no JOIN chains, no `jsonb_path_query` fallbacks. Full design in [docs/dict/PATH_QUERY_API_PLAN.md](docs/dict/PATH_QUERY_API_PLAN.md).

---

## 4. Open-sourcing today's Pro features

Several capabilities that currently live in `redb.Postgres.Pro` / `redb.MSSql.Pro` will be moved to the open-source providers. This is a deliberate shift: the OSS edition should be capable of running serious production workloads on its own.

| Capability | Today | Plan |
|---|---|---|
| Optimised SQL generation (`ProSqlBuilderBase`, `ProFilterExpressionParser`) | Pro only | **PLANNED → OSS** |
| Change-tracking save strategy (`PropsSaveStrategy.ChangeTracking`) | Pro only | **PLANNED → OSS** |
| Schema migrations (`redb.Core.Pro/Migration`) | Pro only | **PLANNED → OSS** |
| Pro lazy props loader | Pro only | **PLANNED → OSS** (basic version) |

No timeline commitment yet — these move once the API has stabilised and the test coverage in `redb.Tests.Integration` covers both code paths.

---

## 5. What stays Pro

After the migration above, the Pro edition focuses on **performance for large object graphs** and **operational tooling**. The OSS edition will remain fully usable on its own; Pro is for teams that need the extra throughput.

- **Parallel materialisation** — `ProPropsMaterializer` / `ProLazyPropsLoader`: parallel hydration of large object graphs and list results across CPU cores. Significant speed-up on wide objects (10+ array/dictionary props).
- **Tree-diff on `_values`** — server-side incremental change detection for tree updates, so saving a large tree writes only the rows that actually changed (instead of replacing the subtree).
- License management and telemetry hooks.

These features are deeply tied to the parallelisation primitives and incremental-write infrastructure, and are unlikely to be open-sourced.

---

## 6. Research and longer-term ideas

Things we are looking at, with no commitment yet:

- RESEARCH — More LLM-friendly schema discovery: `redb.CLI` emitting a machine-readable scheme catalogue suitable for prompt grounding (see [docs/LLM_DISCOVERY_AND_TRAINING.md](docs/LLM_DISCOVERY_AND_TRAINING.md)).
- RESEARCH — Cluster mode for high-concurrency writes — builds on the existing per-connection cache domains; adds coordinated invalidation across nodes (see [docs/CLUSTER_CONCURRENCY_PLAN.md](docs/CLUSTER_CONCURRENCY_PLAN.md)).
- RESEARCH — Native `Include` for `RedbObject` graph navigation across schemes (see [docs/INCLUDE_REDBOBJECT_PLAN.md](docs/INCLUDE_REDBOBJECT_PLAN.md)).
- RESEARCH — `Where` expressions over tree shape (not just data) and `LINQ-over-tree` syntactic sugar (see [docs/TreeLinqDemo.md](docs/TreeLinqDemo.md)).

---

## 7. How to influence this roadmap

- Open a GitHub issue describing the use case (not just the feature) — concrete user stories shape priority.
- Pro customers can request items via their support channel.
- Connector roadmap for `redb.Route` lives in the [redbase-app/redb-route](https://github.com/redbase-app/redb-route) repo — see its `CONNECTORS_ROADMAP.md`.

---

*Last updated: 2026-05*
