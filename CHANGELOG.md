# Changelog

All notable changes to RedBase will be documented in this file.
This changelog covers the **NuGet-published packages** only:

| Package | Edition |
|---------|---------|
| `RedBase.Core` | Free |
| `RedBase.Postgres` | Free |
| `RedBase.MSSql` | Free |
| `RedBase.Export` | Free |
| `RedBase.Core.Pro` | Pro |
| `RedBase.Postgres.Pro` | Pro |
| `RedBase.MSSql.Pro` | Pro |
| `RedBase.CLI` | Tool |

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
