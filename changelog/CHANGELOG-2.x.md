# redb changelog: 2.x

Releases of the 2.x line, moved here unchanged from CHANGELOG.md when the main file started to
follow the current major only. Newer releases: [CHANGELOG.md](../CHANGELOG.md).

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
  [redb.Tests.Integration/Tests/Base/WhereTestsBase.cs](../redb.Tests.Integration/Tests/Base/WhereTestsBase.cs)
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
