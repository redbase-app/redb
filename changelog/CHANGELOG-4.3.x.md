# redb changelog: 4.3.x

Releases of the 4.3 line, moved here unchanged from CHANGELOG.md, which keeps the latest released line only.
Newer releases: [CHANGELOG.md](../CHANGELOG.md).

## [4.3.1] — 2026-10-15

`redb.Templates` only; the libraries stay on 4.3.0.

### Fixed — the module templates deploy onto a Tsak worker of the same release

`redb.Templates` 4.3.0 installed the `redb.*` 4.3.0 packages, but the deploy files of `redb-worker`,
`redb-chat`, `redb-app` and `redb-bff` still started `redb-tsak-stack:4.2.0-net10` in
`deploy/docker-compose.tsak.yml`. A module built on 4.3.0 does not load on a 4.2 worker: Tsak compares the
major and minor version of the module with its own. The compose file starts `redb-tsak-stack:4.3.0-net10`
now, and `deploy/shipped-module-deps.txt` is taken from `redb-tsak-worker:4.3.0` (the same 182 assemblies).
A project generated from 4.3.0 can be fixed by hand: set `TSAK_VERSION=4.3.0-net10`, or change the default in
its compose file.


## [4.3.0] — 2026-10-14


> **`redb.Templates` — why the bump.** No template change of its own. The pack moves to 4.3.0 with the
> libraries because each template pins the `redb.*` packages of its release: a project generated from it now
> installs 4.3.0 and gets the behaviour changes below, among them `EnablePvtPrefilter` on by default on Pro
> and `CountAsync`/`AnyAsync` following `Take` and `Skip`.

### Added

- **redb.Export, redb.CLI: flat export.** `redb flat` (API: `FlatExportService`) unfolds `*Props` classes into
  ordinary relational tables and copies the data into them: a shared `objects` header table, a table per
  scheme, and a table per nested class, array and dictionary, named by property path. Source and target are
  independent connections of any two providers; `--ddl-only` prints the DDL without opening a database.
  One-way, ids kept, hashes not copied. `redb.Export` now references `redb.Core`, and `IDataProvider` gained
  `ExecuteAsync` and `ReadDateTimeOffset`. See `redb.CLI/README.md`.

- **redb.SQLite: `SqliteDataSource.MmapSizeBytes` and `CacheSizeBytes`.** Memory-mapped I/O and page cache
  size for every connection. Both default to 0, which keeps SQLite's own settings.

### Changed

- **PostgreSQL Pro: a new "streams" query form, and aggregates that no longer lose objects.** With
  `EnablePvtPrefilter` on, pages with `Take`, `CountAsync`, `AnyAsync`, aggregates, `GroupBy` and windows
  read scalar, dictionary and ListItem fields as one `LEFT JOIN _values` per field over the objects of the
  scheme instead of a `GROUP BY` pivot; array `Contains` and `Any` become `EXISTS` over the element rows.
  2-6x faster on the queries measured over 275k objects, with no per-object hash. **Behaviour change, planner on or off:**
  aggregates, `GroupBy` and windows now range over the same objects as `CountAsync` and `ToListAsync`. An
  object with no value for a field used to drop out of them silently, so `Where(x => x.F == null)` followed
  by `Agg.Count()` returned 0, `Agg.Count()` shrank when another aggregate read a sparse field, and a
  nullable grouping key produced no NULL group. `Agg.Count()` now counts every object of the filtered set
  and `GroupBy` returns the NULL group. A batch mixing base-field and Props aggregates no longer doubles the
  base-field result. Aggregates over `TreeQuery(root)` range over the subtree instead of the whole scheme.
  `OrderBy` and `GroupBy` over a Props field named like a base field (`Name`, `Key`, `Note`, ...) read the
  Props field; they used to read the object's own column, which is what `OrderByRedb` and `GroupByRedb` are for.

- **PostgreSQL: seven redundant indexes leave the schema; PVT module 0.7.13.** Each was fully replaced by
  another index of the same table: `IX__values__object_structure_array_index` (same key as
  `IX__values__object_structure_lookup`), `ix__objects_parent_scheme`, `IX__structures__not_collection`, and
  the single-column `IX__functions__schemes`, `IX__permissions__roles`, `IX__dependencies__schemes_1`,
  `IX__users_roles__roles` that a UNIQUE constraint already leads with. New databases no longer create them;
  an existing database drops them on the first start with this version, when the module version check
  re-applies the module. `DROP INDEX` takes a short exclusive lock on its table.

- **PostgreSQL: the stock value index `IX__values__structure_object_lookup` is retired; PVT module 0.7.14.**
  New databases no longer create it and an existing database drops it on the first start with this
  version. It stood next to the covering index of the streams form over the same rows, each about as large
  as the other (1.7 GB on a 38M-row `_values`), and both were maintained on every write. Pro with the
  planner on reads the covering index and the object-led ones: measured on that database, the streams
  queries kept their times. **What pays for it:** Pro with the planner off on a sorted page over a whole
  scheme (2.9 s to 6.2 s on 81k objects), and one Free query shape, a list over a whole scheme with three
  conditions (8.8 s to 18-20 s). If queries got slower after the upgrade, the statement
  that brings the index back is in `redbPostgre.sql` and in `v2-pvt/00_module_init.sql`:
  `CREATE INDEX CONCURRENTLY IF NOT EXISTS "IX__values__structure_object_lookup" ON _values (...)`.
  The module drops it again on its next version change, so report the case.

- **Pro: `EnablePvtPrefilter` is on by default.** Set it to `false` to keep the aggregate form only.

- **SQL Server Pro: the streams form.** Pages, counts, aggregates, `GroupBy`, windows and tree queries, as on
  PostgreSQL Pro, with the same behaviour change for objects without a value and for Props fields named like
  base fields. Trees collect their nodes level by level into a table variable instead of a recursive CTE.

- **SQLite Pro: the streams form.** The same queries as on SQL Server Pro, and the same behaviour change.

- **Pro, all three: `OrderByRedb` over a base field sorts and pages inside the streams.**

- **Pro: the indexes of the streams form are built at start-up.** With `EnablePvtPrefilter` on, every start
  checks the catalog and builds a missing index from `Sql/pvt_streams_indexes.sql` of the provider package
  (PostgreSQL: three, `CONCURRENTLY`; SQL Server: one; SQLite: four), then refreshes statistics. Start-up waits
  for the build: minutes on a large `_values`. SQL Server without Enterprise blocks writes to `_values` while it
  builds; SQLite has one writer anyway. With `AutoApplyDatabaseUpgrades` off, or a role that may not create
  indexes, start-up only logs a warning naming the missing indexes and the file to run. A dropped index comes
  back on the next start; on SQL Server disable it instead. PostgreSQL: the covering index
  `IX__values__structure_object_scalar` grows under workloads that rewrite most rows daily; lower
  `autovacuum_vacuum_scale_factor` for `_values` and watch its size.

- **SQL Server (PVT module 0.2.29) and SQLite (schema version 10): the redundant indexes PostgreSQL dropped in
  0.7.13 leave the schema too.** `IX__values__object_structure_array_index`, `IX__structures__not_collection`,
  `IX__functions__schemes`, `IX__permissions__roles`, `IX__dependencies__schemes_1`, `IX__users_roles__roles`,
  and on SQL Server `ix__objects_parent_scheme`. New databases no longer create them; an existing one drops
  them on the first start with this version (SQLite: once). The statements that bring them back are in the
  comment above the drop: `v2-pvt/00_module_init.sql` for SQL Server, `ApplySchemaUpgradesAsync` step 10 for
  SQLite. `IX__values__structure_object_lookup` stays on both: the streams form reads string fields through it.

### Fixed

- **`Select` projections no longer return half-loaded objects.** A selector that named a Props field next to
  the object itself, its whole `Props`, a method taking the object, or a member read off a `??`, a call
  result, a conditional or a cast (`new { O = o, o.Props.Code }`, `new { o.Props.N, L = (o.Props.S ?? "").Length }`)
  loaded only the named field on Pro; everything else arrived as defaults, silently. Such selectors now load
  the object whole. A query stays usable after a projection was run over it: `q.Select(...)` followed by
  `q.ToListAsync()` used to return objects with the projected fields only. On PostgreSQL Pro a projection of
  base fields alone (`Select(o => o.Id)`) no longer loads the Props of every object it returns.

- **`CountAsync` and `AnyAsync` follow `Take` and `Skip`. Behaviour change, every provider.** They answer for
  the rows `ToListAsync` of the same query returns: `Take(10).CountAsync()` is at most 10, `Skip(n).CountAsync()`
  is what is left after the first n, `Take(0).AnyAsync()` is false. Both used to answer for the whole filtered
  set whatever the paging said. Code that counted a paged query to get the total must count the query before
  `Skip`/`Take` instead. `AllAsync` is unchanged and still checks every row of the filtered set.

- **Pro, all three: aggregates, groupings, windows and tree queries with a filter over an array, a dictionary
  entry or a field of a nested class.** They counted wrong or failed; the filtered set is now picked first.

- **PostgreSQL Pro, SQL Server Pro: `WhereHasAncestor` and `WhereHasDescendant` apply the rest of the query.**
  `Where`, `CountAsync` and paging next to them were ignored, and under a root the root itself came back.

- **SQL Server Pro: a tree query with a long list of ids no longer takes minutes** (385 s to 0.5 s on a subtree count).

- **redb.Export: an import into SQLite stores dates and decimals the way the provider reads them.** Dates
  were written as text and decimals as text, while the SQLite provider keeps and compares them as numbers
  (UTC Julian day, `REAL`): every date range over an imported database came back empty and date sorting went
  by string. Dates are now written as Julian days and decimals as `REAL`, and an export from SQLite decodes
  dates itself. A database imported earlier keeps its text values: import it again.
