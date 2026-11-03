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

Pending changes live in `changelog/unreleased/` until the release; `pwsh scripts/changelog.ps1 -Preview` shows them.

## [4.6.0] — 2026-11-03

### Breaking

- **Custom query providers: array groupings take one request.** `ExecuteArrayGroupedAggregateAsync` is replaced by
  `ExecuteArrayGroupQueryAsync(ArrayGroupQuery)`, grouped windows take `havingJson`, and the grouping interfaces drop the
  `class, new()` constraint on the item type. Only code that implements `IRedbQueryProvider` or `IRedbQueryable` changes. Packages: `redb.Core`. Upgrade: [UPGRADING.md](UPGRADING.md#custom-query-providers-one-method-for-array-groupings).
- **Custom contexts: `IRedbContext.QueryRowsAsJsonAsync(sql, parameters)`** returns the rows of a statement as a JSON
  array of objects, as one command and one telemetry span. Only code that implements `IRedbContext` or derives from
  `RedbContextBase` changes. Packages: `redb.Core`. Upgrade: [UPGRADING.md](UPGRADING.md#custom-contexts-queryrowsasjsonasync).
- **Custom list providers: `IListProvider.GetListItemsByIdsAsync(ids)`** reads list items by their ids in one query, and
  `ISqlDialect.ListItems_SelectByIds()` is its statement. Only code that implements `IListProvider` or `ISqlDialect`
  changes. Packages: `redb.Core`. Upgrade: [UPGRADING.md](UPGRADING.md#custom-list-providers-getlistitemsbyidsasync).

### Added

- **GroupByArray groups inner arrays, arrays of simple values and fields of classes in the element.** `SelectMany` over
  an inner array, `s => s` on `string[]`, `int[]` and other value arrays, `Agg.Count(g, condition)` (also for `GroupBy`),
  `WithWindow` after `GroupByArray`, real SQL from `ToSqlStringAsync`, and a tree query groups only its subtree.
  See README, GroupBy. Packages: `redb.Core`, `redb.Core.Pro`, `redb.Postgres`, `redb.MSSql`, `redb.SQLite`.

### Changed

- **Groupings throw instead of answering wrong.** A computed key or aggregate, an unsupported array path or condition now
  throws `NotSupportedException`; a provider row without a selected column throws `InvalidOperationException` instead of
  giving a default value. `Having(...).WithWindow(...)` keeps the HAVING, and `Win.Ntile(n)` uses `n`. Packages: `redb.Core`, `redb.Core.Pro`, `redb.Postgres`, `redb.MSSql`, `redb.SQLite`. Upgrade: [UPGRADING.md](UPGRADING.md#grouping-refusals-instead-of-empty-or-default-answers).
- **Pro analytics over array, dictionary and nested-class filters: one statement on SQL Server and SQLite.**
  Aggregates, GroupBy, windows, GroupByArray and their tree forms read such a filter inside the same statement instead
  of passing the ids back as a parameter; on SQL Server a grouping over 67 000 such objects went from 2.4 s to 0.3-0.5 s.
  A `!= null` on a member of the GroupByArray path is no longer evaluated. SQL previews show the embedded filter. Packages: `redb.Core.Pro`, `redb.MSSql.Pro`, `redb.SQLite.Pro`, `redb.Postgres.Pro`.
- **Manual index scripts in `sql/maintenance/` of every provider.** `free_value_indexes.sql` brings back the value
  indexes the free provider read before the PVT module retired them, under names upgrades do not drop; run it if free
  queries got slower. New SQL Server and SQLite databases no longer put `_date_modify` into three `_objects` indexes,
  and `rebuild_objects_indexes_without_date_modify.sql` brings an existing database to that shape. Nothing runs these
  scripts automatically. Packages: `redb.Postgres`, `redb.MSSql`, `redb.SQLite`.

### Fixed

- **`GroupBy(x => x.Status)` with a list item as the key returned an item that carried only its id**; the key is now
  the item a loaded object carries (Id, IdList, Value, Alias, IdObject), on every provider, flat and over a tree, in
  GroupBy, GroupByArray and grouped windows. The SQL is unchanged; the items are read in one extra query. Packages: `redb.Core`.
- **Pro: `AggregateAsync` over the elements of an array on a tree query** (`Agg.Sum(x.Props.Levels.Select(v => v))`)
  threw NotSupportedException; it now returns the sum, average, minimum, maximum and count of the elements, as the
  flat query and Free do. Packages: `redb.Core.Pro`, `redb.Postgres.Pro`, `redb.MSSql.Pro`, `redb.SQLite.Pro`.
- **SQL Server Pro: `GroupByArray` with a condition on the objects was several times slower with the query planner on
  than off**; it now runs as fast either way (the example E142 went from 0.9 s to 0.25 s with the planner on). Packages: `redb.MSSql.Pro`.
- **SQL Server Pro: a tree `GroupBy(...).WithWindow(...)` with a filter on a nested-class field** failed with
  "Invalid column name"; it now runs like the flat form. Packages: `redb.MSSql.Pro`.
- **Free: analytics skipped objects that store no value for the fields they read.** `Agg.Count()` next to an aggregate
  over an array counted only objects with elements, and `GroupBy` (also with `WithWindow`) lost the group of objects
  whose key is unset. Both now match LINQ over the same objects, so such counts can grow: the example E150 reports
  101 831 employees where it reported 101 800. A Props filter on a tree query keeps its speed: about a second over a
  subtree of 100 thousand nodes. Packages: `redb.Postgres`, `redb.MSSql`, `redb.SQLite`.
- **Free: a list ordered by a field some objects leave unset returned only the objects that have it**; all objects
  come back now, the unset ones first or last as the order says. The same holds for Skip/Take, Count and Any over
  such an order, flat and over a tree. Packages: `redb.Postgres`, `redb.MSSql`, `redb.SQLite`.
- **Free: `WhereDescendantsOf(node, maxDepth)` ignored its `maxDepth`** and returned the whole subtree of the node. A second
  tree filter was dropped: `WhereDescendantsOf(node).WhereLeaves()` returned every descendant, now the leaves under the
  node. On SQL Server, `TreeQuery(root, maxDepth).WhereLeaves()` returned the leaves below `maxDepth` too; it now returns
  the leaves within it, as the other providers do. Packages: `redb.Postgres`, `redb.MSSql`, `redb.SQLite`.
- **Free: `WhereLevel` on `TreeQuery(node)` counted levels from the root of the whole tree; it now counts from the node
  the query starts from, as `maxDepth` and Pro do.** `TreeQuery<T>()` without a start node is unchanged. Packages: `redb.Postgres`, `redb.MSSql`, `redb.SQLite`.
- **Free: `TreeQuery(root, maxDepth)` returned the whole subtree** (4.0.0 to 4.5.1), and a subtree deeper than the props
  depth (10 by default) was cut there. Lists, counts and analytics of a tree query now stop at `maxDepth`, and without it
  they cover the whole subtree. Packages: `redb.Postgres`, `redb.MSSql`, `redb.SQLite`.
- **Pro: `GroupByArray` ran several metadata queries per call**, one for each segment of the array path and each element
  field, and its SQL preview ran them again. The field metadata is now cached as for the other query forms: a repeated
  call runs the data statement alone. Packages: `redb.Postgres.Pro`, `redb.MSSql.Pro`, `redb.SQLite.Pro`.
- **Pro: `GroupBy(x => x.Status.Value)` on a list item field returned the id of the item as the key** when the query
  planner was turned off (`EnablePvtPrefilter = false`); it now returns the text of the item, as with the planner on.
  Grouped windows and the tree forms likewise. Packages: `redb.Postgres.Pro`, `redb.MSSql.Pro`, `redb.SQLite.Pro`.
- **Pro: `WhereHasAncestor` and `WhereHasDescendant` on a tree query were ignored by aggregates, GroupBy, windows and
  `GroupByArray`; the figures covered the whole tree.** They now cover the nodes the list returns. On PostgreSQL,
  `CountAsync` of `TreeQuery(several roots, maxDepth)` with the prefilter planner off no longer fails with a syntax error. Packages: `redb.Postgres.Pro`, `redb.MSSql.Pro`, `redb.SQLite.Pro`.
- **Pro: `WhereLevel` on a tree query was ignored by GroupBy, windows and aggregates; the figures covered the whole
  tree.** They now cover the chosen levels, as the list does (`SumAsync`, `AggregateAsync`, `GroupBy`, `WithWindow`,
  `GroupBy(...).WithWindow(...)`). Packages: `redb.Postgres.Pro`, `redb.MSSql.Pro`, `redb.SQLite.Pro`.
- **SQLite: the grouping, window and aggregate statements now appear in telemetry as commands**, as on the other
  providers. Packages: `redb.SQLite`, `redb.SQLite.Pro`.
- **SQLite Free: `Agg.Average` over the elements of an integer array was rounded down to a whole number**; it is now
  the exact average (1, 2, 2 average 1.67, not 1). Packages: `redb.SQLite`.

## Earlier versions

Earlier releases are archived by line in [changelog/](changelog/): [4.5.x](changelog/CHANGELOG-4.5.x.md), [4.4.x](changelog/CHANGELOG-4.4.x.md), [4.3.x](changelog/CHANGELOG-4.3.x.md), [4.2.x](changelog/CHANGELOG-4.2.x.md), [4.1.x](changelog/CHANGELOG-4.1.x.md), [4.0.x](changelog/CHANGELOG-4.0.x.md), [3.x](changelog/CHANGELOG-3.x.md), [2.x](changelog/CHANGELOG-2.x.md), [1.x](changelog/CHANGELOG-1.x.md).
