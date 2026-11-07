# redb changelog: 4.1.x

Releases of the 4.1 line, moved here unchanged from CHANGELOG.md, which keeps the latest released line only.
Newer releases: [CHANGELOG.md](../CHANGELOG.md).

## [4.1.1] — 2026-09-25
### Fixed
- **A window frame on Pro reached SQL as `CURRENT ROW` whatever it said.** The core writes the frame as
  `{type, start:{kind, offset}, end:{kind, offset}}`; the three Pro providers read another shape
  (`StartType`, `EndType` at the top level) and found none of it, so `Frame.Rows().Preceding(1)` or a
  frame over the whole partition became `ROWS BETWEEN CURRENT ROW AND CURRENT ROW`: a running sum
  returned the current row, with no error. The tests passed, because they compared with `>=`, which a
  one-row frame also satisfies. One compiler in `redb.Core.Pro` now serves PostgreSQL, SQL Server and
  SQLite, reads the shape the core writes, and refuses a bound it does not understand instead of
  replacing it with `CURRENT ROW`. The Free providers were not affected.
- **The permission cache answered for the wrong database.** It is shared by the process and was keyed
  by user and object only, while ids are per database: two databases hand out the same ids in the same
  order, and the system admin is id 1 in every one. In a process that serves several databases (Tsak
  with named instances), a permission worked out in one was served for another - with the default
  `DefaultCheckPermissionsOnDelete = true`, that decided whether a delete went through. The key now
  starts with the service's cache domain, and invalidation touches that database's entries only.
  Other nodes of a cluster still keep an entry until its five-minute lifetime runs out.
- **redb.Export: an import is one transaction, checks the file is whole, and restores ids correctly.**
  - On SQL Server the id sequence came back one step behind: the exporter reads the last id handed out,
    and `RESTART WITH` names the next one, so the first object saved after an import took an id an
    imported row already had. It now restarts one past it, as PostgreSQL and SQLite already did.
  - Rows went into the target tables batch by batch as they were read, the constraints and triggers came
    back only after a successful run, and nothing rolled back: an import that stopped half-way left a
    half-filled database with its integrity switched off - and, with cleaning, an emptied one. The whole
    import, cleaning included, now runs in one transaction that commits at the end or rolls back.
  - A file cut short imported up to the cut and reported success. The footer the exporter writes lists
    how many rows of each kind it wrote; the import now requires it and compares every count before the
    commit, so a short or damaged file changes nothing.
  - `_schemes._tags`, `_structures._tags` and `_structures._unique_scope` were not exported: after a round
    trip the markers were gone and every collection key fell back to the default scope. They travel now;
    files written before this change import as before, with those columns empty.
  - The cleaning step no longer swallows errors: SQL Server decides between TRUNCATE and DELETE from the
    catalog instead of trying and ignoring the failure, and SQLite skips a table it does not have by
    looking, not by catching - a lock or a missing permission used to leave a table full and the import
    went on mixing old and new rows.
  - `IDataProvider` gained `BeginImportAsync`, `CommitImportAsync` and `AbortImportAsync`.
- The documentation of `RedbServiceConfiguration.EnablePvtPrefilter` said the prefilter was PostgreSQL only.
  It is implemented by all three Pro providers - PostgreSQL, SQL Server and SQLite - and has been for
  several releases; the sentence was left behind when the prefilter reached the other two.
- **Tree queries and groupings that answered wrong without an error.**
  - A window frame on a tree query (Pro) was dropped: the tree read the frame in a shape the core does not
    write, found nothing, and every frame became `ROWS UNBOUNDED PRECEDING` - a sum over the whole tree came
    back as a running total. The tree now uses the same frame compiler as the flat query.
  - `TreeQuery(...).GroupByArray(...).Having(...)` without a `Where` lost the HAVING and returned every group.
  - A composite grouping key with a member that is not a field (`Initial = e.FirstName.Substring(0, 1)`,
    `Length = c.Value.Length`) was skipped, or read as a field path it is not, and the grouping ran on other
    keys. Tree, array and tree-window groupings now refuse such a key with `NotSupportedException` naming the
    member, as the flat grouping already did.
  - `Having` on a tree grouping changed the grouping itself, so a second query branched from it inherited the
    first one's condition. It returns a copy now, as the flat grouping does.
  - `WithMaxDepth` on a tree query dropped the props depth, lazy references, projection and distinct settings
    set before it. It keeps every setting now.
- **SQL Server (Free): `GroupBy(...).Having(...)` ignored the HAVING and returned every group.** The provider
  passed NULL for it and `dbo.pvt_build_groupby_sql` never emitted one. It compiles HAVING now, aggregates
  through the same builder as the SELECT list; a HAVING shape it does not know makes the call fail instead of
  being dropped. Tree groupings on SQL Server Free take the same path and are fixed with it. The HAVING test
  suite now runs on SQL Server Free too - it ran on the other five hosts only, which is how this went unseen.
- **SQL Server (Free): an array grouping that aggregated one item field twice failed** (`Min(c => c.Value)` and
  `Max(c => c.Value)` in one select: "Invalid column name 'Value'"). The second aggregate reuses the joined
  column now.
- **SQL Server (Free): HAVING of an array grouping on an item field failed, and an unknown HAVING node was
  dropped.** `GroupByArray(...).Having(g => Agg.Max(g, c => c.Value) == ...)` compiled the field as `[Value]`, a
  column the query does not have, so SQL Server refused it. A HAVING node the translator did not know became
  `1=1` and every group came back. The translator now reads item fields through the joined columns (a field
  only the HAVING aggregates gets its own join) and refuses any shape it does not know.
- SQL Server module version 0.2.21: the functions are reinstalled on the first start.

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
