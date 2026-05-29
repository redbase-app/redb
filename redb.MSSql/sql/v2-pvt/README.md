# v2-pvt (MSSql) — PVT-based search engine

Mirror of `redb.Postgres/sql/v2-pvt/`. Migration plan: see
[`docs/MsSqlPvtQuery/PLAN.md`](../../../docs/MsSqlPvtQuery/PLAN.md).
Delta register: [`docs/MsSqlPvtQuery/MSSQL-FREE-GAPS.md`](../../../docs/MsSqlPvtQuery/MSSQL-FREE-GAPS.md).

## File order

Files are picked up alphabetically by numeric prefix and concatenated
into `redb_init.sql` by `redb.MSSql.csproj` (`<SqlPvtBundle>` ItemGroup).
For standalone deploy use `_bundle.ps1` to produce `pvt_bundle.sql`.

| Prefix | Purpose |
|---|---|
| `00_module_init.sql` | Precondition checks, `DROP` all `dbo.pvt_*`, `dbo.pvt_module_version()`. **MUST be first.** |
| `01..07`             | Forked helpers from `sql/deprecated/redb_facets_search.sql` / `redb_lazy_loading_search.sql` (TODO). |
| `10..17`             | PVT-specific helpers: field-path resolver, CTE builder, WHERE compiler, expression engine (TODO). |
| `20_pvt_build_query_sql.sql` | Main orchestrator: builds the inner `_id`-list SQL for `Where + ToList / Count / Exists` (TODO). |
| `21..26`             | Projection / aggregate / groupby / array-groupby / window builders (TODO). |
| `99_smoke_*.sql`     | Smoke tests. **Excluded from the embedded bundle**; deploy manually. |

## Version policy

`dbo.pvt_module_version()` returns semver. Major bumps require redeploy
of the C# client (signature/result-shape break). Minor bumps are additive.
The C# init-time check (`Query_PvtModuleVersionFunction()` in
`MsSqlDialect.cs`) verifies the deployed module on `InitializeAsync()`.

## SQL-injection safety

All `pvt_*` builders construct SQL via `QUOTENAME()` for identifiers and
`'''' + REPLACE(@v, '''', '''''') + ''''` for literals. **No user value
is ever concatenated raw.** See PLAN.md §7.

## Notes

- Reference for T-SQL form/syntax: `redb.MSSql.Pro/Query/PivotSqlGenerator.cs`
  and `ProSqlBuilder.cs` (see `MSSQL-FREE-GAPS.md` §2.2).
- Reference for algorithm: `redb.Postgres/sql/v2-pvt/*.sql` (PG v0.5.0).
