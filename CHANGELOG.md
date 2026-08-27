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

## [Unreleased]

### Fixed
- **An empty `IN` set threw instead of matching nothing (`RedBase.Postgres.Pro`).**
  `.Where(x => wanted.Contains(x.Department))` with an empty `wanted` failed with
  `42883: operator does not exist: text = bigint`. An empty `object[]` gives Npgsql no element type
  to infer, so it sent `bigint[]`, and comparing a text column against it is not an operator that
  exists. Any code that builds its filter list dynamically could reach this with an empty selection.
  The set now compiles to `FALSE`, which is what membership in an empty set means. MSSql and SQLite
  survived the same input by accident of their spellings and are unchanged.

- **The PVT prefilter ignored membership and kept only one conjunct (`RedBase.Core.Pro` + all three
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

### Added
- **Nine differential tests for membership and ListItem fields (`redb.Tests.Integration`).**
  Membership over a string field, a numeric field and an empty set; a three-condition conjunction;
  and a separate suite for ListItem accessors, which are the one place where a single structure
  carries several of them and they do not behave alike: `Status.Id` lives in the row's own
  `_listitem` column and is a plain row predicate, while `Status.Value` and `Status.Alias` live in
  `_list_items` and need a join no row predicate can express. The suite pins that the first is taken,
  the other two are refused, an array of ListItem is refused, and a filter naming `.Id` and `.Value`
  together still yields one branch on the right column.

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
- **`array.Contains` over a nullable array property stopped parsing on .NET 10 (`RedBase.Core`).**
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

- **The PVT prefilter refused three shapes it should have accepted (`RedBase.Core.Pro`).**

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

- **Leaves were grouped by structure alone when merging a conjunction (`RedBase.Core.Pro`).**
  `Status.Id` and `Status.Value` share a structure id and differ only in column, so a merged branch
  could have spliced a string pattern onto a bigint column. Unreachable before, because every
  accepted field had a one-to-one structure-to-column mapping; reachable the moment `ListItem.Id` was
  let in, which is why it is fixed first. Grouping is now by structure and column together.

### Added
- **The prefilter planner explains itself in `ToSqlStringAsync` (`RedBase.Core.Pro` + all three Pro
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
> **`ExpressionSqlCache` and `CompiledQuery` are gone from `RedBase.Core`.** That is a public API
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
  (`RedBase.Postgres`, `RedBase.SQLite`).** MSSQL has had `IX__objects__id_parent` with
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
- **PVT prefilter — a cutting step before the pivot aggregate (`RedBase.Core.Pro` + all three Pro
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
  (`RedBase.Core`, `RedBase.Core.Pro`, all three providers).** `Contains(needle,
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
- **Pro discarded the configured SQL dialect (`RedBase.Postgres.Pro`).** `ProRedbService` resolved both
  `ISqlDialect` and `ISqlDialectPro` from the container but handed only the base one to
  `ProQueryableProvider`; `ProQueryProvider` then narrowed it with `as ProPostgreSqlDialect`, a base
  instance failed that cast, and the fallback silently constructed `new ProPostgreSqlDialect()` with no
  configuration at all. Latent for as long as the dialect carried no settings — `StringCollation` was
  the first, and it vanished on every Pro query while working correctly on Free. The Pro dialect is now
  passed through explicitly and preferred over the base one wherever both are available.
- **Temporal semantics: `DateTime` keeps its clock reading on every read path, and `DateOnly` works at
  all (`RedBase.Core`, `RedBase.Core.Pro`, all three providers).**
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
- **Phantom `_date_delete` base field (`RedBase.Core`, `RedBase.Core.Pro`).** `BaseFieldMapper` and
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
  returning `false` (`RedBase.Core`, GitHub #4).** The method is `Task<bool>` documented as "true if
  changed", so a change-password form built against the contract 500'd on the most common user error.
  A wrong current password now returns `false`; precondition failures (null args, disabled/system user)
  still throw. Regression test on all six Free/Pro fixtures.

- **SQLite: any property type change crashed `InitializeAsync` with `no such table:
  migrate_structure_type` (`RedBase.SQLite`, GitHub #5).** The scheme-sync path runs
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
- **`ExpressionSqlCache` and `CompiledQuery` (`RedBase.Core`).** Both were public types that nothing
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
- **The prefilter steps aside on SQLite when a limit meets no ordering (`RedBase.SQLite.Pro`).**
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

- **The string value index in SQLite lost its length guard (`RedBase.SQLite`).**
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
  (`RedBase.Core`, all providers).** A `SUM`/`AVG` with no matching rows is `NULL` in SQL (an aggregate
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
> **The whole ecosystem moves together**, as it has since 3.3.3: the core packages (`RedBase.Core`,
> the three providers Free/Pro, `RedBase.Export`, `RedBase.CLI`, `RedBase.Templates`,
> `RedBase.Licensing`), all of `redb.Route`, plus `redb.Tsak` and `redb.Identity` — the latter two
> carry no code changes of their own and are rebuilt onto the new core.
>
> **Why they are not left behind on 3.4.0.** Tsak's shared-runtime layer is gated on the **minor**
> version, so a 3.4.x worker refuses to start with a 3.5.0 framework dropped into `Libs/shared` —
> "patch the framework without rebuilding the worker" holds within a minor only. Since Identity runs
> as modules inside Tsak, leaving both behind would mean the two fixes below that affect every
> consumer — the stale props-cache entry and the order-dependent `Dictionary` hash — never reach
> their users at all.

### Fixed
- **Hashing threw on Blazor WebAssembly, breaking every write (`RedBase.Core`, `RedBase.Core.Pro`).**
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

- **Android apps shipped ~360 KB of unusable Linux binaries in every APK (`RedBase.SQLite`).** The Free
  tier's native loadable extension was packed into the idiomatic `runtimes/<rid>/native/` layout — but
  the .NET Android SDK harvests every `runtimes/*/native/*.so` and maps it into the APK by architecture,
  so `linux-arm64/redbsqlite.so` landed in `lib/arm64-v8a/` and `linux-x64/redbsqlite.so` in
  `lib/x86_64/`. Besides the dead weight, this raised `XA0141`: Android 16 requires 16 KB page
  alignment, so the app would fail to start there. Affected every Android consumer, including those on
  `RedBase.SQLite.Pro` (which depends on the Free package).

  The extension now ships under `buildTransitive/native/<rid>/`, where no platform SDK looks for it, and
  `redb.SQLite.targets` performs the delivery. The targets file previously handled only the
  no-RuntimeIdentifier case — RID-specific publish relied on NuGet flattening `runtimes/` — so it was
  extended to cover both. The output layout is unchanged (`runtimes/<rid>/native/`), so extension
  resolution behaves exactly as before on desktop and server.

- **Props cache could serve a stale ("dirty") object mutated in place after it was cached
  (`RedBase.Core` + all providers).** `MemoryRedbObjectCache` stores a **reference** to the object,
  not a copy. When a caller mutated a loaded object in place *before* saving it, the cache — pointing
  at the same object — instantly reflected the mutation, but its stored hash (fixed at `Set`) did not.
  A subsequent `LoadAsync` saw hash-matches-DB and returned the mutated object as if it were the
  committed state. The cache now recomputes the object's live hash on every read
  (`Get` / `GetWithoutHashValidation` / `GetInternal`) and compares it to the hash captured at `Set`;
  if they differ, the cached snapshot is dirty → treated as a MISS so the caller reloads the committed
  state from the DB. Surfaces only with `EnablePropsCache=true` (off by default). No clone/allocation
  on read — just the hash recompute.

- **`RedbHash` was order-dependent for `Dictionary` properties (`RedBase.Core` + all providers).** A
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

- **Integer aggregate results collapsed to 0 on MSSql (`RedBase.Core`, all providers via MSSql).**
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
> (`RedBase.Core`, the three providers Free/Pro, `RedBase.Export`, `RedBase.CLI`) move together to
> **3.4.0**; `redb.Route`, `redb.Tsak` and `redb.Identity` keep their own version lines.

### Added
- **Explicit scheme names — `[RedbScheme(Name = "...")]` (`RedBase.Core` + all providers).** Until now
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

- **Scheme-name validation in MSSql and SQLite (`RedBase.MSSql`, `RedBase.SQLite`).** Both providers
  now carry a `_schemes` name-validation trigger mirroring the PostgreSQL `validate_scheme_name()`
  rule for rule, so a name accepted by one provider is accepted by all three. Applies to
  **newly created databases only** — `EnsureDatabaseAsync` skips initialisation when `_schemes`
  already exists. The C#-level validation covers databases of any age.

- **`RedbServiceConfiguration.ThrowOnSchemeMismatch` (`RedBase.Core`, default `false`).** Chooses how
  `LoadAsync<TProps>` reacts when the object's scheme does not match `TProps` (see Fixed): `false`
  returns `null` (so a soft-deleted object, scheme `-10`, reads as `null` — what soft-delete callers
  expect); `true` throws `RedbSchemeMismatchException` to surface a genuine type mistake loudly.

### Changed
- **The SQLite native extension is renamed `redb` → `redbsqlite` (`RedBase.SQLite`, Free tier).**
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
- **Concurrent start-up of several nodes could fail to create a scheme (`RedBase.Core` + all
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

- **Pro data migrations never ran — on any provider (`RedBase.Core.Pro`, `RedBase.SQLite`,
  `RedBase.MSSql`).** Four separate defects stacked on top of each other, and the feature had no test
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

- **A scheme's `_alias` was never updated after creation (`RedBase.Core` + all providers).** It was
  written once on `INSERT` and then frozen: changing `[RedbScheme("...")]` on a class left the old
  value in the database forever. Structure aliases have always synchronised (`Structures_UpdateAlias`);
  schemes simply had no equivalent. They do now — the attribute is the source of truth, removing it
  resets `_alias` to `NULL`, and a value edited by hand in the database is overwritten on the next sync.

- **`ClrSchemeTypeIndex` pinned hot-reloaded plugin modules in memory (`RedBase.Core`).** The
  process-global `schemeName → Type` index held **strong** `Type` references, and a `Type` keeps its
  `AssemblyLoadContext` alive — so a collectible ALC (Tsak hot-swap) could never be collected, and after
  a reload the stale instance produced a false `RedbSchemeNameConflictException` against the fresh one,
  blocking the module from loading. Entries are now `WeakReference<Type>` (dead ones pruned on lookup),
  and two instances of the same `FullName` from different ALCs are recognised as a reload, not a name
  clash. Distinct types sharing one explicit `Name` still conflict, by design.

- **`LoadAsync<TProps>` did not verify the loaded object's scheme (`RedBase.Core` + all providers).**
  Loading an object of one scheme under an unrelated `TProps` deserialised garbage into the Props and —
  with `EnablePropsCache` — cached it under `objectId`, so a later `GetWithoutHashValidation` kept
  returning the garbage. `LoadAsync<TProps>` now checks the object's `_id_scheme` against the scheme
  `TProps` maps to before anything is cached. By default a mismatch returns `null` — so a soft-deleted
  object (scheme `-10`, set by `SoftDeleteAsync`) reads as `null`, which is what soft-delete callers
  expect; set `RedbServiceConfiguration.ThrowOnSchemeMismatch = true` to instead throw
  `RedbSchemeMismatchException` on a genuine type mistake. Either way garbage never reaches the cache.
  The untyped `LoadAsync(objectId)` is never affected.

- **Invalid explicit scheme names failed one at a time (`RedBase.Core`).** Auto-sync validates every
  `[RedbScheme(Name = "...")]` and (by design) refuses to boot on an invalid name — but it stopped at
  the first offender, so a codebase with several had to be fixed one rerun at a time. Names are now all
  validated up front and reported together in a single `AggregateException`.

### Removed
- **Dead metadata-cache interface layer (`RedBase.Core`).** `ICompositeMetadataCache`,
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
