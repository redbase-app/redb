# redb changelog: 4.2.x

Releases of the 4.2 line, moved here unchanged from CHANGELOG.md, which keeps the latest released line only.
Newer releases: [CHANGELOG.md](../CHANGELOG.md).

## [4.2.2] — 2026-10-04
### Security

- **Every SQL sink that shoved a caller-supplied string into the statement refuses it now** (external
  security review of the SQL sinks). PostgreSQL was already fail-closed (values through `%L`, identifiers
  through `%I`, an unknown field raises), so the holes were in the two SQL modules and in shared C# code:

  - **SQL Server Free: the filter builder echoed a raw name inside a comment.** `pvt_b2_expr_sql` answered an
    unknown `$field` and an unknown `$expr` node with `/*unknown-b2-...: <name>*/NULL`, and
    `pvt_build_window_expr` did the same with the window function name, so a `*/` in the name closed the
    comment and the rest of it became live T-SQL. All three paths call `dbo.pvt_fail` now, where the
    PostgreSQL module raises.
  - **SQLite Free: the operand builder pasted the operand as it stood.** `pvtAppendOperand` (`redb_pvt.c`)
    spliced the text of a JSON string unquoted whenever the column was not a text column, so
    `{"Age": "1 OR 1=1 --"}` reached the statement verbatim. The builder is total now: a string against a
    non-text column must be a plain numeric literal, a number is validated as a literal before it is
    spliced, and any other token type (array, object, unknown) is refused through the new `pvt_refuse(...)`
    function. No splice can carry anything but a validated number.
  - **SQLite Free: the extension loader stayed on.** `SqliteDataSource.LoadNativeExtension` called
    `EnableExtensions(true)` and never turned it back off, so any injection on that connection could call
    `load_extension()` and run arbitrary code. The flag goes back to `false` in a `finally`.
  - **Window function name, every dialect, Free and Pro.** `WindowFuncRequest.Func` is a public settable
    string and every provider appended it as `func + "()"`, so a name carrying a quote, a bracket or a
    comment marker reached the statement. The new `redb.Core.Query.Window.WindowFunctionName` accepts a
    plain identifier only (letters, digits, underscore, up to 64 characters) and throws otherwise; the nine
    call sites (three Free, six Pro) go through it.
  - **`QuoteIdentifier`, every dialect, wrapped the name without escaping.** `a]b; SELECT 1--` closed the
    identifier on SQL Server, `a"b` did on PostgreSQL and SQLite. Each dialect doubles its closing quote now.
  - **`UserProviderBase.EscapeLikeWildcards` on SQL Server.** T-SQL LIKE reads `[` as a character-class
    opener, so an exact email search for `[s]` matched a user it must not match. The escape follows a
    dialect flag now (`LikeBracketIsWildcard`, true on SQL Server only); PostgreSQL and SQLite keep `[` as
    an ordinary character, where escaping it would change the search itself.

### Fixed

- **`DeleteSubtree` is atomic.** `TreeProviderBase.DeleteSubtreeWithUserAsync` deleted the subtree values in
  one statement and the objects in the next; when the second failed (a live foreign key pointing into the
  subtree) the first was already committed, and the objects stayed with their values gone. Both statements
  run in one `ExecuteAtomicAsync` now.
- **A dictionary field is refused when its runtime value cannot be stored.** A property typed
  `IDictionary<K,V>` (or `Dictionary<K,V>`) whose instance was a `SortedDictionary`, a `ConcurrentDictionary`,
  an `ImmutableDictionary` or any other `IDictionary<K,V>` implementation other than `Dictionary<K,V>` was
  skipped in silence, and under DeleteInsert the stored values had already been deleted, so the field
  disappeared. The save path refuses it now, naming the field and the runtime type, and it refuses while the
  value records are built, i.e. before anything is written. Pinned by `SqliteDictionaryInterfaceRefusalTests`
  (the refusal leaves no object behind, and a plain dictionary in the same interface-typed property
  round-trips).
- **SQL Server Free: a `$case` expression now contributes its fields.** The `$case` branch of
  `pvt_extract_field_pairs` was unreachable: the value of `$case` is an array, so the generic array descent
  took it first and walked the `{when,then}` arm objects as plain objects. That harvested `when`/`then`/`else`
  as field paths and never reached the `{"$field": ...}` references inside the arms, so a filter whose
  expression used `$case` over a props field reached the expression builder without that field (a silently
  NULL term before 0.2.27, an error after it). The generic descent skips `$case` now and the dedicated branch
  walks the arm values, the way the PostgreSQL module already did. Module version **0.2.28**; pinned by the
  module's own smoke runner (`MsSqlModuleSmokeTests`, 195 cases, none failing).

### Changed

- **A request that used to be answered with a comment, a `NULL` or a pasted name throws now.** These shapes
  come from hand-built JSON filters and window requests, not from LINQ: an unknown `$field` or `$expr` node
  and an unsupported window function name on SQL Server Free (module 0.2.28), a non-numeric string against a
  non-text column on SQLite Free (extension 0.6.8), a `WindowFuncRequest.Func` that is not a plain identifier
  on any provider. The exception message says what was refused.
- **Versions: the SQL Server module is 0.2.28, the SQLite extension 0.6.8** (`99_module_version.sql` with
  `MsSqlDialect.Query_PvtRequiredVersion()`, and `PVT_MODULE_VERSION` in `redb_pvt.c` with
  `SqliteDialect.Query_PvtRequiredVersion()`). The module ships inside the package and rolls over on the next
  start; the extension is a native binary, and `redb.SQLite` carries it rebuilt for win-x64, linux-x64 and
  linux-arm64 (there is no macOS binary, as before). A stale extension is refused at start-up on the version
  mismatch instead of running the old sinks quietly, which is what the gate is for.
- **Documentation brought back in line with the code** (external review 0, 1.9, 4.6). The storage note in the
  README now says that each property is a row in `_values` with an indexed, type-driven value column, instead
  of implying a dedicated column per property; `decimal` is documented as `NUMERIC(38,18)` (`REAL` on SQLite
  by default, `TEXT` in the exact mode) rather than `numeric(18,4)`; and the user-table no longer claims
  `SHA256 + salt`, which is only the legacy format, since bcrypt is the default. The same stale claim was
  fixed in the `SqliteUserProvider` comment, in the Serena memories, and in the `SqliteDialect` note that
  described the SQLite v2-pvt SQL folder as deployed SQL.

### Added

- **Tests for every sink above and for the atomic delete**: `MsSqlInjectionHardeningTests`,
  `SqlitePvtOperandInjectionTests` (refusal, subquery operand, numeric string still accepted),
  `WindowFunctionNameTests`, `QuoteIdentifierDoublingTests`, `SqliteDeleteSubtreeAtomicityTests`,
  `SqliteDictionaryInterfaceRefusalTests`.

### Added — an `ActivitySource` for redb.Core database commands

- **`redb.Core` now declares its database operations on an OpenTelemetry `ActivitySource` named
  `redb.Core`** (`redb.Core.Diagnostics.RedbActivitySource`). Every command that goes through a context
  shortcut (`QueryAsync`, `QueryFirstOrDefaultAsync`, `ExecuteScalarAsync`, `QueryScalarListAsync`,
  `ExecuteAsync`, the JSON calls) opens one span carrying the operation name (`db.operation` = `query` /
  `scalar` / `execute` / `json`) and `db.system = redb`, and marks it an error when the command fails. A
  caller's cancellation is recorded but not marked an error. The statement's text and its parameters are
  never put on the span.
- **Off by default.** The source is inert until a consumer subscribes with
  `AddSource(RedbActivitySource.SourceName)`; with no listener the shortcut calls the connection straight
  through, so nothing is emitted and nothing is allocated. This is the shape EF Core uses for its own
  `Microsoft.EntityFrameworkCore` source: the engine owns the source and the application opts in by name.
  In a route, a bean that reaches REDB through `redb.Route.Core` now shows the time spent in the database
  as a child span of the route instead of an empty gap. The nested `Npgsql` / `SqlClient` provider spans
  sit under it when those sources are subscribed too.

## [4.2.1] — 2026-10-02
### Fixed — a module packed by `redb-worker` now loads on a Tsak worker

The template's packing script put the manifest, the module config and the module assembly into the `.tpkg`
and nothing else, so a module that uses a transport the worker does not ship (`redb.Route.File`,
`redb.Route.GenericFile`) failed on its first start with `FileNotFoundException` — the `.tpkg` is the unit
of deployment, and a consumer has no way to add the missing assembly afterwards. The package now carries
every dependency the worker does not already provide; what the worker provides is listed in the
`deploy/shipped-module-deps.txt` of each module template, and the real contents are written next to the
package as `<package>.contents.txt`.

### Fixed — the Razor template answers the same numbers on every machine

`redb-razor` bound `decimal` with the server's culture: on a machine set to a comma decimal separator the
price a browser posted as `123.45` was rejected with `The value '123.45' is not valid for 'Price'.` — a
template that works in one region and fails in the next. The site pins the invariant culture and prints
prices with it, so a form behaves the same everywhere; the README shows the one-line change that follows
the caller's locale instead.

### Fixed — the Blazor WebAssembly client ships the .NET 10 asset layout

`redb-app` served `_framework/blazor.webassembly.js` while the .NET 10 SDK publishes the fingerprinted
layout, and the client host project missed `OverrideHtmlAssetPlaceholders`, so a published page could go
out with a literal `#[.{fingerprint}]` still in its markup. The index carries the import map, the preload
links and the fingerprinted loader now, and the property is set.

### Fixed — the chat template reads its configuration before it creates anything

`redb-chat` hard-coded `SetMinimumLevel(LogLevel.Warning)`, so `Logging:LogLevel:Default` — and the
environment variable behind it — could not switch the route-step logging on, which is the one thing you
need while wiring a model. It also created `redbchat.db` before noticing that the LLM key was missing. The
level comes from configuration, the key is checked first, and the README states the HTTP endpoint the
template exposes.

### Fixed — the console template builds clean, and the worker's README is true again

`dotnet new redb` built with a `CS8602` warning on the nullable result of `LoadAsync`; the warning is gone.
The `redb-worker` README no longer claims that a file which is not valid XML lands in `data/error` — it is
answered with a `Rejected` receipt and archived like any other schema violation.

## [4.2.0] — 2026-09-30
### Added
- **`redb.Templates` is a collection of seven templates.** Each one runs as is: SQLite with Pro, the PVT
  prefilter and change tracking on, the props cache off with the lines that turn it on in a comment. Every
  project that has a database also ships a `Dockerfile` and compose files for PostgreSQL and SQL Server.
  - `redb`: the console app, as before.
  - `redb-razor`: a Razor Pages site - a product list with search, sorting and paging in the database, and a
    create / edit form.
  - `redb-blazor`: the same as a Blazor site (Interactive Server), plus a category tree. Components take an
    `IRedbService` per operation (`RedbWork`), not one per circuit.
  - `redb-worker`: an integration worker - a folder inbox, an XSD check, one `.Transacted()` write to redb, a
    receipt in the outbox; a repeated order is answered as a duplicate through the object's unique key.
  - `redb-chat`: an LLM chat on redb.Route.Llm with the history in redb, in the console and over HTTP.
    `--tools none|shell|mcp` adds a read-only system-command tool or the tools of an MCP server; `--audit`
    stamps the user and audit tags on every message and reads them back with a LINQ query. The API key goes
    into `appsettings.json`; a comment there shows how to switch to DeepSeek.
  - `redb-app`: a Blazor WebAssembly client and a REST DSL API with sign-in (JWT through `InboundAuth=Bearer`),
    CORS for development and nginx on one origin for deployment.
  - `redb-bff`: a Blazor Server backend-for-frontend with a cookie session, and a backend of redb.Route
    controllers that only the web server may call, with a service key.

  The route-based templates (`redb-worker`, `redb-chat`, and the servers of `redb-app` and `redb-bff`) are a
  module with `InitRoute.main` plus a host that calls it. The same module runs in its own host or on a Tsak
  worker: `deploy/pack-tpkg.ps1` packs it, `deploy/docker-compose.tsak.yml` runs it on the Tsak stack image,
  and one set of setting keys and environment variables works in both places.

### Changed
- **The `redb` console template turns on the PVT prefilter and change tracking** in its Pro variant; they
  were commented out. The props cache stays off, with the lines that turn it on next to it.
- **SQL Server Pro: string matching follows the contract of COLLATION.md, as SQL Server Free does.** Plain
  `Contains` / `StartsWith` / `EndsWith` were forced case-sensitive (`COLLATE Latin1_General_CS_AS`) whatever the
  column said; they now keep the column collation - case-insensitive on the default one. The `*IgnoreCase` forms
  were forced to `Latin1_General_CI_AI`, which also ignored accents (`muller` found `Müller`); they now fold case
  only, with `LOWER`. On the same database Free and Pro answered the same query differently; they agree now.
  Code that relied on Pro's case-sensitive plain match on a case-insensitive database should say so with an
  ordinal re-check in memory, as COLLATION.md describes. The prefilter renderer changed with it, so it stays a
  superset of the filter.

- **Configuration copying has one list of settings.** `RedbServiceConfiguration.CopyFrom` copies every setting,
  `CopyBehaviourFrom` every setting except the connection identity (`ConnectionString`, `CacheDomain`); `Clone`,
  `ApplyTemporary` and the validator's auto-fix go through them. The `Clone` extension method in
  `RedbServiceConfigurationExtensions` is removed - the instance method always won over it, so only an explicit
  static call reached it.
- **The obsolete `InitializeAsync` / `AutoSyncSchemesAsync` extensions delegate to `IRedbService.InitializeAsync`.**
  They carried their own copy of the start-up sequence: schemes synchronized in parallel on one service instance,
  no up-front validation of scheme names, and every synchronization error swallowed.

- **SQL Server (Free): the filter and aggregate builders refuse what they cannot read.** An operator they did
  not know became `1=1` (the condition vanished) or `1=0` (no rows - and every row under `$not`); an unknown
  comparison became `=`; a value that was not a number became 0; an aggregate they could not compile became a
  NULL column. They now fail through one helper, `dbo.pvt_fail`, with a message that starts with "redb:", where
  the PostgreSQL module raises. LINQ does not produce these shapes; JSON filters sent directly do.
- **The SQL modules' smoke runners run in the integration suite** (`PostgresModuleSmokeTests`,
  `MsSqlModuleSmokeTests`). They were run by hand only.
- **PostgreSQL Pro: the pivot takes a scalar field with `max()` instead of `(array_agg(...))[1]`.** The array
  per object and per field kept the planner on a sorted GroupAggregate; `max()` lets it hash-aggregate. A
  filtered, sorted page over 200 000 objects went from 1.1-1.4 s to 0.84-0.98 s. The FILTER leaves one row per
  object, so the value is the same. PostgreSQL 14 has no `max()` for every type: `bool` fields take `bool_or`,
  `Guid` and `byte[]` fields keep `array_agg`. One helper (`PvtPick`) now renders this for every Pro PostgreSQL
  query shape (flat, tree, aggregate, grouping, window, grouped window, array grouping) instead of 26 copies.

### Fixed
- **Pro (all three providers): `ToAggregateSqlStringAsync` showed the aggregate without the query's `Where`.**
  The preview built the SQL with no filter while `AggregateAsync` runs with it, so the preview of a filtered sum
  was a sum over the whole scheme. It now passes the same filter. Only the preview was wrong; results were not.
- **SQL Server (Free), JSON filters, found by the smoke runner once it ran:** `$arrayAt` read its operand as the
  index and never compared the value - it now takes `{"index":N,"value":V}` as PostgreSQL does; `$arrayCountLte`
  was spelled `$arraycountle` and matched nothing; `$ilike` on a field was missing; `{"Dict.ContainsKey":"key"}`
  with a bare string key was never recognised. Each returned no rows without an error. SQL Server module
  version 0.2.26.
- **Configuration copies lost five settings.** `Clone` did not copy `AutoApplyDatabaseUpgrades`, `StringCollation`,
  `EnablePvtPrefilter`, `PropsSaveStrategy` and `DefaultCheckPermissionsOnQuery`. The auto-fix of an invalid
  configuration (`GetValidatedRedbServiceConfiguration` without throwing) returned such a copy, so it could switch
  automatic schema upgrades back on for an operator who had turned them off.
- **`ApplyTemporary` left its change behind.** The builder form edited the live configuration before taking the
  snapshot meant to undo it, so the scope ended with the temporary values in place. The configuration form
  applied and restored fourteen settings out of thirty-six. Both apply and restore every behaviour setting now,
  and never touch the connection string or the cache domain.
- **Pro: a stored collection element that did not convert to the element type was lost without a word.** The
  same failure had three outcomes, chosen by the shape of the collection: a dictionary entry vanished, an array
  element became the type's default (0), a list element was skipped. Loading now fails with the value, the target
  type and the original error.
- **SQLite: a column value the row mapper could not convert left the property at its default.** Two silent
  `catch` blocks made the row read as if the column were NULL. It fails now, naming the column, the value and the
  property. Conversions use the invariant culture (a decimal text like `1.5` was parsed with the current one).
- **Tree projections (`TreeQueryableBase.Select`): `Take` / `Skip` / `Distinct` after `Where` / `OrderBy`.**
  `Where` and `OrderBy` ran in memory after the projection while `Take` and `Skip` went to SQL before it, so the
  page was cut from the unfiltered, unsorted nodes; `Distinct` went to the source query, where every node is
  distinct. They now run in call order after the projection, as the flat projection has done since S-4.
- **Free: `DistinctByRedb` / `DistinctBy` on a tree query returned every node.** The flat query passed the
  distinct key to `pvt_build_query_sql`; the three Free tree providers passed none. They pass it now. The SQL
  builders then had to honour it in the tree shape too: SQL Server ignored the key in the tree branch without
  props fields, and the SQLite native extension ignored it in the wide shape (a tree without props fields, or a
  filter with an absence check). SQL Server module version 0.2.24, SQLite extension version 0.6.7 - the SQLite
  native library is rebuilt for Windows x64 and Linux x64/arm64.
- **Pro: `DistinctByRedb` / `DistinctBy` on a tree query.**
  - SQLite Pro ignored the distinct key on a tree and returned every node: the key was computed and never put
    into the SQL. The tree now ranks rows with `ROW_NUMBER()` as the flat query does.
  - `CountAsync` of a distinct tree query was wrong on all three Pro providers. PostgreSQL counted every node
    (`COUNT(DISTINCT o._id)` - the ids are distinct whatever the key is); SQL Server and SQLite looked for a
    PostgreSQL `DISTINCT ON`, did not find it, and returned the first column of the first row - an object id.
    The count is now the number of distinct keys, and a count that cannot find the distinct query refuses.
- **SQL Server (Free): an unknown nested field resolved to its parent.** T-SQL keeps a variable's old value when
  `SELECT @v = ...` finds no row, so `dbo.pvt_resolve_field_path` returned the parent structure for a child the
  scheme does not have (`Contacts[].NoSuchField` became the `Contacts` array), and a filter, sort or grouping on
  that path read the wrong structure. The dictionary child and the list-item accessor had the same trap.
- **SQL Server (Free): the array grouping skipped a key or aggregate field the item does not have.** The grouping
  ran on the remaining keys, the aggregate vanished from the result, and with no key left the flat list of items
  came back instead of groups. Such a request is refused now, as is an unknown aggregate function.
- **Pro: HAVING constants.** An array or object in `$const` was bound as its raw JSON text and compared as a
  string; it is refused now. The unreachable fallback operator `=` of the translator is a refusal too.
- SQL Server module version 0.2.26: the functions are reinstalled on the first start.
