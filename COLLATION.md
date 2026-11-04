# Case-insensitive search in RedBase

**Audience:** anyone whose data is not written in English.

By default, a case-insensitive search in RedBase folds case the way the database does, and two of the
three databases fold **ASCII only**. `Contains("привет", OrdinalIgnoreCase)` finds `HELLO` but not
`ПРИВЕТ`. One setting fixes it:

```csharp
services.AddRedb(options => options
    .UsePostgres(connectionString)
    .Configure(c => c.StringCollation = "und-x-icu"));
```

Read the rest of this page before you switch it on: it changes how PostgreSQL uses indexes, and it
does not fix everything the name suggests.

---

## Do you have this problem?

| Provider | Affected | Why |
|---|---|---|
| **PostgreSQL** | only if the database was created with `LC_CTYPE=C` or `POSIX` | folding comes from the collation's ctype |
| **SQLite** | **always** | `LIKE`, `lower()`, `upper()` and `COLLATE NOCASE` are ASCII-only in every SQLite build; the one shipped with RedBase has no ICU |
| **SQL Server** | no | the default collation is case-insensitive for every script |

Check PostgreSQL in one line:

```sql
SELECT 'Привет' ILIKE '%привет%';   -- false means you are affected
```

For SQLite there is nothing to check. It is always affected.

### PostgreSQL: decide by the database ctype

```sql
SELECT datctype FROM pg_database WHERE datname = current_database();
```

The ctype of the database decides both whether you need the setting and whether a substring search can
be indexed at all.

| `datctype` | What to do |
|---|---|
| UTF-8 (`en_US.utf8`, `ru_RU.utf8`, `C.UTF-8`) | Plain `ILIKE` already folds non-ASCII case. Leave `StringCollation` unset; the stock trigram index `IX__values__String_pattern` serves the search. Setting it here buys no correctness and costs the index (see below). |
| `C` or `POSIX` | `StringCollation` gives correct folding. But pg_trgm takes letters from the database ctype, not from the collation, and under `C` it builds no trigram from non-ASCII text (`show_trgm('привет')` is empty). A non-ASCII substring search therefore reads every row on every tier, and no index changes that. The only way to an indexed non-ASCII search is a database with a UTF-8 ctype (a new database, or dump and restore), after which the row above applies. |

On a `C` ctype with the setting on, the index described below keeps patterns with ASCII letters indexed;
it does nothing for the others.

---

## What the setting fixes

Every script whose case mapping is one character to one character. There is **no per-language work**:
one setting covers all of them at once.

| | Example |
|---|---|
| Cyrillic | `ПРИВЕТ` found by `привет` |
| Greek | `ΑΘΗΝΑ` found by `αθηνα` |
| Hungarian | `ŐSZ ÚTON` found by `ősz úton` |
| Polish | `ŻÓŁW ŁÓDŹ` found by `żółw` |
| Czech | `ŘEKA ČESKÁ` found by `řeka česká` |
| French | `ÉTÉ À PARIS` found by `été à paris` |

It applies to the whole family together, so a search can never disagree with a comparison:

```csharp
.Where(x => x.Text.Contains(needle, StringComparison.OrdinalIgnoreCase))
.Where(x => x.Text.StartsWith(needle, StringComparison.OrdinalIgnoreCase))
.Where(x => x.Text.EndsWith(needle, StringComparison.OrdinalIgnoreCase))
.Where(x => x.Text.ToLower().Contains(needle))
.Where(x => x.Text.ToUpper().Contains(needle))
```

---

## What it does not fix

Three classes, and no choice of collation reconciles them. They are stated here rather than
discovered later.

### Diacritics are not case

`Müller` is **not** found by `muller`, anywhere, on any provider. That is accent-insensitivity, a
different feature. PostgreSQL refuses it outright for pattern matching: the nondeterministic collation
it would require is rejected with *"nondeterministic collations are not supported for ILIKE"*. If you
need it, the tool is `unaccent()` plus an expression index, not a collation.

### German ß against SS

Pattern matching folds character by character and cannot change a string's length, while `UPPER` can
and does expand `ß` to `SS`. So the two disagree with each other on the same database.

`Straße` found by `strasse`? **PostgreSQL and SQLite: no. SQL Server: yes.** The providers genuinely
differ here, and RedBase does not paper over it.

### Turkish and Azeri dotted i

`İSTANBUL` found by `istanbul`? **PostgreSQL and SQLite: no. SQL Server: yes.**

`İ` (U+0130) folds to `i` plus a combining dot, not to a plain `i`. This one is not merely unfixed but
unfixable by a single installation-wide setting: Turkish wants `I`→`ı`, while every other locale wants
`I`→`i`. Choosing `tr-x-icu` would fix Turkish and break everyone else. The right scope for it is a
per-field or per-query collation, which RedBase does not offer today.

---

## The cost on PostgreSQL: indexes

A collated operand cannot use an index built with the database's own collation. Which searches that
touches depends on the tier, because only one of them reads a trigram index at all:

| Tier | How a `Contains` / `StartsWith` / `EndsWith` filter runs | Trigram index read |
|---|---|---|
| **Pro, planner on** (`EnablePvtPrefilter`, on by default in `RedbServiceConfiguration` and in Tsak) | the predicate goes down to `_values`: `dv._String LIKE`, or `(dv._String COLLATE "<collation>") ILIKE` with the setting on | yes |
| **Pro, planner off** | the free forms (row below) | no |
| **Free** | the values of an object are pivoted first, then the pivoted value is filtered | no, with or without the setting |

So the setting costs an index on Pro with the planner on, and only there. Turn it on without acting
and every ignore-case search of that tier stops reading `IX__values__String_pattern` and reads every row
of `_values` instead. Nothing errors. The index shows no reads and only the response time changes.

Measured on 1 000 000 string values of one field, 100 of them holding `ПРИВЕТ`, PostgreSQL 18, the
predicate Pro pushes down (`... ILIKE '%привет%'`):

| Database ctype | Index on `_values` | Predicate | Plan | Time |
|---|---|---|---|---|
| `en_US.utf8` | stock `IX__values__String_pattern` | `_String ILIKE` (setting off) | Bitmap Index Scan | 0.34 ms |
| `en_US.utf8` | stock | `(_String COLLATE "und-x-icu") ILIKE` | Seq Scan | 564 ms |
| `en_US.utf8` | trigram over `(_String COLLATE "und-x-icu")` | same | Bitmap Index Scan | 0.68 ms |
| `C` | either | same, Cyrillic pattern | Seq Scan (no trigrams, see the ctype section) | 550 ms |
| `C` | trigram over the collated column | same, pattern `'%number 12345%'` | Bitmap Index Scan | 28 ms |

The fix is an index over the same expression as the predicate. It must match the configured collation
exactly, so it differs per project:

```sql
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX CONCURRENTLY "IX__values__String_pattern_und_x_icu"
    ON _values USING gin ((_String COLLATE "und-x-icu") gin_trgm_ops)
    WHERE _String IS NOT NULL;
```

[`redb.Postgres/sql/maintenance/string_collation_index.sql`](redb.Postgres/sql/maintenance/string_collation_index.sql)
runs it for any collation (`psql -v collation=und-x-icu -f string_collation_index.sql`) and rebuilds an
index left INVALID by an interrupted build. RedBase finds the index by what it is, not by its name: a GIN
index on `_values` with `gin_trgm_ops` over `_String` under the configured collation. Pro with the
planner on checks for it at every start and logs a Warning while it is missing or INVALID, naming the
collation, the statement, the script and what the ctype of the database means for it.

### Building it at start-up: `CreateStringCollationIndex`

**Under ctype `C` the index serves patterns with ASCII letters only; under a UTF-8 ctype, leaving
`StringCollation` unset serves every pattern with the stock index and needs no second index.** With that
in mind:

```csharp
.Configure(c =>
{
    c.StringCollation = "und-x-icu";
    c.CreateStringCollationIndex = true;   // Tsak: Tsak:Redb:CreateStringCollationIndex
})
```

Off by default. On, Pro with the planner on builds `IX__values__String_pattern_<collation>` at start-up
when no index of the collation exists (the name takes the collation in lower case, every character other
than a letter or a digit as `_`). The build is `CREATE INDEX CONCURRENTLY` on a connection of its own, under
the schema lock, so several nodes starting together build it once; reads and writes go on, and start-up
waits until it is built, which takes minutes on a large `_values`. An index left INVALID by an interrupted
build is dropped and built again. An index that already serves the collation, under any name, is kept.

Nothing is built, and start-up logs a Warning and goes on, when `AutoApplyDatabaseUpgrades` is off or the
role may not create an index: run the script instead. The free provider and Pro with the planner off read
no trigram index; there the option logs one Warning and builds nothing.

### Its price

The cost is a second index on the largest table, about the size of the table itself (138 MB beside a
120 MB `_values` in the measurement above, 20 s to build), updated on each save of a string value.

`IX__values__String_pattern` is not dead once the collation is set: case-sensitive `Contains`,
`StartsWith` and `EndsWith` still run as plain `LIKE` without `COLLATE` and read it. Drop it only if the
project does no case-sensitive substring search; the statement is at the end of the script, commented out.

SQLite needs no such thing: `LIKE '%x%'` never used an index there in the first place.

---

## What the value means per provider

The setting is one switch with three implementations, because the providers differ in kind and not
merely in syntax.

| Provider | What happens |
|---|---|
| **PostgreSQL** | the name is a real collation, attached as `COLLATE "..."` to each folded operand. It must exist in `pg_collation` and be **deterministic**; ICU collations such as `und-x-icu` and `ru-x-icu` qualify |
| **SQLite** | there is nothing to attach a collation to, so the provider instead replaces the built-in `like`, `lower` and `upper` with Unicode-aware ones on every connection. The value is only a switch; it is still validated, so a typo fails at startup rather than turning into silence |
| **SQL Server** | ignored. Its default collation already folds every script, and overriding a database deliberately created case-sensitive would defeat that decision |

Leave it unset and nothing changes anywhere: the generated SQL is byte for byte what it was.

### A note for SQLite

SQLite maps the case-sensitive and case-insensitive string operators onto the same `LIKE`, and always
has, because its `LIKE` is case-insensitive for ASCII regardless. Enabling the setting therefore widens
that to every script: a case-**sensitive** `Contains` will start matching a differently-cased Cyrillic
string too. This is opt-in, but it is a real change in behaviour.

---

## Choosing a name

`und-x-icu` is the root ICU collation and the right default for a multilingual database. Use a
language-specific one such as `ru-x-icu` or `de-x-icu` only if you have a reason; they differ in
sorting rules more than in case folding, and a language-specific choice makes the Turkish trade-off
above worse rather than better.

Names are validated when assigned, so a typo fails at startup with a message naming the setting rather
than as a driver error on the first search. The name is also quoted where it is emitted, since a
collation is an identifier and cannot be sent as a query parameter.

List what your server offers:

```sql
SELECT collname FROM pg_collation WHERE collprovider = 'i' ORDER BY 1;
```

## Plain matching follows the database (BR-6, contract)

`StartsWith` / `Contains` / `EndsWith` **without** a comparison argument translate to `LIKE` and
keep each database's own case rules - redb does not decide for the programmer:

| Provider | Plain `LIKE` semantics |
|---|---|
| **PostgreSQL** | case-sensitive |
| **SQLite** | ASCII case-insensitive (non-ASCII is case-sensitive) |
| **MSSQL** | whatever the column collation says - case-insensitive on the default |

The same prefix query can therefore return different sets on different providers - by contract.
When you need one behaviour everywhere, say so explicitly: the `*IgnoreCase` forms
(`StartsWith(x, StringComparison.OrdinalIgnoreCase)`, `ContainsIgnoreCase`, ...) fold case on every
provider, and `StringCollation` extends the folding beyond ASCII (the subject of this document).
For a case-sensitive contract on top of a case-insensitive database, filter the candidates again
in memory with an ordinal comparison - the server-side match stays an index-friendly prefilter.

