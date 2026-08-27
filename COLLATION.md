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

**Read this one twice.** A collated operand cannot use an index built with the database's own
collation. Turn the setting on without acting, and a trigram search silently stops using its index and
becomes a full scan. Nothing errors. Only the response time changes.

Measured on 40 000 rows, forcing the planner to prefer an index:

| Query | Plan |
|---|---|
| `s ILIKE '%привет%'` | Bitmap Index Scan |
| `(s COLLATE "und-x-icu") ILIKE '%привет%'` | Seq Scan, and the index cannot be used at all |

The fix is an index whose expression matches the predicate. RedBase does not create it for you,
because indexes on `_values` are a decision about your data volume, not something a library should
make behind your back:

```sql
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX CONCURRENTLY ix_values_string_trgm_icu
    ON _values USING gin ((_String COLLATE "und-x-icu") gin_trgm_ops);
```

With that index in place the plan returns to a Bitmap Index Scan. The cost is a second index on the
largest table in the database, so weigh it against how much case-insensitive searching you actually do.

SQLite needs no such thing: `LIKE '%x%'` never used an index there in the first place.

### Planned: RedBase should offer to create it

Leaving this to the reader is the weak part of the current design. The setting that causes the
regression ships in the box, the index that repairs it does not, and the failure mode is silence —
nothing errors, the query just gets slower, and whoever turns `StringCollation` on months later has
no reason to connect the two.

What is intended for a later release: an opt-in that creates the matching expression index as part of
schema initialisation, keyed off the configured collation, so the index and the predicate cannot
drift apart. Opt-in rather than automatic, because a second GIN index on `_values` is a real cost on
a large installation and `CREATE INDEX CONCURRENTLY` cannot run inside the transaction that
initialisation uses — it needs its own connection and its own decision.

Until that exists, the statement above is the supported answer, and this section is the reminder that
we know it is homework we handed to you.

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
