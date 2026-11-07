# Dates and times in RedBase

**Audience:** anyone storing a date or a time in a `*Props` class.

One rule decides everything else:

> **`DateTime` is a reading on a clock. `DateTimeOffset` is a moment in time.**

`14:00` written as a `DateTime` comes back as `14:00` on any machine, in any zone, through any read
path. A `DateTimeOffset` carries a real instant and keeps native .NET semantics, exactly as you would
expect outside RedBase.

Pick by what the value *means*, not by which type you happen to have:

| You are storing | Use | Why |
|---|---|---|
| A birthday, a contract date, a shift that starts at 09:00 local everywhere | `DateTime` or `DateOnly` | the number is the fact; a zone would corrupt it |
| When a payment cleared, when a log line was emitted, a token expiry | `DateTimeOffset` | the instant is the fact; the wall clock differs per viewer |

---

## The full type table

| C# type | Semantics | Stored in | Notes |
|---|---|---|---|
| `DateTime` | zone-less clock reading | `_values._DateTimeOffset` | `Kind` is set to `Utc` as a label, not a conversion |
| `DateTimeOffset` | instant with an offset | `_values._DateTimeOffset` | read back normalised to UTC (`+00:00`), same instant |
| `DateOnly` | zone-less date | `_values._DateTimeOffset` | midnight of that date |
| `TimeOnly` | zone-less time of day | `_values._String` | invariant text |
| `TimeSpan` | duration | `_values._String` | invariant text, keeps days and sign |

Base fields of every object (`DateCreate`, `DateModify`, `DateBegin`, `DateComplete`) are
`DateTimeOffset`, therefore instants. They line up with the database's own `now()` defaults.

---

## The one trap worth knowing

Base fields are `DateTimeOffset`, Props fields may be `DateTime`. Assigning the same value to both
does not give you the same value back:

```csharp
var t = DateTime.Now;          // 14:00 in a UTC+03:00 zone

obj.DateBegin  = t;            // base field is DateTimeOffset -> implicit conversion applies the
                               //   local offset -> stored instant 11:00Z
obj.Props.When = t;            // Props DateTime -> the reading is kept -> stored 14:00
```

Three hours apart, from one expression. This is the contract working as designed, not a defect: the
two types mean different things. It is invisible on a machine running in UTC and reproducible on
every other one, which is why it tends to surface late.

If you want both to agree, be explicit about which meaning you want:

```csharp
obj.DateBegin  = DateTimeOffset.UtcNow;      // instant
obj.Props.When = DateTime.UtcNow;            // reading, already UTC, no ambiguity
```

---

## Locale: nothing to configure

The text form of `DateOnly`, `TimeOnly` and `TimeSpan` is culture-invariant on both ends, in every
tier and on every provider. A row written on a machine running `ru-RU` reads identically on one
running `en-US`. There is no setting, and the host's `CultureInfo` cannot influence a stored value.

The only thing to be aware of: rows written by **RedBase 3.5 or earlier** used the writing machine's
short date/time pattern. Those still load, through a fallback that tries the invariant form first and
the current culture last. If you moved such a database to a host with a different culture, rewrite the
affected rows once and the fallback stops mattering.

---

## Precision

| Provider | Storage | Effective resolution |
|---|---|---|
| SQL Server | `DATETIMEOFFSET(7)` | 100 ns, identical to a .NET tick, lossless |
| PostgreSQL | `timestamptz` | 1 microsecond |
| SQLite | `REAL` Julian day | about 40 microseconds, and 1 millisecond when the object is read through the JSON projection |

If you compare timestamps for exact equality, this matters: `DateTime.UtcNow` survives a round trip
unchanged on SQL Server, loses sub-microsecond digits on PostgreSQL, and loses sub-millisecond digits
on SQLite. Round to the precision you actually need before storing, or compare with a tolerance.

---

## Querying

Everything you would expect works server-side, on all providers, for `DateTime`, `DateTimeOffset` and
`DateOnly`:

```csharp
// comparison and ranges
.Where(e => e.HireDate >= cutoff)
.Where(e => e.HireDate >= start && e.HireDate < end)

// equality finds the row it was written from
.Where(e => e.HireDate == exact)

// ordering
.OrderBy(e => e.HireDate)

// date parts on base fields
.WhereRedb(o => o.DateCreate.Year == 2025)
```

Two writings of the same instant select the same rows, so `10:30+03:00` and `07:30Z` are one cutoff.

`TimeOnly` and `TimeSpan` support **exact equality**. Ordered comparison on them is not supported:
they live in a text column and SQL orders them lexicographically, so `"10.02:00:00"` sorts below
`"2.02:00:00"`. If you need to order or range over a duration, store it as a number of ticks or
seconds in a `long`.

`MinAsync` and `MaxAsync` over a **Props** date field are supported on SQLite only. The aggregate
contract returns a number, which suits SQLite's numeric date storage and cannot represent the ISO
string PostgreSQL and SQL Server return. `MinRedbAsync` and `MaxRedbAsync` over **base** date fields
work everywhere.

---

## Upgrading an existing database

`DateOnly` properties did not round-trip before RedBase 3.6: the value was written correctly and
dropped on the way out, so every `DateOnly` materialised as `0001-01-01`. Databases created by 3.6 or
later are correct out of the box. To fix an existing one, run the migration for your provider once:

```
redb.Postgres/sql/migrate_dateonly_db_type.sql
redb.MSSql/sql/migrate_dateonly_db_type.sql
redb.SQLite/sql/migrate_dateonly_db_type.sql
```

It is idempotent, changes no stored value, and refreshes the metadata cache of every scheme that has
a `DateOnly` field. Stored data needs no rewriting: it was always in the right column.
