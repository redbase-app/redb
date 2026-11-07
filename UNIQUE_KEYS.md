# Unique Keys in redb

redb enforces uniqueness **in the database**, never by a read-then-write check that races.
There are two independent mechanisms, and they compose:

| | Object key (`ValueUnique`) | Property key (`[RedbUnique]`) |
|---|---|---|
| Lives on | the object row itself (`_objects._value_unique`) | a Props field (`_values._unique`) |
| Type | `string`, up to 440 characters | any scalar: string, integer types, `Guid`, `bool`, `double`, `decimal`, date/time types, `byte[]` |
| Scope | one value per **scheme** | one value per **scheme** per field |
| Set by | the application, explicitly | computed from the field value on save |
| Index | `UIX__objects__scheme_unique` | `UIX__values__structure_unique` |
| Typical use | external identifier of the whole object | business keys: SKU, email, passport number |

Both indexes are partial (`WHERE ... IS NOT NULL`), so objects without keys cost nothing —
no index entries, no uniqueness checks on write.

## The object key: `ValueUnique`

Every `RedbObject` carries an optional string key, unique within its scheme:

```csharp
var order = new RedbObject<OrderProps>
{
    name = "order-1042",
    value_unique = "ERP-ORD-1042",          // alias: ValueUnique
    Props = new OrderProps { Total = 99.90m }
};
await redb.SaveAsync(order);                 // a second object with this key is rejected
```

- `null` never participates: any number of objects may leave the key empty.
- The 440-character limit is enforced in C# (`RedbUniqueKeyValueException`), so every
  provider behaves identically — SQLite does not check VARCHAR lengths on its own.
- Soft delete releases the key: the trash does not squat on business identifiers.

### Upsert by the object key

`SaveByUniqueAsync` resolves the row by `(scheme, ValueUnique)` and saves onto it —
creating the object when the key is free, updating the current holder when it is taken:

```csharp
var flag = new RedbObject<FeatureFlagProps>
{
    value_unique = "checkout-v2",
    Props = new FeatureFlagProps { Enabled = true }
};
var id = await redb.SaveByUniqueAsync(flag); // exactly one object per key, concurrency-safe
```

A lost object-key race is retried once onto the winner's row, so concurrent upserts of the
same key are idempotent. A `[RedbUnique]` **property** violation inside the same save is
never retried — it is not the object key's business and surfaces as-is.

## Property keys: `[RedbUnique]`

Mark a scalar property of the Props class — or of a nested class reached without crossing a
collection — and its value becomes unique within the scheme:

```csharp
public class ProductProps
{
    [RedbUnique]
    public string? Sku { get; set; }         // root scalar key

    public IdentityBlock? Identity { get; set; }
}

public class IdentityBlock
{
    [RedbUnique]
    public string? Passport { get; set; }    // nested scalar key - same index, same contract
}
```

The database stores a hash of the value's **canonical form** in `_values._unique`; the
unique index enforces it. `null` values get no key and never take part in uniqueness.

### Subtree keys: the whole class, array or dictionary

`[RedbUnique]` on a nested-class, array or dictionary property makes the **content of the
whole subtree** the key — something classic SQL only approximates with indexed views or
trigger-maintained hash columns:

```csharp
public class TariffProps
{
    [RedbUnique]
    public TariffConfig? Config { get; set; }        // no two tariffs with identical config

    [RedbUnique]
    public List<long>? Slots { get; set; }           // ordered: [1,2] and [2,1] coexist

    [RedbUnique]
    public Dictionary<string, long>? Limits { get; set; } // unordered: {a:1,b:2} IS {b:2,a:1}
}
```

Semantics follow the canonical forms the storage already maintains:

- a **dictionary** is an unordered set of pairs; a **list/array** is ordered;
- **references** (`RedbObject<T>`, list items) canonicalise by id — `[RedbUnique]` on a
  collection of references reads as "unique SET of references";
- an absent (`null`) or **empty** collection claims no key;
- lookup by value works: `GetByUniqueAsync(p => p.Config, new TariffConfig { ... })`
  canonicalises the passed value exactly the way the save did (reference collections are
  outside the by-value lookup — enforcement still applies, only the lookup form differs).

### Element scopes: unique elements instead of unique content

`Scope` on a collection key switches from the subtree-content reading to **element keys**:

```csharp
public class TeamProps
{
    [RedbUnique(Scope = UniqueScope.Collection)]
    public List<RedbObject<UserProps>>? Members { get; set; } // no duplicate links in one team

    [RedbUnique(Scope = UniqueScope.Scheme)]
    public List<string>? Emails { get; set; }   // an email appears in ONE team, whichever list
}
```

- `Collection` — no duplicate elements inside one object's collection; different objects may
  repeat each other freely (each element's canonical form is salted with the owning
  collection's identity). A global by-value lookup is meaningless here and is rejected.
- `Scheme` — element values unique across every object of the scheme, and globally probeable:
  `GetByUniqueAsync(p => p.Emails, "a@x.io")` returns the holder.
- Reference elements canonicalise by **target id**: the key follows the link, not the linked
  object's content.
- `Scope` on a scalar or a nested class is rejected — the bare attribute already carries the
  strongest reading there.

What is rejected — at `SyncSchemeAsync`, with `RedbUniqueKeyDefinitionException`, not at
insert where a misplaced key would silently never fire:

- a property inside an **element** of a collection of classes: every element of every object
  shares one structure, so uniqueness would degenerate to one value across all elements;
- enums (stored as list-item references, no canonical value form) and other non-scalar,
  non-subtree types;
- `Scope` anywhere but on a collection property.

### Violations

Every provider surfaces one typed exception:

```csharp
try
{
    await redb.SaveAsync(duplicate);
}
catch (RedbUniqueViolationException ex)
{
    // ex.Kind:           ObjectKey | Property | Unknown - branch on this, not on strings
    // ex.SchemeName:     scheme of the object being saved
    // ex.PropertyName:   the [RedbUnique] property, when the driver names the key tuple
    // ex.ConstraintName: the index the database reported
    // ex.Cause:          the original driver exception, always attached
}
```

PostgreSQL and MSSQL name the violated index and tuple; SQLite names only the columns —
the `Kind` is classified correctly everywhere, but `PropertyName` detail may be absent
on SQLite.

### Lookup by key value

One server-side hash probe, no scan, returns the loaded object or `null`:

```csharp
var bySku      = await redb.GetByUniqueAsync<ProductProps>(p => p.Sku, "SKU-1042");
var byPassport = await redb.GetByUniqueAsync<ProductProps>(p => p.Identity!.Passport, "740-123");
var byPath     = await redb.GetByUniqueAsync<ProductProps>("Identity.Passport", "740-123");
```

### Key lifecycle

- **Update** releases the old value and guards the new one in the same save.
- **Soft delete** releases the key to the next claimant.
- **Batch saves** enforce keys exactly like single saves.

### Recompute (backfill and repair)

Keys are recomputed automatically at scheme synchronisation when they cannot be trusted:
the attribute appeared on an already-populated structure, the canonicalisation version
moved, or rows exist with a value but no key (the trace of a SQL-side writer). Explicit
repair is available too:

```csharp
var report = await redb.RecomputeUniqueAsync<ProductProps>("Identity.Passport");
// report.HashedRows, report.Duplicates - first row of a duplicate group keeps the key,
// the rest stay OUTSIDE the index (their _unique is NULL) and are reported, never thrown:
// uniqueness holds for every keyed row; resolving the duplicates is the application's call.
```

## Schema delivery

Existing databases receive the columns and indexes automatically on `InitializeAsync`
through each provider's versioned upgrade mechanism — no manual migration.

## See also

- Runnable example: `redb.Examples`, **E003** (both mechanisms end to end).
- Design history and decisions: `docs/V4/SUBTREE_UNIQUE_PLAN.md`.
