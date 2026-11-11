# REDB Examples Project

Public code examples for REDB documentation website.

## Project Structure

```
redb.Examples/
├── Models/
│   └── ExampleModels.cs      # EmployeeProps, ProjectMetricsProps, etc.
├── Examples/
│   ├── ExampleBase.cs        # Base class + ExampleMetaAttribute
│   ├── E000_BulkInsert.cs    # Setup: creates 100 employees (run first!)
│   ├── E001_SaveAsync.cs     # CRUD examples
│   ├── E010_WhereSimple.cs   # Query examples
│   └── ...
├── Output/
│   ├── ExampleResult.cs      # Result record + ExampleTier enum
│   └── TablePrinter.cs       # Console table formatter
└── Program.cs                # Entry point, runs all examples
```

## Data Model

All examples use `EmployeeProps` from `Models/ExampleModels.cs`.  
**DO NOT copy models from redb.ConsoleTest** - we have our own with business-friendly names.

### EmployeeProps Fields

| Category | Fields | Notes |
|----------|--------|-------|
| Basic | FirstName, LastName, Age, Position, Salary, Department, HireDate | Simple types |
| Arrays | Skills[], SkillLevels[], Certifications[] | string[], int[] |
| Nested | HomeAddress, WorkAddress (with Building) | Business classes |
| Contacts | Contacts[] | Array of business classes |
| References | CurrentProject, PastProjects[] | RedbObject<T> |
| Dictionaries | PhoneDirectory, BonusByYear, OfficeLocations | Various key/value types |
| Pro Dict | DepartmentHistory, PerformanceReviews, ProjectMetrics | Complex keys/values |

## Example Format

### 1. Required Attribute

```csharp
[ExampleMeta(
    "E010",                    // ID: E + 3 digits
    "Where - Filter by Salary", // Title for website
    "Query",                   // Category: CRUD, Query, Trees, Bulk, Dict, etc.
    ExampleTier.Free,          // Tier: Free, Pro, or Enterprise
    1,                         // Difficulty: 1-5
    "Where", "Query", "Filter" // Tags for search
)]
```

### 2. Class Structure

```csharp
using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Short description of what this example demonstrates.
/// </summary>
[ExampleMeta("E0XX", "Title", "Category", ExampleTier.Free, 1, "Tag1", "Tag2")]
public class E0XX_ExampleName : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // YOUR CODE HERE
        // Use data from E000_BulkInsert (100 employees already exist)

        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var result = await query.ToListAsync();
        sw.Stop();

        return Ok("E0XX", "Title", ExampleTier.Free, sw.ElapsedMilliseconds,
            [$"Line 1: what query does", $"Result: {result.Count} items"]);
    }
}
```

### 3. SQL Output

Every query example MUST include commented ToSqlStringAsync:

```csharp
// Uncomment to see generated SQL:
// var sql = await query.ToSqlStringAsync();
// Console.WriteLine(sql);
```

## ID Numbering Scheme

| Range | Category | Examples |
|-------|----------|----------|
| **E000-E009** | **Setup/Bulk** | |
| E000 | Bulk Insert | Creates 100 employees (run first!) |
| E001 | SaveAsync | Create single object |
| E002 | LoadAsync | Load by ID |
| E003 | DeleteAsync | Delete object |
| **E010-E019** | **Where - Basic** | |
| E010 | Where simple | `Salary > 75000` |
| E011 | Where AND | `Age >= 30 && Salary > 70000` |
| E012 | Where OR | `Age < 30 \|\| Salary > 90000` |
| E013 | Where NOT | `Position != "Manager"` |
| E014 | Where range | `Salary > 60000 && Salary < 90000` |
| E015 | Where multiple AND | `A && B && C && D` (3+ conditions) |
| E016 | Where chain | `.Where(A).Where(B).Where(C)` |
| **E020-E029** | **Where - DateTime** | |
| E020 | DateTime equals | `HireDate == specificDate` |
| E021 | DateTime greater | `HireDate >= startDate` |
| E022 | DateTime range | `HireDate >= start && HireDate < end` |
| E023 | DateTime year/month | `.Year == 2024` or `.Month == 6` |
| **E030-E039** | **Where - String** | |
| E030 | String.Contains | `LastName.Contains("Smith")` |
| E031 | String.StartsWith | `FirstName.StartsWith("John")` |
| E032 | String.EndsWith | `Email.EndsWith("@company.com")` |
| E033 | String equality | `Department == "Engineering"` |
| **E040-E049** | **Where - Nested** | |
| E040 | Nested property | `HomeAddress.City == "London"` |
| E041 | Deep nested | `HomeAddress.Building.Floor > 5` |
| E042 | Nested with AND | `HomeAddress.City == "X" && WorkAddress.City == "Y"` |
| **E050-E059** | **Arrays (Pro)** | |
| E050 | Array.Contains | `Skills.Contains("C#")` |
| E051 | Array multiple OR | `Skills.Contains("A") \|\| Skills.Contains("B")` |
| E052 | Array NOT Contains | `!Skills.Contains("intern")` |
| E053 | Array indexer | `Skills[0] == "C#"` |
| E054 | Array + other conditions | `Skills.Contains("X") && Age > 30` |
| **E060-E069** | **Dictionaries (Pro)** | |
| E060 | Dict ContainsKey | `PhoneDirectory.ContainsKey("desk")` |
| E061 | Dict indexer | `PhoneDirectory["desk"] == "x3001"` |
| E062 | Dict<K, Class> nested | `OfficeLocations["HQ"].City == "NYC"` |
| E063 | Dict<Tuple, V> key | `PerformanceReviews[(2024, "Q1")]` |
| E064 | Dict int key | `BonusByYear[2023] > 5000` |
| **E070-E079** | **Aggregates** | |
| E070 | CountAsync | Total count |
| E071 | CountAsync + Where | Filtered count |
| E072 | FirstOrDefaultAsync | First matching |
| E073 | AnyAsync | Check existence |
| **E080-E089** | **Sorting/Paging** | |
| E080 | OrderBy | Sort ascending |
| E081 | OrderByDescending | Sort descending |
| E082 | ThenBy | Secondary sort |
| E083 | Skip/Take | Pagination |
| E084 | OrderBy + Where | Sort filtered |
| **E090-E099** | **Trees (Pro)** | |
| E090 | TreeQuery basics | Get tree structure |
| E091 | GetAncestors | Parent chain |
| E092 | GetDescendants | All children |
| E093 | Tree + Where | Filter in tree |
| **E100-E109** | **Advanced** | |
| E100 | ChangeTracking | Auto-detect changes |
| E101 | Projection | Select specific fields |
| E102 | Complex OR+AND | `(A \|\| B) && (C \|\| D)` |
| E103 | NOT with nested | `!(A && B)` |

## Tier Guidelines

### Free (ExampleTier.Free)
- Basic CRUD: SaveAsync, LoadAsync, DeleteAsync
- Simple Where: ==, >, <, AND, OR
- Basic sorting: OrderBy, ThenBy
- Paging: Skip, Take
- CountAsync, FirstOrDefaultAsync

### Pro (ExampleTier.Pro)
- Array operations: .Contains(), indexer e.Skills[0]
- Dictionary operations: .ContainsKey(), indexer e.Dict["key"]
- Complex nested queries: e.HomeAddress.Building.Floor
- Bulk operations: SaveAsync(IEnumerable), COPY protocol
- Tree queries: TreeQuery, GetAncestors, GetDescendants
- Tuple dictionary keys
- RedbObject in Dictionary values

## Important Rules

1. **Data source**: All examples use data from E000_BulkInsert (100 employees).  
   Run E000 first, then other examples query existing data.

2. **No test copying**: Do NOT copy tests from redb.ConsoleTest 1:1.  
   Rewrite them as clean, short examples for website display.

3. **Source for ideas**: Look at `redb.ConsoleTest/TestStages/` for what to cover:
   - Stage13_LinqQueries.cs - basic queries
   - Stage16_AdvancedLinq.cs - complex queries
   - Stage17_AdvancedLinqOperators.cs - operators
   - Stage33_TreeLinqQueries.cs - tree operations
   - Stage42_BulkInsertPerformanceTest.cs - bulk ops
   - Stage63_ProExpressionsTest.cs - Pro features
   - Stage65_ArrayExpressionsTest.cs - array queries

4. **Keep it short**: Examples should be 20-40 lines, not 200.

5. **Meaningful output**: Return 1-2 lines describing what happened:
   ```csharp
   [$"Filter: Salary > 75000", $"Found: {result.Count} employees"]
   ```

6. **XML Summary**: Add /// summary for parser to extract description.

## Running Examples

```bash
# Run all examples
dotnet run --project redb.Examples

# Run some of them by id
dotnet run --project redb.Examples -- E002 E010 E070

# Print the saved results again without running anything
dotnet run --project redb.Examples -- --results
```

The database is chosen in `Program.cs` (`ConfigureServices`): one of the `.UsePostgres(...)`, `.UseMsSql(...)`,
`.UseSqlite(...)` lines is active, and `AddRedb` / `AddRedbPro` selects the edition. The project starts as Pro; for
the Free edition replace `AddRedbPro` with `AddRedb` and set `PropsSaveStrategy` to `DeleteInsert`.

Every run stores its results in `redb_examples_results.db`, a SQLite file next to the project, and prints the table
from it: one row per example, one line per pass with the time and the number of SQL statements, then the average
and the median time per pass as bars. The passes are named by a key: the database (`postgres`, `mssql`, `sqlite`),
`.pro` for the Pro edition, `.ef` for EF Core, `.repeat` for the second pass and `.cache` for redb with its props
cache. Results of other databases and earlier runs stay in the file, so they sit side by side.

## Comparing with EF Core

Each example has an EF Core twin in `ExamplesEf/` (`E010_WhereSimple_ef.cs` next to `E010_WhereSimple.cs`). A twin
does the same task with the standard EF Core API over ordinary relational tables: the same classes unfolded by
`redb flat`, 54 tables for all models, 41 of them for `EmployeeProps`. The twins run on PostgreSQL only, and only
when redb itself runs as Pro on PostgreSQL, so that both sides work on the same server. Where EF Core has no
built-in way (window functions, recursive tree queries with a depth), the twin reports `N/A`.

To repeat the comparison:

```bash
# 1. Two empty databases on your PostgreSQL server: one for redb, one for the flat tables of EF Core
createdb redb_examples
createdb redb_examples_flat_ef

# 2. In Program.cs: AddRedbPro with the .UsePostgres(...) line pointing at redb_examples,
#    and FlatEfConnection pointing at redb_examples_flat_ef. Then build.
dotnet build redb.Examples
dotnet build redb.CLI

# 3. The flat schema for EF Core: the CLI prints it from the compiled models, psql runs it
dotnet run --project redb.CLI --no-build -- flat \
  -a redb.Examples/bin/Debug/net10.0/redb.Examples.dll -tp postgres --ddl-only > flat.sql
psql -d redb_examples_flat_ef -v ON_ERROR_STOP=1 -1 -f flat.sql

# 4. Seed both sides with the same employees (100 000 by default, a reporting tree)
dotnet run --project redb.Examples --no-build -- E203
dotnet run --project redb.Examples --no-build -- E203_ef

# 5. Run everything: redb, EF Core, the repeats, then the table and the bars
dotnet run --project redb.Examples --no-build
```

Notes:

- Step 3 comes before any run: as soon as a twin is selected, the program opens the flat database.
- In Windows PowerShell 5 the `>` redirect writes UTF-16, which psql does not read: use
  `| Out-File -Encoding ascii flat.sql` there. PowerShell 7 and bash need nothing.
- A seed appends and never clears. `REDB_SEED_COUNT` sets the size (`REDB_SEED_COUNT=2000` for a quick trial) and
  `REDB_SEED_BATCH` the batch, 1000 by default. With `REDB_SEED_COUNT` set, `E203` seeds its twin as well, and a
  run of everything seeds again, so unset it after the seed.
- The run of everything does not seed and keeps the stored seed times.
- `Ef/developer_indexes.sql` holds the indexes a developer would add for the twins' queries; `E203_ef` drops them
  before the load and creates them again after it, inside its time.
- The time of an example is one measured run. Initialization on both sides is outside it.

## Import to Website

Examples are parsed by `redb.Doc.Import`:

```bash
# Import examples to database
dotnet run --project redb.Doc.Import -- --examples

# Import everything (docs + examples)
dotnet run --project redb.Doc.Import
```

Parser extracts:
- Metadata from `[ExampleMeta]` attribute
- Description from `/// <summary>` XML comment
- Code from `RunAsync` method body

## Template: Free Example

```csharp
using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Filter employees by age using Where clause.
/// </summary>
[ExampleMeta("E021", "Where - Filter by Age", "Query",
    ExampleTier.Free, 1, "Where", "Filter", "Age")]
public class E021_WhereAge : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        var query = redb.Query<EmployeeProps>()
            .Where(e => e.Age >= 30 && e.Age < 40)
            .Take(100);

        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var result = await query.ToListAsync();
        sw.Stop();

        return Ok("E021", "Where - Filter by Age", ExampleTier.Free, sw.ElapsedMilliseconds,
            [$"Filter: Age 30-39", $"Found: {result.Count} employees"]);
    }
}
```

## Template: Pro Example

```csharp
using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Query employees by skill using array Contains.
/// </summary>
[ExampleMeta("E040", "Array Contains - Find by Skill", "Query",
    ExampleTier.Free, 2, "Array", "Contains", "Skills")]
public class E040_ArrayContains : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Pro: Query array property with Contains
        var query = redb.Query<EmployeeProps>()
            .Where(e => e.Skills!.Contains("C#"))
            .Take(100);

        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var result = await query.ToListAsync();
        sw.Stop();

        return Ok("E040", "Array Contains - Find by Skill", ExampleTier.Free, sw.ElapsedMilliseconds,
            [$"Filter: Skills contains 'C#'", $"Found: {result.Count} developers"]);
    }
}
```

## Checklist for New Example

- [ ] ID follows numbering scheme (E0XX)
- [ ] Title is clear and descriptive
- [ ] Category matches content
- [ ] Tier is correct (Free vs Pro)
- [ ] Tags are relevant for search
- [ ] XML summary is present
- [ ] Uses data from E000_BulkInsert
- [ ] Has commented ToSqlStringAsync
- [ ] Output is 1-2 meaningful lines
- [ ] Code is under 40 lines

## Links

- Documentation (EN): [redbase.app](https://redbase.app)
- Documentation (RU): [redb.ru](https://redb.ru)
- API Reference: [redbase-app.github.io/redb](https://redbase-app.github.io/redb/)
- Examples online (EN): [redbase.app/examples](https://redbase.app/examples)
- Examples online (RU): [redb.ru/examples](https://redb.ru/examples)
- GitHub: [github.com/redbase-app/redb](https://github.com/redbase-app/redb)
