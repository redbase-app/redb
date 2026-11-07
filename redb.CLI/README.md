# REDB CLI

Command-line tool for REDB database management: schema creation, data export and import, and
unfolding redb objects into ordinary relational tables.

## Installation

Two ways: a standalone executable that needs no .NET on the machine, or a .NET tool.

### Standalone executable

Every release of [redbase-app/redb](https://github.com/redbase-app/redb/releases) carries the CLI as one
self-contained file for `win-x64`, `linux-x64` and `linux-arm64`, signed with cosign:

```bash
VER=4.4.1
BASE=https://github.com/redbase-app/redb/releases/download/v$VER
curl -LO $BASE/redb-cli-$VER-linux-x64.tar.gz
curl -LO $BASE/redb-cli-$VER-linux-x64.tar.gz.bundle
curl -LO $BASE/cosign.pub

# Check the signature (https://docs.sigstore.dev/cosign/system_config/installation/)
cosign verify-blob --key cosign.pub --bundle redb-cli-$VER-linux-x64.tar.gz.bundle redb-cli-$VER-linux-x64.tar.gz

tar -xzf redb-cli-$VER-linux-x64.tar.gz
sudo install -m 755 redb /usr/local/bin/redb
redb --version
```

On Windows take `redb-cli-<version>-win-x64.zip` and its `.bundle`, check it the same way, unpack it and put the
folder with `redb.exe` on `PATH`. `checksums.txt` of the release lists the SHA-256 of every archive.

The CLI works on the schema and the data directly, so it does not need the native SQLite extension that
applications on `redb.SQLite` load.

### .NET tool

**Requires the .NET 10 runtime.** From 3.7.1 the tool targets `net10.0`; earlier versions targeted
`net8.0` and installed on any .NET 8 or newer runtime. Roll-forward only moves up, never down, so a
machine carrying only .NET 8 or .NET 9 cannot install this version — install .NET 10 there.

Older versions are not a workaround: 3.7.0 and earlier are unlisted and carry a known high-severity
advisory in a transitive dependency ([GHSA-2m69-gcr7-jv3q](https://github.com/advisories/GHSA-2m69-gcr7-jv3q)),
fixed in 3.7.1. Both .NET 8 and .NET 9 reach end of support on 10 November 2026 anyway; .NET 10 is
supported until 14 November 2028.

```bash
# Install as a global .NET tool
dotnet tool install --global redb.CLI

# Or run directly from the project
dotnet run --project redb.CLI -- <command> [options]
```

## Commands

### `init` — Create Database Schema

Creates all REDB tables, sequences, functions and views in an existing empty database.

```bash
redb init -p postgres -c "Host=localhost;Database=mydb;Username=postgres;Password=pass"
redb init -p mssql -c "Server=localhost;Database=mydb;User Id=sa;Password=pass;TrustServerCertificate=True"
redb init -p sqlite -c "Data Source=app.db"
```

| Option | Alias | Required | Description |
|--------|-------|----------|-------------|
| `--provider` | `-p` | Yes | Database provider: `postgres`, `mssql`, `sqlite` |
| `--connection` | `-c` | Yes | ADO.NET connection string |
| `--verbose` | `-v` | No | Show detailed output |

### `schema` — Export Schema Script

Outputs the full SQL initialization script to a file or stdout. Useful for DBA review or CI/CD pipelines.

```bash
# Write to file
redb schema -p postgres -o redb_schema.sql

# Output to stdout
redb schema -p postgres

# Pipe to psql
redb schema -p postgres | psql -d mydb
```

| Option | Alias | Required | Description |
|--------|-------|----------|-------------|
| `--provider` | `-p` | Yes | Database provider: `postgres`, `mssql`, `sqlite` |
| `--output` | `-o` | No | Output file path. If omitted, writes to stdout |

### `export` — Export Data

Exports the entire database (or specific schemes) to a `.redb` file (JSONL format, optionally compressed).

```bash
# Full export with compression
redb export -p postgres -c "Host=localhost;Database=redb;Username=postgres;Password=pass" \
  -o backup.redb --compress --batch-size 100000 -v

# Export specific schemes only
redb export -p postgres -c "..." -o partial.redb --schemes 100,200,300

# Dry run — show what would be exported
redb export -p postgres -c "..." -o test.redb --dry-run
```

| Option | Alias | Required | Description |
|--------|-------|----------|-------------|
| `--provider` | `-p` | Yes | Database provider: `postgres`, `mssql`, `sqlite` |
| `--connection` | `-c` | Yes | Connection string |
| `--output` | `-o` | Yes | Output file path (`.redb`) |
| `--compress` | | No | Compress with ZIP |
| `--schemes` | | No | Export only specified scheme IDs (comma-separated) |
| `--batch-size` | | No | Batch size for read operations (default: 1000) |
| `--dry-run` | | No | Show what would be done without execution |
| `--verbose` | `-v` | No | Enable verbose output |

### `import` — Import Data

Imports data from a `.redb` file into the database.

```bash
# Import with clean (drops existing data first)
redb import -p postgres -c "Host=localhost;Database=redb;Username=postgres;Password=pass" \
  -i backup.redb --clean -v --batch-size 100000

# Dry run
redb import -p mssql -c "..." -i backup.redb --dry-run
```

| Option | Alias | Required | Description |
|--------|-------|----------|-------------|
| `--provider` | `-p` | Yes | Database provider: `postgres`, `mssql`, `sqlite` |
| `--connection` | `-c` | Yes | Connection string |
| `--input` | `-i` | Yes | Input file path (`.redb`) |
| `--clean` | | No | Truncate all REDB tables before import |
| `--batch-size` | | No | Batch size for bulk insert (default: 1000) |
| `--dry-run` | | No | Show what would be done without execution |
| `--verbose` | `-v` | No | Enable verbose output |

### `flat` — Unfold Objects into Relational Tables

Builds ordinary relational tables from your `*Props` classes and copies the data of a redb database
into them. Use it to hand data to reporting and BI tools, to another system, or to read it with plain
SQL or any ORM. The transfer is one-way: the flat tables are a copy, changes made to them do not go
back to redb.

Source and target are two independent connections. They may be the same provider or different ones,
and the target may be the source database itself.

```bash
# Print the DDL of the flat tables. No database is opened.
redb flat -a MyApp/bin/Release/net10.0/MyApp.Models.dll -tp postgres --ddl-only

# Unfold and copy, PostgreSQL -> PostgreSQL
redb flat -a MyApp/bin/Release/net10.0/MyApp.Models.dll \
  -sp postgres -sc "Host=localhost;Database=redb;Username=postgres;Password=pass" \
  -tp postgres -tc "Host=localhost;Database=redb_flat;Username=postgres;Password=pass" \
  --batch-size 10000 -v

# Props classes spread over several projects, target on another provider
redb flat -a App.Orders.dll App.Catalog.dll --types OrderProps,ProductProps \
  -sp postgres -sc "..." -tp mssql -tc "..."
```

| Option | Alias | Required | Description |
|--------|-------|----------|-------------|
| `--assembly` | `-a` | Yes | Path to an assembly that declares Props classes. Several paths may be given, or the option repeated |
| `--target-provider` | `-tp` | Yes | Provider of the target database: `postgres`, `mssql`, `sqlite` |
| `--target-connection` | `-tc` | Unless `--ddl-only` | Connection string of the database that receives the flat tables |
| `--source-provider` | `-sp` | Unless `--ddl-only` | Provider of the redb database to read |
| `--source-connection` | `-sc` | Unless `--ddl-only` | Connection string of the redb database to read |
| `--types` | | No | Props classes to unfold, by name or full name (comma-separated). Default: every class marked with `[RedbScheme]` in the given assemblies |
| `--ddl-only` | | No | Print the DDL for the target provider and exit |
| `--batch-size` | | No | Rows buffered per table before a bulk insert (default: 1000) |
| `--verbose` | `-v` | No | Enable verbose output |

#### Before you run it

- **The target database must exist and must not contain the flat tables.** The tool creates tables,
  it does not create the database, and it never uses `IF NOT EXISTS`: a name that is already taken
  stops the run.
- **Point `-a` at the build output of your application** (`bin/...`), where all its assemblies lie
  together. The dependencies of each assembly are resolved from the folder it lies in.
- **A class without `[RedbScheme]` has to be named in `--types`.**
- **A type referenced through `RedbObject<T>` must be part of the run.** The reference becomes a
  foreign key to the table of `T`; if `T` is not exported the tool refuses before writing anything.
- **The scheme must match the class.** A property with no structure in the source database stops the
  run: synchronise the scheme with the current version of the class first.
- Everything is written in one transaction of the target. After an error the target is left as it was.

#### What you get

| In your Props | In the target database |
|---|---|
| Every object of the source | A row in `objects`: id, parent, owner, dates, name, note and the `_value_*` columns |
| A Props class | A table named after the class (`EmployeeProps`), one row per object, keyed by the object id |
| A scalar property | A column named after the property |
| A nested class | A table named by the property path (`EmployeeProps.HomeAddress`), one-to-one with its owner |
| An array or list | A table (`EmployeeProps.Skills`) with `_idx` and either `_value` or the columns of the element class |
| A dictionary | A table with `_key` and either `_value` or the columns of the value class |
| A dictionary with a tuple key | The same, with one column per component: `_key_Item1`, `_key_Item2`, ... |
| `RedbObject<T>` | A column holding the id of the target object, with a foreign key to the table of `T` |
| `RedbListItem` | A column holding the list item id; the lists themselves are copied to `lists` and `list_items` |
| A tree | `objects._id_parent`, a foreign key to `objects`; parents and children of different schemes are supported |

Rules that apply everywhere:

- Names are taken from the code as they are and are always quoted, so in PostgreSQL they are
  case-sensitive: `SELECT "FirstName" FROM "EmployeeProps"`.
- Service columns start with an underscore: `_id`, `_owner_id`, `_idx`, `_key`, `_value`.
- A name longer than 63 bytes is cut and gets `~` plus eight hex digits of a hash, the same on every
  provider.
- Ids are kept: a row in a scheme table has the id of its object.
- Hashes and unique-key columns of redb are not copied.
- Users, roles and permissions are not copied.

#### Limitations

- An empty collection and a missing one look the same in the flat tables: neither has rows.
- SQL Server: a `TimeSpan` is stored as `time` and cannot hold 24 hours or more; a text dictionary
  key is limited to 430 characters.
- SQL Server as a source is read under `SNAPSHOT` isolation, which has to be allowed in that database.
- SQLite as both source and target in one file needs the file in WAL mode.
- Soft-deleted objects are present in `objects`. A `RedbObject<T>` reference that still points to one
  cannot get its foreign key, and the run is rolled back.

## Supported Providers

| Provider | Aliases | Connection String Example |
|----------|---------|--------------------------|
| PostgreSQL | `postgres`, `postgresql`, `pgsql` | `Host=localhost;Port=5432;Database=redb;Username=postgres;Password=pass;Timeout=600` |
| SQL Server | `mssql`, `sqlserver` | `Server=localhost;Database=redb;User Id=sa;Password=pass;TrustServerCertificate=True;Command Timeout=600` |
| SQLite | `sqlite` | `Data Source=app.db` |

## Typical Workflows

### New Project Setup

```bash
# 1. Create database (psql / SSMS)
createdb myapp

# 2. Initialize REDB schema
redb init -p postgres -c "Host=localhost;Database=myapp;Username=postgres;Password=pass" -v

# 3. Run your application
dotnet run --project MyApp
```

### Database Migration (Postgres → MSSQL)

```bash
# 1. Export from Postgres
redb export -p postgres -c "Host=src;Database=redb;..." -o data.redb --compress -v

# 2. Init schema in MSSQL
redb init -p mssql -c "Server=dst;Database=redb;..." -v

# 3. Import into MSSQL
redb import -p mssql -c "Server=dst;Database=redb;..." -i data.redb --clean -v
```

### Backup & Restore

```bash
# Backup
redb export -p postgres -c "..." -o "backup_$(date +%Y%m%d).redb" --compress

# Restore (clean + import)
redb import -p postgres -c "..." -i backup_20260213.redb --clean -v
```

### Flat Tables for Reporting

```bash
# 1. Create an empty target database
createdb myapp_flat

# 2. Review the tables that will be created
redb flat -a MyApp/bin/Release/net10.0/MyApp.Models.dll -tp postgres --ddl-only

# 3. Unfold and copy
redb flat -a MyApp/bin/Release/net10.0/MyApp.Models.dll \
  -sp postgres -sc "Host=localhost;Database=myapp;..." \
  -tp postgres -tc "Host=localhost;Database=myapp_flat;..." -v
```

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Error (connection failed, invalid options, SQL error) |

## License

Apache License 2.0 — same as redb.Core.

## Links

- Documentation (EN): [redbase.app](https://redbase.app)
- Documentation (RU): [redb.ru](https://redb.ru)
- API Reference: [redbase-app.github.io/redb](https://redbase-app.github.io/redb/)
- NuGet: [nuget.org/packages/redb.CLI](https://www.nuget.org/packages/redb.CLI)
- GitHub: [github.com/redbase-app/redb](https://github.com/redbase-app/redb)
