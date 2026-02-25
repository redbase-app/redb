# REDB CLI

Command-line tool for REDB database management: schema creation, data export and import.

## Installation

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
```

| Option | Alias | Required | Description |
|--------|-------|----------|-------------|
| `--provider` | `-p` | Yes | Database provider: `postgres`, `mssql` |
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
| `--provider` | `-p` | Yes | Database provider: `postgres`, `mssql` |
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
| `--provider` | `-p` | Yes | Database provider: `postgres`, `mssql` |
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
| `--provider` | `-p` | Yes | Database provider: `postgres`, `mssql` |
| `--connection` | `-c` | Yes | Connection string |
| `--input` | `-i` | Yes | Input file path (`.redb`) |
| `--clean` | | No | Truncate all REDB tables before import |
| `--batch-size` | | No | Batch size for bulk insert (default: 1000) |
| `--dry-run` | | No | Show what would be done without execution |
| `--verbose` | `-v` | No | Enable verbose output |

## Supported Providers

| Provider | Aliases | Connection String Example |
|----------|---------|--------------------------|
| PostgreSQL | `postgres`, `postgresql`, `pgsql` | `Host=localhost;Port=5432;Database=redb;Username=postgres;Password=pass;Timeout=600` |
| SQL Server | `mssql`, `sqlserver` | `Server=localhost;Database=redb;User Id=sa;Password=pass;TrustServerCertificate=True;Command Timeout=600` |

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

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Error (connection failed, invalid options, SQL error) |

## License

MIT — same as redb.Core.
