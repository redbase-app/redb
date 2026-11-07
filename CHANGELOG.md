# Changelog

All notable changes to RedBase will be documented in this file.
This changelog covers the **NuGet-published packages** only:

| Package | Edition |
|---------|---------|
| `redb.Core` | Free |
| `redb.Postgres` | Free |
| `redb.MSSql` | Free |
| `redb.SQLite` | Free |
| `redb.Export` | Free |
| `redb.Core.Pro` | Pro |
| `redb.Postgres.Pro` | Pro |
| `redb.MSSql.Pro` | Pro |
| `redb.SQLite.Pro` | Pro |
| `redb.Licensing` | Pro |
| `redb.CLI` | Tool |
| `redb.Templates` | Tool |

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Pending changes live in `changelog/unreleased/` until the release; `pwsh scripts/changelog.ps1 -Preview` shows them.

## [4.7.0] — 2026-11-06

### Added

- **`dotnet new redb-route-xmljson`: XML to JSON and back without a class.** A Tsak module whose XML routes turn an
  order into JSON by an XSD (arrays, numbers, booleans) and an HTTP API that turns it back into namespaced XML and
  checks it; the same round trip in C# with `MarshalXmlJson` / `UnmarshalXmlJson`. Packages: `redb.Templates`.

### Changed

- **`redb-route-beans` shows the bean step and a bean call in a condition.** The order route prices a line with
  `<bean ref="#pricing" method="Gross(...)"/>` and marks bulk lines with `<filter expr="bean:pricing.IsBulk(header.qty)">`;
  `PriceCalculator` has the synchronous `IsBulk(int)` the condition calls. Packages: `redb.Templates`.

### Deprecated

- **.NET 8 and .NET 9 targets end with 4.7.** Microsoft's support for both ends on 10 November 2026, so 4.7.0 is the
  last release whose packages target them; from 4.8.0 the packages target .NET 10 and later. Fixes on the 4.7.x line
  are still built for .NET 8, 9 and 10 while those lines can be built without known vulnerabilities. Move your projects
  to .NET 10 before taking 4.8.

## Earlier versions

Earlier releases are archived by line in [changelog/](changelog/): [4.6.x](changelog/CHANGELOG-4.6.x.md), [4.5.x](changelog/CHANGELOG-4.5.x.md), [4.4.x](changelog/CHANGELOG-4.4.x.md), [4.3.x](changelog/CHANGELOG-4.3.x.md), [4.2.x](changelog/CHANGELOG-4.2.x.md), [4.1.x](changelog/CHANGELOG-4.1.x.md), [4.0.x](changelog/CHANGELOG-4.0.x.md), [3.x](changelog/CHANGELOG-3.x.md), [2.x](changelog/CHANGELOG-2.x.md), [1.x](changelog/CHANGELOG-1.x.md).
