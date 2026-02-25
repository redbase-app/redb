# Changelog

All notable changes to RedBase will be documented in this file.
This changelog covers the **NuGet-published packages** only:

| Package | Edition |
|---------|---------|
| `RedBase.Core` | Free |
| `RedBase.Postgres` | Free |
| `RedBase.MSSql` | Free |
| `RedBase.Export` | Free |
| `RedBase.Core.Pro` | Pro |
| `RedBase.Postgres.Pro` | Pro |
| `RedBase.MSSql.Pro` | Pro |
| `RedBase.CLI` | Tool |

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.2.15] — 2026-02-18

### Fixed
- **Props cache: objects without hash skipped during Query** — when `EnablePropsCache = true`, `LoadPropsForManyAsync` filtered objects by `hash.HasValue` for cache lookup but never added hashless objects back to `needToLoad`. Result: their Props were never materialized, returning empty/null. Fix: hashless objects are now always added to `needToLoad` set.
- **Missing `_hash` column in nested object materialization SQL** — `Materialization_SelectObjectsByIds` (MSSQL and Postgres) did not include `_hash` in the SELECT, so nested `RedbObject<T>` loaded during STEP 5-6 always had `hash = null`. When the parent was subsequently saved, nested objects were written back with `_hash = NULL`, corrupting previously valid hashes. Fix: added `_hash AS Hash` to both dialect queries.
- **String-based ArrayIndex sorting in ChangeTracking and Materialization** — `.OrderBy(v => v.ArrayIndex)` used lexicographic comparison where `"10" < "2"`, breaking element order for arrays with 10+ items. This caused false UPDATE operations on nested object values (e.g. Plan features) during `SaveAsync` with ChangeTracking. Fix: use `int.TryParse` numeric sorting for Array collections in `ProPropsMaterializer` (3 places) and `ValueTreeBuilder` (2 places). Dictionary collections retain string key sorting.
- **Duplicate RedbObject reference in SaveAsync** — `CollectNestedRedbObjectsFromProperties` did not use the `processed` HashSet to deduplicate objects, causing `ArgumentException: An item with the same key has already been added` in ChangeTracking `ToDictionary` when multiple properties (e.g. two `LicenseInfo` entries) referenced the same `RedbObject` (same ID). Fix: check `processed.Add(redbObj.Id)` before `collector.Add(redbObj)` for all collection paths (single, array, dictionary). Also made `ToDictionary` in `PrepareValuesWithTreeChangeTrackingImpl` defensive via `GroupBy` to prevent crashes from any upstream duplicates.
- **MERGE duplicate row error in ChangeTracking updates** — `BulkUpdateValuesAsync` could receive values with duplicate `_id` from the ChangeTracking diff pipeline, causing `SqlException: The MERGE statement attempted to UPDATE or DELETE the same row more than once`. Fix: added `DeduplicateValueUpdates` guard before all `BulkUpdateValuesAsync` calls in `CommitAllChangesBatch`, `SaveBatchWithChangeTrackingStrategy`, and `ApplyTreeChanges` (array hash updates). Duplicates are logged as warnings for diagnostics.
- **Missing `ArrayParentId` for RedbObject references inside business class array elements** — `ProcessSingleIRedbObject` did not set `ArrayParentId` when saving an `IRedbObject` reference field (e.g. `LicenseInfo.Plan`) nested inside a business class array element. All such values had `_array_parent_id=NULL`, making them indistinguishable in the value tree when the same structure appeared in multiple array elements. This caused ChangeTracking tree comparison to produce duplicate updates for the same DB row. Fix: `ProcessIRedbObjectField` now receives and passes `parentValueId` to `ProcessSingleIRedbObject` and `ProcessIRedbObjectArray`.

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
