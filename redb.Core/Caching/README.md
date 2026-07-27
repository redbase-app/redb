# Система кеширования метаданных REDB

## Обзор

Кеш метаданных минимизирует обращения к БД за схемами, структурами и типами. Работает прозрачно
для пользовательского кода.

> **История.** До версии 3.3.4 этот файл описывал слой интерфейсов
> `ICompositeMetadataCache` / `ISchemeMetadataCache` / `IStructureMetadataCache` /
> `ITypeMetadataCache` / `IStaticMetadataCache`, режим `MetadataCacheType.StaticInRedbObject` и
> методы вида `WarmupCacheAsync` / `InvalidateSchemeCompletely`. Ни один из этих интерфейсов не был
> реализован и ни один не использовался — вся ветка удалена вместе с описанием. Ниже — реальный
> состав слоя.

## Состав

| Класс | Область видимости | Что хранит |
|---|---|---|
| `GlobalMetadataCache` | **по домену кеша** | схемы (`SchemeByName`, `SchemeById`), типы, структуры, проекция `scheme_id → CLR-тип` |
| `GlobalListCache` | по домену кеша | справочники (`_lists`), с TTL |
| `GlobalPropsCache` | по домену кеша | объекты/props, поверх `IRedbObjectCache` |
| `ClrSchemeTypeIndex` | **на процесс** | `имя схемы → CLR-тип` |
| `MemoryRedbObjectCache` | реализация `IRedbObjectCache` | in-memory хранилище объектов для `GlobalPropsCache` |

### Почему два уровня видимости

`scheme_id` — факт конкретной базы: в разных БД у одной и той же схемы разные идентификаторы.
Поэтому всё, что завязано на `scheme_id`, живёт **по домену**: домен задаётся
`RedbServiceConfiguration.GetEffectiveCacheDomain()` и изолирует кеши разных подключений.

Соответствие `имя схемы → CLR-тип` от базы не зависит вообще: имя берётся из `[RedbScheme]` на
типе. Поэтому `ClrSchemeTypeIndex` — статический, один на процесс, общий для всех доменов. Он ещё и
самозалечивающийся: загрузка сборки (в том числе в плагинный `AssemblyLoadContext`) поднимает
счётчик поколений, и индекс перестраивается на следующем обращении.

## Инвалидация

`GlobalMetadataCache` ключуется и по имени, и по id — инвалидировать нужно оба:

```csharp
Cache.InvalidateScheme(scheme.Id);
Cache.InvalidateScheme(scheme.Name);
```

**При переименовании схемы** (`[RedbScheme(Name = "...")]`, см. `docs/SCHEME_EXPLICIT_NAME_PLAN.md`)
ключей три — id, имя, под которым схему нашли, и новое имя; плюс немедленная перерегистрация в
процесс-глобальном индексе, чтобы не ждать следующего скана сборок:

```csharp
Cache.InvalidateScheme(scheme.Id);
Cache.InvalidateScheme(previousName);
Cache.InvalidateScheme(targetName);
ClrSchemeTypeIndex.Register(targetName, type);
```

Проекция `scheme_id → CLR-тип` при переименовании **не трогается**: переименование — это UPDATE
одной строки `_schemes._name`, идентификатор схемы не меняется. Поэтому полиморфная загрузка
переименование переживает без каких-либо действий.

## Статистика и диагностика

```csharp
var stats = redb.Cache.GetStatistics();
var (names, ids) = redb.Cache.GetClrTypeStatistics();
var diagnostics = redb.GetCacheDiagnosticInfo();   // ISchemeCacheProvider
```

- `CacheStatistics` / `PropsCacheStatistics` — счётчики попаданий и промахов.
- `CacheDiagnosticInfo` (+ `CacheHealthStatus`, `MemoryUsageInfo`, `PerformanceInfo`) — то, что
  возвращает `ISchemeCacheProvider.GetCacheDiagnosticInfo()`.

## Важно про сырой SQL

Кеш не видит записей в обход библиотеки. Если тест или миграция правит `_schemes` напрямую —
`Cache.Clear()` обязателен, иначе следующий поиск ответит из кеша схемой, которой уже нет.
