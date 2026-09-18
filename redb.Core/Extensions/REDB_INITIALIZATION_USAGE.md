# Инициализация redb при старте приложения

`IRedbService.InitializeAsync` готовит процесс к работе с базой: проверяет схему, синхронизирует схемы моделей и
прогревает кэши. Вызывается один раз при старте, до первого обращения к данным.

## Что делает `InitializeAsync`

Шаги по порядку (`RedbServiceBase.InitializeAsync`):

1. **Проверяет, что в базе есть схема redb** (таблица `_schemes`). Если нет, старт останавливается с
   `RedbSchemaMissingException`. Создание схемы включается явно, см. «Схема базы» ниже.
2. **Сверяет версию SQL-модуля** с той, что нужна этой сборке. PostgreSQL и SQL Server при расхождении применяют
   встроенный скрипт обновления, если это разрешено (`AutoApplyDatabaseUpgrades`). SQLite применяет свои обновления
   файла и, на Free, проверяет версию нативного расширения.
3. Настраивает сериализатор на полиморфную десериализацию по схеме.
4. **Синхронизирует схемы** всех классов с атрибутом `[RedbScheme]` из просканированных сборок, по одной. Неверные
   явные имена схем (`[RedbScheme(Name = "...")]`) проверяются заранее и сообщаются все сразу.
5. Синхронизирует служебную схему пользовательских настроек.
6. Инициализирует `RedbObjectFactory` и глобальный провайдер схем для `RedbObject`.
7. Прогревает кэш метаданных, если `WarmupMetadataCacheOnInit = true` (по умолчанию).
8. Создаёт props-кэш, если `EnablePropsCache = true`.
9. Строит реестр типов для полиморфных операций с деревьями.

Перегрузки:

```csharp
Task InitializeAsync(params Assembly[] assemblies);
Task InitializeAsync(bool ensureCreated, params Assembly[] assemblies);
```

`ensureCreated: true` сначала вызывает `EnsureDatabaseAsync()`: создаёт схему в пустой базе, а в существующей только
сверяет и обновляет модуль. Вызов идемпотентен, его можно оставить включённым.

## Регистрация

```csharp
// Free
services.AddRedb(options => options
    .UsePostgres(connectionString)          // или .UseMsSql(...) / .UseSqlite(...)
    .Configure(c =>
    {
        c.EnablePropsCache = true;          // по желанию
    }));

// Pro: то же самое, пакеты redb.*.Pro
services.AddRedbPro(options => options
    .UsePostgres(connectionString)
    .Configure(c => { }));
```

`IRedbService` регистрируется как scoped, один экземпляр держит одно соединение с базой. Сервис можно взять и из корня
контейнера: так делают консоли, код старта и именованные инстансы Tsak, это поддерживается. Ограничение одно, и оно не
зависит от того, откуда взят сервис: два явных вызова (`SaveAsync`, `LoadAsync`, `Query`) одновременно на одном
экземпляре получают отказ «IRedbService used concurrently». Ленивые загрузки через сервис из корня этим не ограничены,
они идут в свежих областях его контейнера. Поэтому там, где работа идёт параллельно (запросы, задания, обработчики
сообщений), каждый поток берёт свой сервис из области.

## Консольное приложение

Хоста нет, поэтому инициализацию вызывает сам `Main`. Область здесь не обязательна, консоль работает в одном потоке;
она показывает форму, которая переносится в многопоточный хост:

```csharp
static async Task Main(string[] args)
{
    var services = new ServiceCollection();
    services.AddLogging(b => b.AddConsole());
    services.AddRedb(options => options.UseSqlite("Data Source=app.db"));

    await using var provider = services.BuildServiceProvider();
    await using var scope = provider.CreateAsyncScope();
    var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();

    await redb.InitializeAsync(ensureCreated: true);

    var id = await redb.SaveAsync(new RedbObject<Product>
    {
        Name = "MacBook Pro 16",
        Props = new Product { Price = 2499.99m }
    });
}

[RedbScheme]
public class Product
{
    public decimal Price { get; set; }
}
```

Полный рабочий пример: шаблон `redb-console` (`dotnet new install redb.Templates`, затем `dotnet new redb`).

## ASP.NET Core и Generic Host

`AddRedb` и `AddRedbPro` регистрируют фоновую службу `RedbInitHostedService`. При старте хоста она берёт сервис из
области и вызывает `InitializeAsync(ensureCreated: EnsureCreated)`, раньше остальных служб redb: фоновое удаление
читает таблицу объектов. Поэтому в хосте есть два способа.

**Только инициализация.** Достаточно включить создание схемы, код в `Program.cs` не нужен:

```csharp
builder.Services.AddRedb(options => options
    .UsePostgres(connectionString)
    .Configure(c => c.EnsureCreated = true));

var app = builder.Build();
app.Run();
```

Без `EnsureCreated = true` служба не создаёт схему, и в пустой базе старт хоста остановится с
`RedbSchemaMissingException`.

**Инициализация, за которой идёт своя подготовка**: начальные данные, схемы классов без атрибута. Явный блок перед
`app.Run()`:

```csharp
var app = builder.Build();

using (var scope = app.Services.CreateScope())
{
    var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
    await redb.InitializeAsync(ensureCreated: true);

    await redb.SyncSchemeAsync<ReportProps>();          // класс без [RedbScheme]
    await scope.ServiceProvider.GetRequiredService<SeedDataService>().SeedAllAsync();
}

app.Run();
```

Фоновая служба при этом тоже отработает при старте хоста. Повторный проход идемпотентен: схема к этому моменту уже
есть.

## Какие сборки сканируются

Без аргументов сканируются сборки, **уже загруженные** в `AssemblyLoadContext.Default` на момент вызова. В
синхронизацию не попадут:

- сборка с моделями, на которую есть ссылка, но ни один её тип ещё не использовался: среда загружает сборки лениво;
- модули и плагины, загруженные в собственный `AssemblyLoadContext`;
- классы без атрибута `[RedbScheme]`.

Для первых двух случаев сборки передаются явно, это заодно ускоряет старт:

```csharp
await redb.InitializeAsync(ensureCreated: true,
    typeof(Company).Assembly,
    typeof(Employee).Assembly);
```

Класс без атрибута синхронизируется отдельным вызовом `redb.SyncSchemeAsync<T>()`.

Фоновая служба хоста вызывает `InitializeAsync` без сборок. Если модели лежат в сборках из списка выше, нужен явный
блок из раздела про хост.

## Схема базы: создание и обновление

| Задача | Как |
|---|---|
| Создать схему из приложения | `InitializeAsync(ensureCreated: true)`, в хосте `EnsureCreated = true` |
| Создать схему из командной строки | `redb init --provider postgres --connection "..."` |
| Отдать скрипт схемы администратору | `redb.GetSchemaScript()` или `redb schema --provider postgres -o schema.sql` |
| Запретить приложению менять схему | `AutoApplyDatabaseUpgrades = false` |
| Отдать обновление администратору | `redb.GetUpgradeScript()` или `redb schema --upgrade --provider postgres -o upgrade.sql` |

`redb` в таблице это инструмент `redb.CLI` (`dotnet tool install -g redb.CLI`). Провайдеры: `postgres`, `mssql`,
`sqlite`.

Для SQLite скрипта обновления нет: файл базы принадлежит процессу и обновляется сам, `AutoApplyDatabaseUpgrades` на
нём не действует. На PostgreSQL и SQL Server `ensureCreated` не создаёт саму базу: база из строки подключения должна
существовать. SQLite создаёт файл сам.

## Ошибки при старте

| Исключение | Причина | Что делать |
|---|---|---|
| `RedbSchemaMissingException` | В базе нет схемы redb, создание не включено | Сначала проверить строку подключения, затем включить `ensureCreated` или выполнить скрипт схемы |
| `RedbSchemaOutdatedException` | Версия SQL-модуля не та, а применять обновление запрещено или роли не хватает прав | Отдать администратору скрипт обновления |
| `RedbSchemeNameException`, для нескольких классов `AggregateException` | Недопустимое явное имя в `[RedbScheme(Name = ...)]` | Исправить имена, в сообщении перечислены все |
| `InvalidOperationException` о нативном расширении SQLite | Рядом с приложением лежит библиотека `redbsqlite` другой версии | Взять библиотеку из пакета; путь к файлу есть в сообщении |

Ошибка синхронизации схемы отдельного класса пишется в лог с именем типа и прерывает старт.

## Устаревшее

- Методы-расширения `RedbServiceInitializationExtensions.InitializeAsync` и `AutoSyncSchemesAsync` помечены
  `[Obsolete]`. Вызывайте `IRedbService.InitializeAsync`: он включает синхронизацию схем.
- Регистрация через `AddDbContext<RedbContext>` и `AddScoped<IRedbService, RedbService>()` из прежней версии этого
  документа не работает. Используйте `AddRedb` или `AddRedbPro`.
