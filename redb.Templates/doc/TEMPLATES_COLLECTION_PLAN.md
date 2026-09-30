# Коллекция шаблонов redb.Templates: план

Внутренний документ, на русском. Всё, что видит пользователь (README пакета, README внутри каждого
шаблона, комментарии в коде), пишется на английском.

Статус (2026-09-30): написаны `redb` (префильтр + ChangeTracking), `redb-razor`, `redb-blazor`, `redb-worker`,
`redb-chat`, `redb-app`, `redb-bff`, смоук `scripts/smoke-templates.ps1` (кейсы по опциям), README пакета. 2026-10-01: смоук на локальных 4.2.0 (nupkg/) — все 12 кейсов собираются, `redb` (Pro и Free) запускается; веб/воркер/чат, docker и путь Tsak запуском не проверены. Identity — отдельной волной позже. Пины `redb-console` подняты на 4.2.0 при бампе; в новых шаблонах версия в одном свойстве `RedbVersion` = 4.2.0 (у многопроектных в
`Directory.Build.props`) — добавить их в шаг 2 чек-листа.

Решения по ходу (2026-09-30):

- Шаблоны на маршрутах (`redb-worker`, `redb-chat`, API `redb-app`, бэкенд `redb-bff`) устроены как
  SerialNumbersDemo: проект-модуль с `InitRoute.main`, `*.config.json`, `manifest.json` (модуль Tsak,
  `deploy/pack-tpkg.ps1`, `deploy/docker-compose.tsak.yml`) и проект-хост, который строит `RouteContext` и
  вызывает тот же `main`. Настройки модуля — свойства контекста; слои: config.json модуля, затем
  `Tsak:Contexts:{ctx}:Override` (одни и те же переменные окружения в обоих хостах).
- `redb-chat`: `--front` не нужен, консоль и HTTP всегда оба; варианты `--tools none|shell|mcp` и `--audit`.
  История в redb (`RedbConversationStore`). HTTP по умолчанию на 127.0.0.1. Инструмент shell — только
  одиночные read-only программы, без cmd/sh. MCP — filesystem-сервер через npx (нужен Node.js).
- `redb-blazor`: компоненты не берут `IRedbService` напрямую, а открывают его на операцию через `RedbWork`
  (как `IDbContextFactory` в EF Core): scoped-сервис живёт весь circuit.
- `redb-app`: API на REST DSL; `POST /api/auth/login` выдаёт JWT, остальное `InboundAuth=Bearer` +
  `IHttpTokenValidator` из реестра; CORS через `RestOptions.ExtraConsumerOptions`, preflight отвечает
  middleware до проверки токена. Пользователи из конфига — заглушка до redb.Identity.

## 1. Цель

Превратить пакет `redb.Templates` из одного консольного шаблона в набор простых шаблонов. Каждый
разворачивается одной командой `dotnet new <имя>` и запускается `dotnet run` без правок. Код короткий,
комментарии на английском объясняют, что происходит и где это менять.

Требования ко всем шаблонам:

- база везде, где нужна, одна: **SQLite Pro** (`redb.SQLite.Pro`, файл рядом с приложением), без
  параметров `--db`/`--pro` в новых шаблонах; запуск без стенда;
- конфигурация redb одинаковая во всех шаблонах (см. §2.1);
- никаких заглушек, секретов в коде и «TODO: доделать»: шаблон запускается и работает;
- всё, что пользователь обязан вписать сам (пароль БД, ключ LLM), лежит в `appsettings.json` с
  комментарием в коде и строкой в README шаблона;
- версии пакетов одинаковые во всех шаблонах (**4.2.0**: владелец 2026-09-30 бампает всё на 4.2.0, выпуск ещё не готов; шаблоны пишем под 4.2.0).

## 2. Решения владельца (2026-09-23)

| Вопрос | Решение |
|---|---|
| Вход в систему в шаблонах 4 и 5 | простой, пользователи из конфига: `redb-app` JWT (как в tsum), `redb-bff` cookie (как Tsak.Web); redb.Identity последним, отдельной волной |
| Бэкенд API | REST DSL и контроллеры redb.Route (`redb.Route.Http`, `redb.Route.Controllers`); ASP.NET-контроллеры и Minimal API не используем |
| Режим Blazor в шаблоне 4 | WASM, как в tsum (сначала выбирали Auto; 2026-09-24 заменено: Auto требует серверного хоста и сливает `redb-app` с `redb-bff`) |
| Ключ LLM в шаблоне 7 | пустое поле в `appsettings.json`, в коде комментарий, куда вписать; README шаблона об этом же |
| Шаблон 5 | BFF без Tsak; устройство смотрим у Tsak (Tsak.Web + контроллеры в Tsak.Core) |
| Короткие имена | `redb`, `redb-razor`, `redb-blazor`, `redb-app`, `redb-bff`, `redb-worker`, `redb-chat` |

### 2.1 Конфигурация redb во всех шаблонах

Решение владельца 2026-09-23: SQLite Pro, префильтр и ChangeTracking включены, кэш Props выключен, но в
комментарии написано, как его включить.

```csharp
services.AddRedbPro(options => options
    .UseSqlite("Data Source=app.db")
    .Configure(c =>
    {
        c.EnablePvtPrefilter = true;                              // narrows the object set before the pivot step
        c.PropsSaveStrategy = PropsSaveStrategy.ChangeTracking;   // saves only what changed
        // Props cache (off by default). To turn it on:
        // c.EnablePropsCache = true;
        // c.PropsCacheMaxSize = 10_000;                       // entries per process
        // c.PropsCacheTtl = TimeSpan.FromMinutes(60);
    }));
```

У консольного `redb` параметры `--db`/`--pro` остаются, в ветке Pro эти две строки тоже включаются
(сейчас они там закомментированы).

Уточнение про хостинг Blazor/Razor: страницы Razor и Blazor хостятся только на ASP.NET Core
(`Microsoft.NET.Sdk.Web`), так устроен и Tsak.Web. Правило «ASP.NET не используем» относится к API:
данные и действия идут через маршруты redb.Route, а не через `ControllerBase`/`MapGet`.

## 3. Материал для образца

Материал только для просмотра. Код не копируем, берём устройство.

| Шаблон | Что смотреть |
|---|---|
| `redb-app` | `C:\Work\ews\tsum_web` (слои Models/Domain/Api/Web, REST DSL, страницы списков), `C:\Work\ews\honest-service` |
| `redb-bff` | `redb.Tsak/src/redb.Tsak.Web` (cookie-сессия, BFF), `redb.Tsak/src/redb.Tsak.Core/Controllers` (`RedbController`), `redb.Route/src/redb.Route.Controllers/README.md` |
| `redb-worker` | `redb.Route/demos/SerialNumbersDemo` |
| `redb-chat` | `redb.Route/demos/Llm.HttpShell`, `Llm.McpShell`, `Llm.AuditShell` |
| все | существующий `templates/redb-console`: условия `#if` по провайдеру, комментарии к строкам подключения |

EWS (tsum, honest) в публичных материалах не называем: ни в README, ни в комментариях, ни в статьях.

## 4. Шаблоны

### 4.1 `redb`: консоль (есть)

Остаётся как есть. Папку переименовать не обязательно, `shortName` уже `redb`. Правки: общий README
пакета со списком всех шаблонов.

Оценка: 0.5 сессии.

### 4.2 `redb-razor`: сайт на Razor Pages

Один проект. Схема `Product` (или `Note`), страницы список / создание / правка / удаление, фильтр и
сортировка на стороне сервера (LINQ redb, без выборки в память), постраничный вывод.

Оценка: ~0.5k строк, 1 сессия.

### 4.3 `redb-blazor`: сайт на Blazor (Interactive Server)

Один проект. То же, что `redb-razor`, плюс дерево (иерархия redb) как второй пример. Сервисный слой
обращается к `IRedbService` напрямую, без API.

Оценка: ~0.6k строк, 1–1.5 сессии.

### 4.4 `redb-app`: приложение Blazor WASM + API на redb.Route

Проекты:

- `*.Models`: `*Props`-классы и DTO, общие для сервера и клиента;
- `*.Api`: маршруты redb.Route, REST DSL (`redb.Route.Http`), доступ к redb;
- `*.Web`: Blazor WASM (`WebAssembly.DevServer` для `dotnet run`), ходит в API по HTTP; в деплое статика за nginx с прокси `/api/`.

Функции: вход (как в tsum: маршрут логина выдаёт JWT, API проверяет его через `inboundAuth=bearer` у `redb.Route.Http`; пользователи из конфига), список с фильтром и страницами, карточка с правкой,
дерево. Один пример «действия» (смена статуса) через `direct:`-маршрут.

Два процесса, как в образце (решение владельца 2026-09-23). В tsum: `tsum.Api` — маршруты с
`InitRoute.cs` (модуль Tsak), слушает `:5090`; `tsum.Web` — WASM, адрес API в
`wwwroot/appsettings.Development.json` (`ApiBaseUrl`), в контейнере nginx отдаёт статику и проксирует
`/api/` на воркер. В шаблоне: `ApiBaseUrl` в конфиге Web, README с двумя `dotnet run` (или одним
`docker compose up`).

Оценка: 1.5–2.5k строк, 2–3 сессии.

### 4.5 `redb-bff`: BFF на Blazor + контроллеры redb.Route

Проекты:

- `*.Backend`: хост redb.Route, контроллеры `RedbController` за `redb.Route.Http`, redb;
- `*.Web`: Blazor (Server), cookie-сессия; браузер держит только cookie, сервер ходит в Backend
  (как Tsak.Web к воркеру).

Отличие от `redb-app`: API оформлен контроллерами (`[Route]`, `[HttpGet]`, привязка параметров),
а не маршрутами REST DSL, и фронт работает как BFF.

Оценка: 1.5–2.5k строк, 2–3 сессии.

### 4.6 `redb-worker`: интеграционный воркер

По мотивам SerialNumbersDemo, урезано до того, что работает без стенда:

папка `inbox` → архив исходного файла → XSD-валидация + `Unmarshal` → `.Transacted()` (redb +
дубликаты по уникальному ключу) → ответ в `outbox`; ошибки в `error`; `OnException` отдельным
построителем маршрутов.

SFTP, AS2, SQL Server в шаблон не входят; в README шаблона одна строка на каждый: какой пакет
подключить и какую строку `From(...)` поменять.

Оценка: ~0.8k строк, 1.5–2 сессии.

### 4.7 `redb-chat`: простой чат на redb.Route.Llm

Один шаблон с параметром `--front console|http|mcp` (вместо трёх почти одинаковых шаблонов) и флагом
`--audit` (журнал вызовов в redb, как в AuditShell).

Ключ: `appsettings.json`, секция `Llm` с `Provider`, `ModelId`, `ApiKey` (пусто). По умолчанию
`anthropic` (как в демо); в комментарии рядом написано, как перейти на `deepseek`: поменять `Provider` и
`ModelId`, вписать ключ DeepSeek. При пустом ключе
приложение при старте пишет понятную ошибку: какой файл открыть и какое поле заполнить. Переменная
окружения тоже работает (стандартная конфигурация .NET), в README это одна строка.

Оценка: ~0.5k строк, 1.5–2 сессии.

## 5. Общая обвязка

- README пакета: таблица шаблонов, параметры, по одному примеру команды на шаблон.
- README в каждом шаблоне (английский): запуск, что куда вписать, как сменить провайдер.
- Смоук-скрипт `scripts/smoke-templates.ps1`: для каждого шаблона `dotnet new <имя>` во
  временную папку → `dotnet build`; для консоли и воркера ещё короткий `dotnet run`. Запускает
  владелец перед выпуском.

Оценка: 1 сессия.

## 5.1 Развёртывание

Решение владельца 2026-09-23. Запуск из коробки идёт на SQLite. Файлы развёртывания показывают, как
приложение выходит в работу; для запуска они не нужны.

| Шаблон | Что кладём |
|---|---|
| `redb-razor`, `redb-blazor` | `Dockerfile` |
| `redb-app`, `redb-bff` | `Dockerfile` сайта; для части на маршрутах — два пути (см. ниже) |
| `redb-worker`, `redb-chat` | два пути: свой хост (`Dockerfile`) или модуль в Tsak |
| все, где есть база | `deploy/docker-compose.postgres.yml` и `deploy/docker-compose.mssql.yml`: приложение + сервер БД |

Два пути для кода на маршрутах:

- **свой хост**: приложение само поднимает `RouteContext`, свой `Dockerfile`;
- **модуль Tsak**: те же маршруты в `InitRoute`, упаковка и выкладка в Tsak; README шаблона ссылается на
  `tsak-worker` и документацию Tsak, код Tsak в шаблон не копируется.

Переход с SQLite на серверную базу для compose: в `Program.cs` рядом с `.UseSqlite(...)` закомментированы
`.UsePostgres(...)`/`.UseMsSql(...)`, в `.csproj` комментарий, какой пакет поставить вместо
`redb.SQLite.Pro`; строка подключения берётся из конфигурации (переменная окружения в compose).
Префильтр и ChangeTracking остаются включёнными на любом провайдере.

## 6. Порядок и объём

| Шаг | Шаблон | Сессии |
|---|---|---|
| 1 | обвязка + README пакета + `redb` | 1–1.5 |
| 2 | `redb-razor` | 1 |
| 3 | `redb-blazor` | 1–1.5 |
| 4 | `redb-chat` | 1.5–2 |
| 5 | `redb-worker` | 1.5–2 |
| 6 | `redb-app` | 2–3 |
| 7 | `redb-bff` | 2–3 |
| 8 | вход через redb.Identity (в `redb-app`/`redb-bff`) | позже, отдельный план |

Итого без Identity: 10–14 сессий на сами шаблоны, плюс развёртывание (§5.1): Dockerfile и два compose
на шаблон ~0.3 сессии; путь «модуль Tsak» для worker/chat/бэкендов app и bff ~0.5–1 сессия на шаблон
(маршруты в отдельном проекте, свой хост и `InitRoute` Tsak поверх него, проверка упаковки). Всего
14–20 сессий.

Задача непростая (владелец, 2026-09-23): каждый шаблон проверяется запуском из коробки и каждым путём
развёртывания, а не только сборкой.

## 7. Поддержка

Каждый выпуск: поднять версии пакетов во всех шаблонах и прогнать смоук. Добавить шаг в флоу выпуска
(`RELEASE_ARTIFACTS.md`), когда шаблонов станет больше одного.

## 8. Открытые вопросы

Открытых вопросов нет (2026-09-24).

## 9. Что проверить первым прогоном (2026-09-30)

Код написан без сборки. Места, где API сверено по исходникам, но поведение не проверено запуском:

- `redb-chat --tools mcp`: запуск `npx` через `cmd /c` на Windows, `McpTransport.Stdio` из модуля.
- `redb-bff`: привязка `[FromQuery]` при отсутствующем параметре (ожидаю null), `[FromRoute("id")] long`,
  статус через `Exchange.Out.Headers[redbHttp.ResponseCode]` из контроллера.
- `redb-bff` Web: вход статическим SSR (`[ExcludeFromInteractiveRouting]` + `AcceptsInteractiveRouting()`),
  POST `/auth/login` и `/auth/logout` — эндпоинты веб-хоста для cookie, как в Tsak.Web; данные идут только
  через бэкенд.
- `redb-app`: preflight CORS при `InboundAuth=Bearer` (middleware отвечает до проверки токена — по коду
  `SharedHttpServerManager`), JSON-привязка camelCase.
- Модули на Tsak: `Microsoft.IdentityModel.JsonWebTokens` (`redb-app`) берётся из worker-а, в `.tpkg` не кладётся.
- Все шаблоны: исключения `template.json` на уровне source (свой список заменяет стандартный — стандартные
  пути перечислены явно), замена `sourceName` в нижнем регистре (`redbworker`, `redbchat`, ...) в именах
  контекстов и переменных окружения.
