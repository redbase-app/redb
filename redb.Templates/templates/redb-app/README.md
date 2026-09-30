# RedbApp

A Blazor WebAssembly application with an API on redb.Route and RedBase: sign-in, a product list with
search, sorting and paging, a create / edit form, and a category tree. SQLite with Pro, so it runs as is.

Two processes, as in production: the API, and the client that runs in the browser. The API is a module
that runs in its own host or on a Tsak worker.

## Run

In two terminals, from the project folder:

```bash
dotnet run --project RedbApp.Host     # the API on http://localhost:5092
dotnet run --project RedbApp.Web      # the client on http://localhost:5082
```

Open http://localhost:5082 and sign in as `admin` / `admin`. On the first run the API creates
`redbapp.db`, builds the RedBase tables and adds sample products and categories.

Or both in containers: `cp deploy/.env.example deploy/.env`, set `APP_SIGNING_KEY` and
`APP_ADMIN_PASSWORD`, then `docker compose -f deploy/docker-compose.yml up --build` and open
http://localhost:8080.

## Projects

| Project | What it is |
|---------|------------|
| `RedbApp.Models` | What the API and the browser exchange. No RedBase: the client references it |
| `RedbApp.Api` | The module: `InitRoute.main`, the REST routes, the redb Props classes, the services |
| `RedbApp.Host` | The API's own host: redb registration and settings, then `InitRoute.main` |
| `RedbApp.Web` | The Blazor WebAssembly client |

## Where things are

| File | What it does |
|------|--------------|
| `RedbApp.Api/Routes/AuthRouteBuilder.cs` | `POST /api/auth/login` -> a signed token |
| `RedbApp.Api/Auth/JwtTokens.cs` | Issues the token, and checks it on every other call as the `IHttpTokenValidator` |
| `RedbApp.Api/Routes/ApiRouteBuilder.cs` | The REST declarations: each verb forwards to a `direct:` route |
| `RedbApp.Api/Services/ProductService.cs` | Search, sorting and paging in the database; create, update, delete |
| `RedbApp.Api/Services/CategoryService.cs` | The tree: roots, subtrees, add a node, delete a subtree |
| `RedbApp.Api/RedbApp.Api.config.json` | Host, port, CORS origin, token issuer and lifetime |
| `RedbApp.Host/appsettings.json` | `ConnectionStrings:Redb`, the signing key and the users (the `Override` layer) |
| `RedbApp.Web/Services/ApiClient.cs` | The API calls; the token goes with each one, a 401 ends the session |
| `RedbApp.Web/wwwroot/appsettings.Development.json` | Where the client finds the API in development |

## Sign-in

`POST /api/auth/login` checks the login and password and returns a JWT signed with `Auth:SigningKey`.
Every other route of the API is declared with `InboundAuth = Bearer`: a request without a valid token
gets 401 before the route runs, an accepted one carries the user on the exchange
(`ExchangePrincipal.Get(exchange)`).

The users live in configuration (`Auth:Users`), a stand-in that keeps the template small. A real
application keeps them in a store of its own, for example redb.Identity. The signing key in
`RedbApp.Host/appsettings.json` is for your machine only: set your own through the environment everywhere
else (`Tsak__Contexts__redbapp__Override__Auth__SigningKey`).

## Settings

The module reads its settings from its route context. Both hosts fill it the same way: the module's
config file first, then `Tsak:Contexts:redbapp:Override`, which wins. So an environment variable works
the same in the own host and on a Tsak worker.

In development the client runs on another port, so the API answers CORS for `Api:CorsOrigins`. In a
deployment nginx serves the client and forwards `/api/` to the API (`RedbApp.Web/nginx.conf`): one origin,
no CORS.

## RedBase settings

`RedbApp.Host/Program.cs` turns on the PVT prefilter and change tracking. The props cache is off; the
comment next to it shows the three lines that turn it on. On Tsak the same settings are environment
variables of the worker (`Tsak__Redb__...`, see `deploy/docker-compose.tsak.yml`).

## Another database

1. In `RedbApp.Host.csproj` replace `redb.SQLite.Pro` with `redb.Postgres.Pro` or `redb.MSSql.Pro`.
2. In `Program.cs` replace `using redb.SQLite.Pro.Extensions` and `.UseSqlite(...)` as the comment there
   says.
3. Put the connection string into `appsettings.json`. The database must exist: RedBase creates its
   tables, not the database.

## Deploy

All compose files read `deploy/.env` (copy it from `deploy/.env.example`). Run them from the project
folder; each has the commands at the top.

| File | What runs |
|------|-----------|
| `deploy/docker-compose.yml` | The API in its own host with SQLite, and the client behind nginx |
| `deploy/docker-compose.postgres.yml`, `deploy/docker-compose.mssql.yml` | The same with a database server; switch the provider first (see above) |
| `deploy/docker-compose.tsak.yml` | The API as a module on the Tsak stack image, and the client behind nginx. Pack the module first: `pwsh deploy/pack-tpkg.ps1` |

The images: `RedbApp.Host/Dockerfile` (the API) and `RedbApp.Web/Dockerfile` (the client in nginx), both
built from the project folder.
