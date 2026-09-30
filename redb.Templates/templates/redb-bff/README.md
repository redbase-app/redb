# RedbBff

A Blazor backend-for-frontend on RedBase and redb.Route controllers: a product list with search, sorting
and paging, a create / edit form, and a category tree. SQLite with Pro, so it runs as is.

Two processes:

- **the web server** (Blazor, Interactive Server): users sign in here, the browser holds only a session
  cookie;
- **the backend**: redb.Route controllers over RedBase. Its only caller is the web server, with a service
  key. It is a module that runs in its own host or on a Tsak worker.

## Run

In two terminals, from the project folder:

```bash
dotnet run --project RedbBff.Host     # the backend on http://127.0.0.1:5093
dotnet run --project RedbBff.Web      # the web server on http://localhost:5083
```

Open http://localhost:5083 and sign in as `admin` / `admin`. On the first run the backend creates
`redbbff.db`, builds the RedBase tables and adds sample products and categories.

Or both in containers: `cp deploy/.env.example deploy/.env`, set `BFF_SERVICE_KEY` and
`APP_ADMIN_PASSWORD`, then `docker compose -f deploy/docker-compose.yml up --build` and open
http://localhost:8080.

## Projects

| Project | What it is |
|---------|------------|
| `RedbBff.Models` | What the web server and the backend exchange. No RedBase |
| `RedbBff.Backend` | The module: `InitRoute.main`, the controllers, the redb Props classes |
| `RedbBff.Host` | The backend's own host: redb registration and settings, then `InitRoute.main` |
| `RedbBff.Web` | The web server: Blazor pages, the cookie session, the backend client |

## Where things are

| File | What it does |
|------|--------------|
| `RedbBff.Backend/InitRoute.cs` | One HTTP endpoint with the service-key check, dispatching to every controller |
| `RedbBff.Backend/Controllers/ProductsController.cs` | `[Route]`, `[HttpGet("{id}")]`, `[FromQuery]`, `[FromBody]`; search, sorting and paging in the database |
| `RedbBff.Backend/Controllers/CategoriesController.cs` | The tree: roots, subtrees, add a node, delete a subtree |
| `RedbBff.Backend/Controllers/ApiController.cs` | The redb service of the request, status codes, validation |
| `RedbBff.Backend/ServiceKeyValidator.cs` | Lets in only the web server |
| `RedbBff.Host/appsettings.json` | `ConnectionStrings:Redb` and the service key (the `Override` layer) |
| `RedbBff.Web/Program.cs` | The cookie session, sign-in and sign-out, the backend client |
| `RedbBff.Web/Components/Pages/Login.razor` | A plain form: only a plain request can set the session cookie |
| `RedbBff.Web/appsettings.json` | The backend's address and key, and the users |

## Sign-in

The users sign in to the web server. The sign-in form POSTs to `/auth/login`, which checks the user and
sets the session cookie; every page carries `[Authorize]` (`Components/_Imports.razor`). The users live
in configuration (`Web:Users`), a stand-in that keeps the template small; a real application keeps them
in a store of its own, for example redb.Identity.

The web server calls the backend with the service key as a bearer token. The key never reaches the
browser. The development keys in both `appsettings.json` files are for your machine only: set your own
through the environment everywhere else (`Backend__ServiceKey`,
`Tsak__Contexts__redbbff__Override__Api__ServiceKey`).

## Settings

The backend reads its settings from its route context. Both hosts fill it the same way: the module's
config file first, then `Tsak:Contexts:redbbff:Override`, which wins. So an environment variable works the
same in the own host and on a Tsak worker. The backend listens on 127.0.0.1; in a container it listens on
all interfaces but its port stays inside the compose network.

## RedBase settings

`RedbBff.Host/Program.cs` turns on the PVT prefilter and change tracking. The props cache is off; the
comment next to it shows the three lines that turn it on. On Tsak the same settings are environment
variables of the worker (`Tsak__Redb__...`, see `deploy/docker-compose.tsak.yml`).

## Another database

1. In `RedbBff.Host.csproj` replace `redb.SQLite.Pro` with `redb.Postgres.Pro` or `redb.MSSql.Pro`.
2. In `Program.cs` replace `using redb.SQLite.Pro.Extensions` and `.UseSqlite(...)` as the comment there
   says.
3. Put the connection string into `appsettings.json`. The database must exist: RedBase creates its
   tables, not the database.

## Deploy

All compose files read `deploy/.env` (copy it from `deploy/.env.example`). Run them from the project
folder; each has the commands at the top.

| File | What runs |
|------|-----------|
| `deploy/docker-compose.yml` | The backend in its own host with SQLite, and the web server |
| `deploy/docker-compose.postgres.yml`, `deploy/docker-compose.mssql.yml` | The same with a database server; switch the provider first (see above) |
| `deploy/docker-compose.tsak.yml` | The backend as a module on the Tsak stack image, and the web server. Pack the module first: `pwsh deploy/pack-tpkg.ps1` |

The images: `RedbBff.Host/Dockerfile` (the backend) and `RedbBff.Web/Dockerfile` (the web server), both
built from the project folder.
