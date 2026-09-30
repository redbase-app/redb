# RedbRazor

A Razor Pages site on RedBase: a product list with search, sorting and paging, and a create / edit
form. SQLite with Pro, so it runs as is.

## Run

```bash
dotnet run
```

Open http://localhost:5080. On the first run the app creates `app.db` next to itself, builds the
RedBase tables and adds a few sample products.

## Where things are

| File | What it does |
|------|--------------|
| `Program.cs` | RedBase registration and settings, startup initialization, sample data |
| `Models/Product.cs` | The Props class: one scheme, typed properties |
| `Pages/Index.cshtml.cs` | Search, sorting and paging, all in the database |
| `Pages/Edit.cshtml.cs` | Create and edit; with change tracking a save writes only what changed |
| `appsettings.json` | `ConnectionStrings:Redb` |

## RedBase settings

`Program.cs` turns on the PVT prefilter and change tracking. The props cache is off; the comment next
to it shows the three lines that turn it on.

## Another database

1. In `RedbRazor.csproj` replace `redb.SQLite.Pro` with `redb.Postgres.Pro` or `redb.MSSql.Pro`.
2. In `Program.cs` replace `using redb.SQLite.Pro.Extensions` and `.UseSqlite(...)` as the comment
   there says.
3. Put the connection string into `appsettings.json`. The database must exist: RedBase creates its
   tables, not the database.

## Deploy

- `Dockerfile`: the site with SQLite on a volume.
  `docker build -t redbrazor . && docker run -p 8080:8080 -v redbrazor-data:/data redbrazor`
- `deploy/docker-compose.postgres.yml`, `deploy/docker-compose.mssql.yml`: the site with a database
  server. Switch the provider first (see above); each file has the commands at the top.
