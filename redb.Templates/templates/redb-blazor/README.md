# RedbBlazor

A Blazor site (Interactive Server) on RedBase: a product list with search, sorting and paging, a
create / edit form, and a category tree. SQLite with Pro, so it runs as is.

## Run

```bash
dotnet run
```

Open http://localhost:5081. On the first run the app creates `app.db` next to itself, builds the
RedBase tables and adds sample products and categories.

## Where things are

| File | What it does |
|------|--------------|
| `Program.cs` | RedBase registration and settings, startup initialization, sample data |
| `Services/RedbWork.cs` | One `IRedbService` per operation (why: see below) |
| `Models/Product.cs`, `Models/Category.cs` | The Props classes |
| `Components/Pages/Products.razor` | Search, sorting and paging, all in the database |
| `Components/Pages/ProductEdit.razor` | Create and edit; with change tracking a save writes only what changed |
| `Components/Pages/Categories.razor` | The tree: roots, subtrees, add a child, delete a subtree |
| `appsettings.json` | `ConnectionStrings:Redb` |

## Why components do not inject IRedbService

In Blazor Server a scoped service lives as long as the user's circuit, and two event handlers of one
page can run at the same time. An `IRedbService` is one connection and does not take parallel calls.
`RedbWork` opens a scope per operation, the same way `IDbContextFactory` is used with EF Core.

## RedBase settings

`Program.cs` turns on the PVT prefilter and change tracking. The props cache is off; the comment next
to it shows the three lines that turn it on.

## Another database

1. In `RedbBlazor.csproj` replace `redb.SQLite.Pro` with `redb.Postgres.Pro` or `redb.MSSql.Pro`.
2. In `Program.cs` replace `using redb.SQLite.Pro.Extensions` and `.UseSqlite(...)` as the comment
   there says.
3. Put the connection string into `appsettings.json`. The database must exist: RedBase creates its
   tables, not the database.

## Deploy

- `Dockerfile`: the site with SQLite on a volume.
  `docker build -t redbblazor . && docker run -p 8080:8080 -v redbblazor-data:/data redbblazor`
- `deploy/docker-compose.postgres.yml`, `deploy/docker-compose.mssql.yml`: the site with a database
  server. Switch the provider first (see above); each file has the commands at the top.
