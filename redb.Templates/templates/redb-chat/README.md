# RedbChat

A chat with an LLM on redb.Route.Llm. The conversation history is kept in RedBase (SQLite with Pro), so
a chat goes on after a restart. You talk to it in the console or over HTTP.

The same module runs in two places: in its own console host, and on a Tsak worker.

## Run

1. Put your API key into `RedbChat.Host/appsettings.json`, field `ApiKey`. The app stops at startup with
   a message while it is empty. Instead of the file you can set the environment variable
   `Tsak__Contexts__redbchat__Override__Llm__ApiKey`.
2. Start it:

   ```bash
   dotnet run --project RedbChat.Host
   ```

3. Type a message. `/new` starts a new conversation. Over HTTP:

   ```bash
   curl -d "hello" -H "X-Chat-Id: my-chat" http://localhost:5090/api/chat
   ```

   `X-Chat-Id` picks the conversation: the same id continues the same history.

The model is Anthropic Claude Haiku. For DeepSeek, uncomment the two lines under `ApiKey` in
`RedbChat.Host/appsettings.json` (`Provider: deepseek`, `ModelId: deepseek-chat`) and put a DeepSeek key.
Other providers of redb.Route.Llm work the same way.

## Options

| Option | Values | Default | What it adds |
|--------|--------|---------|--------------|
| `--tools` | `none`, `shell`, `mcp` | `none` | `shell`: a route tool that runs a few read-only system commands. `mcp`: the tools of the MCP filesystem server, pinned to `data/files` (needs Node.js) |
| `--audit` | `true`, `false` | `false` | The user and audit tags on every stored message, and an `/audit` console command that reads them back with a LINQ query |

```bash
dotnet new redb-chat -n MyChat --tools shell --audit true
```

## Where things are

| File | What it does |
|------|--------------|
| `RedbChat.Module/InitRoute.cs` | The module's entry point: components, the model, the history store, tools, routes |
| `RedbChat.Module/Routes/ChatRouteBuilder.cs` | `direct:chat` (the chat) and the HTTP endpoint that forwards to it |
| `RedbChat.Module/Tools/` | The tool of `--tools shell` or `--tools mcp` |
| `RedbChat.Module/AuditQueries.cs` | With `--audit`: the query over stored messages |
| `RedbChat.Module/RedbChat.Module.config.json` | Provider, model, system prompt, HTTP host and port |
| `RedbChat.Host/Program.cs` | The own host: redb registration and settings, `InitRoute.main`, the console chat |
| `RedbChat.Host/appsettings.json` | `ConnectionStrings:Redb` and the `Override` layer: the API key |

## Settings

The module reads its settings from its route context. Both hosts fill it the same way: the module's
config file first, then `Tsak:Contexts:redbchat:Override`, which wins. The API key belongs only in the
Override layer, never in the module's config file, which ships inside the package.

The HTTP endpoint listens on 127.0.0.1: it has no authentication and every call costs tokens. The
Dockerfile opens it to the container network (`Http:Host = 0.0.0.0`); put it behind your own gateway
before exposing it further.

## RedBase settings

`RedbChat.Host/Program.cs` turns on the PVT prefilter and change tracking. The props cache is off; the
comment next to it shows the three lines that turn it on. On Tsak the same settings are environment
variables of the worker (`Tsak__Redb__...`, see `deploy/docker-compose.tsak.yml`).

## Another database

1. In `RedbChat.Host.csproj` replace `redb.SQLite.Pro` with `redb.Postgres.Pro` or `redb.MSSql.Pro`.
2. In `Program.cs` replace `using redb.SQLite.Pro.Extensions` and `.UseSqlite(...)` as the comment there
   says.
3. Put the connection string into `appsettings.json`. The database must exist: RedBase creates its
   tables, not the database.

## Deploy

**Own host**

- `Dockerfile`: the host with the history on a volume.
  `docker build -t redbchat .` then
  `docker run -it -p 5090:5090 -v redbchat-data:/data -e Tsak__Contexts__redbchat__Override__Llm__ApiKey=YOUR_KEY redbchat`
- `deploy/docker-compose.postgres.yml`, `deploy/docker-compose.mssql.yml`: the host with a database
  server. Switch the provider first (see above); put `LLM_API_KEY` into `deploy/.env`; each file has the
  commands at the top.

**Tsak**

1. `cp deploy/.env.example deploy/.env` and fill in the Tsak values and `LLM_API_KEY`.
2. `pwsh deploy/pack-tpkg.ps1` packs the module into `deploy/output/RedbChat.tpkg`.
3. `docker compose -f deploy/docker-compose.tsak.yml up -d` starts the Tsak stack with the module.
4. `curl -d "hello" http://localhost:5090/api/chat`; the dashboard is at http://localhost:8085.

With `--tools mcp` in a container, the image must have Node.js for `npx`: the .NET runtime image of the
`Dockerfile` has none, and neither may your Tsak image. Without it the module stops at startup when it
starts the MCP server.
