# RouteModule

A Tsak module with an HTTP JSON API written in XML markup (`urn:redb:route:1.0`):

| Request | Answer |
|---|---|
| `GET /api/hello/{name}` | `200 {"greeting":"Hello, <name>"}` |
| `POST /api/orders` with a body that matches `resources/order.schema.json` | `202 {"accepted":true}` |
| `POST /api/orders` with any other body | `400 {"error":"..."}`, the reason goes to the log |
| `GET /api/openapi.json` | the OpenAPI document of the API |

The module is a package (`.tpkg`) that a Tsak worker loads from its `modules/` folder; there is no `Main`
to run.

## Layout

| Path | What |
|---|---|
| `routes/api.route.xml` | the `<rest>` block, the POST handler and the error answer |
| `routes/ApiRoute.cs` | the same API in C#, under `/api/cs` |
| `resources/order.schema.json` | the JSON schema the POST body is checked against, by both forms |
| `context.xml` | the C# route builder, and room for beans and the one-time init pipeline |
| `config/routemodule.config.json` | identity inside the package: context name and auto start |
| `config/context.sample.json` | deployment sample of the context configuration |
| `.config/dotnet-tools.json` | the `redb-route-xml` tool, same version as the redb packages |

## Build the package

    dotnet tool restore
    dotnet build -p:PackRouteOnBuild=true -p:Version=1.0.0

The build checks the markup, the endpoint options against the connectors in `bin/`, bean references,
resources and the configuration keys, then writes `pkg/routemodule-1.0.0.tpkg`. A broken reference is a
build error.

## Run

Drop `pkg/routemodule-<version>.tpkg` into the `modules/` folder of a Tsak worker (`dotnet new tsak-worker`
from `redb.Tsak.Templates`, or the `redb-tsak-worker` image). Then:

    curl http://localhost:5180/api/hello/world
    curl -X POST http://localhost:5180/api/orders -H "Content-Type: application/json" -d "{\"id\":\"A-1\",\"amount\":10}"
    curl -X POST http://localhost:5180/api/orders -H "Content-Type: application/json" -d "{\"id\":\"A-2\"}"

## The same route in C#

`routes/ApiRoute.cs` is `routes/api.route.xml` written as a `RouteBuilder` with the REST DSL
(`this.Rest(...)`): the same `GET /hello/{name}` and `POST /orders`, the same schema check and 400 answer, under
`/api/cs` on the same port 5180. `context.xml` adds it with
`<routeBuilder type="RouteModule.Routes.ApiRoute, RouteModule"/>`.
Both APIs run with the module and share the HTTP host:

    curl -X POST http://localhost:5180/api/cs/orders -H "Content-Type: application/json" -d "{\"id\":\"A-1\",\"amount\":10}"

The difference to notice: XML names the schema file (`file="resources/order.schema.json"`) and the package
carries it; C# compiles the same file into the assembly (`EmbeddedResource` in `RouteModule.csproj`) and passes
its text to `ValidateJsonSchema`. The OpenAPI document of the C# half is at `/api/cs/openapi.json`.

## Change it

- **Port.** `port` on `<rest>`, 5180 here.
- **Status code.** Set the header `redbHttp.ResponseCode`; no body and no status answer 204.
- **More endpoints.** Add `<get>`, `<put>`, `<delete>`, `<patch>` under `<rest>`. A verb takes either
  `to="direct://..."` or the steps inline.
- **Sign-in.** `inboundAuth="basic"` or `inboundAuth="bearer"` with `tokenValidator="#name"` on `<rest>`.
- **The schema.** Edit `resources/order.schema.json`; the build checks that the file is there.

Settings come from the context configuration, not from the package: copy `config/context.sample.json` as
`context.json` next to the package, or put the keys into the worker's appsettings under
`Tsak:Contexts:modulectx`. Secrets go into environment variables.

## Editor

The `redb Route XML` extension for VS Code (`redb.redb-route-xml`) binds the schema by namespace and
completes elements and endpoint options. Without the extension, generate the schema from the build output
and point the XML extension at it:

    dotnet redb-route-xml xsd bin/Debug/net10.0 --out schema

Documentation: [redbase.app](https://redbase.app)
