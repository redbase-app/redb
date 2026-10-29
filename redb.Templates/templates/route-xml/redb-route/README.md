# RouteModule

A Tsak module whose routes are written in XML markup (`urn:redb:route:1.0`). The module is a package
(`.tpkg`) that a Tsak worker loads from its `modules/` folder; there is no `Main` to run.

## Layout

| Path | What |
|---|---|
| `routes/*.route.xml` | the routes |
| `routes/HelloRoute.cs` | the same route in C#, a `RouteBuilder` |
| `context.xml` | components, context-wide beans, the one-time init pipeline, the C# route builders |
| `resources/` | XSLT and schema files the routes refer to (create the folder when needed) |
| `config/routemodule.config.json` | identity inside the package: context name and auto start |
| `config/context.sample.json` | deployment sample of the context configuration |
| `.config/dotnet-tools.json` | the `redb-route-xml` tool, same version as the redb packages |

## Build the package

    dotnet tool restore
    dotnet build -p:PackRouteOnBuild=true -p:Version=1.0.0

The build checks the markup, the endpoint options against the connectors in `bin/`, bean references
and the configuration keys, then writes `pkg/routemodule-1.0.0.tpkg`. A broken reference is a build
error. `dotnet redb-route-xml check .` runs the same checks without writing the package.

## Run

Drop `pkg/routemodule-<version>.tpkg` into the `modules/` folder of a Tsak worker. A worker of your own
comes from `dotnet new install redb.Tsak.Templates` and `dotnet new tsak-worker`, or use the
`redb-tsak-worker` image. The worker loads the package within seconds and the log shows
`hello from RouteModule` every 5 seconds, and the same line from the C# route.

Settings come from the context configuration, not from the package: copy `config/context.sample.json`
as `context.json` next to the package, or put the keys into the worker's appsettings under
`Tsak:Contexts:modulectx`. Secrets go into environment variables.

## The same route in C#

`routes/HelloRoute.cs` is `routes/main.route.xml` written as a `RouteBuilder`: the same steps, the same
`{{hello.period:5000}}` key, the same log, on a timer of its own (`cs-modulectx-hello`). `context.xml` adds it to the context with
`<routeBuilder type="RouteModule.Routes.HelloRoute, RouteModule"/>`, and `RoutePackageIncludeAssembly` puts
`RouteModule.dll` into the package.

Both routes run with the module: the log shows `hello from RouteModule` and `hello from RouteModule (C#)` every
5 seconds. The difference to notice: the XML route is markup checked by the build, the C# one is a class
checked by the compiler; a step reads the same in both.

## Add a connector

Add the connector package at the same version and use its scheme in a route:

    <PackageReference Include="redb.Route.RabbitMQ" Version="$(RedbVersion)" />

    <from uri="rabbitmq://orders?host={{rabbit.host}}"/>

The worker image already ships the connectors; the reference is there so the build-time check knows
the endpoint options.

## Editor

The `redb Route XML` extension for VS Code (`redb.redb-route-xml`) binds the schema by namespace and
completes elements and endpoint options. Without the extension, generate the schema from the build
output and point the XML extension at it:

    dotnet redb-route-xml xsd bin/Debug/net10.0 --out schema

## More

- Diagram of a route: `dotnet redb-route-xml mermaid routes/main.route.xml`
- C# from a route: `dotnet redb-route-xml csharp routes/main.route.xml --namespace RouteModule.Routes`
- Documentation: [redbase.app](https://redbase.app)
