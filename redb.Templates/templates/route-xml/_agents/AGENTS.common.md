# AGENTS.md

Instructions for AI coding agents (GitHub Copilot, Cursor, Codex, Claude Code and others) working in this
project. Read this file before changing anything.

## What this project is

`RouteModule` is a Tsak module whose routes are written in redb.Route XML markup. It is not an
application: the build produces a package, `pkg/routemodule-<version>.tpkg`, that a Tsak worker loads from
its `modules/` folder into the route context `modulectx`. The `.csproj` exists to restore the connector
packages, so that the build-time check knows their elements and endpoint options, and to compile the C# part:
the same scenario also has a route written in C#, a `RouteBuilder` in `routes/*.cs`, as a real project mixes both.

## Files

| Path | Holds |
|---|---|
| `routes/*.route.xml` | the routes; root `<routes xmlns="urn:redb:route:1.0">` |
| `context.xml` | root `<context xmlns="urn:redb:route:1.0">`: beans (`<bean name="...">`), `<onInit>`, the dead letter channel, context-level blocks |
| `resources/` | XSLT, XSD and JSON schema files the routes name by a path under `resources/` |
| `config/routemodule.config.json` | identity inside the package: `ContextName`, `AutoStart`; nothing else |
| `config/context.sample.json` | the deployment configuration of the context: `Contexts.default`, then `Contexts.modulectx` |

## Rules

- Elements, attributes and endpoint options come from the schema, never from memory. The VS Code
  extension `redb.redb-route-xml` binds it by the namespace and completes options. Without it, run
  `dotnet redb-route-xml xsd bin/Debug/net10.0 --out schema` and read `schema/redb-route-1.0.xsd`;
  `dotnet redb-route-xml catalog bin/Debug/net10.0 --out schema` lists the options of every connector.
- Connector packages: the worker ships every `redb.Route.*` connector in its shared folder and the
  module does not bundle its own copies. The `PackageReference` items of the `.csproj` are there for the
  build-time check. A new connector is one more `PackageReference` of that `redb.Route.*` package with
  `Version="$(RedbVersion)"`.
- A value that differs between deployments is a key: `{{key}}` or `{{key:default}}`. Keys are flat names
  with dots (`rabbit.host`). Every key goes into `config/context.sample.json` under `Contexts.modulectx`
  as `"rabbit.host": "..."`; a nested JSON object (`"rabbit": { "host": ... }`) is not found by
  `{{rabbit.host}}`.
- Secrets (passwords, tokens, keys) are keys without a default: never a literal in the XML, never a real
  value in `context.sample.json`. The build lists the keys without a default in `manifest.json`
  (`RequiredConfigKeys`), and the module stops at init when one has no value.
- A connection or any other object several endpoints share is a `<bean name="...">` in `context.xml`;
  the endpoints refer to it (`connectionFactory=rabbit`, `dataSource=#main-db`, `repository="#name"`).
- What XML cannot express (a lambda, custom logic) is a C# class in this project, declared as
  `<bean name="..." type="Namespace.Type, Assembly">` and called as `<to uri="bean:#name?method=Do"/>`.
- A C# route is a public `RouteBuilder` with no constructor parameters, added by
  `<routeBuilder type="RouteModule.Routes.Name, RouteModule"/>` in `context.xml`; `RoutePackageIncludeAssembly`
  puts the assembly into the package. Its route ids start with `cs-`, and it has its own timers, queues and
  `direct:` names, so the two forms never consume the same thing twice. It reads the same `{{key}}` configuration
  and refers to the beans of `context.xml` by name.
- In XML attributes `&` is written `&amp;` and `<` is written `&lt;`.
- Leave `.config/dotnet-tools.json`, `Directory.Build.props` and `Directory.Build.targets` as they are:
  the version of the `redb-route-xml` tool must equal `RedbVersion`.

## Check every change

    dotnet tool restore
    dotnet build -p:PackRouteOnBuild=true -p:Version=0.0.1
    dotnet redb-route-xml check . --bin bin/Debug/net10.0

The build fails on a markup error, an unknown endpoint option, a bean type or method that does not exist,
or a resource outside `resources/`. Then read `pkg/routemodule-0.0.1/manifest.json`: `Context` must be
`modulectx`, and `RequiredConfigKeys` must hold exactly the keys a deployment has to supply. A change is
done when the build is clean and the manifest says what you meant.

## Run

Copy `config/context.sample.json` as `context.json` next to the package, or put the keys into the worker's
appsettings under `Tsak:Contexts:modulectx`, and drop the `.tpkg` into the worker's `modules/` folder.
Secrets come from the environment of the worker, in the `Override` section, which wins over every file:

    environment:
      Tsak__Contexts__modulectx__Override__rabbit.password: ${RABBIT_PASSWORD}

The key keeps its dots; only the section separators are `__`. `Tsak__Contexts__modulectx__Override__rabbit__password`
would be a nested object and is not found by `{{rabbit.password}}`.

Module deployment: https://github.com/redbase-app/redb-tsak/blob/main/MODULE_DEPLOYMENT.md

## Reference

- XML markup: https://github.com/redbase-app/redb-route/blob/main/redb.Route.Xml/README.md
- A connector: `https://github.com/redbase-app/redb-route/blob/main/redb.Route.<Connector>/README.md`
- `README.md` of this project
- Documentation: https://redbase.app

## This template

