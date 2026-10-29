# RouteModule

A Tsak module whose XML routes (`urn:redb:route:1.0`) call a C# class of the project. Logic that does not
belong in markup lives in `Beans/`; `context.xml` builds the object from configuration and the routes call
its methods. It needs no broker.

| Part | What it shows |
|---|---|
| `Beans/PriceCalculator.cs` | a plain class: a constructor with the VAT rate, a `Currency` property, two methods |
| `context.xml` | `<bean name="pricing" ...>` with `<constructorArg type="System.Decimal">` and `<property>`, values from configuration |
| `routes/orders.route.xml` | `bean:#pricing?method=Gross(${header.qty}, ${header.price})`: arguments are expressions, the result becomes the body; `bean:#pricing?method=Stamp`: the method takes the exchange and sets headers |

The log shows `order order-1: 2 x 12.50 + VAT 0.20 = 30.00 EUR` and, for every fifth order,
`order order-4 refused: The quantity must be positive.`, an exception of the bean handled by `onException`.

The module is a package (`.tpkg`) that a Tsak worker loads from its `modules/` folder; there is no `Main`
to run.

## Layout

| Path | What |
|---|---|
| `Beans/PriceCalculator.cs` | the bean |
| `context.xml` | the `pricing` bean and the C# route builder |
| `routes/orders.route.xml` | the route and the error handling |
| `routes/QuotesRoute.cs` | a second route in C#, with the same bean |
| `config/routemodule.config.json` | identity inside the package: context name and auto start |
| `config/context.sample.json` | deployment sample of the context configuration: `pricing.vat`, `pricing.currency`, `orders.period`, `quotes.period` |
| `.config/dotnet-tools.json` | the `redb-route-xml` tool, same version as the redb packages |

## The same bean from C#

`routes/QuotesRoute.cs` is a `RouteBuilder` next to the XML route: price quotes on a timer of its own
(`{{quotes.period:10000}}`), with the same `pricing` bean of `context.xml` and the same kind of error handling.
`context.xml` adds it with `<routeBuilder type="RouteModule.Routes.QuotesRoute, RouteModule"/>`. The log shows
`quote quote-1: 2 x 8.00 + VAT 0.20 = 19.20 EUR` and, for every fourth quote, `quote quote-3 refused: ...`.

The difference to notice: XML calls the bean by name and method text (`bean:#pricing?method=Gross(...)`),
checked at build; C# takes the same bean from the registry by its name as a `PriceCalculator` and calls
`Gross` and `Stamp` directly, checked by the compiler.

## Build the package

    dotnet tool restore
    dotnet build -p:PackRouteOnBuild=true -p:Version=1.0.0

`RoutePackageIncludeAssembly` in `RouteModule.csproj` puts `RouteModule.dll` into the package. The build
checks that the bean type, its properties and the called methods exist in it, then writes
`pkg/routemodule-1.0.0.tpkg`.

## Run

Drop `pkg/routemodule-<version>.tpkg` into the `modules/` folder of a Tsak worker (`dotnet new tsak-worker`
from `redb.Tsak.Templates`, or the `redb-tsak-worker` image).

## Change it

- **Constructor arguments.** One `<constructorArg>` per parameter, in order; `type=` names the parameter
  type when it is not a string (`System.Decimal`, `System.Int32`, `System.TimeSpan`). An argument can be a
  nested `<bean>`, a `ref=` to another bean or a `<list>`.
- **Properties.** `<property key="Name" value="..."/>`, converted to the property type.
- **Methods.** `method=Name` calls a method that takes `IExchange` (or `IExchange` and a
  `CancellationToken`); `method=Name(expr, ...)` binds each argument from an expression. A method that
  returns a value sets the body; a `Task` is awaited.
- **Dependencies.** A class that needs services from the container (a database, an HTTP client) is better
  registered by code in a module with `InitRoute`; XML refers to it by `#name`.
- **NuGet packages.** A package the bean uses and the worker does not ship goes into the package too: pass
  its assembly to `redb-route-xml pack` with `--entry`.

## Editor

The `redb Route XML` extension for VS Code (`redb.redb-route-xml`) binds the schema by namespace and
completes elements and endpoint options. Without the extension, generate the schema from the build output
and point the XML extension at it:

    dotnet redb-route-xml xsd bin/Debug/net10.0 --out schema

Documentation: [redbase.app](https://redbase.app)
