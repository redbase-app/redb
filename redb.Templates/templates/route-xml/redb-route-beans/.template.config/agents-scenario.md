C# beans, `Beans/PriceCalculator.cs` and `routes/orders.route.xml`: `context.xml` declares
`<bean name="pricing" type="RouteModule.Beans.PriceCalculator, RouteModule">` with a
`<constructorArg type="System.Decimal" value="{{pricing.vat:0.20}}"/>` and a `<property key="Currency">`.
The route calls `bean:#pricing?method=Gross(${header.qty}, ${header.price})` (arguments are expressions,
the return value becomes the body) and `bean:#pricing?method=Stamp` (a method that takes `IExchange`).

Facts to keep: `RoutePackageIncludeAssembly` in the `.csproj` puts this project's assembly into the
package; a constructor argument needs `type=` when the parameter is not a string; a bean with a dependency
the XML cannot build stays out of XML and is registered by code. An exception the bean throws is an
ordinary route failure (`onException` here).

`routes/QuotesRoute.cs` is a second route in C# (`cs-modulectx-quotes`, its own timer `{{quotes.period:10000}}`)
that takes the same bean from the registry, `exchange.Context.GetFromRegistry<PriceCalculator>("pricing")`, and
calls `Gross` and `Stamp` as typed methods.

Usually changed: the class itself, more methods, more beans in `context.xml`.
