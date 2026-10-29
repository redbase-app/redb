The base template: one route, `routes/main.route.xml`, a timer that writes a line to the log every
`{{hello.period:5000}}` ms. `context.xml` is empty apart from commented examples.

Usually changed: the timer becomes a connector endpoint (`rabbitmq://`, `kafka://`, `sql:`, `file://`,
an HTTP endpoint); add the connector package to the `.csproj` and its connection as a bean in `context.xml`.

`routes/HelloRoute.cs` is the same route in C# (`cs-modulectx-hello`, its own timer); both run with the module.
