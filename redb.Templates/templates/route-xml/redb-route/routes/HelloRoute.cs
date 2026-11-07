using redb.Route.Core;

namespace RouteModule.Routes;

/// <summary>
/// The route of routes/main.route.xml written in C#: the same configuration key and the same log, on a timer of
/// its own. Both routes run with the module.
/// </summary>
public sealed class HelloRoute : RouteBuilder
{
    protected override void Configure()
    {
        // {{hello.period:5000}} reads the same key as the XML route; 5000 ms when it is not set.
        From("timer://cs-modulectx-hello?period={{hello.period:5000}}")
            .RouteId("cs-modulectx-hello")
            .Description("Writes a line to the log on every tick, the C# form")
            .SetBody("hello from RouteModule (C#)")
            .To("log://modulectx");
    }
}
