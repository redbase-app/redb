using Microsoft.Extensions.Logging;
using redb.Route.Core;
using redb.Route.Http.Rest;
using redb.Route.Validation;

namespace RouteModule.Routes;

/// <summary>
/// The API of routes/api.route.xml written in C#, under /api/cs on the same port: the same two operations, the same
/// schema check and the same 400 answer. Both APIs run with the module and share one HTTP host.
/// </summary>
public sealed class ApiRoute : RouteBuilder
{
    protected override void Configure()
    {
        // A body that does not match the schema: answer 400 with a JSON error instead of 500.
        OnException<ValidationException>()
            .Handled()
            .Log("request rejected: ${exception.message}", LogLevel.Warning)
            .SetHeader("redbHttp.ResponseCode", 400)
            .SetBody("""{"error":"the body does not match resources/order.schema.json"}""");

        // The OpenAPI document of this half is served at /api/cs/openapi.json.
        var api = this.Rest("/api/cs", o =>
        {
            o.Port = 5180;
            o.Produces = "application/json";
            o.Title = "RouteModule API (C#)";
            o.Version = "1.0.0";
        });

        api.Get("/hello/{name}")
            .Id("cs-modulectx-hello")
            .Param("name", RestParamType.Path, dataType: RestParamDataType.String, description: "Who to greet")
            .Route()
            .SetBody(Expr("""{"greeting":"Hello, ${header.name}"}"""));

        api.Post("/orders")
            .Id("cs-modulectx-orders")
            .Consumes("application/json")
            .To("direct://cs-modulectx-order-in");

        // The POST handler: check the body, then answer 202.
        From("direct://cs-modulectx-order-in")
            .RouteId("cs-modulectx-order-in")
            .Description("Accepts an order checked against the JSON schema, the C# form")
            // The HTTP body arrives as bytes; the schema check and the log want the text.
            .ConvertBody(typeof(string))
            .ValidateJsonSchema(OrderSchema)
            .Log("order accepted: ${body}")
            .SetHeader("redbHttp.ResponseCode", 202)
            .SetBody("""{"accepted":true}""");
    }

    // resources/order.schema.json, compiled into this assembly (EmbeddedResource in RouteModule.csproj).
    private static string OrderSchema { get; } = ReadSchema();

    private static string ReadSchema()
    {
        using var stream = typeof(ApiRoute).Assembly.GetManifestResourceStream("order.schema.json")
            ?? throw new InvalidOperationException("order.schema.json is not embedded in the module assembly.");
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }
}
