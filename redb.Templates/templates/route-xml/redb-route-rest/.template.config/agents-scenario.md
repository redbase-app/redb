An HTTP JSON API (`redb.Route.Http`), `routes/api.route.xml`: `<rest path="/api" port="5180">` with
`GET /api/hello/{name}` and `POST /api/orders`. The POST goes to `direct://modulectx-order-in`, which
converts the body to text (`<convertBody type="System.String"/>`, the HTTP body arrives as bytes),
checks it against `resources/order.schema.json` and answers 202. `onException` turns a
`redb.Route.Validation.ValidationException` into 400.

Facts to keep: `port` on `<rest>` takes a number, not a `{{key}}`; the status code is the header
`redbHttp.ResponseCode`; path parameters arrive as `header.<name>`, query parameters as
`header.query.<name>`; the OpenAPI document is served at `/api/openapi.json`.

Usually changed: more verbs under `<rest>`, the schema in `resources/`, sign-in with `inboundAuth`.

`routes/ApiRoute.cs` is the same API in C# with the REST DSL (`this.Rest("/api/cs", ...)`, same port, ids `cs-...`):
the schema is the same file compiled in as `EmbeddedResource` (`order.schema.json`) and passed to
`ValidateJsonSchema` as text.
