# RouteModule

A Tsak module that works with S3 buckets (AWS S3, or an S3-compatible store), written in XML markup
(`urn:redb:route:1.0`):

| Route | What it does |
|---|---|
| `modulectx-s3-write` | every 20 seconds writes a JSON order as `orders/order-N.json` into the bucket `incoming` |
| `modulectx-s3-read` | every 5 seconds reads the new `*.json` objects under `orders/` of `incoming`, one exchange each; when the exchange succeeded the object moves to the bucket `archive`, a failed one stays and is read again |
| `cs-modulectx-s3-prices-write`, `cs-modulectx-s3-prices-read` | C#: every 30 seconds a CSV price list as `prices/prices-N.csv` into `incoming`; the reader logs its cheapest item and deletes it |

The module is a package (`.tpkg`) that a Tsak worker loads from its `modules/` folder; there is no `Main`
to run.

## Layout

| Path | What |
|---|---|
| `context.xml` | the `s3` connection: endpoint, region, path style, access key, secret key; the C# route builder |
| `routes/objects.route.xml` | the two routes |
| `routes/PriceListRoute.cs` | the price list routes in C# |
| `config/routemodule.config.json` | identity inside the package: context name and auto start |
| `config/context.sample.json` | deployment sample of the context configuration |
| `deploy/docker-compose.s3.yml` | RustFS, an S3-compatible store, and the module on a Tsak worker |
| `.config/dotnet-tools.json` | the `redb-route-xml` tool, same version as the redb packages |

## Build the package

    dotnet tool restore
    dotnet build -p:PackRouteOnBuild=true -p:Version=1.0.0

The build checks the markup, every `s3://` option against the connector, the `s3` bean and the configuration
keys, then writes `pkg/routemodule-1.0.0.tpkg`. No store is needed for the build.

## Try it locally

    dotnet build -p:PackRouteOnBuild=true -p:Version=1.0.0
    cp deploy/.env.example deploy/.env          # then set S3_SECRET_KEY
    docker compose -f deploy/docker-compose.s3.yml up -d

The module log shows `written incoming/orders/order-N.json` and `read orders/order-N.json (... bytes): {...}`; the
console of the store on http://localhost:9001 (access key `modulectx`) shows the objects in `archive`.

## Configuration

| Key | Default | What |
|---|---|---|
| `s3.serviceUrl` | `http://localhost:9000` | the endpoint of an S3-compatible store; empty for AWS |
| `s3.region` | `us-east-1` | the region |
| `s3.forcePathStyle` | `true` | path-style URLs, needed by most S3-compatible stores; `false` for AWS |
| `s3.accessKey` | none, required | the access key |
| `s3.secretKey` | none, required | the secret key; from the environment |
| `s3.inbox` | `incoming` | the bucket the reader polls |
| `s3.archive` | `archive` | where processed objects go |
| `orders.period` | `20000` | the writer's interval, ms |
| `prices.period` | `30000` | the interval of the C# price lists, ms |

In a deployment the secret goes into the environment of the worker, in the `Override` section:

    Tsak__Contexts__modulectx__Override__s3.secretKey: ${S3_SECRET_KEY}

On AWS with an instance profile or a task role, set `UseDefaultCredentialsProvider` to `true` on the `s3` bean
instead of keys.

## Price lists in C#

`routes/PriceListRoute.cs` is a `RouteBuilder` that `context.xml` adds with
`<routeBuilder type="RouteModule.Routes.PriceListRoute, RouteModule"/>`. Through the same `s3` bean it writes CSV
price lists under `prices/` of the inbox bucket on a timer of its own and reads them back, so it never takes an
`orders/*.json` object from the XML reader. The log shows `price list prices/prices-1.csv: cheapest C-3 at 5.25`.

The difference to notice: XML names the object in the URI (`keyName=orders/order-${header.CamelTimerCounter}.json`);
C# builds the CSV with a `StringBuilder` and sets the key in the header `S3Headers.Key`, and the reader parses
the lines with LINQ instead of handing the text on.

## Change it

- **Delete instead of move.** Leave out `moveAfterRead`, `deleteAfterRead` and `destinationBucket`: the object is
  deleted after a successful exchange.
- **Wait for a marker.** `doneFileName=${file:name}.done` reads an object only when its `.done` marker exists.
- **Read in order.** `sortBy=LastModified` (it lists the whole prefix first).
- **Large objects.** `streamBody=true` on the reader, `multiPartUpload=true` on the writer.
- **Other operations.** `s3:ListObjects:<bucket>`, `s3:CopyObject:<bucket>`, `s3:CreateDownloadLink:<bucket>` and the
  rest of the 22 operations of `redb.Route.S3`.

## Editor

The `redb Route XML` extension for VS Code (`redb.redb-route-xml`) binds the schema by namespace and
completes elements and endpoint options. Without the extension, generate the schema from the build output
and point the XML extension at it:

    dotnet redb-route-xml xsd bin/Debug/net10.0 --out schema

Documentation: [redbase.app](https://redbase.app)
