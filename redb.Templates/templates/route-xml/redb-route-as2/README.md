# RouteModule

A Tsak module that exchanges documents over AS2 (RFC 4130), written in XML markup (`urn:redb:route:1.0`) and in C#:

| Route | What it does |
|---|---|
| `modulectx-as2-send` | every minute signs and encrypts a test document and posts it to the partner; the call returns when the partner's signed MDN has been verified and its MIC matched; no confirmation is retried twice, then logged |
| `modulectx-as2-receive` | listens on port 4080 at `/as2/inbound`, decrypts, verifies the partner's signature, answers with the MDN the partner asked for; a resend with the same `Message-ID` gets its MDN again and is not delivered twice |
| `cs-modulectx-as2-invoice` | C#: every 90 seconds an XML invoice under the same agreement, with the same retries; logs whether the MDN confirmed it |

The module is a package (`.tpkg`) that a Tsak worker loads from its `modules/` folder; there is no `Main`
to run.

## Layout

| Path | What |
|---|---|
| `context.xml` | our key pair, the partner's certificate, the agreement `partner` (`As2ConnectionFactory`) and the duplicate memory; the C# route builder |
| `routes/send.route.xml`, `routes/receive.route.xml` | the two directions |
| `routes/InvoiceSendRoute.cs` | the invoice sender in C# |
| `config/routemodule.config.json` | identity inside the package: context name and auto start |
| `config/context.sample.json` | deployment sample of the context configuration |
| `deploy/docker-compose.openas2.yml` | a local exchange: test keys, OpenAS2 4.9.0 as the partner, the module on a Tsak worker |
| `deploy/openas2/partnerships.xml` | the OpenAS2 side of the agreement |
| `.config/dotnet-tools.json` | the `redb-route-xml` tool, same version as the redb packages |

## Build the package

    dotnet tool restore
    dotnet build -p:PackRouteOnBuild=true -p:Version=1.0.0

The build checks the markup, the `as2:` options, the beans and the configuration keys, then writes
`pkg/routemodule-1.0.0.tpkg`. No partner is needed for the build.

## Try it with OpenAS2

    dotnet build -p:PackRouteOnBuild=true -p:Version=1.0.0
    cp deploy/.env.example deploy/.env          # then set AS2_KEY_PASSWORD
    docker compose -f deploy/docker-compose.openas2.yml up -d

The `certs` service makes the test key pairs once (`deploy/keys`, `deploy/openas2/config`). The module log shows
`sent <...>: automatic-action/MDN-sent-automatically; processed, MIC matched` every minute. For the other direction
put a file into `deploy/openas2/data/outbox/modulectx`: OpenAS2 signs, encrypts and posts it, the module logs
`received <...> from openas2, signature valid True` and answers with a signed MDN.

## Configuration

| Key | Default | What |
|---|---|---|
| `as2.keyFile` | none, required | our PKCS#12 key pair |
| `as2.keyPassword` | none, required | its password; from the environment |
| `as2.partnerCertificateFile` | none, required | the partner's certificate (PEM or DER) |
| `as2.ourId` | `modulectx` | our AS2 identifier |
| `as2.partnerId` | `openas2` | the partner's AS2 identifier |
| `as2.partnerUrl` | `http://openas2:10080` | where we post |
| `as2.port` | `4080` | where we listen |
| `as2.sendPeriod` | `60000` | the test sender's interval, ms |
| `as2.invoicePeriod` | `90000` | the C# invoice sender's interval, ms |

In a deployment the password goes into the environment of the worker, in the `Override` section:

    Tsak__Contexts__modulectx__Override__as2.keyPassword: ${AS2_KEY_PASSWORD}

## Invoices in C#

`routes/InvoiceSendRoute.cs` is a `RouteBuilder` that `context.xml` adds with
`<routeBuilder type="RouteModule.Routes.InvoiceSendRoute, RouteModule"/>`. On a timer of its own it sends an XML
invoice through the same agreement `partner`, signed and encrypted like the X12 document, with the same two
retries. The log shows `invoice <...@redb.route>: delivered and confirmed` next to the XML sender's lines.

The difference to notice: C# builds the invoice with LINQ to XML, sets the MIME type of the document on the
message (`exchange.In.ContentType = "application/xml"`) and branches on the typed outcome of the MDN,
`As2Headers.MdnConfirmed`, instead of logging the disposition text.

## Change it

- **A real partner.** Its identifier, URL and certificate; the algorithms (`SignAlg`, `EncryptAlg`) and `Compress`
  as agreed. `sha-1` and `3des` need `AllowLegacyAlgorithms`.
- **Asynchronous MDN.** `MdnMode` `Async` with `AsyncMdnUrl` on send and an `as2:` endpoint with `mode=mdn`;
  `AsyncMdnAllowedHosts` on receive.
- **TLS.** `as2s://` to send over HTTPS; `useTls` with `sslCertPath` on the receiver, a certificate of its own, not the
  S/MIME key.
- **Documents.** Replace the timer with `file://`, `sftp://` or a queue, and the receiver's log with the real work.
- **Duplicates across restarts.** `as2-received` keeps the ids in memory; a durable repository keeps them.

## Editor

The `redb Route XML` extension for VS Code (`redb.redb-route-xml`) binds the schema by namespace and
completes elements and endpoint options. Without the extension, generate the schema from the build output
and point the XML extension at it:

    dotnet redb-route-xml xsd bin/Debug/net10.0 --out schema

Documentation: [redbase.app](https://redbase.app)
