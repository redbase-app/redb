# RouteModule

A Tsak module that exchanges documents over AS4 (eDelivery AS4 1.16 common profile), written in XML markup
(`urn:redb:route:1.0`) and in C#:

| Route | What it does |
|---|---|
| `modulectx-as4-send` | sends a test invoice to the partner every minute and logs the partner's receipt |
| `modulectx-as4-receive` | receives the partner's messages on `POST /as4/in`, port 4090, and logs them |
| `cs-modulectx-as4-credit-note` | C#: sends a test credit note to the same partner every 90 seconds, as `application/xml`, with the same reception awareness |

Every message is signed, its payload compressed and encrypted for the partner. The answer is a signed
non-repudiation receipt in the same HTTP response: AS4 has no MDN (that is AS2), and the send step returns
only when the receipt has been verified. A send without a receipt is tried again twice, half a minute apart,
with the same message id; the receiver gives a repeated message its receipt again and does not deliver it
twice.

The module is a package (`.tpkg`) that a Tsak worker loads from its `modules/` folder; there is no `Main`
to run.

## Layout

| Path | What |
|---|---|
| `context.xml` | our node (`as4-node`): party id and key pair; the agreement with the partner (`partner`); the received-ids store; the C# route builder |
| `routes/send.route.xml` | the sending route and its error handling |
| `routes/receive.route.xml` | the receiving route |
| `routes/CreditNoteSendRoute.cs` | the credit note sender in C# |
| `config/routemodule.config.json` | identity inside the package: context name and auto start |
| `config/context.sample.json` | deployment sample of the context configuration |
| `deploy/docker-compose.holodeck.yml` | a local exchange: test keys, the module on a Tsak worker, Holodeck B2B as the partner |
| `deploy/holodeck/` | Holodeck's image, certificate manager, P-Modes and a message to submit |
| `.config/dotnet-tools.json` | the `redb-route-xml` tool, same version as the redb packages |

## Build the package

    dotnet tool restore
    dotnet build -p:PackRouteOnBuild=true -p:Version=1.0.0

The build checks the markup, every `as4:` option against the connector, the beans and the configuration
keys, then writes `pkg/routemodule-1.0.0.tpkg`. No partner and no keys are needed for the build.

## Run a local exchange

The partner is [Holodeck B2B](https://github.com/holodeck-b2b/Holodeck-B2B) 8.1.1, an open source AS4
message service handler. `deploy/` is a test stand: keys made on the spot, fixed test passwords in Holodeck's
configuration, no TLS. It is not a deployment, and the Holodeck image it builds is not one for production.

1. `cp deploy/.env.example deploy/.env` and set `AS4_KEY_PASSWORD`.
2. `docker compose -f deploy/docker-compose.holodeck.yml up -d --build`

   The `certs` service writes the module's key pair to `deploy/keys` and Holodeck's keystores to
   `deploy/holodeck/certs`, once. The Holodeck image is built from the release archive, checked against its
   SHA-256 (`deploy/holodeck/Dockerfile`).
3. Module to Holodeck: the worker log shows `sent <message id>, receipt <receipt id>` every minute, and
   Holodeck stores each message it took:

       docker compose -f deploy/docker-compose.holodeck.yml logs -f module
       docker compose -f deploy/docker-compose.holodeck.yml exec holodeck ls data/msg_in

   Holodeck starts in about half a minute; a send before that fails and is retried.
4. Holodeck to the module: submit the sample message.

       docker compose -f deploy/docker-compose.holodeck.yml exec holodeck sh -c "cp samples/invoice-1.xml data/msg_out/ && cp samples/invoice-1.mmd data/msg_out/"

   The worker log shows `received ... from partner`; Holodeck writes the module's receipt to `data/msg_in`
   once it has verified it.

`docker compose -f deploy/docker-compose.holodeck.yml down` stops it; delete `deploy/keys` and
`deploy/holodeck/certs` for new keys.

## Deploy

1. Our key pair: a PKCS#12 file with the certificate the partner knows. The partner's certificate: a file
   (`.cer`, `.crt`, PEM or DER).
2. Give the context its configuration: copy `config/context.sample.json` as `context.json` next to the
   package, or put the keys into the worker's appsettings under `Tsak:Contexts:modulectx`. The key password
   goes through the environment, `Tsak__Contexts__modulectx__Override__as4.keyPassword`.
3. Drop `pkg/routemodule-<version>.tpkg` into the `modules/` folder of a Tsak worker (`dotnet new
   tsak-worker` from `redb.Tsak.Templates`, or the `redb-tsak-worker` image).
4. Publish the receiver to partners over HTTPS: a reverse proxy that terminates TLS in front of port 4090.

## Configuration

| Key | Default | What |
|---|---|---|
| `as4.keyFile` | none, required | our key pair, PKCS#12 |
| `as4.keyPassword` | none, required | its password; from an environment variable |
| `as4.partnerCertificateFile` | none, required | the partner's certificate: verifies its signatures, we encrypt for it |
| `as4.ourPartyId` | `urn:redb:example:module` | our `eb:PartyId` |
| `as4.hostName` | `module.example` | the host part of the message ids we make: the public name of the access point |
| `as4.partnerPartyId` | `urn:redb:example:holodeck` | the partner's `eb:PartyId` |
| `as4.partnerAddress` | `http://holodeck:8080/holodeckb2b/as4` | the partner's address, `http://` or `https://`, with a query where it has one (a Domibus domain, `?domain=name`): the `Address` of the agreement |
| `as4.agreement` | `urn:redb:example:agreement` | `eb:AgreementRef`; the partner selects its P-Mode by it |
| `as4.service` | `urn:redb:example:as4` | `eb:Service` |
| `as4.action` | `StoreMessage` | `eb:Action` |
| `as4.port` | `4090` | the receiver's port |
| `as4.sendPeriod` | `60000` | the test sender's interval, ms |
| `as4.creditNotePeriod` | `90000` | the C# credit note sender's interval, ms |

## Credit notes in C#

`routes/CreditNoteSendRoute.cs` is a `RouteBuilder` that `context.xml` adds with
`<routeBuilder type="RouteModule.Routes.CreditNoteSendRoute, RouteModule"/>`. On a timer of its own it sends a
credit note through the same node `as4-node` to the same partner, with the same two redeliveries when no
receipt comes and none after an ebMS error. The log shows `sent credit note <id>, receipt <id>` next to the XML
sender's lines.

The difference to notice: C# sets the content type on the message (`exchange.In.ContentType =
"application/xml"`), so the payload travels with `MimeType` `application/xml` where the XML sender's goes as
`application/octet-stream`; the document comes from LINQ to XML, and the error handlers name the exception types
as types (`As4ReceiptException`, `As4ErrorSignalException`).

## Change it

- **The document.** Replace the timer and `setBody` in `routes/send.route.xml` with the real source (a file
  consumer, a queue, an HTTP call). The content type the source gives the body travels as the `MimeType` part
  property; a body set in the route goes as `application/octet-stream`.
- **The receiving side.** Replace the log in `routes/receive.route.xml` with the real work. The receipt goes
  when the route has finished; a failure answers the partner with an ebMS error, and the partner resends.
- **More partners.** One `As4Partner` bean per partner in the node's `Partners`; `partner=<name>` on a
  sending endpoint picks one, the receiver finds it by the party id and the agreement.
- **Trust in a certification authority** instead of a pinned certificate: `PartnerTrustAnchors` on the
  partner, with revocation checks on the node. See the `redb.Route.As4` README.
- **Four corner topology.** `RequireFourCornerProperties` is off for a point-to-point exchange; turn it on
  where `originalSender` and `finalRecipient` are required.
- **Received ids that survive a restart.** Replace the `as4-received` bean with the redb-stored repository
  shown in `context.xml`.

A complete access point that passes the eDelivery conformance scenarios is in the redb.Route repository,
`demos/As4ConformanceDemo`.

## Editor

The `redb Route XML` extension for VS Code (`redb.redb-route-xml`) binds the schema by namespace and
completes elements and endpoint options. Without the extension, generate the schema from the build output
and point the XML extension at it:

    dotnet redb-route-xml xsd bin/Debug/net10.0 --out schema

Documentation: [redbase.app](https://redbase.app)
