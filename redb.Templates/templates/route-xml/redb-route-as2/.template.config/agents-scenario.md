AS2 (`redb.Route.As2`, RFC 4130), `routes/send.route.xml` and `routes/receive.route.xml`: `context.xml` holds our
key pair (`as2-key`, PKCS#12), the partner's certificate (`partner-cert`), the agreement `partner`
(`As2ConnectionFactory`: `As2From` us, `As2To` the partner, `PartnerUrl`, sign, encrypt, signed MDN,
`RequireValidMdn`) and the duplicate memory `as2-received`. The sender posts with `as2:/send?connectionFactory=partner`
and reads `redbAs2.mdnDisposition` and `redbAs2.mdnMicStatus`; the receiver listens on
`as2:/as2/inbound?port={{as2.port:4080}}&amp;connectionFactory=partner&amp;idempotentRepository=as2-received`.

Facts to keep: with a connection factory every agreement option lives on the factory, the URI carries none; the
partner certificate is pinned; `as2.keyFile`, `as2.keyPassword` and `as2.partnerCertificateFile` are required keys and
the password comes from the environment; `deploy/docker-compose.openas2.yml` makes test keys and runs OpenAS2 4.9.0
as the partner (`deploy/openas2/partnerships.xml`).

Usually changed: the identifiers, the partner URL, the algorithms, the source of the documents (a file or SFTP
consumer instead of the timer), what the receiver does with a document.

`routes/InvoiceSendRoute.cs` is a second sender in C# on its own timer (`as2.invoicePeriod`) under the same
agreement: an XML invoice from LINQ to XML, `exchange.In.ContentType = "application/xml"`, its own `OnException`
with two retries, and a branch on `As2Headers.MdnConfirmed`. One `as2:` receiver per path: keep it in XML.
