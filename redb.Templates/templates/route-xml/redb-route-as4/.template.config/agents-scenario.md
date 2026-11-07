AS4, eDelivery AS4 1.16 common profile (`redb.Route.As4`). `routes/send.route.xml`: a timer sends a test document
with `as4:?connectionFactory=as4-node&amp;partner=partner` to the `Address` of the agreement; `routes/receive.route.xml`: `as4:/as4/in` on port
4090. Beans in `context.xml`: `as4-key` (our PKCS#12 key pair), `partner-cert`, `partner` (`As4Partner`, the
agreement), `as4-node` (`As4ConnectionFactory`), `as4-received` (ids of received messages).

Facts to keep: the answer is a signed receipt in the HTTP response, there is no MDN (that is AS2); signing,
encryption and compression are mandated by the profile; keys and certificates come from files named by
configuration keys, never from the markup; the partner's ebMS error (`As4ErrorSignalException`) is not retried.
Keys: `as4.keyFile`, `as4.keyPassword`, `as4.partnerCertificateFile` required, the rest have defaults.
`deploy/docker-compose.holodeck.yml` is a test stand with Holodeck B2B as the partner, not a deployment.

Usually changed: the source of the documents, the receiving route's work, more `As4Partner` beans,
`PartnerTrustAnchors` (trust in a certification authority) instead of a pinned certificate.

`routes/CreditNoteSendRoute.cs` is a second sender in C# on its own timer (`as4.creditNotePeriod`) to the same
partner: `exchange.In.ContentType = "application/xml"` gives the payload its MimeType, and its own two
`OnException` blocks repeat the reception awareness of `send.route.xml` (that file's handlers cover XML routes
only). One `as4:` receiver per path: keep it in XML.
