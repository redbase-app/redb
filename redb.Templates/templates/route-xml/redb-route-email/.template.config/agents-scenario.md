E-mail (`redb.Route.Mail`), `routes/send.route.xml` and `routes/read.route.xml`: two beans in `context.xml`, `smtp` and `imap`
(`MailConnectionFactory`: Host, Port, Security `None`, `StartTls` or `Ssl`, Username, Password). The sender posts
with `smtp://...?connectionFactory=smtp&amp;from={{mail.from}}&amp;to={{mail.to}}`, the subject from the header
`redbMail.Subject`; the reader polls `imap://...?connectionFactory=imap&amp;fetchFilter=Unseen&amp;postProcess=MarkRead`
(the text body is the body; `redbMail.From`, `redbMail.Subject`, `redbMail.HasAttachments` and the rest are headers).

Facts to keep: `disconnect=true` on the sender opens a connection per message (a server closes an idle one, a pooled
connection would then fail the next send); the sender's `onException` lives in its own file, an `onException` with
`handled="true"` next to the reader would mark a failed message read; `{{key}}` is resolved in URIs and bean values, not in a log `<message>` or `setHeader value=`; the
recipients set on the URI can be overridden per message by the headers `redbMail.To`, `redbMail.Cc`, `redbMail.Bcc`;
a message the route failed stays unseen and is read again; `mail.password` comes from the environment.
`deploy/docker-compose.mail.yml` runs GreenMail with one mailbox.

Usually changed: the servers and security, the recipients, the folder and the filter (`subjectFilter`, `fromFilter`,
`searchQuery`), `postProcess` (`MarkRead`, `Delete`, `Move` with `moveTo`), what the reader does with a message.

`routes/HtmlReportRoute.cs` is a second sender in C# on its own timer (`mail.htmlPeriod`): HTML built in code,
`contentType=text/html`, subject in `MailHeaders.Subject`, its own `OnException` with two retries (the one of
`send.route.xml` does not cover a C# builder). Keep one IMAP reader per mailbox folder: the XML one.
