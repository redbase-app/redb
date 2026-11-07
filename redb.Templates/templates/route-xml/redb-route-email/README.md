# RouteModule

A Tsak module that sends and reads e-mail, written in XML markup (`urn:redb:route:1.0`) and in C#:

| Route | What it does |
|---|---|
| `modulectx-mail-send` | every minute sends a short report over SMTP to `mail.to` |
| `modulectx-mail-read` | every 30 seconds reads the unseen messages of the IMAP inbox, one exchange each, and marks each read once the route finished it |
| `cs-modulectx-mail-html` | C#: every two minutes sends an HTML report with a table to `mail.to`, with the same retries |

The module is a package (`.tpkg`) that a Tsak worker loads from its `modules/` folder; there is no `Main`
to run.

## Layout

| Path | What |
|---|---|
| `context.xml` | the `smtp` and `imap` connections: host, port, security, user, password; the C# route builder |
| `routes/send.route.xml` | the sender and its retries |
| `routes/read.route.xml` | the reader; a separate file so that the sender's error handling does not mark a failed message read |
| `routes/HtmlReportRoute.cs` | the HTML sender in C# |
| `config/routemodule.config.json` | identity inside the package: context name and auto start |
| `config/context.sample.json` | deployment sample of the context configuration |
| `deploy/docker-compose.mail.yml` | GreenMail with one test mailbox, and the module on a Tsak worker |
| `.config/dotnet-tools.json` | the `redb-route-xml` tool, same version as the redb packages |

## Build the package

    dotnet tool restore
    dotnet build -p:PackRouteOnBuild=true -p:Version=1.0.0

The build checks the markup, every `smtp://` and `imap://` option against the connector, the beans and the
configuration keys, then writes `pkg/routemodule-1.0.0.tpkg`. No mail server is needed for the build.

## Try it with GreenMail

    dotnet build -p:PackRouteOnBuild=true -p:Version=1.0.0
    cp deploy/.env.example deploy/.env          # then set MAIL_PASSWORD
    docker compose -f deploy/docker-compose.mail.yml up -d

The module sends to its own mailbox, so the log shows both directions: `sent Report 1`, then
`received from modulectx@example.org: Report 1 / Report number 1 ...`.

## Configuration

| Key | Default | What |
|---|---|---|
| `mail.smtpHost`, `mail.smtpPort`, `mail.smtpSecurity` | `localhost`, `587`, `StartTls` | the outgoing server |
| `mail.imapHost`, `mail.imapPort`, `mail.imapSecurity` | `localhost`, `993`, `Ssl` | the mailbox server |
| `mail.username` | none, required | the login of both servers |
| `mail.password` | none, required | its password; from the environment |
| `mail.from`, `mail.to` | none, required | the sender and the recipient of the reports |
| `mail.sendPeriod`, `mail.pollPeriod` | `60000`, `30000` | the intervals, ms |
| `mail.htmlPeriod` | `120000` | the interval of the C# HTML report, ms |

In a deployment the password goes into the environment of the worker, in the `Override` section:

    Tsak__Contexts__modulectx__Override__mail.password: ${MAIL_PASSWORD}

## An HTML report in C#

`routes/HtmlReportRoute.cs` is a `RouteBuilder` that `context.xml` adds with
`<routeBuilder type="RouteModule.Routes.HtmlReportRoute, RouteModule"/>`. On a timer of its own it sends through
the same `smtp` bean to the same recipient, with the same two retries; the XML reader then receives it from the
mailbox like the plain reports. The log shows `sent HTML report 1`.

The difference to notice: XML sets the subject and the text with expressions; C# builds the HTML in code,
encodes the values with `WebUtility.HtmlEncode`, sets the subject in the header `MailHeaders.Subject` and sends
with `contentType=text/html`. Its retries are its own `OnException`: the one of `send.route.xml` covers the XML
routes only.

## Change it

- **Recipients per message.** Set the headers `redbMail.To`, `redbMail.Cc`, `redbMail.Bcc` before the `smtp://`
  step; they win over the URI.
- **HTML.** `contentType=text/html` on the sender, `alternativeBody=true` for a plain text part too.
- **Attachments.** `attachments=` on the sender; on the reader `fetchAttachments` and `maxAttachmentSize`.
- **Which messages.** `subjectFilter`, `fromFilter`, `searchQuery`, another `folder`; `idle=true` for IMAP push
  instead of polling.
- **After reading.** `postProcess=Delete`, or `Move` with `moveTo=Processed`.
- **OAuth.** `AccessToken` and `AuthMechanism` on the bean instead of a password.

## Editor

The `redb Route XML` extension for VS Code (`redb.redb-route-xml`) binds the schema by namespace and
completes elements and endpoint options. Without the extension, generate the schema from the build output
and point the XML extension at it:

    dotnet redb-route-xml xsd bin/Debug/net10.0 --out schema

Documentation: [redbase.app](https://redbase.app)
