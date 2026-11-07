# RouteModule

A Tsak module that works with an LDAP directory, written in XML markup (`urn:redb:route:1.0`) and in C#:

| Route | What it does |
|---|---|
| `modulectx-ldap-watch` | every user entry once at start, then every entry whose `modifyTimestamp` changed, polled every 10 seconds; one exchange per entry |
| `modulectx-ldap-count` | a search every minute: how many users the directory holds |
| `modulectx-ldap-check-password` | `direct://modulectx-check-password`: a bind with the credentials of the headers `redbLdap.AuthDn` and `redbLdap.AuthPassword`; `redbLdap.BindResult` is 0 when they are right |
| `cs-modulectx-ldap-list` | C#: a search every minute that lists the users with their mail addresses |

The module is a package (`.tpkg`) that a Tsak worker loads from its `modules/` folder; there is no `Main`
to run.

## Layout

| Path | What |
|---|---|
| `context.xml` | the `ldap` connection: server, port, service account; the C# route builder |
| `routes/directory.route.xml` | the three routes |
| `routes/DirectoryListRoute.cs` | the user list in C# |
| `config/routemodule.config.json` | identity inside the package: context name and auto start |
| `config/context.sample.json` | deployment sample of the context configuration |
| `deploy/docker-compose.ldap.yml` | OpenLDAP with the test entries of `deploy/ldap/users.ldif`, and the module on a Tsak worker |
| `.config/dotnet-tools.json` | the `redb-route-xml` tool, same version as the redb packages |

## Build the package

    dotnet tool restore
    dotnet build -p:PackRouteOnBuild=true -p:Version=1.0.0

The build checks the markup, every `ldap:` option against the connector, the `ldap` bean and the configuration
keys, then writes `pkg/routemodule-1.0.0.tpkg`. No directory is needed for the build.

## Try it with OpenLDAP

    dotnet build -p:PackRouteOnBuild=true -p:Version=1.0.0
    cp deploy/.env.example deploy/.env          # then set LDAP_ADMIN_PASSWORD
    docker compose -f deploy/docker-compose.ldap.yml up -d

The module log shows `directory entry added: uid=alice,ou=users,dc=example,dc=org` and the same for bob, then
`users in the directory: 2` every minute. Add an entry with `ldapadd` in the `openldap` container and the watch logs
it within 10 seconds.

## Configuration

| Key | Default | What |
|---|---|---|
| `ldap.server` | `localhost` | directory host |
| `ldap.port` | `389` | 389, or 636 with `Ssl` on the bean |
| `ldap.bindDn` | none, required | the service account |
| `ldap.bindPassword` | none, required | its password; from the environment |
| `ldap.baseDn` | none, required | the base of the password check |
| `ldap.usersDn` | none, required | where the users are |
| `ldap.countPeriod` | `60000` | the search interval, ms |
| `ldap.listPeriod` | `60000` | the interval of the C# user list, ms |

In a deployment the password goes into the environment of the worker, in the `Override` section:

    Tsak__Contexts__modulectx__Override__ldap.bindPassword: ${LDAP_PASSWORD}

## The user list in C#

`routes/DirectoryListRoute.cs` is a `RouteBuilder` that `context.xml` adds with
`<routeBuilder type="RouteModule.Routes.DirectoryListRoute, RouteModule"/>`. On a timer of its own it sends the
same kind of `SEARCH` through the same `ldap` bean and logs
`users: alice <alice@example.org>, bob <bob@example.org>`.

The difference to notice: XML reads the result through headers and expressions (`redbLdap.ResultCount`); C#
takes the body as the list of `LdapEntry` objects it is and reads each attribute with `entry.GetString("mail")`.

## Change it

- **Active Directory.** `changeTrackingMode=usn` on the watch, `ldap.port` 636 with `Ssl`, the AD names in the
  filter (`(objectClass=user)`, `sAMAccountName`).
- **Deletions.** `detectDeletions=true` on the watch reports entries that disappeared.
- **Calling the password check.** From an HTTP route of `redb.Route.Http`: set `redbLdap.AuthDn` and
  `redbLdap.AuthPassword` from the request, `<to uri="direct://modulectx-check-password"/>`, answer by
  `redbLdap.BindResult`.
- **Writing.** `ldap:ADD:<dn>` with a dictionary body, `ldap:MODIFY:<dn>`, `ldap:DELETE:<dn>`.

## Editor

The `redb Route XML` extension for VS Code (`redb.redb-route-xml`) binds the schema by namespace and
completes elements and endpoint options. Without the extension, generate the schema from the build output
and point the XML extension at it:

    dotnet redb-route-xml xsd bin/Debug/net10.0 --out schema

Documentation: [redbase.app](https://redbase.app)
