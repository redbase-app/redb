# RedbIdentityApp

An application on redb.Identity: a Tsak module that runs in the same worker as Identity, and a Blazor web
server where users sign in with OpenID Connect. The sample data is a list of notes per user and, for
administrators, Identity's user list.

- **The module** (`RedbIdentityApp.Module`) is a redb.Route REST API. Every request carries a bearer token,
  and the module asks Identity about it (token introspection). Identity is in the same process, so the
  call goes over `direct-vm`: no network, no TLS, no serialization.
- **The web server** (`RedbIdentityApp.Web`) is the backend-for-frontend. Users sign in at Identity (code
  flow with PKCE); the browser holds only a session cookie, the tokens stay on the server, and the server
  calls the module with the user's access token.

The web server is optional: `dotnet new redb-identity-app --web false` creates the module alone, for an API
that other clients call. `--framework net8.0`, `net9.0` or `net10.0` (the default) sets the target framework of
every project.

## Run

Identity and the module run in the `redb-identity-full` image (Tsak worker, Tsak dashboard, Identity's own
sign-in and admin UI). From the project folder:

```bash
cp deploy/.env.example deploy/.env          # set the secrets: 32 random characters or more each
pwsh deploy/pack-tpkg.ps1                   # -> deploy/output/RedbIdentityApp.tpkg
docker compose -f deploy/docker-compose.yml up -d --build
```

Open http://localhost:5085 and sign in as `admin` with `IDENTITY_ADMIN_PASSWORD`. Identity answers at
https://localhost:5002 with the image's self-signed certificate: the browser warns once.

To work on the web server, stop its container (`docker compose -f deploy/docker-compose.yml stop web`) and
run it on the host: `dotnet run --project RedbIdentityApp.Web`. In the Development environment it accepts
Identity's self-signed certificate and takes its client secret from `appsettings.Development.json`, the same
value as `WEB_CLIENT_SECRET` in `deploy/.env.example`.

After a change to the module, pack it again: the worker reloads `deploy/output/RedbIdentityApp.tpkg` on its
own.

## Projects

| Project | What it is |
|---------|------------|
| `RedbIdentityApp.Contracts` | What the web server and the module exchange. No RedBase |
| `RedbIdentityApp.Module` | The module: `InitRoute.main`, the controllers, the calls into Identity |
| `RedbIdentityApp.Web` | The web server: Blazor pages, the OpenID Connect sign-in, the token store, the module client |

## Where things are

| File | What it does |
|------|--------------|
| `RedbIdentityApp.Module/InitRoute.cs` | The API endpoint with the bearer check, the gate, the controllers; the route for Identity's events |
| `RedbIdentityApp.Module/Identity/TokenIntrospector.cs` | Introspection over direct-vm and its cache |
| `RedbIdentityApp.Module/Identity/BearerTokenValidator.cs` | What a token becomes: a principal, a 401, or "Identity is down" |
| `RedbIdentityApp.Module/Identity/IdentityGate.cs` | 503 when Identity is down, 403 without the API scope |
| `RedbIdentityApp.Module/Identity/IdentityManagement.cs` | The management API in process, with the module's service token |
| `RedbIdentityApp.Module/Identity/IdentitySetup.cs` | The module's scopes and clients at Identity, created and kept up to date |
| `RedbIdentityApp.Module/Identity/RevocationEvents.cs` | Drops cached token checks when Identity reports a revocation |
| `RedbIdentityApp.Module/Controllers/` | `/api/me`, `/api/notes`, `/api/admin/users` |
| `RedbIdentityApp.Web/Program.cs` | The cookie session, the OpenID Connect handler, sign-in and sign-out |
| `RedbIdentityApp.Web/Auth/` | The server-side token store and the refresh |
| `RedbIdentityApp.Web/Services/ModuleClient.cs` | The module's API with the user's token |

## How the module checks a token

The API endpoint has `inboundAuth=bearer`: redb.Route takes the bearer token and hands it to
`BearerTokenValidator`, which asks Identity (RFC 7662). The module introspects as its own client, the
**resource client** `redbidentityapp-api`, with its own secret. Identity decides whether the token is good:
signature, expiry, revocation. The module checks what is left to a resource server (RFC 9068 §4): the token
is an access token, and its audience names the resource client. A token names that client in `aud` only when
it was issued with a scope whose `Resources` list it, the API scope `redbidentityapp.api` or the administrative
one. Identity itself answers `active: false` to a client outside a token's audience; the module checks `aud`
as well, so a token issued for another client or API is turned away even by an Identity that does not.

| The request | The answer |
|-------------|------------|
| No token, or one Identity calls inactive (expired, revoked, issued for another API) | 401, `WWW-Authenticate: Bearer realm="redbidentityapp", error="invalid_token"` |
| A good token without the API scope | 403, `WWW-Authenticate: Bearer error="insufficient_scope", scope="redbidentityapp.api"` |
| A good token, an administrative call, no administrative scope | 403, the same header with `scope="redbidentityapp.admin"` |
| Identity down, not started yet, or refusing the module's own secret | 503 with `Retry-After`; never 401, because the token may be good |

Answers are cached by the SHA-256 of the token (the token itself is not stored):

- an active answer for `Identity:Introspection:CacheSeconds` (60), never past the token's own expiry;
- an inactive one for `InactiveCacheSeconds` (10);
- concurrent requests with one token share one introspection;
- nothing is cached when Identity could not be asked.

### Revocation

A cached answer outlives a revocation by at most the cache time, unless Identity reports it. The compose
file makes it report: Identity's audit sends its events to `direct-vm://redbidentityapp-identity-events`
(`Identity:Audit:Targets` of `identity.core`), and the module drops the cached answers of the user named in a
revocation, a sign-out, a password change, a disabled or deleted account; a changed client or signing key
drops all of them, and so does an event that names no user. Removing the module means removing that audit target too, otherwise Identity logs an
error for every event it cannot deliver.

## Administrators and the management API

The administrative scope `redbidentityapp.admin` is attached to Identity's `admin` role, and Identity's
`AdminScopeEntitlement:GatedScopes` lists it (compose file): Identity puts it into a token only for users
whose role carries it, and drops it for everyone else. The web server asks for it for every user; the
module answers 403 when it is missing. A role only entitles a scope the client asked for, so a token of some
other client never carries the module's administrative scope. Assign the `admin` role in Identity's UI to make someone an
administrator; the change applies at their next sign-in or token refresh.

`/api/admin/users` reads Identity's management API in process. The call runs as the module's service
client (Identity's `SeedBackchannelClient`, scope `identity:manage`), not as the user: the user's token
proves the right, the module does the work. Every management route is closed until the same exchange has
passed Identity's management gate with that token (`IdentityManagement.CallAsync`).

## What the module sets up at Identity

On every start, in the background, the module brings Identity to its settings (`IdentitySetup`):

1. the scopes `redbidentityapp.api` and `redbidentityapp.admin`, with the resource client in their `Resources`;
2. the resource client, allowed to introspect;
3. the web server's client: confidential, code flow with PKCE, refresh tokens, no consent page, the redirect
   addresses of `Identity:WebRedirectUris` and `Identity:WebPostLogoutRedirectUris`;
4. the administrative scope on the `admin` role.

A client or scope that exists already is updated to the settings (permissions, redirect addresses,
resources), so a changed address takes effect at the next start. Secrets cannot be read back: after a
changed secret Identity answers `invalid_client`, the log names the client, and the fix is to rotate the
secret in Identity's UI and set the new value, or to start with an empty database.

While Identity is still starting, the setup retries; the API answers 503 meanwhile.

## The web server

- Sign-in: `/account/login` redirects to Identity; the answer comes back on `/signin-oidc` in the query of a
  top-level redirect, so every cookie can be `SameSite=Lax`.
- The tokens go to `UserTokenStore` (an `IDistributedCache`, encrypted with DataProtection); the cookie holds
  a random session key and the granted scopes. A cookie whose tokens are gone is no session, and the user
  goes through Identity again, usually without a password.
- `AccessTokens` refreshes the access token a minute before it expires, one refresh per session at a time,
  because Identity rotates refresh tokens. A refused refresh ends the session; Identity being unreachable
  does not: the current token is used while it is valid, then the page says to try again.
- Sign-out (`POST /account/logout`) revokes the refresh token at Identity and ends the session there too.

The token store is in memory: one server, and a restart signs users out of this application. For several
replicas register a shared cache (`AddStackExchangeRedisCache`) and a shared DataProtection key ring.

## Settings

The module reads its route context: `RedbIdentityApp.Module.config.json` first, then
`Tsak:Contexts:redbidentityapp:Override`, which wins. The secrets exist only in the Override layer, as
environment variables of the worker (`deploy/.env`):

| Environment variable | What it is |
|----------------------|------------|
| `Tsak__Contexts__redbidentityapp__Override__Identity__ResourceClientSecret` | The resource client's secret |
| `Tsak__Contexts__redbidentityapp__Override__Identity__ManagementClientSecret` | Identity's `SeedBackchannelClient:ClientSecret` |
| `Tsak__Contexts__redbidentityapp__Override__Identity__WebClientSecret` | The web server's client secret, also `Identity__ClientSecret` of the web server |

The web server reads `appsettings.json`: Identity's address, its client, the scopes, the module's address.

## Deploy

| File | What runs |
|------|-----------|
| `deploy/docker-compose.yml` | Identity and the module in the `redb-identity-full` image on SQLite, and the web server |
| `deploy/docker-compose.postgres.yml`, `deploy/docker-compose.mssql.yml` | Add them with a second `-f`: the same on a database server |

For a real deployment:

- give Identity a real certificate and its public https address as the issuer
  (`Identity:Issuer` of `identity.core` and `IdentityTransport:Issuer` of `identity.http`), and drop the
  development certificate lines of `RedbIdentityApp.Web/Dockerfile`;
- set the web server's public address in `Identity:WebRedirectUris` and `Identity:WebPostLogoutRedirectUris`
  of the module, and `Identity__Authority` of the web server;
- keep the module's API port inside the worker's network: only the web server calls it.

`pack-tpkg.ps1` puts the manifest, the module config, the module DLL and the dependencies the worker does
not provide into the package. `deploy/shipped-module-deps.txt` lists what the worker provides, including
`redb.Identity.Contracts.dll`, which comes from Identity's contracts carrier: a second copy in the package
would split the contract types between the module and Identity.

## Talking to Identity over the network instead

This template puts the module next to Identity on purpose: the calls are in process. An application that
runs elsewhere uses Identity's HTTP endpoints instead (discovery, `/connect/token`, `/connect/introspect`,
the management API under `/api/v1/identity`), as the scripts in redb.Identity's `demos` folder do.
