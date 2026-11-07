LDAP (`redb.Route.Ldap`), `routes/directory.route.xml`: the bean `ldap` (`LdapConnectionFactory`: Server, Port,
BindDn, BindPassword) in `context.xml`; every URI says `connectionFactory=ldap`. Three routes:
`ldap:WATCH:{{ldap.usersDn}}?...&amp;initialLoad=true` (every entry once, then the changed ones; the body is an
`LdapEntry` with `Dn`, `Attributes`, `ChangeType`), `ldap:SEARCH:...` on a timer (the count is
`redbLdap.ResultCount`), and `direct://modulectx-check-password`, a `ldap:BIND` with the credentials in the headers
`redbLdap.AuthDn` and `redbLdap.AuthPassword` (`redbLdap.BindResult` 0 = accepted, 49 = invalid credentials).

Facts to keep: the URI form is `ldap:OPERATION:baseDn?options`; a filter with parentheses goes into `filter=` as is;
`{{key}}` is resolved in URIs and bean values, not in `setHeader value=`, so credentials for BIND come from the
caller's headers; `ldap.bindPassword` is a required key from the environment. `deploy/docker-compose.ldap.yml`
starts OpenLDAP with `deploy/ldap/users.ldif`.

Usually changed: the base DNs and the filter, the attributes, what the watch does with an entry, the caller of the
password check (an HTTP route of `redb.Route.Http`).

`routes/DirectoryListRoute.cs` is a C# `SEARCH` on its own timer (`ldap.listPeriod`) that reads the body as
`IEnumerable<LdapEntry>` and logs the users with their mail addresses. Only one `ldap:WATCH` per directory and
filter: keep the watch in XML.
