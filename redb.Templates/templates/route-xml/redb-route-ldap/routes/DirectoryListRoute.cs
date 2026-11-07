using redb.Route.Core;
using redb.Route.Ldap;

namespace RouteModule.Routes;

/// <summary>
/// A sibling of routes/directory.route.xml in C#: the user list with their mail addresses, on a timer of its own,
/// through the same ldap connection bean. A search only reads, so it never competes with the XML watch.
/// </summary>
public sealed class DirectoryListRoute : RouteBuilder
{
    protected override void Configure()
    {
        // Every {{ldap.listPeriod:60000}} ms, the first time 10 seconds after start: a SEARCH whose body is the list of
        // entries, read as typed LdapEntry objects.
        From("timer://cs-modulectx-ldap-list?period={{ldap.listPeriod:60000}}&delay=10000")
            .RouteId("cs-modulectx-ldap-list")
            .Description("Lists the users and their mail addresses")
            .To("ldap:SEARCH:{{ldap.usersDn}}?connectionFactory=ldap&filter=(objectClass=inetOrgPerson)&attributes=uid,mail")
            .Process(exchange =>
            {
                var users = ((IEnumerable<LdapEntry>)exchange.In.Body!)
                    .Select(entry => $"{entry.GetString("uid")} <{entry.GetString("mail")}>")
                    .Order(StringComparer.Ordinal);
                exchange.In.Body = "users: " + string.Join(", ", users);
            })
            .Log("${body}");
    }
}
