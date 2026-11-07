SQL outbox (`redb.Route.Sql`), `routes/outbox.route.xml`: a timer inserts rows into `orders_outbox`, a poll
consumer (`mode=Poll`) reads the rows with status 0 and sets status 1 (`onSuccess`) or status 2 and the
message (`onFailure`, `:#redbError`). The connection is the bean `main-db` (`SqlConnectionFactory`) in
`context.xml` with `ProviderName` `Microsoft.Data.Sqlite`, `Npgsql` or `Microsoft.Data.SqlClient`; its
`onInit` creates the table in that database's dialect.

Facts to keep: parameters are `:#name` bound from `param.name=...`, a header or a column of the polled row;
the SQL text must not contain `?`; do not add `transacted=true` to the poll (a failure would roll back its
`onFailure` and the row would be read again forever), and do not put an `onException` with
`handled="false"` around it in this version.

Key: `db.connection` (SQLite has a default file).

`routes/OutboxReportRoute.cs` is a C# report on its own timer (`report.period`): a read-only
`SELECT status, COUNT(*) ... GROUP BY status` with `outputType=SelectList` through `#main-db`. A C# route must not
poll `orders_outbox` for status 0: the XML reader owns those rows.
