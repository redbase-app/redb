SQL batches (`redb.Route.Sql`), `routes/orders.route.xml`: a timer sends single orders (headers `orderId`,
`amount`) to `seda://modulectx-orders`; `<aggregate strategy="groupedExchange" completionSize="20">` makes a
batch, and one `sql:INSERT ...?batchSize=100&amp;breakBatchOnError=false` writes it in one transaction, each
item binding `:#orderId` and `:#amount` from its own headers. A row the database refuses (the table has
`CHECK (amount >= 0)`) is undone under its savepoint and listed in `redbSql.batchErrors`. The connection is
the bean `main-db` in `context.xml` with `ProviderName` `Microsoft.Data.Sqlite`, `Npgsql` or
`Microsoft.Data.SqlClient`.

Key: `db.connection` (SQLite has a default file).

Usually changed: the table and the columns; `completionSize` and `completionTimeout` (rows per
transaction); `breakBatchOnError` left out for all-or-nothing batches.

`routes/OrdersBatchRoute.cs` writes the same table in C# on its own timer (`batch.period`): the body is a
`List<Order>` of a C# record and `:#orderId`, `:#amount` bind from its properties; no aggregator.
