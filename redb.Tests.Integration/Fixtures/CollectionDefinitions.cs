namespace redb.Tests.Integration.Fixtures;

[CollectionDefinition("Postgres")]
public class PostgresCollection : ICollectionFixture<PostgresFixture>;

[CollectionDefinition("MsSql")]
public class MsSqlCollection : ICollectionFixture<MsSqlFixture>;

[CollectionDefinition("PostgresPro")]
public class PostgresProCollection : ICollectionFixture<PostgresProFixture>;

[CollectionDefinition("MsSqlPro")]
public class MsSqlProCollection : ICollectionFixture<MsSqlProFixture>;

[CollectionDefinition("Sqlite")]
public class SqliteCollection : ICollectionFixture<SqliteFixture>;

[CollectionDefinition("SqlitePro")]
public class SqliteProCollection : ICollectionFixture<SqliteProFixture>;
