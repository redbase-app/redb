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

// Case folding. Each needs its own collection: the PostgreSQL pair shares one C-ctype database and
// must not interleave, and SQLite installs the feature per connection on a file of its own.
[CollectionDefinition("PostgresCollation")]
public class PostgresCollationCollection : ICollectionFixture<PostgresCollationFixture>;

[CollectionDefinition("PostgresNoCollation")]
public class PostgresNoCollationCollection : ICollectionFixture<PostgresNoCollationFixture>;

[CollectionDefinition("SqliteCollation")]
public class SqliteCollationCollection : ICollectionFixture<SqliteCollationFixture>;

[CollectionDefinition("PostgresProCollation")]
public class PostgresProCollationCollection : ICollectionFixture<PostgresProCollationFixture>;
