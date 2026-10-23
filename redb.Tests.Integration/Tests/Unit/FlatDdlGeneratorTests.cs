using redb.Core.Attributes;
using redb.Core.Models.Entities;
using redb.Export.Flat;
using redb.Export.Flat.Model;

namespace redb.Tests.Integration.Tests.Unit;

/// <summary>
/// DDL of the flat export per provider, without a database: the shared key sequence, the key defaults, the delete
/// rules that mirror redb, SQL Server's refusal of a cascade from a table to itself, and the foreign key indexes.
/// The model is built from the classes below without structure ids, which the DDL does not need.
/// </summary>
public class FlatDdlGeneratorTests
{
    [RedbScheme("FlatDdlTarget")]
    public class FlatDdlTargetProps
    {
        public string Name { get; set; } = "";
    }

    [RedbScheme("FlatDdlOwner")]
    public class FlatDdlOwnerProps
    {
        public string Title { get; set; } = "";
        public FlatDdlAddress? Address { get; set; }
        public string[]? Tags { get; set; }
        public RedbObject<FlatDdlTargetProps>? Target { get; set; }
        public RedbListItem? Status { get; set; }
    }

    public class FlatDdlAddress
    {
        public string City { get; set; } = "";
    }

    private static readonly FlatDatabaseModel Model =
        new FlatModelBuilder().Build([typeof(FlatDdlOwnerProps), typeof(FlatDdlTargetProps)]);

    private static FlatTable Table(string name) => Model.Tables.Single(t => t.Name == name);

    private static string Fks(FlatDdlGenerator ddl, string table) =>
        string.Join("\n", ddl.AddForeignKeys(Table(table)));

    private static string Indexes(FlatDdlGenerator ddl) =>
        string.Join("\n", Model.Tables.SelectMany(t => ddl.CreateIndexes(t)));

    [Fact]
    public void Postgres_SequenceKeyDefaultsDeleteRulesAndIndexes()
    {
        var ddl = new FlatDdlGenerator("postgres");

        Assert.Equal("CREATE SEQUENCE \"flat_identity\" AS bigint START WITH 1", ddl.CreateSequence());
        Assert.Equal("ALTER SEQUENCE \"flat_identity\" RESTART WITH 42", ddl.RestartSequence(42));

        // Keys a writer may leave out take the sequence; the scheme table takes the id of its header row.
        const string keyDefault = "DEFAULT nextval('\"flat_identity\"')";
        Assert.Contains(keyDefault, ddl.CreateTable(Table("objects")));
        Assert.Contains(keyDefault, ddl.CreateTable(Table("FlatDdlOwnerProps.Tags")));
        Assert.DoesNotContain(keyDefault, ddl.CreateTable(Table("FlatDdlOwnerProps")));

        Assert.Contains("FOREIGN KEY (\"_id_parent\") REFERENCES \"objects\" (\"_id\") ON DELETE CASCADE", Fks(ddl, "objects"));
        Assert.Contains("FOREIGN KEY (\"_id\") REFERENCES \"objects\" (\"_id\") ON DELETE CASCADE", Fks(ddl, "FlatDdlOwnerProps"));
        Assert.Contains("FOREIGN KEY (\"_owner_id\") REFERENCES \"FlatDdlOwnerProps\" (\"_id\") ON DELETE CASCADE", Fks(ddl, "FlatDdlOwnerProps.Address"));
        Assert.Contains("FOREIGN KEY (\"_owner_id\") REFERENCES \"FlatDdlOwnerProps\" (\"_id\") ON DELETE CASCADE", Fks(ddl, "FlatDdlOwnerProps.Tags"));
        Assert.Contains("FOREIGN KEY (\"_id_object\") REFERENCES \"objects\" (\"_id\") ON DELETE SET NULL", Fks(ddl, "list_items"));

        // A reference to another object keeps the referenced row: no delete rule.
        var target = ddl.AddForeignKeys(Table("FlatDdlOwnerProps")).Single(s => s.Contains("(\"Target\")"));
        Assert.EndsWith("REFERENCES \"FlatDdlTargetProps\" (\"_id\")", target);

        var indexes = Indexes(ddl);
        Assert.Contains("ON \"objects\" (\"_id_parent\")", indexes);
        Assert.Contains("ON \"FlatDdlOwnerProps\" (\"Target\")", indexes);
        Assert.Contains("ON \"FlatDdlOwnerProps\" (\"Status\")", indexes);
        Assert.Contains("ON \"list_items\" (\"_id_list\")", indexes);
        Assert.Contains("ON \"list_items\" (\"_id_object\")", indexes);
    }

    [Fact]
    public void MsSql_SequenceKeyDefaultsAndNoSelfCascade()
    {
        var ddl = new FlatDdlGenerator("mssql");

        Assert.Equal("CREATE SEQUENCE [flat_identity] AS bigint START WITH 1", ddl.CreateSequence());
        Assert.Equal("ALTER SEQUENCE [flat_identity] RESTART WITH 42", ddl.RestartSequence(42));

        const string keyDefault = "DEFAULT (NEXT VALUE FOR [flat_identity])";
        Assert.Contains(keyDefault, ddl.CreateTable(Table("objects")));
        Assert.DoesNotContain(keyDefault, ddl.CreateTable(Table("FlatDdlOwnerProps")));

        // SQL Server refuses a cascade from a table to itself: the parent key goes without it.
        var parent = ddl.AddForeignKeys(Table("objects")).Single(s => s.Contains("([_id_parent])"));
        Assert.EndsWith("REFERENCES [objects] ([_id])", parent);

        // Cascades between different tables stay.
        Assert.Contains("FOREIGN KEY ([_id]) REFERENCES [objects] ([_id]) ON DELETE CASCADE", Fks(ddl, "FlatDdlOwnerProps"));
        Assert.Contains("FOREIGN KEY ([_owner_id]) REFERENCES [FlatDdlOwnerProps] ([_id]) ON DELETE CASCADE", Fks(ddl, "FlatDdlOwnerProps.Tags"));
        Assert.Contains("FOREIGN KEY ([_id_object]) REFERENCES [objects] ([_id]) ON DELETE SET NULL", Fks(ddl, "list_items"));

        var indexes = Indexes(ddl);
        Assert.Contains("ON [objects] ([_id_parent])", indexes);
        Assert.Contains("ON [FlatDdlOwnerProps] ([Target])", indexes);
        Assert.Contains("ON [FlatDdlOwnerProps] ([Status])", indexes);
    }

    [Fact]
    public void Sqlite_NoSequenceInlineForeignKeysAndIndexes()
    {
        var ddl = new FlatDdlGenerator("sqlite");

        // SQLite hands out keys per table: no sequence, no default on the key.
        Assert.Null(ddl.CreateSequence());
        Assert.Null(ddl.RestartSequence(42));
        Assert.DoesNotContain("DEFAULT", ddl.CreateTable(Table("objects")));

        // Foreign keys are part of CREATE TABLE; there is nothing to add after the load.
        Assert.Empty(Model.Tables.SelectMany(t => ddl.AddForeignKeys(t)));
        Assert.Contains("FOREIGN KEY (\"_id_parent\") REFERENCES \"objects\" (\"_id\") ON DELETE CASCADE", ddl.CreateTable(Table("objects")));
        Assert.Contains("FOREIGN KEY (\"_owner_id\") REFERENCES \"FlatDdlOwnerProps\" (\"_id\") ON DELETE CASCADE", ddl.CreateTable(Table("FlatDdlOwnerProps.Tags")));
        Assert.Contains("FOREIGN KEY (\"_id_object\") REFERENCES \"objects\" (\"_id\") ON DELETE SET NULL", ddl.CreateTable(Table("list_items")));

        var indexes = Indexes(ddl);
        Assert.Contains("ON \"objects\" (\"_id_parent\")", indexes);
        Assert.Contains("ON \"FlatDdlOwnerProps\" (\"Target\")", indexes);
    }

    [Theory]
    [InlineData("postgres")]
    [InlineData("mssql")]
    [InlineData("sqlite")]
    public void Script_CountsTablesForeignKeysAndIndexes(string provider)
    {
        var script = new FlatDdlGenerator(provider).Script(Model);

        var foreignKeys = Model.Tables.Sum(t => t.ForeignKeys.Count);
        var indexes = Model.Tables.Sum(t => t.Indexes.Count);
        Assert.StartsWith($"-- redb flat schema: {Model.Tables.Count} tables, {foreignKeys} foreign keys, {indexes} indexes.", script);
        Assert.Equal(indexes, script.Split("CREATE INDEX ").Length - 1);
    }
}
