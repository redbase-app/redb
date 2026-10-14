using System.Text;
using redb.Export.Flat.Dialects;
using redb.Export.Flat.Model;

namespace redb.Export.Flat;

/// <summary>
/// Turns a <see cref="FlatDatabaseModel"/> into DDL for one provider.
/// <para>
/// Tables are created with plain <c>CREATE TABLE</c> - never <c>IF NOT EXISTS</c> - so a name that
/// is already taken fails loudly instead of two classes sharing one table. Where the provider
/// allows it, foreign keys are separate statements meant to run after the data is loaded.
/// </para>
/// </summary>
public sealed class FlatDdlGenerator
{
    private readonly IFlatDialect _dialect;

    /// <summary>Creates a generator for a provider.</summary>
    /// <param name="providerName">Provider name as reported by <c>IDataProvider.Name</c>: <c>postgres</c>, <c>mssql</c> or <c>sqlite</c>.</param>
    public FlatDdlGenerator(string providerName)
        : this(FlatDialects.For(providerName))
    {
    }

    internal FlatDdlGenerator(IFlatDialect dialect)
    {
        _dialect = dialect;
    }

    /// <summary>The <c>CREATE TABLE</c> statement of <paramref name="table"/>.</summary>
    public string CreateTable(FlatTable table)
    {
        var lines = new List<string>();

        foreach (var column in table.Columns)
        {
            var type = _dialect.ColumnType(FlatStorageTypes.Of(column));
            lines.Add($"{_dialect.Quote(column.Name)} {type} {(column.IsNullable ? "NULL" : "NOT NULL")}");
        }

        lines.Add($"CONSTRAINT {_dialect.Quote(table.PrimaryKeyName)} PRIMARY KEY ({_dialect.Quote(table.PrimaryKey)})");

        foreach (var unique in table.UniqueConstraints)
            lines.Add($"CONSTRAINT {_dialect.Quote(unique.Name)} UNIQUE ({string.Join(", ", unique.Columns.Select(_dialect.Quote))})");

        if (_dialect.InlineForeignKeys)
        {
            foreach (var foreignKey in table.ForeignKeys)
                lines.Add($"CONSTRAINT {_dialect.Quote(foreignKey.Name)} {ForeignKeyClause(foreignKey)}");
        }

        var sql = new StringBuilder();
        sql.Append("CREATE TABLE ").Append(_dialect.Quote(table.Name)).AppendLine(" (");
        sql.AppendLine("    " + string.Join("," + Environment.NewLine + "    ", lines));
        sql.Append(')');
        return sql.ToString();
    }

    /// <summary>
    /// The statements that add the foreign keys of <paramref name="table"/>. Empty for a provider
    /// that declares them inside <c>CREATE TABLE</c>.
    /// </summary>
    public IReadOnlyList<string> AddForeignKeys(FlatTable table)
    {
        if (_dialect.InlineForeignKeys)
            return [];

        return table.ForeignKeys
            .Select(foreignKey =>
                $"ALTER TABLE {_dialect.Quote(table.Name)} ADD CONSTRAINT {_dialect.Quote(foreignKey.Name)} {ForeignKeyClause(foreignKey)}")
            .ToList();
    }

    /// <summary>The whole model as one script: every table, then every foreign key.</summary>
    public string Script(FlatDatabaseModel model)
    {
        var statements = model.Tables.Select(table => CreateTable(table))
            .Concat(model.Tables.SelectMany(table => AddForeignKeys(table)));

        return string.Join(Environment.NewLine + Environment.NewLine, statements.Select(s => s + ";"));
    }

    private string ForeignKeyClause(FlatForeignKey foreignKey) =>
        $"FOREIGN KEY ({_dialect.Quote(foreignKey.Column)}) " +
        $"REFERENCES {_dialect.Quote(foreignKey.TargetTable)} ({_dialect.Quote(foreignKey.TargetColumn)})";
}
