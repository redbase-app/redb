using redb.Core;
using redb.Core.Models.Entities;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// The SQLite row mapper (review SL-57). A column value that did not convert to the property type went through two
/// silent catches and the property kept its default - a row read as if the column were NULL. SQLite's dynamic typing
/// lets any column hold text, so such a row is one bad write away. Free and Pro share the connection.
/// </summary>
public abstract class SqliteRowMapperTestsBase
{
    protected readonly IRedbService Redb;

    protected SqliteRowMapperTestsBase(IRedbService redb) => Redb = redb;

    [Fact]
    public async Task AColumnValueThatDoesNotConvert_IsRefused()
    {
        var read = async () => await Redb.Context.QueryFirstOrDefaultAsync<RedbObjectRow>("SELECT 'abc' AS _value_long");

        await read.Should().ThrowAsync<InvalidOperationException>().WithMessage("*_value_long*abc*");
    }

    [Fact]
    public async Task AColumnValueThatConverts_IsRead()
    {
        var row = await Redb.Context.QueryFirstOrDefaultAsync<RedbObjectRow>("SELECT 42 AS _value_long, 'x' AS _name");

        row!.ValueLong.Should().Be(42);
    }
}
