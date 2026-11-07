using redb.Core;
using redb.Core.Attributes;
using redb.Core.Models.Entities;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// A stored collection element the Pro materializer cannot convert to the property's element type (review PR-10).
/// The same failure had three silent outcomes, chosen by the shape of the collection: a dictionary entry vanished,
/// an array element became 0, a list element was skipped. The row here is written by another writer (plain SQL),
/// as a legacy row or a narrowed property type would leave it.
/// </summary>
public abstract class ProMaterializerConversionTestsBase
{
    protected readonly IRedbService Redb;

    protected ProMaterializerConversionTestsBase(IRedbService redb) => Redb = redb;

    private const long TooBigForInt = 5_000_000_000L;

    private async Task<long> SeedWithAnOversizedElementAsync(string field)
    {
        await Redb.SyncSchemeAsync<ConversionProbeProps>();
        var id = await Redb.SaveAsync(new RedbObject<ConversionProbeProps>
        {
            name = "conversion-probe",
            Props = new ConversionProbeProps
            {
                IntList = [1, 7, 3],
                IntArray = [1, 7, 3],
                IntDict = new Dictionary<string, int> { ["a"] = 1, ["b"] = 7 }
            }
        });
        await Redb.Context.ExecuteAsync(
            $"UPDATE _values SET _Long = {TooBigForInt} WHERE _id_object = {id} AND _Long = 7 AND _id_structure = " +
            $"(SELECT _id FROM _structures WHERE _name = '{field}' AND _id_scheme = (SELECT _id_scheme FROM _objects WHERE _id = {id}))");
        await Redb.Context.ExecuteAsync($"UPDATE _objects SET _hash = NULL WHERE _id = {id}");
        return id;
    }

    [Theory]
    [InlineData(nameof(ConversionProbeProps.IntDict))]
    [InlineData(nameof(ConversionProbeProps.IntArray))]
    [InlineData(nameof(ConversionProbeProps.IntList))]
    public async Task AnElementThatDoesNotFitTheElementType_IsRefused(string field)
    {
        var id = await SeedWithAnOversizedElementAsync(field);

        var load = async () => await Redb.LoadAsync<ConversionProbeProps>(id);

        await load.Should().ThrowAsync<InvalidOperationException>().WithMessage($"*{TooBigForInt}*");
    }
}

[RedbScheme(Name = "ConversionProbe")]
public class ConversionProbeProps
{
    public List<int>? IntList { get; set; }
    public int[]? IntArray { get; set; }
    public Dictionary<string, int>? IntDict { get; set; }
}
