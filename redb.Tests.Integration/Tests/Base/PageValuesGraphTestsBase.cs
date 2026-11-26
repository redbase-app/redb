using redb.Core;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// The Pro materializer reads the rows of a page through one index built once per read. A page of two objects of the
/// same scheme with every lookup shape: the root fields, a root class, the fields of a class array element (a class
/// field, a scalar array, a class array under the element), a null element, a dictionary of classes, a scalar array of
/// twelve elements (index 10 and 11 after 9, not after 1), an empty array and an unset one. Each object reads back as it
/// was saved, with nothing of the other object in it.
/// </summary>
public abstract class PageValuesGraphTestsBase
{
    protected readonly IRedbService Redb;

    protected PageValuesGraphTestsBase(IRedbService redb) => Redb = redb;

    private static PageGraphProps Graph(string tag) => new()
    {
        Title = $"title-{tag}",
        Head = new PageGraphLine
        {
            Code = $"head-{tag}",
            Inner = new PageGraphInner { Level = 1, Note = $"head-inner-{tag}", Marks = new List<int> { 3, 1, 2 } },
            Tags = new List<string> { $"h0-{tag}", $"h1-{tag}" },
        },
        Lines = new List<PageGraphLine?>
        {
            new()
            {
                Code = $"line0-{tag}",
                Inner = new PageGraphInner { Level = 10, Note = $"inner0-{tag}", Marks = new List<int> { 7 } },
                Tags = Enumerable.Range(0, 12).Select(i => $"t{i}-{tag}").ToList(),
                Parts = new List<PageGraphInner>
                {
                    new() { Level = 100, Note = $"part0-{tag}", Marks = new List<int> { 1, 2 } },
                    new() { Level = 101, Note = $"part1-{tag}" },
                },
            },
            null,
            new()
            {
                Code = $"line2-{tag}",
                Inner = new PageGraphInner { Level = 12, Note = $"inner2-{tag}" },
                Tags = new List<string>(),
            },
        },
        ByCode = new Dictionary<string, PageGraphLine>
        {
            ["b"] = new() { Code = $"dict-b-{tag}", Inner = new PageGraphInner { Level = 2, Note = $"dict-inner-b-{tag}" } },
            ["a"] = new() { Code = $"dict-a-{tag}", Parts = new List<PageGraphInner> { new() { Level = 3, Note = $"dict-part-{tag}" } } },
        },
        Numbers = Enumerable.Range(0, 12).Select(i => 1000 - i).ToList(),
        Empty = new List<string>(),
        Missing = null,
    };

    [Fact]
    public async Task APageOfTwoObjects_EveryLookupShape_ReadsBackAsSaved()
    {
        await Redb.SyncSchemeAsync<PageGraphProps>();
        var tag = Guid.NewGuid().ToString("N")[..8];
        var first = Graph($"{tag}-1");
        var second = Graph($"{tag}-2");
        var firstId = await Redb.SaveAsync(new RedbObject<PageGraphProps> { name = $"page-values-1-{tag}", Props = first });
        var secondId = await Redb.SaveAsync(new RedbObject<PageGraphProps> { name = $"page-values-2-{tag}", Props = second });

        var page = await Redb.Query<PageGraphProps>()
            .WhereRedb(o => o.Id == firstId || o.Id == secondId)
            .OrderByRedb(o => o.Id)
            .ToListAsync();

        page.Select(o => o.id).Should().Equal(firstId, secondId);
        page[0].Props.Should().BeEquivalentTo(first, o => o.WithStrictOrdering());
        page[1].Props.Should().BeEquivalentTo(second, o => o.WithStrictOrdering());
        page[0].Props.Lines![1].Should().BeNull("a null element keeps its position");
        page[0].Props.Empty.Should().NotBeNull().And.BeEmpty("an empty array reads as empty, not as unset");
        page[0].Props.Missing.Should().BeNull("an unset array reads as unset");
    }
}
