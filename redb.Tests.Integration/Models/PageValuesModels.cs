using redb.Core.Attributes;

namespace redb.Tests.Integration.Models;

/// <summary>
/// A graph that asks the Pro materializer every kind of lookup over the rows of a page: root fields, a root class, a
/// class array whose elements carry a class field, a scalar array and a class array of their own, a dictionary of
/// classes, a scalar array of more than ten elements (numeric, not text, order of the array index).
/// </summary>
[RedbScheme("TestPageValuesGraph")]
public class PageGraphProps
{
    public string? Title { get; set; }
    public PageGraphLine? Head { get; set; }
    public List<PageGraphLine?>? Lines { get; set; }
    public Dictionary<string, PageGraphLine>? ByCode { get; set; }
    public List<int>? Numbers { get; set; }
    public List<string>? Empty { get; set; }
    public List<string>? Missing { get; set; }
}

/// <summary>An element: its fields hang under the element row.</summary>
public class PageGraphLine
{
    public string? Code { get; set; }
    public PageGraphInner? Inner { get; set; }
    public List<string>? Tags { get; set; }
    public List<PageGraphInner>? Parts { get; set; }
}

/// <summary>The class inside an element, with an array of its own.</summary>
public class PageGraphInner
{
    public int Level { get; set; }
    public string? Note { get; set; }
    public List<int>? Marks { get; set; }
}
