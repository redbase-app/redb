using System;
using System.Collections.Generic;

namespace redb.Examples.Ef.Entities;

public partial class ExampleResultProps_Results
{
    public long _id { get; set; }

    public long _owner_id { get; set; }

    public string _key { get; set; } = null!;

    public int? Count { get; set; }

    public int? Time { get; set; }

    public string? Status { get; set; }

    public virtual ExampleResultProps _owner { get; set; } = null!;
}
