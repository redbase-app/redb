using System;
using System.Collections.Generic;

namespace redb.Examples.Ef.Entities;

public partial class ExampleResultProps
{
    public long _id { get; set; }

    public string? Tier { get; set; }

    public virtual ICollection<ExampleResultProps_Results> ExampleResultProps_Results { get; set; } = new List<ExampleResultProps_Results>();

    public virtual objects objects { get; set; } = null!;
}
