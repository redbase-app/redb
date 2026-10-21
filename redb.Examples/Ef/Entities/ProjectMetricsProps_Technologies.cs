using System;
using System.Collections.Generic;

namespace redb.Examples.Ef.Entities;

public partial class ProjectMetricsProps_Technologies
{
    public long _id { get; set; }

    public long _owner_id { get; set; }

    public int _idx { get; set; }

    public string? _value { get; set; }

    public virtual ProjectMetricsProps _owner { get; set; } = null!;
}
