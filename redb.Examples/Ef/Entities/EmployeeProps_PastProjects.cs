using System;
using System.Collections.Generic;

namespace redb.Examples.Ef.Entities;

public partial class EmployeeProps_PastProjects
{
    public long _id { get; set; }

    public long _owner_id { get; set; }

    public int _idx { get; set; }

    public long? _value { get; set; }

    public virtual EmployeeProps _owner { get; set; } = null!;

    public virtual ProjectMetricsProps? _valueNavigation { get; set; }
}
