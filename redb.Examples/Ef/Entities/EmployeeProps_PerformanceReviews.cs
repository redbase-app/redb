using System;
using System.Collections.Generic;

namespace redb.Examples.Ef.Entities;

public partial class EmployeeProps_PerformanceReviews
{
    public long _id { get; set; }

    public long _owner_id { get; set; }

    public int _key_Item1 { get; set; }

    public string _key_Item2 { get; set; } = null!;

    public string? _value { get; set; }

    public virtual EmployeeProps _owner { get; set; } = null!;
}
