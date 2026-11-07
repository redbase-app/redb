using System;
using System.Collections.Generic;

namespace redb.Examples.Ef.Entities;

public partial class EmployeeProps_BonusByYear
{
    public long _id { get; set; }

    public long _owner_id { get; set; }

    public int _key { get; set; }

    public decimal? _value { get; set; }

    public virtual EmployeeProps _owner { get; set; } = null!;
}
