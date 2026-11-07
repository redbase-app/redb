using System;
using System.Collections.Generic;

namespace redb.Examples.Ef.Entities;

public partial class EmployeeProps_HomeAddress
{
    public long _id { get; set; }

    public long _owner_id { get; set; }

    public string? City { get; set; }

    public string? Street { get; set; }

    public virtual EmployeeProps_HomeAddress_Building? EmployeeProps_HomeAddress_Building { get; set; }

    public virtual EmployeeProps _owner { get; set; } = null!;
}
