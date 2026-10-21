using System;
using System.Collections.Generic;

namespace redb.Examples.Ef.Entities;

public partial class EmployeeProps_OfficeLocations
{
    public long _id { get; set; }

    public long _owner_id { get; set; }

    public string _key { get; set; } = null!;

    public string? City { get; set; }

    public string? Street { get; set; }

    public virtual EmployeeProps_OfficeLocations_Building? EmployeeProps_OfficeLocations_Building { get; set; }

    public virtual EmployeeProps _owner { get; set; } = null!;
}
