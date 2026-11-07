using System;
using System.Collections.Generic;

namespace redb.Examples.Ef.Entities;

public partial class EmployeeProps_DepartmentHistory_Leaders
{
    public long _id { get; set; }

    public long _owner_id { get; set; }

    public int _idx { get; set; }

    public string? Name { get; set; }

    public int? Priority { get; set; }

    public string? Description { get; set; }

    public virtual EmployeeProps_DepartmentHistory _owner { get; set; } = null!;
}
