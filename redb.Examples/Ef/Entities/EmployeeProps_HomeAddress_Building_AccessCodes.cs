using System;
using System.Collections.Generic;

namespace redb.Examples.Ef.Entities;

public partial class EmployeeProps_HomeAddress_Building_AccessCodes
{
    public long _id { get; set; }

    public long _owner_id { get; set; }

    public int _idx { get; set; }

    public int? _value { get; set; }

    public virtual EmployeeProps_HomeAddress_Building _owner { get; set; } = null!;
}
