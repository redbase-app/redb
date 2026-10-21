using System;
using System.Collections.Generic;

namespace redb.Examples.Ef.Entities;

public partial class EmployeeProps_EmergencyAddress_Building_ParkingSpots
{
    public long _id { get; set; }

    public long _owner_id { get; set; }

    public int _idx { get; set; }

    public string? _value { get; set; }

    public virtual EmployeeProps_EmergencyAddress_Building _owner { get; set; } = null!;
}
