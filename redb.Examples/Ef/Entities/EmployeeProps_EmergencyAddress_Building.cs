using System;
using System.Collections.Generic;

namespace redb.Examples.Ef.Entities;

public partial class EmployeeProps_EmergencyAddress_Building
{
    public long _id { get; set; }

    public long _owner_id { get; set; }

    public int? Floor { get; set; }

    public string? Name { get; set; }

    public virtual ICollection<EmployeeProps_EmergencyAddress_Building_AccessCodes> EmployeeProps_EmergencyAddress_Building_AccessCodes { get; set; } = new List<EmployeeProps_EmergencyAddress_Building_AccessCodes>();

    public virtual ICollection<EmployeeProps_EmergencyAddress_Building_Amenities> EmployeeProps_EmergencyAddress_Building_Amenities { get; set; } = new List<EmployeeProps_EmergencyAddress_Building_Amenities>();

    public virtual ICollection<EmployeeProps_EmergencyAddress_Building_ElevatorFloors> EmployeeProps_EmergencyAddress_Building_ElevatorFloors { get; set; } = new List<EmployeeProps_EmergencyAddress_Building_ElevatorFloors>();

    public virtual ICollection<EmployeeProps_EmergencyAddress_Building_ParkingSpots> EmployeeProps_EmergencyAddress_Building_ParkingSpots { get; set; } = new List<EmployeeProps_EmergencyAddress_Building_ParkingSpots>();

    public virtual EmployeeProps_EmergencyAddress _owner { get; set; } = null!;
}
