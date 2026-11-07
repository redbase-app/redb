using System;
using System.Collections.Generic;

namespace redb.Examples.Ef.Entities;

public partial class EmployeeProps_OfficeLocations_Building
{
    public long _id { get; set; }

    public long _owner_id { get; set; }

    public int? Floor { get; set; }

    public string? Name { get; set; }

    public virtual ICollection<EmployeeProps_OfficeLocations_Building_AccessCodes> EmployeeProps_OfficeLocations_Building_AccessCodes { get; set; } = new List<EmployeeProps_OfficeLocations_Building_AccessCodes>();

    public virtual ICollection<EmployeeProps_OfficeLocations_Building_Amenities> EmployeeProps_OfficeLocations_Building_Amenities { get; set; } = new List<EmployeeProps_OfficeLocations_Building_Amenities>();

    public virtual ICollection<EmployeeProps_OfficeLocations_Building_ElevatorFloors> EmployeeProps_OfficeLocations_Building_ElevatorFloors { get; set; } = new List<EmployeeProps_OfficeLocations_Building_ElevatorFloors>();

    public virtual ICollection<EmployeeProps_OfficeLocations_Building_ParkingSpots> EmployeeProps_OfficeLocations_Building_ParkingSpots { get; set; } = new List<EmployeeProps_OfficeLocations_Building_ParkingSpots>();

    public virtual EmployeeProps_OfficeLocations _owner { get; set; } = null!;
}
