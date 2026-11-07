using System;
using System.Collections.Generic;

namespace redb.Examples.Ef.Entities;

public partial class CityProps_Coordinates
{
    public long _id { get; set; }

    public long _owner_id { get; set; }

    public int _idx { get; set; }

    public double? _value { get; set; }

    public virtual CityProps _owner { get; set; } = null!;
}
