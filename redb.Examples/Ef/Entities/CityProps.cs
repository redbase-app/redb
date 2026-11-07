using System;
using System.Collections.Generic;

namespace redb.Examples.Ef.Entities;

public partial class CityProps
{
    public long _id { get; set; }

    public string? Name { get; set; }

    public int? Population { get; set; }

    public string? Region { get; set; }

    public bool? IsCapital { get; set; }

    public virtual ICollection<CityProps_Coordinates> CityProps_Coordinates { get; set; } = new List<CityProps_Coordinates>();

    public virtual objects objects { get; set; } = null!;
}
