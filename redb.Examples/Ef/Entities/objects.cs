using System;
using System.Collections.Generic;

namespace redb.Examples.Ef.Entities;

public partial class objects
{
    public long _id { get; set; }

    public long? _id_parent { get; set; }

    public long _id_scheme { get; set; }

    public long _id_owner { get; set; }

    public long _id_who_change { get; set; }

    public DateTime _date_create { get; set; }

    public DateTime _date_modify { get; set; }

    public DateTime? _date_begin { get; set; }

    public DateTime? _date_complete { get; set; }

    public long? _key { get; set; }

    public string? _name { get; set; }

    public string? _note { get; set; }

    public long? _value_long { get; set; }

    public string? _value_string { get; set; }

    public Guid? _value_guid { get; set; }

    public bool? _value_bool { get; set; }

    public double? _value_double { get; set; }

    public decimal? _value_numeric { get; set; }

    public DateTime? _value_datetime { get; set; }

    public byte[]? _value_bytes { get; set; }

    public virtual CityProps? CityProps { get; set; }

    public virtual DepartmentProps? DepartmentProps { get; set; }

    public virtual EmployeeProps? EmployeeProps { get; set; }

    public virtual ExampleResultProps? ExampleResultProps { get; set; }

    public virtual ICollection<objects> Inverse_id_parentNavigation { get; set; } = new List<objects>();

    public virtual PersonProps? PersonProps { get; set; }

    public virtual ProjectMetricsProps? ProjectMetricsProps { get; set; }

    public virtual objects? _id_parentNavigation { get; set; }

    public virtual ICollection<list_items> list_items { get; set; } = new List<list_items>();
}
