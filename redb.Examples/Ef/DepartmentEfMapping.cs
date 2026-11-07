using Microsoft.EntityFrameworkCore;
using redb.Core.Models.Entities;
using redb.Examples.Ef.Entities;
using Biz = redb.Examples.Models;

namespace redb.Examples.Ef;

/// <summary>
/// The department tree of the examples through EF Core: a node is a header row in <c>objects</c> (its parent in
/// <c>_id_parent</c>) with a <c>DepartmentProps</c> row, turned into the <see cref="TreeRedbObject{TProps}"/> redb
/// hands out. EF has no recursive query and no depth parameter, so its twins cover the operations on one level:
/// create, delete, move, count, roots, leaves and direct children.
/// </summary>
internal static class DepartmentEfMapping
{
    /// <summary>A new node under <paramref name="parent"/> (a root when null). Keys come from the database.</summary>
    public static objects ToEntity(string name, string code, string description, decimal budget, objects? parent)
    {
        var now = DateTime.UtcNow;
        return new objects
        {
            _id_scheme = EfSchemeIds.Department,
            _id_owner = 0,
            _id_who_change = 0,
            _date_create = now,
            _date_modify = now,
            _name = name,
            _id_parentNavigation = parent,
            DepartmentProps = new DepartmentProps
            {
                Name = name,
                Code = code,
                Description = description,
                Budget = budget,
                IsActive = true
            }
        };
    }

    /// <summary>Every department node with its header, as a query.</summary>
    public static IQueryable<DepartmentProps> Nodes(this FlatContext db) => db.DepartmentProps.Include(d => d.objects);

    /// <summary>The business object of a loaded department node, without parent or children.</summary>
    public static TreeRedbObject<Biz.DepartmentProps> ToBusiness(DepartmentProps d)
    {
        var h = d.objects;
        return new TreeRedbObject<Biz.DepartmentProps>
        {
            id = h._id,
            parent_id = h._id_parent,
            scheme_id = h._id_scheme,
            owner_id = h._id_owner,
            who_change_id = h._id_who_change,
            date_create = h._date_create,
            date_modify = h._date_modify,
            date_begin = h._date_begin,
            date_complete = h._date_complete,
            key = h._key,
            name = h._name,
            note = h._note,
            value_long = h._value_long,
            value_string = h._value_string,
            value_guid = h._value_guid,
            value_bool = h._value_bool,
            value_double = h._value_double,
            value_numeric = h._value_numeric,
            value_datetime = h._value_datetime,
            value_bytes = h._value_bytes,
            Props = new Biz.DepartmentProps
            {
                Name = d.Name ?? string.Empty,
                Code = d.Code ?? string.Empty,
                Description = d.Description ?? string.Empty,
                IsActive = d.IsActive ?? false,
                Budget = d.Budget ?? 0m
            }
        };
    }
}
