using Microsoft.EntityFrameworkCore;
using redb.Core.Models.Entities;
using redb.Examples.Ef.Entities;
using Biz = redb.Examples.Models;

namespace redb.Examples.Ef;

/// <summary>
/// Persons and list items of the examples through EF Core: <c>PersonProps.Status</c> points at a row of
/// <c>list_items</c>, <c>PersonProps.Roles</c> is a table of such pointers. Turned into the business class with
/// <see cref="RedbListItem"/> values, as redb hands them out.
/// </summary>
internal static class PersonEfMapping
{
    /// <summary>A new person header with its Props; keys come from the database.</summary>
    public static objects ToEntity(string name, int age, string email, long? statusItemId, IReadOnlyList<long> roleItemIds)
    {
        var now = DateTime.UtcNow;
        return new objects
        {
            _id_scheme = EfSchemeIds.Person,
            _id_owner = 0,
            _id_who_change = 0,
            _date_create = now,
            _date_modify = now,
            _name = name,
            PersonProps = new PersonProps
            {
                Name = name,
                Age = age,
                Email = email,
                Status = statusItemId,
                PersonProps_Roles = roleItemIds.Select((id, i) => new PersonProps_Roles { _idx = i, _value = id }).ToList()
            }
        };
    }

    /// <summary>Every person with its header, status item and role items, as a query.</summary>
    public static IQueryable<PersonProps> Persons(this FlatContext db) => db.PersonProps
        .Include(p => p.objects)
        .Include(p => p.StatusNavigation)
        .Include(p => p.PersonProps_Roles).ThenInclude(r => r._valueNavigation)
        .AsSplitQuery();

    /// <summary>The list item as redb's business value.</summary>
    public static RedbListItem ToItem(list_items i) => new()
    {
        Id = i._id,
        IdList = i._id_list,
        Value = i._value ?? string.Empty,
        Alias = i._alias,
        IdObject = i._id_object
    };

    /// <summary>The business object of a person loaded through <see cref="Persons"/>.</summary>
    public static RedbObject<Biz.PersonProps> ToBusiness(PersonProps p)
    {
        var h = p.objects;
        return new RedbObject<Biz.PersonProps>
        {
            id = h._id,
            parent_id = h._id_parent,
            scheme_id = h._id_scheme,
            owner_id = h._id_owner,
            who_change_id = h._id_who_change,
            date_create = h._date_create,
            date_modify = h._date_modify,
            name = h._name,
            Props = new Biz.PersonProps
            {
                Name = p.Name ?? string.Empty,
                Age = p.Age ?? 0,
                Email = p.Email ?? string.Empty,
                Status = p.StatusNavigation is { } status ? ToItem(status) : null,
                Roles = p.PersonProps_Roles.Count == 0
                    ? null
                    : p.PersonProps_Roles.OrderBy(r => r._idx)
                        .Where(r => r._valueNavigation != null)
                        .Select(r => ToItem(r._valueNavigation!))
                        .ToList()
            }
        };
    }
}
