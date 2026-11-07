namespace redb.Core.Models.Entities
{
    /// <summary>
    /// The one place that turns a raw <c>_objects</c> row into an object header. Every provider
    /// used to carry its own copy of this column list, and a column added to the row
    /// (<c>value_unique</c>) reached some copies and not others.
    /// </summary>
    public static class RedbObjectRowExtensions
    {
        /// <summary>A typed object carrying the row's header; Props stay unloaded.</summary>
        public static RedbObject<TProps> ToRedbObject<TProps>(this RedbObjectRow row) where TProps : class, new()
        {
            var obj = new RedbObject<TProps>();
            row.ApplyHeaderTo(obj);
            return obj;
        }

        /// <summary>
        /// Copies the row's header columns onto an existing object of any RedbObject shape - the
        /// non-generic one, a typed one, or one created by reflection for a scheme's CLR type.
        /// Props are not touched.
        /// </summary>
        public static void ApplyHeaderTo(this RedbObjectRow row, RedbObject target)
        {
            target.id = row.Id;
            target.name = row.Name;
            target.scheme_id = row.IdScheme;
            target.parent_id = row.IdParent;
            target.owner_id = row.IdOwner;
            target.who_change_id = row.IdWhoChange;
            target.date_create = row.DateCreate;
            target.date_modify = row.DateModify;
            target.date_begin = row.DateBegin;
            target.date_complete = row.DateComplete;
            target.key = row.Key;
            target.value_long = row.ValueLong;
            target.value_string = row.ValueString;
            target.value_guid = row.ValueGuid;
            target.value_bool = row.ValueBool;
            target.value_double = row.ValueDouble;
            target.value_numeric = row.ValueNumeric;
            target.value_datetime = row.ValueDatetime;
            target.value_bytes = row.ValueBytes;
            target.value_unique = row.ValueUnique;
            target.note = row.Note;
            target.hash = row.Hash;
        }
    }
}
