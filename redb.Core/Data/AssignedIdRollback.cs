using System.Collections.Generic;
using redb.Core.Models.Contracts;

namespace redb.Core.Data
{
    /// <summary>
    /// Ids a save assigns to the caller's objects inside a transaction that rolls back go back to 0 on those objects,
    /// so saving them again inserts new rows instead of addressing rows that were never committed. Only ids the save
    /// assigned: an object that had its id before keeps it. Only the object's own id: a copy the application made
    /// elsewhere (another object's property, a variable) is not reset.
    /// </summary>
    internal static class AssignedIdRollback
    {
        /// <summary>
        /// Registers the reset for <paramref name="objects"/>, whose ids the save has just assigned. Outside a
        /// transaction nothing is registered: the ids then come from committed ranges and the save owns its own rollback.
        /// </summary>
        public static void ResetOnRollback(IRedbContext context, IReadOnlyList<IRedbObject> objects)
        {
            if (objects.Count == 0)
                return;
            var assigned = new (IRedbObject Obj, long Id)[objects.Count];
            for (var i = 0; i < objects.Count; i++)
                assigned[i] = (objects[i], objects[i].Id);
            TransactionHooks.OnCompleted(context, committed =>
            {
                if (committed)
                    return;
                foreach (var (obj, id) in assigned)
                {
                    // An id the application changed since is its own: left alone.
                    if (obj.Id == id)
                        obj.Id = 0;
                }
            });
        }
    }
}
