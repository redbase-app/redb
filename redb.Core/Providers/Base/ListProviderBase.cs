using redb.Core.Attributes;
using redb.Core.Caching;
using redb.Core.Data;
using redb.Core.Models.Configuration;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Core.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace redb.Core.Providers.Base
{
    /// <summary>
    /// Base class for IListProvider implementations.
    /// Contains all business logic for list and list item operations with caching.
    /// SQL queries are abstracted via ISqlDialect.
    /// </summary>
    public abstract class ListProviderBase : IListProvider
    {
        protected readonly IRedbContext Context;
        protected readonly RedbServiceConfiguration Configuration;
        protected readonly ISqlDialect Sql;
        protected readonly ILogger? Logger;
        protected readonly GlobalListCache ListCache;

        /// <inheritdoc />
        public Func<IReadOnlyCollection<long>, CancellationToken, Task<IReadOnlyDictionary<long, IRedbObject>>>? LinkedObjectsBatchLoader { get; set; }

        /// <summary>
        /// Hand-out preload (PreloadListItemLinkedObjects, default on): resolve the linked
        /// objects of the items in ONE batch and publish them, so a later touch of
        /// <see cref="RedbListItem.Object"/> is a field read - no database call, no blocked
        /// thread (a hot loop over fresh lazy items froze a production process, 2026-09-09).
        /// Items already carrying their object are skipped; a missing id stays lazy.
        /// </summary>
        private async Task PreloadLinkedObjectsAsync(IReadOnlyList<RedbListItem> items, CancellationToken cancellationToken)
        {
            var batchLoader = LinkedObjectsBatchLoader;
            if (batchLoader == null || !Configuration.PreloadListItemLinkedObjects) return;

            var ids = new List<long>();
            foreach (var item in items)
                if (item is { IsObjectLoaded: false, IdObject: long id })
                    ids.Add(id);
            if (ids.Count == 0) return;

            var byId = await batchLoader(ids, cancellationToken);
            foreach (var item in items)
                if (!item.IsObjectLoaded && item.IdObject is long id && byId.TryGetValue(id, out var obj))
                    item.Publish(obj, Context);
        }

        /// <summary>
        /// Binds the items to this provider's database, and a not-shared item to this scope as its origin, before they leave
        /// it. The item keeps no loader: <see cref="RedbListItem.Object"/> loads on the scope current for the reader, else on
        /// the origin while it lives (owner decision 2026-09-15). An item of the list cache is shared and has no origin.
        /// </summary>
        protected List<RedbListItem> Attach(List<RedbListItem> items)
        {
            foreach (var item in items)
                Bind(item);
            return items;
        }

        /// <inheritdoc cref="Attach(List{RedbListItem})"/>
        protected RedbListItem? Attach(RedbListItem? item)
        {
            if (item != null)
                Bind(item);
            return item;
        }

        private void Bind(RedbListItem item)
        {
            item._cacheDomain ??= ListCache.Domain;
            if (!item._isShared)
                item._origin ??= RedbServiceBase.ServiceOf(Context)?.AsOrigin.SelfReference;
        }

        protected ListProviderBase(
            IRedbContext context, 
            RedbServiceConfiguration configuration,
            ISqlDialect sql,
            ISchemeSyncProvider schemeSync,
            ILogger? logger = null)
        {
            Context = context ?? throw new ArgumentNullException(nameof(context));
            Configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            Sql = sql ?? throw new ArgumentNullException(nameof(sql));
            ListCache = schemeSync?.ListCache ?? throw new ArgumentNullException(nameof(schemeSync));
            Logger = logger;
        }
        
        // === CRUD for lists ===
        
        public async Task<RedbList?> GetListAsync(long listId, CancellationToken cancellationToken = default)
        {
            var cached = ListCache.GetList(listId);
            if (cached != null) return cached;
            
            var list = await Context.QueryFirstOrDefaultAsync<RedbList>(Sql.Lists_SelectById(), new object[] { listId }, cancellationToken);
            if (list == null) return null;
            
            ListCache.CacheList(list);
            return list;
        }
        
        public async Task<RedbList?> GetListByNameAsync(string name, CancellationToken cancellationToken = default)
        {
            var cached = ListCache.GetListByName(name);
            if (cached != null) return cached;
            
            var list = await Context.QueryFirstOrDefaultAsync<RedbList>(Sql.Lists_SelectByName(), new object[] { name }, cancellationToken);
            if (list == null) return null;
            
            ListCache.CacheList(list);
            return list;
        }
        
        public async Task<List<RedbList>> GetAllListsAsync(CancellationToken cancellationToken = default)
        {
            var lists = await Context.QueryAsync<RedbList>(Sql.Lists_SelectAll(), System.Array.Empty<object>(), cancellationToken);
            
            foreach (var list in lists)
            {
                ListCache.CacheList(list);
            }
            
            return lists;
        }
        
        /// <summary>
        /// Get list with all its items loaded.
        /// </summary>
        public async Task<RedbList?> GetListWithItemsAsync(long listId, CancellationToken cancellationToken = default)
        {
            var list = await GetListAsync(listId);
            if (list == null) return null;
            
            var items = await GetListItemsAsync(listId);
            list.SetItems(items);
            
            return list;
        }
        
        /// <summary>
        /// Get list by name with all its items loaded.
        /// </summary>
        public async Task<RedbList?> GetListByNameWithItemsAsync(string name, CancellationToken cancellationToken = default)
        {
            var list = await GetListByNameAsync(name);
            if (list == null) return null;
            
            var items = await GetListItemsAsync(list.Id);
            list.SetItems(items);
            
            return list;
        }
        
        public async Task<RedbList> SaveListAsync(IRedbList list, CancellationToken cancellationToken = default)
        {
            RedbList entity;
            
            if (list.Id == 0)
            {
                var newId = await Context.NextObjectIdAsync();
                entity = new RedbList
                {
                    Id = newId,
                    Name = list.Name,
                    Alias = list.Alias
                };
                await Context.ExecuteAsync(Sql.Lists_Insert(), new object[] { entity.Id, entity.Name, entity.Alias }, cancellationToken);
            }
            else
            {
                var existing = await Context.QueryFirstOrDefaultAsync<RedbList>(Sql.Lists_SelectById(), new object[] { list.Id }, cancellationToken);
                if (existing == null)
                    throw new InvalidOperationException($"List with ID {list.Id} not found");
                    
                entity = new RedbList { Id = list.Id, Name = list.Name, Alias = list.Alias };
                await Context.ExecuteAsync(Sql.Lists_Update(), new object[] { entity.Name, entity.Alias, entity.Id }, cancellationToken);
            }
            
            ListCache.InvalidateList(entity.Id);
            return entity;
        }
        
        public async Task<bool> DeleteListAsync(long listId, CancellationToken cancellationToken = default)
        {
            if (await IsListUsedInStructuresAsync(listId))
            {
                return false;
            }
            
            var result = await Context.ExecuteAsync(Sql.Lists_Delete(), new object[] { listId }, cancellationToken);
            
            if (result == 0) return false;
            
            ListCache.InvalidateList(listId);
            return true;
        }
        
        // === CRUD for list items ===
        
        public async Task<RedbListItem?> GetListItemAsync(long itemId, CancellationToken cancellationToken = default)
        {
            var item = ListCache.GetListItem(itemId)
                ?? await Context.QueryFirstOrDefaultAsync<RedbListItem>(Sql.ListItems_SelectById(), new object[] { itemId }, cancellationToken);
            if (item == null) return null;
            Attach(item);
            await PreloadLinkedObjectsAsync(new[] { item }, cancellationToken);
            return item;
        }

        public async Task<List<RedbListItem>> GetListItemsAsync(long listId, CancellationToken cancellationToken = default)
        {
            var items = ListCache.GetListItems(listId);
            if (items == null)
            {
                items = await Context.QueryAsync<RedbListItem>(Sql.ListItems_SelectByListId(), new object[] { listId }, cancellationToken);
                // Cached - and so shared - once the transaction commits: until then the items are the reader's own.
                Caching.CachePublication.AfterCommit(Context, () => ListCache.CacheListItems(listId, items));
            }
            Attach(items);
            await PreloadLinkedObjectsAsync(items, cancellationToken);
            return items;
        }
        
        public async Task<RedbListItem?> GetListItemByValueAsync(long listId, string value, CancellationToken cancellationToken = default)
        {
            var items = await GetListItemsAsync(listId);
            return items.FirstOrDefault(i => i.Value == value);
        }
        
        public async Task<RedbListItem> SaveListItemAsync(IRedbListItem item, CancellationToken cancellationToken = default)
        {
            RedbListItem entity;
            
            if (item.Id == 0)
            {
                var existing = await Context.QueryFirstOrDefaultAsync<RedbListItem>(
                    Sql.ListItems_SelectByListIdAndValue(), new object[] { item.IdList, item.Value }, cancellationToken);
                
                if (existing != null)
                {
                    entity = existing;
                    entity.Alias = item.Alias;
                    entity.IdObject = item.IdObject;
                    await Context.ExecuteAsync(Sql.ListItems_UpdateAliasAndObject(), new object[] { entity.Alias, entity.IdObject, entity.Id }, cancellationToken);
                }
                else
                {
                    entity = new RedbListItem
                    {
                        Id = await Context.NextObjectIdAsync(),
                        IdList = item.IdList,
                        Value = item.Value,
                        Alias = item.Alias,
                        IdObject = item.IdObject
                    };
                    await Context.ExecuteAsync(Sql.ListItems_Insert(), new object[] { entity.Id, entity.IdList, entity.Value, entity.Alias, entity.IdObject }, cancellationToken);
                }
            }
            else
            {
                var existing = await Context.QueryFirstOrDefaultAsync<RedbListItem>(
                    Sql.ListItems_SelectById(), new object[] { item.Id }, cancellationToken);
                if (existing == null)
                    throw new InvalidOperationException($"ListItem with ID {item.Id} not found");
                    
                entity = existing;
                entity.Value = item.Value;
                entity.Alias = item.Alias;
                entity.IdObject = item.IdObject;
                await Context.ExecuteAsync(Sql.ListItems_Update(), new object[] { entity.Value, entity.Alias, entity.IdObject, entity.Id }, cancellationToken);
            }
            
            ListCache.InvalidateListItems(entity.IdList);
            return Attach(entity)!;
        }
        
        public async Task<bool> DeleteListItemAsync(long itemId, CancellationToken cancellationToken = default)
        {
            var entity = await Context.QueryFirstOrDefaultAsync<RedbListItem>(
                Sql.ListItems_SelectById(), new object[] { itemId }, cancellationToken);
                
            if (entity == null) return false;
            
            var listId = entity.IdList;
            await Context.ExecuteAsync(Sql.ListItems_Delete(), new object[] { itemId }, cancellationToken);
            
            ListCache.InvalidateListItems(listId);
            return true;
        }
        
        public async Task<List<RedbListItem>> AddItemsAsync(IRedbList list, IEnumerable<IRedbListItem> items, CancellationToken cancellationToken = default)
        {
            var entities = new List<RedbListItem>();
            
            foreach (var item in items)
            {
                var entity = new RedbListItem
                {
                    Id = item.Id == 0 ? await Context.NextObjectIdAsync() : item.Id,
                    IdList = list.Id,
                    Value = item.Value,
                    Alias = item.Alias,
                    IdObject = item.IdObject
                };
                entities.Add(entity);
                
                await Context.ExecuteAsync(Sql.ListItems_Insert(), new object[] { entity.Id, entity.IdList, entity.Value, entity.Alias, entity.IdObject }, cancellationToken);
            }
            
            ListCache.InvalidateListItems(list.Id);
            return Attach(entities);
        }
        
        public async Task<List<RedbListItem>> AddItemsAsync(IRedbList list, IEnumerable<string> values, IEnumerable<string>? aliases = null, CancellationToken cancellationToken = default)
        {
            var valuesList = values.ToList();
            var aliasesList = aliases?.ToList();
            
            var itemsToAdd = valuesList.Select((value, i) => (IRedbListItem)new RedbListItem
            {
                IdList = list.Id,
                Value = value,
                Alias = aliasesList != null && i < aliasesList.Count ? aliasesList[i] : null
            });
            
            return await AddItemsAsync(list, itemsToAdd);
        }
        
        public async Task<RedbList> SaveListWithItemsAsync(IRedbList list, CancellationToken cancellationToken = default)
        {
            var savedList = await SaveListAsync(list);
            
            // 1. Get current items from DB
            var dbItems = await Context.QueryAsync<RedbListItem>(Sql.ListItems_SelectByListId(), new object[] { savedList.Id }, cancellationToken);
            var dbItemIds = dbItems.Select(i => i.Id).ToHashSet();
            
            // 2. Get IDs from memory (existing items only, Id > 0)
            var memoryItemIds = list.Items
                .Where(i => i.Id > 0)
                .Select(i => i.Id)
                .ToHashSet();
            
            // 3. Find deleted items: exist in DB but not in memory
            var toDeleteIds = dbItemIds.Except(memoryItemIds).ToList();
            
            // 4. Delete removed items from DB
            foreach (var itemId in toDeleteIds)
            {
                await Context.ExecuteAsync(Sql.ListItems_Delete(), new object[] { itemId }, cancellationToken);
            }
            
            // 5. Add new items (Id == 0)
            var newItems = list.Items
                .Where(item => item.Id == 0)
                .Select(item => new RedbListItem
                {
                    IdList = savedList.Id,
                    Value = item.Value,
                    Alias = item.Alias,
                    IdObject = item.IdObject
                })
                .Cast<IRedbListItem>()
                .ToList();
            
            if (newItems.Any())
            {
                await AddItemsAsync(savedList, newItems);
            }
            
            // 6. Invalidate cache if items were deleted
            if (toDeleteIds.Any())
            {
                ListCache.InvalidateListItems(savedList.Id);
            }
            
            // 7. Load fresh items from DB and attach to result
            var items = await GetListItemsAsync(savedList.Id);
            savedList.SetItems(items);
            
            return savedList;
        }
        
        // === Specific methods ===
        
        public async Task<List<RedbListItem>> GetItemsByObjectReferenceAsync(long objectId, CancellationToken cancellationToken = default)
        {
            return Attach(await Context.QueryAsync<RedbListItem>(Sql.ListItems_SelectByObjectId(), new object[] { objectId }, cancellationToken));
        }
        
        public async Task<bool> IsListUsedInStructuresAsync(long listId, CancellationToken cancellationToken = default)
        {
            var result = await Context.ExecuteScalarAsync<long?>(Sql.Lists_IsUsedInStructures(), new object[] { listId }, cancellationToken);
            return result.HasValue;
        }
        
        public async Task<RedbList> SyncListFromEnumAsync<TEnum>(string? listName = null, CancellationToken cancellationToken = default) where TEnum : struct, Enum
        {
            var enumType = typeof(TEnum);
            var name = listName ?? enumType.Name;
            
            var list = await GetListByNameAsync(name);
            if (list == null)
            {
                list = RedbList.Create(name, $"Enum: {name}");
                list = await SaveListAsync(list);
            }
            
            var existingItems = await GetListItemsAsync(list.Id);
            var existingValues = existingItems.Select(i => i.Value).ToHashSet();
            
            var enumValues = Enum.GetValues<TEnum>();
            var newItems = new List<RedbListItem>();
            
            foreach (var enumValue in enumValues)
            {
                var valueName = enumValue.ToString();
                if (!existingValues.Contains(valueName))
                {
                    var field = enumType.GetField(valueName);
                    var aliasAttr = field?.GetCustomAttribute<RedbAliasAttribute>();
                    var alias = aliasAttr?.Alias;
                    
                    var item = (RedbListItem)list.CreateItem(valueName, alias: alias);
                    newItems.Add(item);
                }
            }
            
            if (newItems.Any())
            {
                await AddItemsAsync(list, newItems);
            }
            
            return list;
        }
    }
}

