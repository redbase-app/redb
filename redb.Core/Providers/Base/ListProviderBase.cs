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
        
        public async Task<RedbList?> GetListAsync(long listId)
        {
            var cached = ListCache.GetList(listId);
            if (cached != null) return cached;
            
            var list = await Context.QueryFirstOrDefaultAsync<RedbList>(Sql.Lists_SelectById(), listId);
            if (list == null) return null;
            
            ListCache.CacheList(list);
            return list;
        }
        
        public async Task<RedbList?> GetListByNameAsync(string name)
        {
            var cached = ListCache.GetListByName(name);
            if (cached != null) return cached;
            
            var list = await Context.QueryFirstOrDefaultAsync<RedbList>(Sql.Lists_SelectByName(), name);
            if (list == null) return null;
            
            ListCache.CacheList(list);
            return list;
        }
        
        public async Task<List<RedbList>> GetAllListsAsync()
        {
            var lists = await Context.QueryAsync<RedbList>(Sql.Lists_SelectAll());
            
            foreach (var list in lists)
            {
                ListCache.CacheList(list);
            }
            
            return lists;
        }
        
        /// <summary>
        /// Get list with all its items loaded.
        /// </summary>
        public async Task<RedbList?> GetListWithItemsAsync(long listId)
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
        public async Task<RedbList?> GetListByNameWithItemsAsync(string name)
        {
            var list = await GetListByNameAsync(name);
            if (list == null) return null;
            
            var items = await GetListItemsAsync(list.Id);
            list.SetItems(items);
            
            return list;
        }
        
        public async Task<RedbList> SaveListAsync(IRedbList list)
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
                await Context.ExecuteAsync(Sql.Lists_Insert(), entity.Id, entity.Name, entity.Alias);
            }
            else
            {
                var existing = await Context.QueryFirstOrDefaultAsync<RedbList>(Sql.Lists_SelectById(), list.Id);
                if (existing == null)
                    throw new InvalidOperationException($"List with ID {list.Id} not found");
                    
                entity = new RedbList { Id = list.Id, Name = list.Name, Alias = list.Alias };
                await Context.ExecuteAsync(Sql.Lists_Update(), entity.Name, entity.Alias, entity.Id);
            }
            
            ListCache.InvalidateList(entity.Id);
            return entity;
        }
        
        public async Task<bool> DeleteListAsync(long listId)
        {
            if (await IsListUsedInStructuresAsync(listId))
            {
                return false;
            }
            
            var result = await Context.ExecuteAsync(Sql.Lists_Delete(), listId);
            
            if (result == 0) return false;
            
            ListCache.InvalidateList(listId);
            return true;
        }
        
        // === CRUD for list items ===
        
        public async Task<RedbListItem?> GetListItemAsync(long itemId)
        {
            var cached = ListCache.GetListItem(itemId);
            if (cached != null) return cached;
            
            return await Context.QueryFirstOrDefaultAsync<RedbListItem>(Sql.ListItems_SelectById(), itemId);
        }
        
        public async Task<List<RedbListItem>> GetListItemsAsync(long listId)
        {
            var cached = ListCache.GetListItems(listId);
            if (cached != null) return cached;
            
            var items = await Context.QueryAsync<RedbListItem>(Sql.ListItems_SelectByListId(), listId);
            
            ListCache.CacheListItems(listId, items);
            return items;
        }
        
        public async Task<RedbListItem?> GetListItemByValueAsync(long listId, string value)
        {
            var items = await GetListItemsAsync(listId);
            return items.FirstOrDefault(i => i.Value == value);
        }
        
        public async Task<RedbListItem> SaveListItemAsync(IRedbListItem item)
        {
            RedbListItem entity;
            
            if (item.Id == 0)
            {
                var existing = await Context.QueryFirstOrDefaultAsync<RedbListItem>(
                    Sql.ListItems_SelectByListIdAndValue(), item.IdList, item.Value);
                
                if (existing != null)
                {
                    entity = existing;
                    entity.Alias = item.Alias;
                    entity.IdObject = item.IdObject;
                    await Context.ExecuteAsync(Sql.ListItems_UpdateAliasAndObject(), 
                        entity.Alias, entity.IdObject, entity.Id);
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
                    await Context.ExecuteAsync(Sql.ListItems_Insert(),
                        entity.Id, entity.IdList, entity.Value, entity.Alias, entity.IdObject);
                }
            }
            else
            {
                var existing = await Context.QueryFirstOrDefaultAsync<RedbListItem>(
                    Sql.ListItems_SelectById(), item.Id);
                if (existing == null)
                    throw new InvalidOperationException($"ListItem with ID {item.Id} not found");
                    
                entity = existing;
                entity.Value = item.Value;
                entity.Alias = item.Alias;
                entity.IdObject = item.IdObject;
                await Context.ExecuteAsync(Sql.ListItems_Update(),
                    entity.Value, entity.Alias, entity.IdObject, entity.Id);
            }
            
            ListCache.InvalidateListItems(entity.IdList);
            return entity;
        }
        
        public async Task<bool> DeleteListItemAsync(long itemId)
        {
            var entity = await Context.QueryFirstOrDefaultAsync<RedbListItem>(
                Sql.ListItems_SelectById(), itemId);
                
            if (entity == null) return false;
            
            var listId = entity.IdList;
            await Context.ExecuteAsync(Sql.ListItems_Delete(), itemId);
            
            ListCache.InvalidateListItems(listId);
            return true;
        }
        
        public async Task<List<RedbListItem>> AddItemsAsync(IRedbList list, IEnumerable<IRedbListItem> items)
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
                
                await Context.ExecuteAsync(Sql.ListItems_Insert(),
                    entity.Id, entity.IdList, entity.Value, entity.Alias, entity.IdObject);
            }
            
            ListCache.InvalidateListItems(list.Id);
            return entities;
        }
        
        public async Task<List<RedbListItem>> AddItemsAsync(IRedbList list, IEnumerable<string> values, IEnumerable<string>? aliases = null)
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
        
        public async Task<RedbList> SaveListWithItemsAsync(IRedbList list)
        {
            var savedList = await SaveListAsync(list);
            
            // 1. Get current items from DB
            var dbItems = await Context.QueryAsync<RedbListItem>(Sql.ListItems_SelectByListId(), savedList.Id);
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
                await Context.ExecuteAsync(Sql.ListItems_Delete(), itemId);
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
        
        public async Task<List<RedbListItem>> GetItemsByObjectReferenceAsync(long objectId)
        {
            return await Context.QueryAsync<RedbListItem>(Sql.ListItems_SelectByObjectId(), objectId);
        }
        
        public async Task<bool> IsListUsedInStructuresAsync(long listId)
        {
            var result = await Context.ExecuteScalarAsync<long?>(Sql.Lists_IsUsedInStructures(), listId);
            return result.HasValue;
        }
        
        public async Task<RedbList> SyncListFromEnumAsync<TEnum>(string? listName = null) where TEnum : struct, Enum
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

