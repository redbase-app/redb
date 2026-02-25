using redb.Core.Models.Contracts;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Serialization;

namespace redb.Core.Models.Entities
{
    /// <summary>
    /// REDB list entity with direct data storage (Aggregate Root).
    /// Maps to _lists table in PostgreSQL.
    /// </summary>
    public class RedbList : IRedbList
    {
        /// <summary>
        /// Unique list identifier.
        /// </summary>
        [JsonPropertyName("id")]
        public long Id { get; set; }
        
        /// <summary>
        /// List name.
        /// </summary>
        [JsonPropertyName("name")]
        public string Name { get; set; } = string.Empty;
        
        /// <summary>
        /// List alias (short name).
        /// </summary>
        [JsonPropertyName("alias")]
        public string? Alias { get; set; }
        
        // === Items collection (Aggregate Root) ===
        
        private List<RedbListItem> _items = new();

        /// <summary>
        /// Collection of list items (read-only).
        /// </summary>
        [JsonIgnore]
        public IReadOnlyCollection<IRedbListItem> Items => _items.AsReadOnly();
        
        /// <summary>
        /// Internal items list (for provider).
        /// </summary>
        internal List<RedbListItem> ItemsInternal => _items;
        
        /// <summary>
        /// Set items collection (for mapping).
        /// </summary>
        public void SetItems(IEnumerable<RedbListItem> items)
        {
            _items = items?.ToList() ?? new List<RedbListItem>();
        }

        // === Constructors ===
        
        /// <summary>
        /// Default constructor for deserialization and mapping.
        /// </summary>
        public RedbList()
        {
        }
        
        /// <summary>
        /// Constructor with name.
        /// </summary>
        public RedbList(string name, string? alias = null)
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
            Alias = alias;
        }

        // === Static factory methods ===
        
        /// <summary>
        /// Create new list.
        /// </summary>
        public static RedbList Create(string name, string? alias = null)
        {
            return new RedbList(name, alias);
        }

        // === DDD methods for Items ===
        
        /// <summary>
        /// Add item to list (Aggregate Root pattern).
        /// Automatically sets IdList.
        /// </summary>
        public IRedbListItem AddItem(string value, long? idObject = null, string? alias = null)
        {
            var item = new RedbListItem 
            { 
                IdList = this.Id, 
                Value = value, 
                Alias = alias,
                IdObject = idObject
            };
            _items.Add(item);
            return item;
        }
        
        /// <summary>
        /// Add item with linked object (Aggregate Root pattern).
        /// Automatically extracts Id from object.
        /// </summary>
        public IRedbListItem AddItem(string value, IRedbObject linkedObject, string? alias = null)
        {
            var item = new RedbListItem(this, value, alias, linkedObject);
            _items.Add(item);
            return item;
        }

        /// <summary>
        /// Create item without adding to collection.
        /// Useful for separate save.
        /// </summary>
        public IRedbListItem CreateItem(string value, long? idObject = null, string? alias = null)
        {
            return new RedbListItem 
            { 
                IdList = this.Id, 
                Value = value, 
                Alias = alias,
                IdObject = idObject
            };
        }
        
        /// <summary>
        /// Create item with linked object.
        /// </summary>
        public IRedbListItem CreateItem(string value, IRedbObject linkedObject, string? alias = null)
        {
            return new RedbListItem(this, value, alias, linkedObject);
        }

        /// <summary>
        /// Remove item from list.
        /// </summary>
        public bool RemoveItem(IRedbListItem item)
        {
            if (item is RedbListItem redbItem)
            {
                return _items.Remove(redbItem);
            }
            
            var toRemove = _items.FirstOrDefault(i => i.Id == item.Id);
            if (toRemove != null)
            {
                return _items.Remove(toRemove);
            }
            
            return false;
        }

        /// <summary>
        /// Find item by value.
        /// </summary>
        public IRedbListItem? FindItemByValue(string value)
        {
            return _items.FirstOrDefault(i => i.Value == value);
        }
        
        /// <summary>
        /// Indexer to access list items by value.
        /// </summary>
        public IRedbListItem? this[string value] => FindItemByValue(value);
        
        /// <summary>
        /// Clear items collection.
        /// </summary>
        public void ClearItems()
        {
            _items.Clear();
        }

        public override string ToString()
        {
            var aliasStr = !string.IsNullOrEmpty(Alias) ? $" ({Alias})" : "";
            return $"List {Id}: {Name}{aliasStr}";
        }
    }
}
