using redb.Core.Attributes;
using redb.Core.Models.Contracts;
using System;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace redb.Core.Models.Entities
{
    /// <summary>
    /// REDB list item entity with direct data storage.
    /// Maps to _list_items table in PostgreSQL.
    /// </summary>
    public class RedbListItem : IRedbListItem
    {
        /// <summary>
        /// Unique item identifier.
        /// </summary>
        [JsonPropertyName("id")]
        public long Id { get; set; }
        
        /// <summary>
        /// List identifier this item belongs to.
        /// </summary>
        [JsonPropertyName("id_list")]
        public long IdList { get; set; }
        
        /// <summary>
        /// Item value.
        /// </summary>
        [JsonPropertyName("value")]
        public string Value { get; set; } = string.Empty;
        
        /// <summary>
        /// Linked object identifier (optional).
        /// </summary>
        [JsonPropertyName("id_object")]
        public long? IdObject { get; set; }
        
        /// <summary>
        /// Item alias (display name).
        /// </summary>
        [JsonPropertyName("alias")]
        public string? Alias { get; set; }
        
        // === Global object loader (for lazy loading) ===
        
        private static Func<long, Task<IRedbObject?>>? _globalObjectLoader;
        
        /// <summary>
        /// Set global object loader for all ListItems.
        /// </summary>
        public static void SetGlobalObjectLoader(Func<long, Task<IRedbObject?>> loader)
        {
            _globalObjectLoader = loader ?? throw new ArgumentNullException(nameof(loader));
        }
        
        /// <summary>
        /// Check if global loader is available.
        /// </summary>
        public static bool IsObjectLoaderAvailable => _globalObjectLoader != null;

        // === Lazy loading fields ===
        
        private IRedbObject? _object = null;
        private bool _objectLoaded = false;
        private readonly object _lazyLoadLock = new();
        
        /// <summary>
        /// Linked object with lazy loading.
        /// </summary>
        [JsonPropertyName("object")]
        [RedbIgnore]
        public IRedbObject? Object
        {
            get
            {
                if (!_objectLoaded && IdObject.HasValue && _globalObjectLoader != null)
                {
                    lock (_lazyLoadLock)
                    {
                        if (!_objectLoaded && IdObject.HasValue && _globalObjectLoader != null)
                        {
                            try
                            {
                                _object = Task.Run(async () => 
                                    await _globalObjectLoader(IdObject.Value)).GetAwaiter().GetResult();
                                _objectLoaded = true;
                            }
                            catch (Exception ex)
                            {
                                throw new InvalidOperationException(
                                    $"Error lazy loading Object for ListItem {Id}: {ex.Message}", ex);
                            }
                        }
                    }
                }
                return _object;
            }
            set
            {
                _object = value;
                _objectLoaded = true;
                IdObject = value?.Id > 0 ? value.Id : null;
            }
        }

        /// <summary>
        /// Check if item is object reference.
        /// </summary>
        [JsonIgnore]
        public bool IsObjectReference => IdObject.HasValue;

        // === Constructors ===
        
        /// <summary>
        /// Default constructor for deserialization and mapping.
        /// </summary>
        public RedbListItem()
        {
        }
        
        /// <summary>
        /// Constructor for creating item linked to list.
        /// </summary>
        public RedbListItem(IRedbList list, string? value, string? alias = null, long? idObject = null)
        {
            if (list == null) throw new ArgumentNullException(nameof(list));
            
            IdList = list.Id;
            Value = value;
            Alias = alias;
            IdObject = idObject;
        }
        
        /// <summary>
        /// Constructor for creating item with linked object.
        /// </summary>
        public RedbListItem(IRedbList list, string? value, string? alias, IRedbObject linkedObject)
        {
            if (list == null) throw new ArgumentNullException(nameof(list));
            if (linkedObject == null) throw new ArgumentNullException(nameof(linkedObject));
            
            IdList = list.Id;
            Value = value;
            Alias = alias;
            IdObject = linkedObject.Id > 0 ? linkedObject.Id : null;
            _object = linkedObject;
            _objectLoaded = true;
        }

        // === Static factory methods ===
        
        /// <summary>
        /// Create ListItem for specific list.
        /// </summary>
        public static RedbListItem ForList(IRedbList list, string? value, string? alias = null, long? idObject = null)
        {
            return new RedbListItem(list, value, alias, idObject);
        }
        
        /// <summary>
        /// Create ListItem with linked object.
        /// </summary>
        public static RedbListItem ForList(IRedbList list, string? value, string? alias, IRedbObject linkedObject)
        {
            return new RedbListItem(list, value, alias, linkedObject);
        }

        /// <summary>
        /// Get display value.
        /// </summary>
        public string GetDisplayValue()
        {
            if (!string.IsNullOrEmpty(Alias))
                return Alias;
                
            if (!string.IsNullOrEmpty(Value))
                return Value;
            
            if (IdObject.HasValue)
                return $"Object #{IdObject}";
                
            return $"Item #{Id}";
        }

        public override string ToString()
        {
            var displayValue = GetDisplayValue();
            var aliasStr = !string.IsNullOrEmpty(Alias) && Alias != displayValue ? $" ({Alias})" : "";
            return $"ListItem {Id}: {displayValue}{aliasStr} [List: {IdList}]";
        }
    }
}
