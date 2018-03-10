using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Collections.ObjectModel;

namespace Exigo.ClientReporting
{
    public interface IKeyedEntity<TKey>
    {
        TKey GetKey();
    }

    public interface ICachedCollection<TItem> : ICollection<TItem>
    {
        DateTime Modified { get; set; }
    }

    public class KeyedEntityList<TKey, TItem> : KeyedCollection<TKey, TItem>, IReadOnlyKeyedList<TKey, TItem>, ICachedCollection<TItem> where TItem : IKeyedEntity<TKey>
    {
        protected override TKey GetKeyForItem(TItem item)
        {
            return item.GetKey();
        }

        #region IReadOnlyKeyedList<TKey,TItem> Members

        public bool ContainsKey(TKey key)
        {
            return base.Contains(key);
        }

        public bool TryGetValue(TKey key, out TItem item)
        {
            if (base.Dictionary == null)
            {
                item = default(TItem);
                return false;
            }
            return base.Dictionary.TryGetValue(key, out item);
        }

        #endregion

        DateTime _modifed;
        public DateTime Modified
        {
            get { return _modifed; }
            set { _modifed = value; }
        }
    }
}
