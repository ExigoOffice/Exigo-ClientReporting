using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Exigo.ClientReporting
{
    public interface IReadOnlyKeyedList<TKey, TItem> : IEnumerable<TItem>
    {
        TItem this[TKey key] { get; }
        bool ContainsKey(TKey key);
        bool TryGetValue(TKey key, out TItem item);
        int Count { get; }
    }
}
