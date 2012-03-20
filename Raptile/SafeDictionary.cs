using System;
using System.Collections.Generic;

namespace Raptile
{
    internal class SafeDictionary<TKey, TValue>
    {
        private readonly object _padlock = new object();
        private readonly Dictionary<TKey, TValue> _dictionary;

        public SafeDictionary(int capacity)
        {
            _dictionary = new Dictionary<TKey, TValue>(capacity);
        }

        public SafeDictionary()
        {
            _dictionary = new Dictionary<TKey, TValue>();
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            return _dictionary.TryGetValue(key, out value);
        }

        public TValue this[TKey key]
        {
            get
            {
                return _dictionary[key];
            }
            set
            {
                lock (_padlock)
                    _dictionary[key] = value;
            }
        }

        public int Count
        {
            get { return _dictionary.Count; }
        }

        public ICollection<KeyValuePair<TKey, TValue>> GetList()
        {
            return _dictionary;
        }

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            return ((ICollection<KeyValuePair<TKey, TValue>>)_dictionary).GetEnumerator();
        }

        public void Add(TKey key, TValue value)
        {
            lock (_padlock)
                _dictionary.Add(key, value);
        }

        public TKey[] Keys()
        {
            lock (_padlock)
            {
                var keys = new TKey[_dictionary.Keys.Count];
                _dictionary.Keys.CopyTo(keys, 0);
                return keys;
            }
        }

        public bool Remove(TKey key)
        {
            lock (_padlock)
                return _dictionary.Remove(key);
        }
    }

    //------------------------------------------------------------------------------------------------------------------

    internal static class FastDateTime
    {
        public static TimeSpan LocalUtcOffset;

        public static DateTime Now
        {
            get { return DateTime.UtcNow + LocalUtcOffset; }
        }

        static FastDateTime()
        {
            LocalUtcOffset = TimeZone.CurrentTimeZone.GetUtcOffset(DateTime.Now);
        }
    }

    //------------------------------------------------------------------------------------------------------------------
}
